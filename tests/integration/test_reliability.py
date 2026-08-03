import asyncio
import json
import os
import subprocess
from datetime import UTC, datetime
from hashlib import sha256
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

pytestmark = pytest.mark.integration

API_URL = os.getenv("METRINGEST_TEST_URL", "http://localhost:8000")
KAFKA_URL = os.getenv("METRINGEST_KAFKA_URL", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("METRINGEST_SCHEMA_REGISTRY_URL", "http://localhost:8081")
API_KEY = os.getenv("METRINGEST_API_KEY", "development-key")
AUTH_HEADERS = {"X-API-Key": API_KEY}


def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def payload(device_id: str, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid4()),
        "device_id": device_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "temperature": 72.4,
        "voltage": 12.1,
        "status": "OK",
        "region": "us-east",
    }


async def send_raw_payloads(*values: dict) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_URL,
        acks="all",
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    await producer.start()
    try:
        for value in values:
            await producer.send_and_wait(
                "raw-telemetry",
                value,
                key=value.get("event", {}).get("device_id", "unknown").encode(),
            )
    finally:
        await producer.stop()


async def send_raw_event(event: dict) -> None:
    await send_raw_payloads(
        {"event": event, "received_at": datetime.now(UTC).isoformat()}
    )


async def wait_for(client, path, predicate, timeout=60):
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            response = await client.get(path)
            last = response
            if response.is_success and predicate(response.json()):
                return response.json()
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.5)
    pytest.fail(f"timed out waiting for {path}: {last.text if last else 'no response'}")


def postgres_scalar(query: str) -> str:
    return compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "telemetry",
        "-d",
        "telemetry",
        "-Atc",
        query,
    ).stdout.strip()


async def wait_for_postgres(query: str, expected: str, timeout=60) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    last = None
    while asyncio.get_running_loop().time() < deadline:
        last = postgres_scalar(query)
        if last == expected:
            return
        await asyncio.sleep(0.5)
    pytest.fail(f"timed out waiting for PostgreSQL value {expected!r}; got {last!r}")


async def wait_for_kafka(timeout=60) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    consecutive_successes = 0
    while asyncio.get_running_loop().time() < deadline:
        try:
            compose(
                "exec",
                "-T",
                "kafka",
                "kafka-topics",
                "--bootstrap-server",
                "localhost:9092",
                "--list",
            )
            # A single-broker development cluster can briefly answer metadata requests before
            # its coordinators are ready after restart. Require a stable window so later
            # consumer-group recovery tests do not race that partial readiness state.
            consecutive_successes += 1
            if consecutive_successes == 5:
                return
        except subprocess.CalledProcessError:
            consecutive_successes = 0
        await asyncio.sleep(2)
    pytest.fail("timed out waiting for Kafka readiness")


async def wait_for_schema_registry(timeout=60) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient(timeout=2) as client:
        while asyncio.get_running_loop().time() < deadline:
            try:
                response = await client.get(f"{SCHEMA_REGISTRY_URL}/subjects")
                if response.is_success:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
    pytest.fail("timed out waiting for Schema Registry readiness")


@pytest.mark.asyncio
async def test_event_traverses_api_kafka_worker_postgres_and_redis():
    device_id = f"integration-e2e-{uuid4().hex}"
    event = payload(device_id)
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        response = await client.post("/telemetry", json=event)
        assert response.status_code == 202
        recent = await wait_for(
            client,
            f"/devices/{device_id}/recent",
            lambda body: any(item["event_id"] == event["event_id"] for item in body["events"]),
        )
        assert recent["events"][0]["device_id"] == device_id
        latest = await client.get(f"/devices/{device_id}/latest")
        assert latest.status_code == 200
        assert latest.json()["event_id"] == event["event_id"]


@pytest.mark.asyncio
async def test_ingestion_requires_authentication_and_enforces_request_size():
    event = payload(f"integration-admission-{uuid4().hex}")
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
        unauthorized = await client.post("/telemetry", json=event)
        oversized = await client.post(
            "/telemetry",
            content=b"x" * 1_048_577,
            headers=AUTH_HEADERS,
        )

    assert unauthorized.status_code == 401
    assert oversized.status_code == 413


@pytest.mark.asyncio
async def test_distributed_event_quota_returns_429_with_retry_metadata():
    fingerprint = sha256(API_KEY.encode()).hexdigest()[:16]
    compose("exec", "-T", "redis", "redis-cli", "DEL", f"admission:{fingerprint}")
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=60,
        headers=AUTH_HEADERS,
    ) as client:
        first = await client.post("/telemetry/batch", json=[{}] * 500)
        second = await client.post("/telemetry/batch", json=[{}] * 500)
        limited = await client.post("/telemetry/batch", json=[{}] * 100)

    assert first.status_code == 202
    assert second.status_code == 202
    assert limited.status_code == 429
    assert int(limited.headers["retry-after"]) >= 1
    assert limited.headers["x-ratelimit-remaining"] == "0"


@pytest.mark.asyncio
async def test_kafka_outage_returns_retryable_backpressure_response():
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        compose("stop", "kafka")
        try:
            response = await client.post(
                "/telemetry",
                json=payload(f"integration-kafka-backpressure-{uuid4().hex}"),
            )
        finally:
            compose("start", "kafka")
            await wait_for_kafka()

    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"


@pytest.mark.asyncio
async def test_registry_outage_uses_cached_schema_and_degrades_health():
    device_id = f"integration-registry-outage-{uuid4().hex}"
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        compose("stop", "schema-registry")
        try:
            response = await client.post("/telemetry", json=payload(device_id))
            health = await client.get("/health")
        finally:
            compose("start", "schema-registry")
            await wait_for_schema_registry()

    assert response.status_code == 202
    assert health.status_code == 200
    assert health.json()["status"] == "degraded"
    assert health.json()["dependencies"]["schema_registry"] is False


@pytest.mark.asyncio
async def test_mixed_schema_versions_unknown_version_isolation_and_audited_replay():
    prefix = f"integration-schema-{uuid4().hex}"
    legacy = payload(f"{prefix}-legacy")
    current = payload(f"{prefix}-current")
    unknown = payload(f"{prefix}-unknown")
    following = payload(f"{prefix}-following")
    now = datetime.now(UTC).isoformat()
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        before = (await client.get("/pipeline/stats")).json()["dlq_count"]
        await send_raw_payloads(
            {"event": legacy, "received_at": now},
            {
                "schema_version": 1,
                "schema_id": 1,
                "event": current,
                "received_at": now,
            },
            {
                "schema_version": 999,
                "schema_id": 999,
                "event": unknown,
                "received_at": now,
            },
        )
        response = await client.post("/telemetry", json=following)
        assert response.status_code == 202
        await wait_for(
            client,
            f"/pipeline/stats?device_prefix={prefix}",
            lambda body: body["event_count"] == 3 and body["dlq_count"] >= before + 1,
        )

        error_id = int(
            postgres_scalar(
                """
                SELECT id FROM pipeline_errors
                WHERE reason LIKE 'unsupported schema_version:%'
                ORDER BY id DESC LIMIT 1
                """
            )
        )
        corrected = {
            "schema_version": 1,
            "schema_id": 1,
            "event": unknown,
            "received_at": now,
        }
        dry_run = compose(
            "exec",
            "-T",
            "worker",
            "python",
            "/tools/dlq_replay.py",
            "replay",
            "--error-id",
            str(error_id),
            "--payload-json",
            json.dumps(corrected),
        )
        assert json.loads(dry_run.stdout)["executed"] is False
        executed = compose(
            "exec",
            "-T",
            "worker",
            "python",
            "/tools/dlq_replay.py",
            "replay",
            "--error-id",
            str(error_id),
            "--payload-json",
            json.dumps(corrected),
            "--execute",
        )
        assert json.loads(executed.stdout)["executed"] is True
        await wait_for(
            client,
            f"/pipeline/stats?device_prefix={prefix}",
            lambda body: body["event_count"] == 4,
        )
        assert postgres_scalar(
            f"SELECT replay_count FROM pipeline_errors WHERE id = {error_id}"
        ) == "1"


@pytest.mark.asyncio
async def test_poison_message_goes_to_dlq_without_blocking_partition():
    prefix = f"integration-poison-{uuid4().hex}"
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        before = (await client.get("/pipeline/stats")).json()["dlq_count"]
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL, acks="all")
        await producer.start()
        try:
            await producer.send_and_wait("raw-telemetry", b"{not-json", key=prefix.encode())
        finally:
            await producer.stop()
        response = await client.post("/telemetry", json=payload(prefix))
        assert response.status_code == 202
        await wait_for(
            client,
            f"/pipeline/stats?device_prefix={prefix}",
            lambda body: body["event_count"] == 1 and body["dlq_count"] >= before + 1,
        )


@pytest.mark.asyncio
async def test_worker_recovers_buffered_events():
    prefix = f"integration-worker-recovery-{uuid4().hex}"
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        compose("stop", "worker")
        try:
            for index in range(3):
                response = await client.post("/telemetry", json=payload(f"{prefix}-{index}"))
                assert response.status_code == 202
        finally:
            compose("start", "worker")
        stats = await wait_for(
            client,
            f"/pipeline/stats?device_prefix={prefix}",
            lambda body: body["event_count"] == 3,
        )
        assert stats["event_count"] == 3


@pytest.mark.asyncio
async def test_graceful_worker_shutdown_preserves_every_accepted_event():
    prefix = f"integration-graceful-{uuid4().hex}"
    events = [payload(f"{prefix}-{index}") for index in range(50)]
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=60,
        headers=AUTH_HEADERS,
    ) as client:
        responses = await asyncio.gather(
            *(client.post("/telemetry", json=event) for event in events)
        )
        assert all(response.status_code == 202 for response in responses)
        compose("stop", "worker")
        logs = compose("logs", "--tail=30", "worker").stdout
        assert "worker stopped gracefully" in logs
        compose("start", "worker")
        stats = await wait_for(
            client,
            f"/pipeline/stats?device_prefix={prefix}",
            lambda body: body["event_count"] == len(events),
            timeout=90,
        )
        assert stats["event_count"] == len(events)


@pytest.mark.asyncio
async def test_postgres_outage_buffers_event_until_recovery():
    prefix = f"integration-postgres-outage-{uuid4().hex}"
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        compose("stop", "postgres")
        try:
            response = await client.post("/telemetry", json=payload(prefix))
            assert response.status_code == 202
            await asyncio.sleep(1)
        finally:
            compose("start", "postgres")
        await wait_for(
            client,
            f"/pipeline/stats?device_prefix={prefix}",
            lambda body: body["event_count"] == 1,
            timeout=90,
        )


@pytest.mark.asyncio
async def test_redis_outage_replays_durable_row_and_repairs_cache():
    device_id = f"integration-redis-outage-{uuid4().hex}"
    event = payload(device_id)
    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        compose("stop", "redis")
        try:
            await send_raw_event(event)
            await asyncio.sleep(1)
        finally:
            compose("start", "redis")
        recent = await wait_for(
            client,
            f"/devices/{device_id}/recent",
            lambda body: any(item["event_id"] == event["event_id"] for item in body["events"]),
            timeout=90,
        )
        assert len([item for item in recent["events"] if item["event_id"] == event["event_id"]]) == 1


@pytest.mark.asyncio
async def test_pending_outbox_survives_restart_and_two_dispatchers_publish_each_row_once():
    prefix = f"integration-outbox-{uuid4().hex}"
    events = [payload(f"{prefix}-{index}") for index in range(6)]
    event_ids = {event["event_id"] for event in events}
    quoted_ids = ", ".join(f"'{event_id}'" for event_id in event_ids)

    async with httpx.AsyncClient(
        base_url=API_URL,
        timeout=10,
        headers=AUTH_HEADERS,
    ) as client:
        for event in events:
            response = await client.post("/telemetry", json=event)
            assert response.status_code == 202
        await wait_for_postgres(
            f"""
            SELECT count(*) FROM event_outbox
            WHERE event_id IN ({quoted_ids}) AND published_at IS NOT NULL
            """,
            str(len(events)),
        )

    compose("stop", "worker")
    consumer = AIOKafkaConsumer(
        "validated-telemetry",
        bootstrap_servers=KAFKA_URL,
        group_id=f"outbox-verification-{uuid4().hex}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode()),
    )
    await consumer.start()
    try:
        updated = postgres_scalar(
            f"""
            WITH updated AS (
              UPDATE event_outbox SET published_at = NULL
              WHERE event_id IN ({quoted_ids})
              RETURNING 1
            )
            SELECT count(*) FROM updated
            """
        )
        assert updated == str(len(events))

        compose("up", "--detach", "--scale", "worker=2", "worker")
        received = []
        deadline = asyncio.get_running_loop().time() + 90
        while len(received) < len(events) and asyncio.get_running_loop().time() < deadline:
            batch = await consumer.getmany(timeout_ms=1000)
            received.extend(
                message.value["event_id"]
                for messages in batch.values()
                for message in messages
                if message.value.get("event_id") in event_ids
            )

        assert set(received) == event_ids
        assert len(received) == len(event_ids)
        await wait_for_postgres(
            f"""
            SELECT count(*) FROM event_outbox
            WHERE event_id IN ({quoted_ids}) AND published_at IS NULL
            """,
            "0",
        )
    finally:
        await consumer.stop()
        compose("up", "--detach", "--scale", "worker=1", "worker")


def test_topics_have_multiple_partitions_and_workers_scale():
    describe = compose(
        "exec",
        "-T",
        "kafka",
        "kafka-topics",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--topic",
        "raw-telemetry",
    ).stdout
    assert "PartitionCount: 3" in describe

    compose("up", "--detach", "--scale", "worker=2", "worker")
    try:
        deadline = asyncio.run(_wait_for_two_consumers())
        assert deadline >= 2
    finally:
        compose("up", "--detach", "--scale", "worker=1", "worker")


async def _wait_for_two_consumers() -> int:
    for _attempt in range(30):
        output = compose(
            "exec",
            "-T",
            "kafka",
            "kafka-consumer-groups",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            "telemetry-worker",
            "--members",
        ).stdout
        members = {
            line.split()[1]
            for line in output.splitlines()
            if "telemetry-worker" in line and not line.lstrip().startswith("GROUP")
        }
        if len(members) >= 2:
            return len(members)
        await asyncio.sleep(1)
    return 0
