import asyncio
import os
import subprocess
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaProducer

pytestmark = pytest.mark.integration

API_URL = os.getenv("METRINGEST_TEST_URL", "http://localhost:8000")
KAFKA_URL = os.getenv("METRINGEST_KAFKA_URL", "localhost:29092")


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


@pytest.mark.asyncio
async def test_event_traverses_api_kafka_worker_postgres_and_redis():
    device_id = f"integration-e2e-{uuid4().hex}"
    event = payload(device_id)
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
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
async def test_poison_message_goes_to_dlq_without_blocking_partition():
    prefix = f"integration-poison-{uuid4().hex}"
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
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
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
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
async def test_postgres_outage_buffers_event_until_recovery():
    prefix = f"integration-postgres-outage-{uuid4().hex}"
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
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
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
        compose("stop", "redis")
        try:
            response = await client.post("/telemetry", json=event)
            assert response.status_code == 202
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
