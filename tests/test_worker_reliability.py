import json
from types import SimpleNamespace

import pytest
from app import main as worker


def message(value=b"{}", offset=1):
    return SimpleNamespace(
        topic="raw-telemetry",
        partition=0,
        offset=offset,
        value=value,
    )


@pytest.mark.asyncio
async def test_poison_message_isolated_and_next_record_processed(monkeypatch):
    calls = []

    async def fake_dead_letter(_producer, _pool, record, poison):
        calls.append(("dlq", record.offset, poison.reason))

    async def fake_process(_pool, _redis, envelope):
        calls.append(("processed", envelope.event.event_id))

    monkeypatch.setattr(worker, "write_dead_letter", fake_dead_letter)
    monkeypatch.setattr(worker, "process_event", fake_process)
    valid = {
        "event": {
            "event_id": "event-2",
            "device_id": "device-2",
            "timestamp": "2026-07-24T12:00:00Z",
            "temperature": 72,
            "voltage": 12,
            "status": "OK",
            "region": "us-east",
        },
        "received_at": "2026-07-24T12:00:01Z",
    }

    results = await worker.process_batch(
        object(),
        object(),
        object(),
        [message(b"not-json", 1), message(json.dumps(valid).encode(), 2)],
    )

    assert results == ["dead_letter", "processed"]
    assert calls[0][0:2] == ("dlq", 1)
    assert calls[1] == ("processed", "event-2")


@pytest.mark.asyncio
async def test_mid_batch_failure_stops_later_records_and_leaves_error_for_retry(monkeypatch):
    seen = []

    async def fake_handle(_pool, _redis, _producer, record):
        seen.append(record.offset)
        if record.offset == 11:
            raise ConnectionError("redis unavailable")
        return "processed"

    monkeypatch.setattr(worker, "handle_message", fake_handle)

    with pytest.raises(ConnectionError, match="redis unavailable"):
        await worker.process_batch(
            object(),
            object(),
            object(),
            [message(offset=10), message(offset=11), message(offset=12)],
        )

    assert seen == [10, 11]


def test_failed_batch_rewinds_each_partition_to_first_fetched_offset():
    calls = []

    class Consumer:
        def seek(self, partition, offset):
            calls.append((partition, offset))

    worker.rewind_batch(
        Consumer(),
        {
            "partition-0": [message(offset=10), message(offset=11)],
            "partition-1": [message(offset=20)],
            "partition-2": [],
        },
    )

    assert calls == [("partition-0", 10), ("partition-1", 20)]


@pytest.mark.asyncio
async def test_postgres_failure_prevents_cache_and_downstream_publish(monkeypatch):
    calls = []

    async def failed_persist(_pool, _envelope):
        calls.append("postgres")
        raise ConnectionError("postgres unavailable")

    async def cache_should_not_run(*_args):
        calls.append("redis")

    monkeypatch.setattr(worker, "persist_event", failed_persist)
    monkeypatch.setattr(worker, "update_cache", cache_should_not_run)

    with pytest.raises(ConnectionError, match="postgres unavailable"):
        await worker.process_event(object(), object(), object())

    assert calls == ["postgres"]


@pytest.mark.asyncio
async def test_redis_failure_occurs_after_durable_write_and_before_raw_offset_commit(monkeypatch):
    calls = []

    async def successful_persist(_pool, _envelope):
        calls.append("postgres")
        return {"event_id": "one", "device_id": "device-one"}, True

    async def failed_cache(_redis, _stored):
        calls.append("redis")
        raise ConnectionError("redis unavailable")

    monkeypatch.setattr(worker, "persist_event", successful_persist)
    monkeypatch.setattr(worker, "update_cache", failed_cache)

    with pytest.raises(ConnectionError, match="redis unavailable"):
        await worker.process_event(object(), object(), object())

    assert calls == ["postgres", "redis"]


class FakeTransaction:
    def __init__(self):
        self.exception = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, _exc, _traceback):
        self.exception = exc_type


class FakeOutboxConnection:
    def __init__(self, rows, fail_update=False):
        self.rows = rows
        self.fail_update = fail_update
        self.updates = []
        self.transaction_state = FakeTransaction()

    def transaction(self):
        return self.transaction_state

    async def fetch(self, query, batch_size):
        assert "FOR UPDATE SKIP LOCKED" in query
        assert batch_size > 0
        return self.rows

    async def execute(self, query, row_id):
        assert "published_at = now()" in query
        if self.fail_update:
            raise RuntimeError("process crashed before marker commit")
        self.updates.append(row_id)


class FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return None


class FakeOutboxPool:
    def __init__(self, rows, fail_update=False):
        self.connection = FakeOutboxConnection(rows, fail_update)

    def acquire(self):
        return FakeAcquire(self.connection)


def outbox_row(row_id=1):
    return {
        "id": row_id,
        "event_id": f"event-{row_id}",
        "topic": "validated-telemetry",
        "message_key": f"device-{row_id}",
        "payload": json.dumps({"event_id": f"event-{row_id}"}),
    }


@pytest.mark.asyncio
async def test_outbox_marks_record_only_after_kafka_acknowledges():
    pool = FakeOutboxPool([outbox_row()])
    sent = []

    class Producer:
        async def send_and_wait(self, topic, payload, **kwargs):
            sent.append((topic, payload, kwargs))

    published = await worker.publish_outbox_batch(pool, Producer(), 10)

    assert published == 1
    assert pool.connection.updates == [1]
    assert sent[0][1] == {"event_id": "event-1"}
    assert sent[0][2]["key"] == b"device-1"
    assert sent[0][2]["headers"] == [("event_id", b"event-1")]
    assert pool.connection.transaction_state.exception is None


@pytest.mark.asyncio
async def test_outbox_publish_failure_leaves_record_unmarked_for_restart():
    pool = FakeOutboxPool([outbox_row()])

    class Producer:
        async def send_and_wait(self, *_args, **_kwargs):
            raise ConnectionError("kafka unavailable")

    with pytest.raises(ConnectionError, match="kafka unavailable"):
        await worker.publish_outbox_batch(pool, Producer(), 10)

    assert pool.connection.updates == []
    assert pool.connection.transaction_state.exception is ConnectionError


@pytest.mark.asyncio
async def test_outbox_crash_after_publish_rolls_back_marker_for_idempotent_replay():
    pool = FakeOutboxPool([outbox_row()], fail_update=True)
    sent = []

    class Producer:
        async def send_and_wait(self, *_args, **_kwargs):
            sent.append("acknowledged")

    with pytest.raises(RuntimeError, match="marker commit"):
        await worker.publish_outbox_batch(pool, Producer(), 10)

    assert sent == ["acknowledged"]
    assert pool.connection.updates == []
    assert pool.connection.transaction_state.exception is RuntimeError
