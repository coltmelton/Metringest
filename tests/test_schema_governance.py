import base64
import json

import pytest

from scripts import dlq_replay, schema_contracts


def test_checked_in_schemas_are_valid_and_v1_adds_optional_metadata():
    v0 = schema_contracts.load_schema(0)
    v1 = schema_contracts.load_schema(1)

    assert "schema_version" not in v0["properties"]
    assert v1["properties"]["schema_version"]["const"] == 1
    assert "schema_version" not in v1["required"]
    assert "schema_id" not in v1["required"]


def test_replay_bytes_preserves_raw_and_structured_payloads():
    raw = b"not-json"

    assert dlq_replay.replay_bytes(
        {"raw_base64": base64.b64encode(raw).decode()}
    ) == raw
    assert json.loads(dlq_replay.replay_bytes({"event": {"event_id": "one"}})) == {
        "event": {"event_id": "one"}
    }


@pytest.mark.asyncio
async def test_replay_is_dry_run_by_default_and_audited_after_ack():
    class Pool:
        def __init__(self):
            self.executions = []

        async def fetchrow(self, _query, error_id):
            assert error_id == 7
            return {
                "id": 7,
                "payload": json.dumps({"event": {"device_id": "device-7"}}),
                "reason": "unsupported schema",
                "replay_count": 2,
            }

        async def execute(self, query, error_id):
            self.executions.append((query, error_id))

    class Producer:
        def __init__(self):
            self.messages = []

        async def send_and_wait(self, *args, **kwargs):
            self.messages.append((args, kwargs))

    pool = Pool()
    producer = Producer()
    dry_run = await dlq_replay.replay_error(pool, None, 7, None, False)
    executed = await dlq_replay.replay_error(pool, producer, 7, None, True)

    assert dry_run["executed"] is False
    assert dry_run["next_replay_count"] == 3
    assert executed["executed"] is True
    assert producer.messages[0][1]["key"] == b"device-7"
    assert producer.messages[0][1]["headers"] == [("replay_error_id", b"7")]
    assert pool.executions[0][1] == 7


@pytest.mark.asyncio
async def test_failed_replay_does_not_write_audit_marker():
    class Pool:
        def __init__(self):
            self.executions = []

        async def fetchrow(self, *_args):
            return {
                "id": 8,
                "payload": {"event": {"device_id": "device-8"}},
                "reason": "bad",
                "replay_count": 0,
            }

        async def execute(self, *args):
            self.executions.append(args)

    class Producer:
        async def send_and_wait(self, *_args, **_kwargs):
            raise ConnectionError("Kafka unavailable")

    pool = Pool()
    with pytest.raises(ConnectionError, match="Kafka unavailable"):
        await dlq_replay.replay_error(pool, Producer(), 8, None, True)

    assert pool.executions == []
