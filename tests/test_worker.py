from types import SimpleNamespace

import pytest

from app.worker import persist_batch


class FakePostgres:
    def __init__(self):
        self.calls = []

    async def executemany(self, query, rows):
        self.calls.append((query, rows))


class FakePipeline:
    def __init__(self):
        self.commands = []
        self.executed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        pass

    def lpush(self, *args):
        self.commands.append(("lpush", args))

    def ltrim(self, *args):
        self.commands.append(("ltrim", args))

    def expire(self, *args):
        self.commands.append(("expire", args))

    async def execute(self):
        self.executed = True


class FakeRedis:
    def __init__(self):
        self.pipeline_instance = FakePipeline()
        self.transaction = None

    def pipeline(self, *, transaction):
        self.transaction = transaction
        return self.pipeline_instance


@pytest.mark.asyncio
async def test_persist_batch_uses_one_database_and_redis_round_trip():
    postgres = FakePostgres()
    redis = FakeRedis()
    messages = [
        SimpleNamespace(
            value={"name": "checkout.duration", "value": value, "source": "pytest"}
        )
        for value in (10, 20, 30)
    ]

    count = await persist_batch(postgres, redis, messages)

    assert count == 3
    assert len(postgres.calls) == 1
    assert len(postgres.calls[0][1]) == 3
    assert redis.transaction is False
    assert redis.pipeline_instance.executed is True
    assert len(redis.pipeline_instance.commands) == 9


@pytest.mark.asyncio
async def test_persist_batch_avoids_storage_round_trips_for_empty_poll():
    postgres = FakePostgres()
    redis = FakeRedis()

    assert await persist_batch(postgres, redis, []) == 0
    assert postgres.calls == []
    assert redis.pipeline_instance.executed is False
