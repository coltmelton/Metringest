import asyncpg
from redis.asyncio import Redis

from app.config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS metric_events (
  id UUID PRIMARY KEY, name TEXT NOT NULL, value DOUBLE PRECISION NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL, source TEXT NOT NULL, tags JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS metric_events_name_time_idx ON metric_events (name, occurred_at DESC);
"""


async def open_postgres() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=8)
    async with pool.acquire() as connection:
        await connection.execute(SCHEMA)
    return pool


def open_redis() -> Redis:
    return Redis.from_url(settings.redis_url, decode_responses=True)

