from redis.asyncio import Redis

from app.config import settings

client: Redis | None = None


async def connect() -> None:
    global client
    client = Redis.from_url(settings.redis_url, decode_responses=True)
    await client.ping()


async def disconnect() -> None:
    if client:
        await client.aclose()


def get_client() -> Redis:
    if client is None:
        raise RuntimeError("redis client is not initialized")
    return client
