import asyncio
import logging

import asyncpg

from app.config import settings

pool: asyncpg.Pool | None = None
logger = logging.getLogger(__name__)


async def connect() -> None:
    global pool
    while True:
        try:
            pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=10)
            return
        except Exception:
            logger.exception("database connection failed; retrying")
            await asyncio.sleep(3)


async def disconnect() -> None:
    if pool:
        await pool.close()


def get_pool() -> asyncpg.Pool:
    if pool is None:
        raise RuntimeError("database pool is not initialized")
    return pool
