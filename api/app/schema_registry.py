import asyncio
import json
import logging
from pathlib import Path

import httpx

from app.config import settings

logger = logging.getLogger(__name__)
schema_id: int | None = None


def load_schema(version: int) -> dict:
    path = Path(settings.schema_directory) / f"raw-telemetry-v{version}.json"
    return json.loads(path.read_text())


async def register_contracts() -> int:
    async with httpx.AsyncClient(base_url=settings.schema_registry_url, timeout=10) as client:
        response = await client.put(
            f"/config/{settings.schema_subject}",
            json={"compatibility": "BACKWARD_TRANSITIVE"},
        )
        response.raise_for_status()
        latest_id = None
        for version in (0, 1):
            response = await client.post(
                f"/subjects/{settings.schema_subject}/versions",
                json={"schemaType": "JSON", "schema": json.dumps(load_schema(version))},
            )
            response.raise_for_status()
            latest_id = int(response.json()["id"])
        if latest_id is None:
            raise RuntimeError("no telemetry schemas were registered")
        return latest_id


async def connect() -> None:
    global schema_id
    while True:
        try:
            schema_id = await register_contracts()
            logger.info(
                "schema contracts registered",
                extra={"service": settings.service_name, "schema_id": schema_id},
            )
            return
        except Exception:
            logger.exception("schema registry connection failed; retrying")
            await asyncio.sleep(3)


async def healthy() -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            response = await client.get(f"{settings.schema_registry_url}/subjects")
            return response.is_success
    except httpx.HTTPError:
        return False


def current_schema_id() -> int:
    if schema_id is None:
        raise RuntimeError("schema ID is not initialized")
    return schema_id
