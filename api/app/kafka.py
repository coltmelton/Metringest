import json
import asyncio
import logging
from datetime import datetime

from aiokafka import AIOKafkaProducer

from app.config import settings

producer: AIOKafkaProducer | None = None
logger = logging.getLogger(__name__)


def json_serializer(value: dict) -> bytes:
    return json.dumps(value, default=_json_default).encode("utf-8")


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    raise TypeError(f"{value!r} is not JSON serializable")


async def connect() -> None:
    global producer
    while True:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=json_serializer,
                acks="all",
            )
            await producer.start()
            return
        except Exception:
            logger.exception("kafka producer connection failed; retrying")
            await asyncio.sleep(3)


async def disconnect() -> None:
    if producer:
        await producer.stop()


async def publish(event: dict) -> None:
    if producer is None:
        raise RuntimeError("kafka producer is not initialized")
    await producer.send_and_wait(settings.raw_topic, event)
