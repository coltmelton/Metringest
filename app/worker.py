import asyncio
import json

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.schemas import MetricEvent
from app.storage import open_postgres, open_redis


async def persist_batch(postgres, redis, messages) -> int:
    """Persist one Kafka batch before its offsets are committed."""
    events = [MetricEvent.model_validate(message.value) for message in messages]
    if not events:
        return 0

    await postgres.executemany(
        """INSERT INTO metric_events (id, name, value, occurred_at, source, tags)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb) ON CONFLICT (id) DO NOTHING""",
        [
            (
                event.id,
                event.name,
                event.value,
                event.timestamp,
                event.source,
                json.dumps(event.tags),
            )
            for event in events
        ],
    )

    async with redis.pipeline(transaction=False) as pipe:
        for event in events:
            key = f"metric:{event.name}:recent"
            pipe.lpush(key, event.model_dump_json())
            pipe.ltrim(key, 0, 49)
            pipe.expire(key, 86400)
        await pipe.execute()
    return len(events)


async def run() -> None:
    postgres = await open_postgres()
    redis = open_redis()
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="metringest-workers",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode()),
    )
    await consumer.start()
    try:
        while True:
            batches = await consumer.getmany(
                timeout_ms=settings.worker_batch_wait_ms,
                max_records=settings.worker_batch_size,
            )
            messages = [message for partition in batches.values() for message in partition]
            if messages:
                await persist_batch(postgres, redis, messages)
                await consumer.commit()
    finally:
        await consumer.stop()
        await redis.aclose()
        await postgres.close()


if __name__ == "__main__":
    asyncio.run(run())
