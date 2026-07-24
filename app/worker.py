import asyncio
import json

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.schemas import MetricEvent
from app.storage import open_postgres, open_redis


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
        async for message in consumer:
            event = MetricEvent.model_validate(message.value)
            await postgres.execute(
                """INSERT INTO metric_events (id, name, value, occurred_at, source, tags)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb) ON CONFLICT (id) DO NOTHING""",
                event.id, event.name, event.value, event.timestamp, event.source, json.dumps(event.tags),
            )
            key = f"metric:{event.name}:recent"
            async with redis.pipeline(transaction=True) as pipe:
                await pipe.lpush(key, event.model_dump_json()).ltrim(key, 0, 49).expire(key, 86400).execute()
            await consumer.commit()
    finally:
        await consumer.stop()
        await redis.aclose()
        await postgres.close()


if __name__ == "__main__":
    asyncio.run(run())
