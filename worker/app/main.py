import asyncio
import base64
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import ValidationError
from redis.asyncio import Redis

from app.config import settings
from app.logging_config import configure_logging
from app.models import TelemetryEnvelope

configure_logging(settings.service_name)
logger = logging.getLogger(__name__)

events_processed_total = Counter("events_processed_total", "Processed telemetry events")
events_failed_total = Counter("events_failed_total", "Failed telemetry events")
dead_letter_count = Counter("dead_letter_count", "Events routed to the dead-letter topic")
duplicates_total = Counter("duplicate_events_total", "Duplicate event IDs ignored")
queue_lag = Gauge("queue_lag", "Kafka consumer partition lag")
processing_latency_ms = Histogram(
    "processing_latency_ms",
    "Worker processing latency in milliseconds",
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
)


@dataclass
class PoisonMessage(Exception):
    reason: str
    payload: dict


def dumps(value: dict) -> bytes:
    return json.dumps(value, default=_json_default).encode("utf-8")


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    raise TypeError(f"{value!r} is not JSON serializable")


def decode_message(value: bytes) -> dict:
    try:
        payload = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PoisonMessage(
            reason=f"invalid JSON: {exc}",
            payload={"raw_base64": base64.b64encode(value).decode("ascii")},
        ) from exc
    if not isinstance(payload, dict):
        raise PoisonMessage(reason="payload must be a JSON object", payload={"value": payload})
    return payload


async def write_dead_letter(producer, pool, message, poison: PoisonMessage) -> None:
    source = {
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
    }
    row = await pool.fetchrow(
        """
        INSERT INTO pipeline_errors(
          source_topic, source_partition, source_offset, payload, reason, created_at
        )
        VALUES($1, $2, $3, $4::jsonb, $5, now())
        ON CONFLICT(source_topic, source_partition, source_offset)
        DO UPDATE SET reason = EXCLUDED.reason
        RETURNING published_at
        """,
        message.topic,
        message.partition,
        message.offset,
        json.dumps(poison.payload, default=_json_default),
        poison.reason,
    )
    if row["published_at"] is None:
        await producer.send_and_wait(
            settings.dead_letter_topic,
            {
                "payload": poison.payload,
                "reason": poison.reason,
                "source": source,
                "failed_at": datetime.now(UTC),
            },
            key=f"{message.topic}:{message.partition}:{message.offset}".encode(),
        )
        await pool.execute(
            """
            UPDATE pipeline_errors SET published_at = now()
            WHERE source_topic = $1 AND source_partition = $2 AND source_offset = $3
            """,
            message.topic,
            message.partition,
            message.offset,
        )
        dead_letter_count.inc()


async def persist_event(pool, envelope: TelemetryEnvelope) -> tuple[dict, bool]:
    event = envelope.event
    processed_at = datetime.now(UTC)
    event_lag_ms = int((processed_at - event.timestamp).total_seconds() * 1000)
    outlier_detected = event.temperature < -40 or event.temperature > 140

    async with pool.acquire() as connection, connection.transaction():
        previous_voltage = await connection.fetchval(
            """
            SELECT voltage FROM telemetry_events
            WHERE device_id = $1 ORDER BY timestamp DESC LIMIT 1
            """,
            event.device_id,
        )
        voltage_drop_detected = (
            previous_voltage is not None and previous_voltage - event.voltage >= 2.5
        )
        avg_temperature_5m = await connection.fetchval(
            """
            SELECT AVG(temperature) FROM telemetry_events
            WHERE device_id = $1
              AND timestamp >= $2::timestamptz - interval '5 minutes'
              AND timestamp <= $2::timestamptz
            """,
            event.device_id,
            event.timestamp,
        )
        avg_temperature_5m = (
            event.temperature if avg_temperature_5m is None else float(avg_temperature_5m)
        )
        row = await connection.fetchrow(
            """
            INSERT INTO telemetry_events(
              event_id, device_id, timestamp, received_at, processed_at,
              temperature, voltage, status, region, avg_temperature_5m,
              voltage_drop_detected, event_lag_ms, outlier_detected
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING *
            """,
            event.event_id,
            event.device_id,
            event.timestamp,
            envelope.received_at,
            processed_at,
            event.temperature,
            event.voltage,
            event.status.value,
            event.region,
            avg_temperature_5m,
            voltage_drop_detected,
            event_lag_ms,
            outlier_detected,
        )
        inserted = row is not None
        if inserted:
            await connection.execute(
                """
                INSERT INTO device_status(
                  device_id, region, last_seen, status, temperature, voltage
                )
                VALUES($1, $2, $3, $4, $5, $6)
                ON CONFLICT (device_id) DO UPDATE SET
                  region = CASE WHEN EXCLUDED.last_seen >= device_status.last_seen
                    THEN EXCLUDED.region ELSE device_status.region END,
                  last_seen = GREATEST(device_status.last_seen, EXCLUDED.last_seen),
                  status = CASE WHEN EXCLUDED.last_seen >= device_status.last_seen
                    THEN EXCLUDED.status ELSE device_status.status END,
                  temperature = CASE WHEN EXCLUDED.last_seen >= device_status.last_seen
                    THEN EXCLUDED.temperature ELSE device_status.temperature END,
                  voltage = CASE WHEN EXCLUDED.last_seen >= device_status.last_seen
                    THEN EXCLUDED.voltage ELSE device_status.voltage END
                """,
                event.device_id,
                event.region,
                event.timestamp,
                event.status.value,
                event.temperature,
                event.voltage,
            )
        else:
            duplicates_total.inc()
            row = await connection.fetchrow(
                "SELECT * FROM telemetry_events WHERE event_id = $1",
                event.event_id,
            )
    return dict(row), inserted


async def update_cache(redis: Redis, event: dict) -> None:
    payload = json.dumps(event, default=_json_default)
    key = f"telemetry:{event['device_id']}:recent"
    async with redis.pipeline(transaction=False) as pipe:
        pipe.set(f"telemetry:{event['device_id']}:latest", payload, ex=86400)
        pipe.lrem(key, 0, payload)
        pipe.lpush(key, payload)
        pipe.ltrim(key, 0, 49)
        pipe.expire(key, 86400)
        await pipe.execute()


async def process_event(pool, redis: Redis, producer, envelope: TelemetryEnvelope) -> bool:
    stored, inserted = await persist_event(pool, envelope)
    await update_cache(redis, stored)
    if inserted:
        await producer.send_and_wait(
            settings.validated_topic,
            {
                "event_id": stored["event_id"],
                "device_id": stored["device_id"],
                "timestamp": stored["timestamp"],
                "avg_temperature_5m": stored["avg_temperature_5m"],
                "voltage_drop_detected": stored["voltage_drop_detected"],
                "event_lag_ms": stored["event_lag_ms"],
                "outlier_detected": stored["outlier_detected"],
            },
            key=stored["device_id"].encode(),
        )
        events_processed_total.inc()
    return inserted


async def handle_message(pool, redis: Redis, producer, message) -> str:
    try:
        payload = decode_message(message.value)
        envelope = TelemetryEnvelope.model_validate(payload)
    except PoisonMessage as poison:
        events_failed_total.inc()
        await write_dead_letter(producer, pool, message, poison)
        return "dead_letter"
    except ValidationError as exc:
        events_failed_total.inc()
        await write_dead_letter(
            producer,
            pool,
            message,
            PoisonMessage(reason=str(exc), payload=payload),
        )
        return "dead_letter"
    await process_event(pool, redis, producer, envelope)
    return "processed"


async def process_batch(pool, redis: Redis, producer, messages) -> list[str]:
    results = []
    for message in messages:
        results.append(await handle_message(pool, redis, producer, message))
    return results


def rewind_batch(consumer, batches) -> None:
    for partition, records in batches.items():
        if records:
            consumer.seek(partition, records[0].offset)


async def run() -> None:
    start_http_server(settings.metrics_port)
    consumer = AIOKafkaConsumer(
        settings.raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=dumps,
        acks="all",
        enable_idempotence=True,
    )
    pool = await connect_database()
    redis = await connect_redis()
    await connect_kafka(consumer, producer)
    logger.info("worker started", extra={"service": settings.service_name})
    try:
        while True:
            batches = await consumer.getmany(
                timeout_ms=settings.batch_wait_ms,
                max_records=settings.batch_size,
            )
            messages = [message for partition in batches.values() for message in partition]
            if not messages:
                continue
            started = perf_counter()
            try:
                await process_batch(pool, redis, producer, messages)
                await consumer.commit()
            except Exception:
                events_failed_total.inc()
                logger.exception(
                    "batch failed; offsets left uncommitted and positions rewound",
                    extra={"service": settings.service_name},
                )
                rewind_batch(consumer, batches)
                await asyncio.sleep(1)
            finally:
                processing_latency_ms.observe((perf_counter() - started) * 1000)
                for partition in consumer.assignment():
                    position = await consumer.position(partition)
                    highwater = consumer.highwater(partition)
                    if highwater is not None:
                        queue_lag.set(max(highwater - position, 0))
    finally:
        await consumer.stop()
        await producer.stop()
        await redis.aclose()
        await pool.close()


async def connect_database() -> asyncpg.Pool:
    while True:
        try:
            return await asyncpg.create_pool(settings.database_url, min_size=1, max_size=10)
        except Exception:
            logger.exception(
                "database connection failed; retrying",
                extra={"service": settings.service_name},
            )
            await asyncio.sleep(3)


async def connect_redis() -> Redis:
    while True:
        try:
            redis = Redis.from_url(settings.redis_url, decode_responses=True)
            await redis.ping()
            return redis
        except Exception:
            logger.exception(
                "redis connection failed; retrying",
                extra={"service": settings.service_name},
            )
            await asyncio.sleep(3)


async def connect_kafka(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer) -> None:
    while True:
        try:
            await consumer.start()
            await producer.start()
            return
        except Exception:
            logger.exception(
                "kafka connection failed; retrying",
                extra={"service": settings.service_name},
            )
            await asyncio.gather(
                consumer.stop(),
                producer.stop(),
                return_exceptions=True,
            )
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(run())
