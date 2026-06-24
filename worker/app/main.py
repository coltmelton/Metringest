import asyncio
import json
import logging
from datetime import datetime, timezone
from time import perf_counter

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import ValidationError

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


def dumps(value: dict) -> bytes:
    return json.dumps(value, default=_json_default).encode("utf-8")


def loads(value: bytes) -> dict:
    return json.loads(value.decode("utf-8"))


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    raise TypeError(f"{value!r} is not JSON serializable")


async def write_dead_letter(
    producer: AIOKafkaProducer,
    pool: asyncpg.Pool,
    payload: dict,
    reason: str,
) -> None:
    dead_letter_count.inc()
    await producer.send_and_wait(
        settings.dead_letter_topic,
        {"payload": payload, "reason": reason, "failed_at": datetime.now(timezone.utc)},
    )
    await pool.execute(
        """
        INSERT INTO pipeline_errors(payload, reason, created_at)
        VALUES($1, $2, now())
        """,
        json.dumps(payload, default=_json_default),
        reason,
    )


async def process_event(
    pool: asyncpg.Pool,
    producer: AIOKafkaProducer,
    envelope: TelemetryEnvelope,
) -> bool:
    event = envelope.event
    processed_at = datetime.now(timezone.utc)
    event_lag_ms = int((processed_at - event.timestamp).total_seconds() * 1000)
    outlier_detected = event.temperature < -40 or event.temperature > 140

    previous_voltage = await pool.fetchval(
        """
        SELECT voltage FROM telemetry_events
        WHERE device_id = $1
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        event.device_id,
    )
    voltage_drop_detected = (
        previous_voltage is not None and previous_voltage - event.voltage >= 2.5
    )

    avg_temperature_5m = await pool.fetchval(
        """
        SELECT AVG(temperature) FROM telemetry_events
        WHERE device_id = $1 AND timestamp >= $2 - interval '5 minutes'
          AND timestamp <= $2
        """,
        event.device_id,
        event.timestamp,
    )
    if avg_temperature_5m is None:
        avg_temperature_5m = event.temperature

    inserted = await pool.fetchval(
        """
        INSERT INTO telemetry_events(
          event_id, device_id, timestamp, received_at, processed_at,
          temperature, voltage, status, region, avg_temperature_5m,
          voltage_drop_detected, event_lag_ms, outlier_detected
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING 1
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
        float(avg_temperature_5m),
        voltage_drop_detected,
        event_lag_ms,
        outlier_detected,
    )

    if not inserted:
        duplicates_total.inc()
        return False

    await pool.execute(
        """
        INSERT INTO device_status(device_id, region, last_seen, status, temperature, voltage)
        VALUES($1, $2, $3, $4, $5, $6)
        ON CONFLICT (device_id) DO UPDATE SET
          region = CASE
            WHEN EXCLUDED.last_seen >= device_status.last_seen THEN EXCLUDED.region
            ELSE device_status.region
          END,
          last_seen = GREATEST(device_status.last_seen, EXCLUDED.last_seen),
          status = CASE
            WHEN EXCLUDED.last_seen >= device_status.last_seen THEN EXCLUDED.status
            ELSE device_status.status
          END,
          temperature = CASE
            WHEN EXCLUDED.last_seen >= device_status.last_seen THEN EXCLUDED.temperature
            ELSE device_status.temperature
          END,
          voltage = CASE
            WHEN EXCLUDED.last_seen >= device_status.last_seen THEN EXCLUDED.voltage
            ELSE device_status.voltage
          END
        """,
        event.device_id,
        event.region,
        event.timestamp,
        event.status.value,
        event.temperature,
        event.voltage,
    )

    await producer.send_and_wait(
        settings.validated_topic,
        {
            "event_id": event.event_id,
            "device_id": event.device_id,
            "timestamp": event.timestamp,
            "avg_temperature_5m": float(avg_temperature_5m),
            "voltage_drop_detected": voltage_drop_detected,
            "event_lag_ms": event_lag_ms,
            "outlier_detected": outlier_detected,
        },
    )
    return True


async def run() -> None:
    start_http_server(settings.metrics_port)
    consumer = AIOKafkaConsumer(
        settings.raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group,
        value_deserializer=loads,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=dumps,
        acks="all",
    )
    pool = await connect_database()
    await connect_kafka(consumer, producer)
    logger.info("worker started", extra={"service": settings.service_name})
    try:
        async for message in consumer:
            started = perf_counter()
            try:
                envelope = TelemetryEnvelope.model_validate(message.value)
                inserted = await process_event(pool, producer, envelope)
                if inserted:
                    events_processed_total.inc()
                await consumer.commit()
            except ValidationError as exc:
                events_failed_total.inc()
                await write_dead_letter(producer, pool, message.value, str(exc))
                await consumer.commit()
            except Exception:
                events_failed_total.inc()
                logger.exception("processing failed", extra={"service": settings.service_name})
                await asyncio.sleep(1)
                continue
            finally:
                processing_latency_ms.observe((perf_counter() - started) * 1000)
                partitions = consumer.assignment()
                for partition in partitions:
                    position = await consumer.position(partition)
                    highwater = consumer.highwater(partition)
                    if highwater is not None:
                        queue_lag.set(max(highwater - position, 0))
    finally:
        await consumer.stop()
        await producer.stop()
        await pool.close()


async def connect_database() -> asyncpg.Pool:
    while True:
        try:
            return await asyncpg.create_pool(settings.database_url, min_size=1, max_size=10)
        except Exception:
            logger.exception("database connection failed; retrying", extra={"service": settings.service_name})
            await asyncio.sleep(3)


async def connect_kafka(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer) -> None:
    while True:
        try:
            await consumer.start()
            await producer.start()
            return
        except Exception:
            logger.exception("kafka connection failed; retrying", extra={"service": settings.service_name})
            try:
                await consumer.stop()
            except Exception:
                pass
            try:
                await producer.stop()
            except Exception:
                pass
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(run())
