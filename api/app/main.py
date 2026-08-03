import json
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated

import asyncpg
from aiokafka.errors import KafkaError
from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError
from redis.exceptions import RedisError

from app import cache, db, kafka, schema_registry
from app.admission import (
    ClientIdentity,
    RequestSizeLimitMiddleware,
    authenticate_client,
    charge_rate_limit,
)
from app.config import settings
from app.logging_config import configure_logging
from app.metrics import (
    MetricsMiddleware,
    admission_rejected_total,
    dependency_ready,
    events_failed_total,
    events_ingested_total,
    metrics_response,
)
from app.models import BatchResponse, TelemetryEnvelope, TelemetryIn, TelemetryRow

configure_logging(settings.service_name)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await db.connect()
    await cache.connect()
    await schema_registry.connect()
    await kafka.connect()
    logger.info("api started", extra={"service": settings.service_name})
    try:
        yield
    finally:
        await kafka.disconnect()
        await cache.disconnect()
        await db.disconnect()


app = FastAPI(title="Distributed Telemetry Pipeline", version="1.0.0", lifespan=lifespan)
app.add_middleware(RequestSizeLimitMiddleware, max_bytes=settings.max_request_bytes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(MetricsMiddleware)


@app.get("/live")
async def live() -> dict:
    return {"status": "alive", "service": settings.service_name}


async def readiness() -> dict:
    checks = {
        "postgres": lambda: db.get_pool().fetchval("SELECT true"),
        "redis": lambda: cache.get_client().ping(),
        "schema_registry": schema_registry.healthy,
    }
    dependencies = {}
    for name, check in checks.items():
        try:
            dependencies[name] = bool(await check())
        except (asyncpg.PostgresError, RedisError, RuntimeError, OSError):
            dependencies[name] = False
        dependency_ready.labels(dependency=name).set(int(dependencies[name]))
    try:
        dependencies["kafka"] = kafka.producer is not None and bool(
            kafka.producer.client.cluster.brokers()
        )
    except (AttributeError, RuntimeError):
        dependencies["kafka"] = False
    dependency_ready.labels(dependency="kafka").set(int(dependencies["kafka"]))
    return {
        "status": (
            "ready" if all(dependencies.values()) else "degraded"
        ),
        "service": settings.service_name,
        "dependencies": dependencies,
    }


@app.get("/health")
async def health() -> dict:
    return await readiness()


@app.get("/ready")
async def ready(response: Response) -> dict:
    result = await readiness()
    if result["status"] != "ready":
        response.status_code = 503
    return result


@app.get("/metrics")
async def metrics():
    return metrics_response()


@app.post("/telemetry", status_code=202)
async def ingest(
    event: TelemetryIn,
    response: Response,
    identity: Annotated[ClientIdentity, Depends(authenticate_client)],
) -> dict:
    await charge_rate_limit(identity, 1, response)
    envelope = TelemetryEnvelope(
        schema_id=schema_registry.current_schema_id(),
        event=event,
        received_at=datetime.now(UTC),
    ).model_dump(mode="json")
    await publish_or_reject(envelope, event.device_id)
    events_ingested_total.inc()
    return {"accepted": True, "event_id": event.event_id}


@app.post("/telemetry/batch", response_model=BatchResponse, status_code=202)
async def ingest_batch(
    events: list[dict],
    response: Response,
    identity: Annotated[ClientIdentity, Depends(authenticate_client)],
) -> BatchResponse:
    if len(events) > settings.max_batch_events:
        admission_rejected_total.labels(reason="batch_too_large").inc()
        raise HTTPException(
            status_code=413,
            detail=f"batch exceeds {settings.max_batch_events} events",
        )
    await charge_rate_limit(identity, len(events), response)
    accepted = 0
    errors: list[dict] = []
    for index, payload in enumerate(events):
        try:
            event = TelemetryIn.model_validate(payload)
            envelope = TelemetryEnvelope(
                schema_id=schema_registry.current_schema_id(),
                event=event,
                received_at=datetime.now(UTC),
            ).model_dump(mode="json")
            await publish_or_reject(envelope, event.device_id)
            accepted += 1
            events_ingested_total.inc()
        except ValidationError as exc:
            events_failed_total.inc()
            errors.append({"index": index, "errors": exc.errors()})
    return BatchResponse(accepted=accepted, rejected=len(errors), errors=errors)


async def publish_or_reject(envelope: dict, device_id: str) -> None:
    try:
        await kafka.publish(envelope, key=device_id)
    except (KafkaError, TimeoutError, RuntimeError) as exc:
        admission_rejected_total.labels(reason="kafka_unavailable").inc()
        logger.warning("ingestion backpressure: Kafka unavailable", exc_info=exc)
        raise HTTPException(
            status_code=503,
            detail="telemetry queue unavailable",
            headers={"Retry-After": "1"},
        ) from exc


@app.get("/devices")
async def devices(region: str | None = None) -> list[dict]:
    query = "SELECT device_id, region, last_seen, status FROM device_status"
    args = []
    if region:
        query += " WHERE region = $1"
        args.append(region)
    query += " ORDER BY device_id"
    rows = await db.get_pool().fetch(query, *args)
    return [dict(row) for row in rows]


@app.get("/devices/{device_id}/telemetry", response_model=list[TelemetryRow])
async def device_telemetry(
    device_id: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    status: str | None = None,
    limit: int = Query(default=500, ge=1, le=5000),
) -> list[dict]:
    conditions = ["device_id = $1"]
    args: list = [device_id]
    if start_time:
        args.append(start_time)
        conditions.append(f"timestamp >= ${len(args)}")
    if end_time:
        args.append(end_time)
        conditions.append(f"timestamp <= ${len(args)}")
    if status:
        args.append(status)
        conditions.append(f"status = ${len(args)}")
    args.append(limit)
    rows = await db.get_pool().fetch(
        f"""
        SELECT * FROM telemetry_events
        WHERE {' AND '.join(conditions)}
        ORDER BY timestamp DESC
        LIMIT ${len(args)}
        """,
        *args,
    )
    return [dict(row) for row in rows]


@app.get("/devices/{device_id}/latest", response_model=TelemetryRow)
async def latest(device_id: str) -> dict:
    row = await db.get_pool().fetchrow(
        """
        SELECT * FROM telemetry_events
        WHERE device_id = $1
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        device_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="device telemetry not found")
    return dict(row)


@app.get("/devices/{device_id}/recent")
async def recent(device_id: str) -> dict:
    values = await cache.get_client().lrange(f"telemetry:{device_id}:recent", 0, 49)
    return {"device_id": device_id, "events": [json.loads(value) for value in values]}


@app.get("/pipeline/stats")
async def pipeline_stats(device_prefix: str | None = None) -> dict:
    if device_prefix:
        event_count = await db.get_pool().fetchval(
            "SELECT count(*) FROM telemetry_events WHERE device_id LIKE $1",
            f"{device_prefix}%",
        )
    else:
        event_count = await db.get_pool().fetchval("SELECT count(*) FROM telemetry_events")
    dlq_count = await db.get_pool().fetchval("SELECT count(*) FROM pipeline_errors")
    outbox_pending_count = await db.get_pool().fetchval(
        "SELECT count(*) FROM event_outbox WHERE published_at IS NULL"
    )
    outbox_published_count = await db.get_pool().fetchval(
        "SELECT count(*) FROM event_outbox WHERE published_at IS NOT NULL"
    )
    return {
        "event_count": event_count,
        "dlq_count": dlq_count,
        "outbox_pending_count": outbox_pending_count,
        "outbox_published_count": outbox_published_count,
    }


@app.get("/dashboard/overview")
async def dashboard_overview() -> dict:
    pool = db.get_pool()
    status_rows = await pool.fetch(
        """
        SELECT status, count(*) AS count
        FROM device_status GROUP BY status ORDER BY status
        """
    )
    region_rows = await pool.fetch(
        """
        SELECT region,
          count(*) AS devices,
          count(*) FILTER (WHERE status != 'OK') AS unhealthy,
          avg(temperature) AS avg_temperature,
          avg(voltage) AS avg_voltage
        FROM device_status GROUP BY region ORDER BY region
        """
    )
    trend_rows = await pool.fetch(
        """
        WITH recent_events AS (
          SELECT processed_at, status, event_lag_ms, temperature, voltage
          FROM telemetry_events
          ORDER BY processed_at DESC
          LIMIT 75000
        ), recent_buckets AS (
          SELECT date_trunc('minute', events.processed_at) AS bucket,
            count(*) AS events,
            count(*) FILTER (WHERE status != 'OK') AS failures,
            avg(event_lag_ms) AS avg_lag_ms,
            avg(temperature) AS avg_temperature,
            avg(voltage) AS avg_voltage
          FROM recent_events AS events
          GROUP BY 1 ORDER BY 1 DESC LIMIT 12
        )
        SELECT * FROM recent_buckets ORDER BY bucket
        """
    )
    latest_rows = await pool.fetch(
        """
        SELECT device_id, region, last_seen, status, temperature, voltage
        FROM device_status ORDER BY last_seen DESC LIMIT 12
        """
    )
    reliability = await pool.fetchrow(
        """
        SELECT
          (SELECT count(*) FROM telemetry_events) AS event_count,
          (SELECT count(*) FROM pipeline_errors) AS dlq_count,
          (SELECT COALESCE(sum(replay_count), 0) FROM pipeline_errors) AS replay_count,
          (SELECT count(*) FROM event_outbox WHERE published_at IS NULL) AS outbox_pending,
          (SELECT COALESCE(EXTRACT(EPOCH FROM now() - MIN(created_at)), 0)
             FROM event_outbox WHERE published_at IS NULL) AS oldest_outbox_seconds,
          (SELECT count(*) FROM device_status WHERE voltage < 10) AS low_voltage_devices,
          (SELECT count(*) FROM device_status
             WHERE temperature < -40 OR temperature > 140) AS temperature_outliers
        """
    )
    return {
        "device_status": {row["status"]: row["count"] for row in status_rows},
        "regions": [dict(row) for row in region_rows],
        "trend": [dict(row) for row in trend_rows],
        "latest_devices": [dict(row) for row in latest_rows],
        "reliability": dict(reliability),
    }


@app.get("/regions/{region}/summary")
async def region_summary(region: str) -> dict:
    row = await db.get_pool().fetchrow(
        """
        SELECT
          region,
          COUNT(*) AS event_count,
          COUNT(DISTINCT device_id) AS device_count,
          AVG(temperature) AS avg_temperature,
          AVG(event_lag_ms) AS avg_event_lag_ms,
          AVG(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failure_rate
        FROM telemetry_events
        WHERE region = $1 AND timestamp > now() - interval '1 hour'
        GROUP BY region
        """,
        region,
    )
    return dict(row) if row else {"region": region, "event_count": 0}


@app.get("/alerts")
async def alerts(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict]:
    rows = await db.get_pool().fetch(
        """
        SELECT * FROM telemetry_events
        WHERE status != 'OK' OR outlier_detected OR voltage_drop_detected
        ORDER BY timestamp DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(row) for row in rows]
