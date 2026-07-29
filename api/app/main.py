import json
import logging
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from app import cache, db, kafka
from app.config import settings
from app.logging_config import configure_logging
from app.metrics import (
    MetricsMiddleware,
    events_failed_total,
    events_ingested_total,
    metrics_response,
)
from app.models import BatchResponse, TelemetryEnvelope, TelemetryIn, TelemetryRow

configure_logging(settings.service_name)
logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Telemetry Pipeline", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(MetricsMiddleware)


@app.on_event("startup")
async def startup() -> None:
    await db.connect()
    await cache.connect()
    await kafka.connect()
    logger.info("api started", extra={"service": settings.service_name})


@app.on_event("shutdown")
async def shutdown() -> None:
    await kafka.disconnect()
    await cache.disconnect()
    await db.disconnect()


@app.get("/health")
async def health() -> dict:
    postgres_ok = bool(await db.get_pool().fetchval("SELECT true"))
    redis_ok = bool(await cache.get_client().ping())
    kafka_ok = kafka.producer is not None and bool(kafka.producer.client.cluster.brokers())
    return {
        "status": "ready" if postgres_ok and redis_ok and kafka_ok else "degraded",
        "service": settings.service_name,
        "dependencies": {
            "postgres": postgres_ok,
            "redis": redis_ok,
            "kafka": kafka_ok,
        },
    }


@app.get("/metrics")
async def metrics():
    return metrics_response()


@app.post("/telemetry", status_code=202)
async def ingest(event: TelemetryIn) -> dict:
    envelope = TelemetryEnvelope(
        event=event, received_at=datetime.now(UTC)
    ).model_dump(mode="json")
    await kafka.publish(envelope, key=event.device_id)
    events_ingested_total.inc()
    return {"accepted": True, "event_id": event.event_id}


@app.post("/telemetry/batch", response_model=BatchResponse, status_code=202)
async def ingest_batch(events: list[dict]) -> BatchResponse:
    accepted = 0
    errors: list[dict] = []
    for index, payload in enumerate(events):
        try:
            event = TelemetryIn.model_validate(payload)
            envelope = TelemetryEnvelope(
                event=event, received_at=datetime.now(UTC)
            ).model_dump(mode="json")
            await kafka.publish(envelope, key=event.device_id)
            accepted += 1
            events_ingested_total.inc()
        except ValidationError as exc:
            events_failed_total.inc()
            errors.append({"index": index, "errors": exc.errors()})
    return BatchResponse(accepted=accepted, rejected=len(errors), errors=errors)


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
