import json
from contextlib import asynccontextmanager
from pathlib import Path

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.schemas import IngestResponse, MetricEvent, MetricSummary
from app.storage import open_postgres, open_redis

ROOT = Path(__file__).parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = open_redis()
    app.state.postgres = await open_postgres()
    app.state.producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    await app.state.producer.start()
    yield
    await app.state.producer.stop()
    await app.state.redis.aclose()
    await app.state.postgres.close()


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")
templates = Jinja2Templates(directory=ROOT / "templates")


def serialize_row(row):
    value = dict(row)
    if isinstance(value.get("tags"), str):
        value["tags"] = json.loads(value["tags"])
    return value


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html", {"app_name": settings.app_name})


@app.get("/api", response_class=HTMLResponse, include_in_schema=False)
async def api_console(request: Request):
    return templates.TemplateResponse(request, "api.html", {"app_name": settings.app_name})


@app.get("/health")
async def health(request: Request):
    redis_ok = bool(await request.app.state.redis.ping())
    postgres_ok = await request.app.state.postgres.fetchval("SELECT true")
    data = {"status": "ready", "services": {"api": True, "redis": redis_ok, "postgres": postgres_ok, "kafka": True}}
    if "text/html" in request.headers.get("accept", ""):
        return templates.TemplateResponse(request, "health.html", {"app_name": settings.app_name, **data})
    return data


@app.post("/v1/metrics", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest(event: MetricEvent, request: Request):
    try:
        await request.app.state.producer.send_and_wait(settings.kafka_topic, event.model_dump(mode="json"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="metric stream is unavailable") from exc
    return IngestResponse(event_id=event.id, stream=settings.kafka_topic)


@app.get("/v1/metrics", response_model=list[MetricSummary])
async def summaries(request: Request, limit: int = Query(20, ge=1, le=100)):
    rows = await request.app.state.postgres.fetch(
        """SELECT name, count(*)::int AS count, avg(value)::float AS average,
        min(value)::float AS minimum, max(value)::float AS maximum,
        (array_agg(value ORDER BY occurred_at DESC))[1]::float AS last_value,
        max(occurred_at) AS updated_at FROM metric_events GROUP BY name
        ORDER BY updated_at DESC LIMIT $1""", limit,
    )
    return [MetricSummary(**dict(row)) for row in rows]


@app.get("/v1/dashboard")
async def dashboard_data(
    request: Request,
    hours: int = Query(1, ge=1, le=168),
):
    """Return the operational overview in one request for dashboard polling."""
    pool = request.app.state.postgres
    totals = await pool.fetchrow(
        """SELECT count(*)::int AS events,
        count(DISTINCT name)::int AS metrics,
        count(DISTINCT source)::int AS sources,
        max(occurred_at) AS last_event
        FROM metric_events WHERE occurred_at >= now() - ($1::int * interval '1 hour')""",
        hours,
    )
    series = await pool.fetch(
        """SELECT date_trunc('minute', occurred_at) AS time,
        count(*)::int AS events, avg(value)::float AS average
        FROM metric_events WHERE occurred_at >= now() - ($1::int * interval '1 hour')
        GROUP BY 1 ORDER BY 1""",
        hours,
    )
    recent = await pool.fetch(
        """SELECT id, name, value, occurred_at, source, tags
        FROM metric_events ORDER BY occurred_at DESC LIMIT 12"""
    )
    return {
        "range_hours": hours,
        "totals": dict(totals),
        "series": [dict(row) for row in series],
        "recent": [serialize_row(row) for row in recent],
    }


@app.get("/v1/analytics")
async def analytics_data(request: Request, hours: int = Query(24, ge=1, le=168)):
    """Return bounded event detail for the exploratory operations charts."""
    rows = await request.app.state.postgres.fetch(
        """SELECT name, value, occurred_at, source, tags FROM metric_events
        WHERE occurred_at >= now() - ($1::int * interval '1 hour')
        ORDER BY occurred_at DESC LIMIT 10000""",
        hours,
    )
    return {"range_hours": hours, "events": [serialize_row(row) for row in rows]}


@app.get("/v1/metrics/{name}/recent")
async def recent(name: str, request: Request):
    values = await request.app.state.redis.lrange(f"metric:{name}:recent", 0, 49)
    return {"name": name, "events": [json.loads(value) for value in values]}
