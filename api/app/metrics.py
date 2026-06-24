from prometheus_client import Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from time import perf_counter

events_ingested_total = Counter("events_ingested_total", "Accepted telemetry events")
events_failed_total = Counter("events_failed_total", "Rejected telemetry events")
api_request_latency_ms = Histogram(
    "api_request_latency_ms",
    "API request latency in milliseconds",
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
)


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        started = perf_counter()
        response = await call_next(request)
        api_request_latency_ms.observe((perf_counter() - started) * 1000)
        return response


def metrics_response() -> Response:
    return Response(generate_latest(), media_type="text/plain; version=0.0.4")
