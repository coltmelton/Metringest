import hmac
import logging
from dataclasses import dataclass
from hashlib import sha256

from fastapi import Header, HTTPException, Response
from redis.exceptions import RedisError
from starlette.responses import JSONResponse

from app import cache
from app.config import settings
from app.metrics import admission_rejected_total

logger = logging.getLogger(__name__)

RATE_LIMIT_SCRIPT = """
local capacity = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2]) * 1000
local cost = tonumber(ARGV[3])
local redis_time = redis.call('TIME')
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_ms')
local tokens = tonumber(state[1]) or capacity
local updated_ms = tonumber(state[2]) or now_ms
local refill_per_ms = capacity / window_ms
tokens = math.min(capacity, tokens + math.max(now_ms - updated_ms, 0) * refill_per_ms)
local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = math.ceil((cost - tokens) / refill_per_ms / 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_ms', now_ms)
redis.call('PEXPIRE', KEYS[1], window_ms * 2)
local reset_after = math.ceil((capacity - tokens) / refill_per_ms / 1000)
return {allowed, math.floor(tokens), reset_after, retry_after}
"""


@dataclass(frozen=True)
class ClientIdentity:
    fingerprint: str


def hash_api_key(api_key: str) -> str:
    return sha256(api_key.encode("utf-8")).hexdigest()


async def authenticate_client(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> ClientIdentity:
    if not x_api_key:
        admission_rejected_total.labels(reason="missing_api_key").inc()
        raise HTTPException(
            status_code=401,
            detail="missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    candidate = hash_api_key(x_api_key)
    if not any(
        hmac.compare_digest(candidate, allowed) for allowed in settings.allowed_api_key_hashes
    ):
        admission_rejected_total.labels(reason="invalid_api_key").inc()
        raise HTTPException(
            status_code=401,
            detail="invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return ClientIdentity(fingerprint=candidate[:16])


async def charge_rate_limit(
    identity: ClientIdentity,
    cost: int,
    response: Response,
) -> None:
    key = f"admission:{identity.fingerprint}"
    try:
        allowed, remaining, reset_after, retry_after = await cache.get_client().eval(
            RATE_LIMIT_SCRIPT,
            1,
            key,
            settings.rate_limit_events,
            settings.rate_limit_window_seconds,
            cost,
        )
    except (RedisError, RuntimeError) as exc:
        admission_rejected_total.labels(reason="rate_limiter_unavailable").inc()
        logger.warning("rate limiter unavailable", exc_info=exc)
        raise HTTPException(
            status_code=503,
            detail="admission control unavailable",
            headers={"Retry-After": "1"},
        ) from exc

    response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_events)
    response.headers["X-RateLimit-Remaining"] = str(max(int(remaining), 0))
    response.headers["X-RateLimit-Reset"] = str(max(int(reset_after), 0))
    if not allowed:
        admission_rejected_total.labels(reason="rate_limit").inc()
        raise HTTPException(
            status_code=429,
            detail="telemetry event rate limit exceeded",
            headers={
                "Retry-After": str(max(int(retry_after), 1)),
                "X-RateLimit-Limit": str(settings.rate_limit_events),
                "X-RateLimit-Remaining": "0",
            },
        )


class RequestTooLarge(Exception):
    pass


class RequestSizeLimitMiddleware:
    def __init__(self, app, max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return

        content_length = next(
            (value for name, value in scope["headers"] if name == b"content-length"),
            None,
        )
        if content_length is not None:
            try:
                if int(content_length) > self.max_bytes:
                    await self._reject(scope, receive, send)
                    return
            except ValueError:
                response = JSONResponse({"detail": "invalid Content-Length"}, status_code=400)
                await response(scope, receive, send)
                return

        received = 0

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise RequestTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except RequestTooLarge:
            await self._reject(scope, receive, send)

    async def _reject(self, scope, receive, send):
        admission_rejected_total.labels(reason="request_too_large").inc()
        response = JSONResponse(
            {"detail": f"request body exceeds {self.max_bytes} bytes"},
            status_code=413,
        )
        await response(scope, receive, send)
