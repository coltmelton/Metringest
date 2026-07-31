from datetime import UTC, datetime

import pytest
from app import admission, kafka
from app.admission import ClientIdentity
from app.config import DEVELOPMENT_API_KEY_HASH, Settings
from app.main import app
from fastapi import HTTPException, Response
from fastapi.testclient import TestClient


def event() -> dict:
    return {
        "event_id": "admission-event",
        "device_id": "admission-device",
        "timestamp": datetime.now(UTC).isoformat(),
        "temperature": 72.4,
        "voltage": 12.1,
        "status": "OK",
        "region": "us-east",
    }


def test_missing_and_invalid_api_keys_are_rejected():
    client = TestClient(app)

    missing = client.post("/telemetry", json=event())
    invalid = client.post("/telemetry", json=event(), headers={"X-API-Key": "wrong"})

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert missing.headers["www-authenticate"] == "ApiKey"


def test_request_body_limit_rejects_before_parsing():
    client = TestClient(app)
    response = client.post(
        "/telemetry",
        content=b"{}",
        headers={
            "X-API-Key": "development-key",
            "Content-Length": str(admission.settings.max_request_bytes + 1),
        },
    )

    assert response.status_code == 413


@pytest.mark.asyncio
async def test_rate_limit_charges_events_and_returns_retry_metadata(monkeypatch):
    class Redis:
        async def eval(self, *_args):
            return [0, 0, 17, 17]

    monkeypatch.setattr(admission.cache, "get_client", lambda: Redis())

    with pytest.raises(HTTPException) as error:
        await admission.charge_rate_limit(ClientIdentity("client"), 25, Response())

    assert error.value.status_code == 429
    assert error.value.headers["Retry-After"] == "17"


def test_valid_key_is_accepted_and_exposes_quota(monkeypatch):
    class Redis:
        async def eval(self, *_args):
            return [1, admission.settings.rate_limit_events - 1, 1, 0]

    async def publish(*_args, **_kwargs):
        return None

    monkeypatch.setattr(admission.cache, "get_client", lambda: Redis())
    monkeypatch.setattr(kafka, "publish", publish)
    client = TestClient(app)

    response = client.post(
        "/telemetry",
        json=event(),
        headers={"X-API-Key": "development-key"},
    )

    assert response.status_code == 202
    assert response.headers["x-ratelimit-limit"] == str(admission.settings.rate_limit_events)
    assert response.headers["x-ratelimit-remaining"] == str(
        admission.settings.rate_limit_events - 1
    )


def test_kafka_timeout_becomes_retryable_503(monkeypatch):
    class Redis:
        async def eval(self, *_args):
            return [1, admission.settings.rate_limit_events - 1, 1, 0]

    async def unavailable(*_args, **_kwargs):
        raise TimeoutError

    monkeypatch.setattr(admission.cache, "get_client", lambda: Redis())
    monkeypatch.setattr(kafka, "publish", unavailable)
    client = TestClient(app)

    response = client.post(
        "/telemetry",
        json=event(),
        headers={"X-API-Key": "development-key"},
    )

    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"


def test_batch_event_ceiling_is_enforced():
    client = TestClient(app)
    response = client.post(
        "/telemetry/batch",
        json=[event()] * (admission.settings.max_batch_events + 1),
        headers={"X-API-Key": "development-key"},
    )

    assert response.status_code == 413


def test_production_cannot_start_with_known_development_key():
    with pytest.raises(ValueError, match="development API key"):
        Settings(
            app_environment="production",
            api_key_hashes=DEVELOPMENT_API_KEY_HASH,
            _env_file=None,
        )
