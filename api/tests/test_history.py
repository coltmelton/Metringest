from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException

from app import main


@pytest.mark.asyncio
async def test_hourly_history_combines_raw_and_rollup_storage(monkeypatch):
    now = datetime.now(UTC)

    class Pool:
        async def fetch(self, query, *_args):
            assert "telemetry_hourly_rollups" in query
            assert "UNION ALL" in query
            return [{
                "bucket": now.replace(minute=0, second=0, microsecond=0),
                "events": 12, "non_ok_events": 2, "avg_temperature": 72.0,
                "min_temperature": 60.0, "max_temperature": 80.0,
                "avg_voltage": 12.0, "min_voltage": 10.0, "max_voltage": 13.0,
                "avg_lag_ms": 25.0, "rollup_events": 10, "raw_events": 2,
            }]

    monkeypatch.setattr(main.db, "get_pool", lambda: Pool())
    result = await main.telemetry_history(
        start=now - timedelta(days=7), end=now, resolution="hour",
        device_id=None, region="us-east", status=None, limit=100,
    )

    assert result["resolution"] == "hour"
    assert result["storage"] == "raw+rollup"
    assert result["points"][0]["events"] == 12


@pytest.mark.asyncio
async def test_raw_history_is_bounded_to_24_hours():
    now = datetime.now(UTC)

    with pytest.raises(HTTPException, match="raw history is limited") as error:
        await main.telemetry_history(
            start=now - timedelta(days=2), end=now, resolution="raw",
            device_id=None, region=None, status=None, limit=100,
        )

    assert error.value.status_code == 422


@pytest.mark.asyncio
async def test_auto_resolution_uses_raw_for_short_ranges(monkeypatch):
    now = datetime.now(UTC)

    class Pool:
        async def fetch(self, query, *_args):
            assert "telemetry_hourly_rollups" not in query
            return [{"event_id": "event-1", "timestamp": now}]

    monkeypatch.setattr(main.db, "get_pool", lambda: Pool())
    result = await main.telemetry_history(
        start=now - timedelta(hours=1), end=now, resolution="auto",
        device_id=None, region=None, status=None, limit=100,
    )

    assert result["resolution"] == "raw"
    assert result["storage"] == "raw"
