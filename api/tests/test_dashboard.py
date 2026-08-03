from datetime import UTC, datetime

import pytest
from app import main


@pytest.mark.asyncio
async def test_dashboard_overview_aggregates_operational_data(monkeypatch):
    now = datetime.now(UTC)

    class Pool:
        def __init__(self):
            self.fetch_calls = 0

        async def fetch(self, query):
            self.fetch_calls += 1
            if "GROUP BY status" in query:
                return [{"status": "OK", "count": 8}, {"status": "WARNING", "count": 2}]
            if "GROUP BY region" in query:
                return [{"region": "us-east", "devices": 10, "unhealthy": 2,
                         "avg_temperature": 71.5, "avg_voltage": 12.1}]
            if "recent_buckets" in query:
                return [{"bucket": now, "events": 50, "failures": 2,
                         "avg_lag_ms": 14, "avg_temperature": 71.5, "avg_voltage": 12.1}]
            return [{"device_id": "sensor-1", "region": "us-east", "last_seen": now,
                     "status": "OK", "temperature": 71.5, "voltage": 12.1}]

        async def fetchrow(self, query):
            assert "pipeline_errors" in query
            return {"event_count": 500, "dlq_count": 1, "replay_count": 1,
                    "outbox_pending": 0, "oldest_outbox_seconds": 0,
                    "low_voltage_devices": 2, "temperature_outliers": 0}

    pool = Pool()
    monkeypatch.setattr(main.db, "get_pool", lambda: pool)

    result = await main.dashboard_overview()

    assert result["device_status"] == {"OK": 8, "WARNING": 2}
    assert result["regions"][0]["unhealthy"] == 2
    assert result["trend"][0]["events"] == 50
    assert result["latest_devices"][0]["device_id"] == "sensor-1"
    assert result["reliability"]["event_count"] == 500
    assert pool.fetch_calls == 4
