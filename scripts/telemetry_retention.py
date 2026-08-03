#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
from datetime import UTC, datetime, timedelta

import asyncpg

ROLLUP_SQL = """
INSERT INTO telemetry_hourly_rollups (
  bucket_start, region, status, device_id, event_count,
  avg_temperature, min_temperature, max_temperature,
  avg_voltage, min_voltage, max_voltage, avg_event_lag_ms,
  first_event_at, last_event_at
)
SELECT date_trunc('hour', event.timestamp), event.region, event.status, event.device_id,
  count(*), avg(event.temperature), min(event.temperature), max(event.temperature),
  avg(event.voltage), min(event.voltage), max(event.voltage), avg(event.event_lag_ms),
  min(event.timestamp), max(event.timestamp)
FROM telemetry_events AS event
JOIN retention_batch AS batch USING (event_id)
GROUP BY 1, 2, 3, 4
ON CONFLICT (bucket_start, region, status, device_id) DO UPDATE SET
  avg_temperature = (
    telemetry_hourly_rollups.avg_temperature * telemetry_hourly_rollups.event_count
    + EXCLUDED.avg_temperature * EXCLUDED.event_count
  ) / (telemetry_hourly_rollups.event_count + EXCLUDED.event_count),
  min_temperature = LEAST(telemetry_hourly_rollups.min_temperature, EXCLUDED.min_temperature),
  max_temperature = GREATEST(telemetry_hourly_rollups.max_temperature, EXCLUDED.max_temperature),
  avg_voltage = (
    telemetry_hourly_rollups.avg_voltage * telemetry_hourly_rollups.event_count
    + EXCLUDED.avg_voltage * EXCLUDED.event_count
  ) / (telemetry_hourly_rollups.event_count + EXCLUDED.event_count),
  min_voltage = LEAST(telemetry_hourly_rollups.min_voltage, EXCLUDED.min_voltage),
  max_voltage = GREATEST(telemetry_hourly_rollups.max_voltage, EXCLUDED.max_voltage),
  avg_event_lag_ms = (
    telemetry_hourly_rollups.avg_event_lag_ms * telemetry_hourly_rollups.event_count
    + EXCLUDED.avg_event_lag_ms * EXCLUDED.event_count
  ) / (telemetry_hourly_rollups.event_count + EXCLUDED.event_count),
  first_event_at = LEAST(telemetry_hourly_rollups.first_event_at, EXCLUDED.first_event_at),
  last_event_at = GREATEST(telemetry_hourly_rollups.last_event_at, EXCLUDED.last_event_at),
  event_count = telemetry_hourly_rollups.event_count + EXCLUDED.event_count
"""


async def inspect_candidates(connection, cutoff: datetime) -> dict:
    row = await connection.fetchrow(
        """
        SELECT count(*) AS eligible_events, min(event.timestamp) AS oldest_event,
          max(event.timestamp) AS newest_event
        FROM telemetry_events AS event
        JOIN event_outbox AS outbox USING (event_id)
        WHERE event.timestamp < $1 AND outbox.published_at IS NOT NULL
        """,
        cutoff,
    )
    pending = await connection.fetchval(
        """
        SELECT count(*) FROM telemetry_events AS event
        JOIN event_outbox AS outbox USING (event_id)
        WHERE event.timestamp < $1 AND outbox.published_at IS NULL
        """,
        cutoff,
    )
    return {
        "eligible_events": row["eligible_events"],
        "protected_pending_events": pending,
        "oldest_event": row["oldest_event"],
        "newest_event": row["newest_event"],
    }


async def execute_batch(connection, cutoff: datetime, batch_size: int) -> dict:
    async with connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock(hashtext('metringest-retention'))")
        await connection.execute(
            """
            CREATE TEMP TABLE retention_batch ON COMMIT DROP AS
            SELECT event.event_id
            FROM telemetry_events AS event
            JOIN event_outbox AS outbox USING (event_id)
            WHERE event.timestamp < $1 AND outbox.published_at IS NOT NULL
            ORDER BY event.timestamp, event.event_id
            LIMIT $2
            FOR UPDATE OF event SKIP LOCKED
            """,
            cutoff,
            batch_size,
        )
        selected = await connection.fetchval("SELECT count(*) FROM retention_batch")
        if not selected:
            return {"selected_events": 0, "rollup_rows": 0, "deleted_events": 0}
        status = await connection.execute(ROLLUP_SQL)
        rollup_rows = int(status.rsplit(" ", 1)[-1])
        deleted = await connection.fetchval(
            """
            WITH removed AS (
              DELETE FROM telemetry_events AS event
              USING retention_batch AS batch
              WHERE event.event_id = batch.event_id
              RETURNING event.event_id
            ) SELECT count(*) FROM removed
            """
        )
        if deleted != selected:
            raise RuntimeError(f"selected {selected} events but deleted {deleted}")
        return {
            "selected_events": selected,
            "rollup_rows": rollup_rows,
            "deleted_events": deleted,
        }


def serialize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__}")


async def run(database_url: str, retention_days: int, batch_size: int, execute: bool) -> dict:
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    connection = await asyncpg.connect(database_url)
    try:
        inspection = await inspect_candidates(connection, cutoff)
        result = {
            "mode": "execute" if execute else "dry-run",
            "cutoff": cutoff,
            "retention_days": retention_days,
            "batch_size": batch_size,
            **inspection,
        }
        if execute:
            result.update(await execute_batch(connection, cutoff, batch_size))
        return result
    finally:
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Roll up delivered telemetry before bounded raw-event retention"
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry"
        ),
    )
    parser.add_argument("--retention-days", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.retention_days < 1 or args.batch_size < 1:
        parser.error("retention-days and batch-size must be positive")
    result = asyncio.run(
        run(args.database_url, args.retention_days, args.batch_size, args.execute)
    )
    print(json.dumps(result, indent=2, default=serialize))


if __name__ == "__main__":
    main()
