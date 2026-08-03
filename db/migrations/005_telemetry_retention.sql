CREATE INDEX IF NOT EXISTS idx_telemetry_retention
  ON telemetry_events (timestamp, event_id);

CREATE TABLE IF NOT EXISTS telemetry_hourly_rollups (
  bucket_start TIMESTAMPTZ NOT NULL,
  region TEXT NOT NULL,
  status TEXT NOT NULL,
  device_id TEXT NOT NULL,
  event_count BIGINT NOT NULL,
  avg_temperature DOUBLE PRECISION NOT NULL,
  min_temperature DOUBLE PRECISION NOT NULL,
  max_temperature DOUBLE PRECISION NOT NULL,
  avg_voltage DOUBLE PRECISION NOT NULL,
  min_voltage DOUBLE PRECISION NOT NULL,
  max_voltage DOUBLE PRECISION NOT NULL,
  avg_event_lag_ms DOUBLE PRECISION NOT NULL,
  first_event_at TIMESTAMPTZ NOT NULL,
  last_event_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (bucket_start, region, status, device_id)
);
