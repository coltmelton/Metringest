CREATE TABLE IF NOT EXISTS telemetry_events (
  event_id TEXT PRIMARY KEY,
  device_id TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL,
  temperature DOUBLE PRECISION NOT NULL,
  voltage DOUBLE PRECISION NOT NULL,
  status TEXT NOT NULL,
  region TEXT NOT NULL,
  avg_temperature_5m DOUBLE PRECISION,
  voltage_drop_detected BOOLEAN NOT NULL DEFAULT FALSE,
  event_lag_ms BIGINT NOT NULL,
  outlier_detected BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
  ON telemetry_events (device_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_region_time
  ON telemetry_events (region, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_alerts
  ON telemetry_events (timestamp DESC)
  WHERE status != 'OK' OR outlier_detected OR voltage_drop_detected;

CREATE TABLE IF NOT EXISTS device_status (
  device_id TEXT PRIMARY KEY,
  region TEXT NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  temperature DOUBLE PRECISION NOT NULL,
  voltage DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_errors (
  id BIGSERIAL PRIMARY KEY,
  payload JSONB NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
