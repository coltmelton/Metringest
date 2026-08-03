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

CREATE INDEX IF NOT EXISTS idx_telemetry_processed_time
  ON telemetry_events (processed_at DESC);

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
  source_topic TEXT NOT NULL,
  source_partition INTEGER NOT NULL,
  source_offset BIGINT NOT NULL,
  payload JSONB NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  replay_count INTEGER NOT NULL DEFAULT 0,
  replayed_at TIMESTAMPTZ,
  UNIQUE(source_topic, source_partition, source_offset)
);

ALTER TABLE pipeline_errors ADD COLUMN IF NOT EXISTS source_topic TEXT;
ALTER TABLE pipeline_errors ADD COLUMN IF NOT EXISTS source_partition INTEGER;
ALTER TABLE pipeline_errors ADD COLUMN IF NOT EXISTS source_offset BIGINT;
ALTER TABLE pipeline_errors ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;
ALTER TABLE pipeline_errors ADD COLUMN IF NOT EXISTS replay_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pipeline_errors ADD COLUMN IF NOT EXISTS replayed_at TIMESTAMPTZ;
CREATE UNIQUE INDEX IF NOT EXISTS pipeline_errors_source_idx
  ON pipeline_errors(source_topic, source_partition, source_offset)
  WHERE source_topic IS NOT NULL;

CREATE TABLE IF NOT EXISTS event_outbox (
  id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE REFERENCES telemetry_events(event_id) ON DELETE CASCADE,
  topic TEXT NOT NULL,
  message_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS event_outbox_pending_idx
  ON event_outbox (id)
  WHERE published_at IS NULL;
