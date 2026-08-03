CREATE INDEX IF NOT EXISTS idx_telemetry_processed_time
  ON telemetry_events (processed_at DESC);
