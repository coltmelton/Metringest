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
