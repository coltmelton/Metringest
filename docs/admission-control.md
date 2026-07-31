# Ingestion admission control

The ingestion endpoints require an `X-API-Key`; health, metrics, and query endpoints remain
unauthenticated so local probes and the dashboard continue to work. Production deployments must
place those read endpoints behind an authenticated gateway or private network if telemetry is
sensitive. CORS configuration is browser policy, not authentication.

## API keys

The API receives only comma-separated SHA-256 hashes through `API_KEY_HASHES`. It hashes the
presented key and uses constant-time comparison. Multiple hashes allow a new key and old key to
overlap during rotation. Use a random high-entropy key because a hash does not protect a weak key
from offline guessing.

Generate a key and its hash without printing the key:

```sh
read -s METRINGEST_NEW_KEY
printf %s "$METRINGEST_NEW_KEY" | shasum -a 256
unset METRINGEST_NEW_KEY
```

`development-key` and `benchmark-key` exist only for the Compose demonstration. When
`APP_ENVIRONMENT` is anything except `development`, startup rejects either known key. A
production deployment must provide its own `API_KEY_HASHES`; clients receive only the raw key.

## Distributed event quota

Every accepted HTTP request first charges its number of submitted telemetry events against a
Redis token bucket keyed by the API-key fingerprint. The Lua script uses Redis server time and
atomically refills, charges, and expires the bucket, so concurrent API instances share one quota.
Invalid batch entries still consume quota, protecting validation capacity from abusive payloads.

Configuration:

| Variable | Development default | Meaning |
| --- | ---: | --- |
| `RATE_LIMIT_EVENTS` | 1000 | Bucket capacity |
| `RATE_LIMIT_WINDOW_SECONDS` | 60 | Time required to refill the full bucket |
| `MAX_REQUEST_BYTES` | 1048576 | Maximum POST body size |
| `MAX_BATCH_EVENTS` | 500 | Maximum events in one batch |
| `KAFKA_PUBLISH_TIMEOUT_SECONDS` | 5 | Maximum queue acknowledgement wait |

Quota rejection returns `429` with `Retry-After`, `X-RateLimit-Limit`, and
`X-RateLimit-Remaining`. Authentication returns `401`; body or batch ceilings return `413`.
Admission fails closed with `503` if Redis is unavailable, because accepting traffic without the
shared quota would let one API replica bypass protection.

## Kafka backpressure and retry semantics

The API waits for Kafka acknowledgement up to the configured timeout. Broker errors, timeout, or
an uninitialized producer return `503` with `Retry-After: 1`; the API does not falsely report the
event as accepted.

A batch can receive `503` after earlier entries were acknowledged. Clients must retain stable
event IDs and retry the complete batch. The worker's PostgreSQL event-ID constraint makes those
retries idempotent. This is an at-least-once API contract, not an atomic multi-event enqueue.

Prometheus exposes `admission_rejected_total{reason=...}` for missing/invalid keys, exhausted
quota, oversize requests/batches, unavailable rate limiting, and Kafka backpressure. Alert on
sustained rejection changes rather than individual expected client errors.
