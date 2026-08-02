# Schema governance and DLQ recovery

The raw Kafka envelope is a versioned contract. Its checked definitions live in `schemas/`, and
CI registers them as JSON Schema contracts under `raw-telemetry-value` with
`BACKWARD_TRANSITIVE` compatibility. CI also submits an intentionally incompatible change and
must observe its rejection. This proves the policy is enforced by a real Schema Registry rather
than by a mocked unit test.

## Evolution policy

- A missing `schema_version` is the legacy v0 contract. The worker upcasts it to v1 in memory.
- New API messages carry `schema_version: 1` and the registry ID returned when the API registered
  the checked contract.
- The v1 schema keeps the two metadata fields optional so existing v0 producers remain valid.
- A worker that receives an unknown version records the original Kafka coordinates and payload in
  PostgreSQL, publishes it to `dead-letter-telemetry`, and continues processing the partition.
- Contract changes must add a checked schema and pass `python scripts/schema_contracts.py` before
  producer or consumer code is deployed.

The API registers contracts at startup and caches the current schema ID. If the registry later
becomes unavailable, already-running API instances can continue producing the known contract,
while `/health` reports `degraded`. A fresh API instance waits for the registry instead of
guessing an ID. Schema Registry is the control plane; Kafka remains the data plane.

## Controlled replay

List recorded failures:

```sh
docker compose exec worker python /tools/dlq_replay.py list --limit 20
```

Inspect a replay without publishing it (the default is always dry-run):

```sh
docker compose exec worker python /tools/dlq_replay.py replay --error-id 42
```

For an unsupported schema, provide a corrected v1 envelope and execute deliberately:

```sh
docker compose exec worker python /tools/dlq_replay.py replay \
  --error-id 42 \
  --payload-json '{"schema_version":1,"schema_id":2,"event":{"event_id":"...","device_id":"...","timestamp":"2026-07-31T19:00:00Z","temperature":72.4,"voltage":12.1,"status":"OK","region":"us-east"},"received_at":"2026-07-31T19:00:00Z"}' \
  --execute
```

Replay preserves the original payload bytes unless an operator supplies a correction. It uses the
stable PostgreSQL error ID in a Kafka header and increments `replay_count` plus `replayed_at` only
after Kafka acknowledges publication. A broker failure therefore does not create a false audit
record. Replays can be repeated safely from the same error record; event IDs provide downstream
deduplication, so operators should retain the original event ID when correcting an envelope.

## Verification commands

```sh
python scripts/schema_contracts.py
pytest tests/test_schema_governance.py -v
pytest tests/integration/test_reliability.py \
  -k 'registry_outage or mixed_schema_versions' -v -s
```

The integration scenarios stop the real registry, verify cached production and degraded health,
restart it, mix v0/v1/unknown messages on Kafka, prove the unknown message cannot block a later
valid record, dry-run its correction, execute it, and verify the PostgreSQL replay audit.
