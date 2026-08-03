# Telemetry retention and hourly rollups

PostgreSQL remains the durable source of truth for the raw-retention window. Older delivered
events can be compacted into per-device, per-region, per-status hourly rows so capacity trends,
signal ranges, averages, and lag history remain queryable without retaining every raw sample.

Retention is deliberately operator-controlled and dry-run by default:

```sh
python scripts/telemetry_retention.py --retention-days 30
python scripts/telemetry_retention.py --retention-days 30 --batch-size 10000 --execute
```

Each execution processes at most one bounded batch. Schedule repeated executions until
`eligible_events` reaches zero. Small transactions limit WAL spikes, long-held row locks, and
replication pressure.

## Safety boundary

An event is eligible only when it is older than the cutoff and its transactional-outbox row has a
non-null `published_at`. Pending outbox rows are counted as `protected_pending_events` and are
never selected. Inside one database transaction, the maintenance command:

1. takes a transaction-scoped advisory lock so only one retention process runs;
2. locks a bounded set of eligible event IDs with `SKIP LOCKED`;
3. adds those events to hourly rollups, including weighted averages for late-arriving batches;
4. deletes exactly the selected raw rows; and
5. commits the rollup and deletion together.

Deleting a raw event cascades its already-published outbox row. This discards delivery bookkeeping
only after downstream Kafka acknowledgement. A count mismatch raises and rolls back the entire
transaction. Redis is unaffected: it remains a bounded, rebuildable recent-value cache rather
than a historical store.

The current implementation retains hourly rollups indefinitely. Production deployments should
back them up and define a separate, substantially longer rollup-retention policy based on legal,
analytics, and storage requirements.

## Historical reads

`GET /telemetry/history` provides one bounded read contract across both storage tiers. Raw
resolution is limited to 24 hours and 5,000 points. Hour resolution is limited to 366 days and
combines retained rollups with raw events that have not yet been compacted. It reports `storage`
as `raw`, `rollup`, or `raw+rollup`, plus requested and effective time bounds. Hour bounds are
expanded to complete UTC hours because partial detail no longer exists after raw retention.

Supported filters are `device_id`, `region`, and `status`. `resolution=auto` selects raw data for
ranges up to 24 hours and hourly data for longer ranges. The operations dashboard explicitly asks
for hourly history and offers bounded 1-day, 7-day, and 30-day signal windows.
