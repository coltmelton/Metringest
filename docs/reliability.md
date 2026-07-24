# Reliability model

This document describes behavior implemented and tested by this repository. PostgreSQL is the
durable source of record. Redis is a rebuildable recent-value cache. Kafka provides at-least-once
delivery between API acceptance and worker offset commit.

## Processing boundary

For each valid Kafka record the worker performs these steps in order:

1. Decode JSON and validate the telemetry envelope.
2. Commit the event and device-status update in one PostgreSQL transaction.
3. Write the stored row to Redis's latest value and bounded recent list.
4. Publish the derived event to `validated-telemetry` for newly inserted rows.
5. Commit the consumed Kafka offsets after the complete fetched batch succeeds.

PostgreSQL and Redis do not share a transaction. A Redis failure can therefore occur after the
PostgreSQL transaction commits. The worker deliberately leaves the Kafka offset uncommitted in
that case. On replay, PostgreSQL's event-ID constraint identifies the duplicate, the worker loads
the durable row, and the idempotent Redis update repairs the cache before the offset is committed.

The Redis recent-list update removes the identical serialized row before pushing it. Replaying an
event after a crash therefore does not create another copy in the bounded list.

| Failure point | Durable row | Redis cache | Kafka offset | Recovery |
| --- | --- | --- | --- | --- |
| Before PostgreSQL commit | No | No | Uncommitted | Entire record retries |
| After PostgreSQL, before Redis | Yes | Missing/stale | Uncommitted | Duplicate insert is ignored; cache is repaired |
| After Redis, before offset commit | Yes | Current | Uncommitted | Idempotent replay, then commit |
| After offset commit | Yes | Current | Committed | Processing complete |

This is eventual consistency with PostgreSQL authority, not atomic dual storage. Redis can be
temporarily stale while unavailable. Reads requiring durable truth must use PostgreSQL.

## Poison messages and DLQ

JSON decoding and Pydantic validation errors are permanent record-level failures. The worker:

1. Stores the original payload, reason, source topic, partition, and offset in `pipeline_errors`.
2. Publishes the failure to `dead-letter-telemetry`, keyed by source coordinates.
3. Marks the database error record as published.
4. Continues through the batch and commits only after all records are processed or isolated.

The unique `(source_topic, source_partition, source_offset)` constraint makes the PostgreSQL DLQ
record idempotent. DLQ publication remains at least once around a crash between Kafka publication
and the database marker update; DLQ consumers should deduplicate by the supplied source key.

Infrastructure exceptions are not poison messages. PostgreSQL, Redis, or Kafka failures abort the
current batch, seek each fetched partition back to its first batch offset, and leave the committed
offset unchanged for replay.

## Batches, partitions, and scaling

The worker fetches up to `WORKER_BATCH_SIZE` records with a maximum wait of
`WORKER_BATCH_WAIT_MS`. A mid-batch infrastructure failure stops later records and leaves all
fetched offsets uncommitted. Earlier durable inserts are safe to replay because event IDs are
unique and cache writes are idempotent.

The Compose initializer creates raw, validated, and DLQ topics with three partitions by default.
The API keys raw events by device ID, preserving per-device partition order while distributing
different devices. The worker has no fixed host port, so it can be scaled:

```sh
docker compose up --detach --scale worker=2 worker
```

Kafka assigns partitions among the consumers in the shared `telemetry-worker` group. Scaling
beyond the raw topic's partition count produces idle consumers rather than more parallelism.

## Verification

```sh
python -m pip install -e ".[dev]"
ruff check api worker tests scripts
pytest -m "not integration" -v
docker compose up --build --detach kafka-init postgres redis api worker
pytest -m integration -v -s
python scripts/benchmark_matrix.py --count 1000 --concurrency-levels 1,10,25,50 --runs 3
```

The integration suite exercises end-to-end delivery, poison isolation, worker restart recovery,
PostgreSQL outage recovery, Redis outage/cache repair, topic partition count, and two-worker
consumer-group membership.

## Benchmark interpretation

The matrix runs every concurrency level repeatedly and reports each run plus median/min/max
persisted events per second. It fails if any request fails or any accepted event is not found in
PostgreSQL. Results describe the tested machine and Compose configuration; they are not a
production capacity guarantee.
