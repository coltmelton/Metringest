# Reliability verification record

This record is intentionally rooted in the `coltmelton/Metringest` implementation and history.
It does not include TelemetryUI/Elixir commits, workflows, or claims from the superseded branch.

## Branch provenance

- Base: `metringest/main`
- Base commit: `a43ed66` (`Build telemetry pipeline MVP`)
- Reliability branch: `feat/reliability-v2`
- Reliability merge: `b30e2eb` (pull request #1)
- Transactional-outbox branch: `feat/transactional-outbox`
- CI target branch: `main`

## Implemented verification

| Area | Evidence |
| --- | --- |
| Poison isolation | Invalid JSON is recorded and published to the DLQ; the next valid record proceeds |
| Mid-batch failure | A transient error stops later records and propagates so offsets remain uncommitted |
| PostgreSQL outage | Accepted Kafka event remains pending and persists after PostgreSQL restarts |
| Redis outage | PostgreSQL row commits, offset remains pending, replay repairs the cache once Redis returns |
| Worker restart | Events accepted with the worker stopped are processed after restart |
| Partitions | Integration test verifies the raw topic has three partitions |
| Worker scaling | Integration test scales to two workers and observes two consumer-group members |
| Benchmarks | Repeated matrix covers multiple concurrency levels and verifies accepted equals persisted |
| CI | One repository workflow runs unit, integration, recovery, scaling, and benchmark smoke checks |

## Local verification — July 24, 2026

Environment:

```text
Python 3.12.13
Docker Compose project: metringest-v2
Kafka topics: 3 partitions each
Worker group: 2 members during scaling test, then returned to 1
```

Final combined validation:

```text
Ruff: All checks passed
Unit tests: 5 passed, 6 integration tests deselected, 0.26 seconds
Integration tests: 6 passed, 5 unit tests deselected, 30.87 seconds
```

The combined integration run passed end-to-end storage, poison/DLQ continuation, stopped-worker
recovery, PostgreSQL outage recovery, Redis outage/cache repair, and partition/worker scaling.

During development, the PostgreSQL outage test exposed that `getmany()` advances the consumer's
in-memory position even when offsets are not committed. The worker was corrected to seek every
fetched partition back to its first batch offset after transient failure. The test then passed
without restarting the worker.

## Repeated benchmark matrix

Command:

```sh
python scripts/benchmark_matrix.py \
  --url http://localhost:18000 \
  --count 500 \
  --concurrency-levels 1,10,25,50 \
  --runs 3 \
  --output benchmark-results/local-matrix.json
```

Every one of the 12 runs accepted and persisted all 500 events with zero failures.

| Concurrency | Runs | Median persisted events/s | Minimum | Maximum |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3 | 303.2 | 278.3 | 312.4 |
| 10 | 3 | 395.2 | 369.6 | 408.3 |
| 25 | 3 | 412.3 | 396.2 | 426.3 |
| 50 | 3 | 335.6 | 330.8 | 335.9 |

The best median in this local matrix occurred at concurrency 25. Concurrency 50 reduced
throughput and increased tail latency, indicating saturation in this single-host Compose setup.
These values are local development evidence, not production capacity claims.

## Transactional outbox verification — July 29, 2026

The follow-up closes the crash window between committing a telemetry row and publishing its
derived Kafka event. The telemetry row, device-status update, and unique outbox row now commit in
one PostgreSQL transaction. Multiple dispatchers claim pending records using
`FOR UPDATE SKIP LOCKED` and mark them published only after Kafka acknowledgement.

Final verification:

```text
Ruff: All checks passed
Unit tests: 8 passed, 7 integration tests deselected
Integration tests: 7 passed, 8 unit tests deselected, 51.37 seconds
Outbox after integration and benchmarks: 0 pending, 1,813 published
Existing-volume migration: applied twice safely with no data reset
```

The new integration scenario reset six durable rows to pending, restarted the worker at two
instances, consumed the resulting validated events, and proved that all six distinct event IDs
were delivered once and all six database markers completed. Unit tests separately simulate:

- Kafka failure before acknowledgement, leaving the row pending.
- Kafka acknowledgement followed by a crash before the PostgreSQL marker commit, causing a safe
  at-least-once replay with the same event ID.
- Marker updates occurring only after the producer call succeeds.

The post-change benchmark used 300 events, two repeated runs, and concurrency levels 1, 10, and
25. All 1,800 events were accepted and persisted with zero request failures.

| Concurrency | Runs | Median persisted events/s | Minimum | Maximum |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 311.1 | 307.1 | 315.0 |
| 10 | 2 | 402.4 | 393.1 | 411.6 |
| 25 | 2 | 331.6 | 267.1 | 396.2 |

The matrix demonstrates that adding durable outbox insertion did not prevent complete
processing under concurrent load. It remains single-host development evidence rather than a
production capacity claim.
