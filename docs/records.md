# Reliability verification record

This record is intentionally rooted in the `coltmelton/Metringest` implementation and history.
It does not include TelemetryUI/Elixir commits, workflows, or claims from the superseded branch.

## Branch provenance

- Base: `metringest/main`
- Base commit: `a43ed66` (`Build telemetry pipeline MVP`)
- Reliability branch: `feat/reliability-v2`
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
