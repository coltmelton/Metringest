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

## Admission-control verification — July 31, 2026

The API now hashes and authenticates ingestion keys, atomically charges submitted event counts
through a shared Redis token bucket, caps request and batch sizes, and returns retryable
backpressure instead of a false acceptance when Kafka is unavailable. Known demonstration keys
are rejected at startup outside development.

Final local verification:

```text
Ruff: All checks passed
API admission unit tests: 7 passed
Worker reliability unit tests: 8 passed
Integration tests: 10 passed, 8 unit tests deselected, 65.77 seconds
Authenticated benchmark: 1,800 accepted, 1,800 persisted, 0 failures
```

The integration suite demonstrated an unauthenticated `401`, oversized-body `413`, distributed
quota `429` with retry metadata, and Kafka-outage `503`. It then passed the existing poison/DLQ,
PostgreSQL outage, Redis cache recovery, outbox restart, partition, and worker-scaling scenarios.

| Concurrency | Runs | Median persisted events/s | Minimum | Maximum |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 268.0 | 264.4 | 271.6 |
| 10 | 2 | 406.8 | 394.7 | 418.8 |
| 25 | 2 | 385.1 | 380.5 | 389.6 |

The isolated benchmark raised the benchmark client's quota to 10,000 events so it measured the
authenticated pipeline rather than the intentional default admission ceiling. These are local
development results, not production capacity claims.

## Schema-governance verification — July 31, 2026

The raw envelope now carries a schema version and registry ID. Checked v0 and v1 JSON Schemas are
registered under a backward-transitive policy at API startup. The worker upcasts legacy v0
messages, isolates unknown versions in the existing DLQ, and provides an operator-controlled
replay command whose database audit is written only after Kafka acknowledgement.

Recorded local verification before the final combined rerun:

```text
Ruff: All checks passed
API unit tests: 7 passed
Worker/schema/replay unit tests: 14 passed, 12 integration tests deselected
Registry contract check: v0 registered; v1 compatible; incompatible mutation rejected
New integration cases: registry outage passed; mixed v0/v1/unknown and audited replay passed
Integration suite: 11 passed; one pre-existing outbox observer race failed after Kafka restart
Isolated outbox recovery rerun on the recovered broker: passed in 17.20 seconds
```

The failed combined run is retained here rather than hidden: the Kafka-outage test restarts the
single local broker, and a later independent verification consumer temporarily lost group
membership during broker recovery. No outbox rows were observed during its 90-second window. The
same outbox test passed immediately when rerun against the recovered broker. This is evidence of
a test-environment readiness race, not a claim that the first run passed; the final combined
run then required five consecutive successful broker metadata checks after restart and passed:

```text
Final integration suite: 12 passed, 14 unit tests deselected, 86.13 seconds
Benchmark: 1,800 accepted, 1,800 persisted, 0 failures
```

| Concurrency | Runs | Median persisted events/s | Minimum | Maximum |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 264.0 | 258.3 | 269.7 |
| 10 | 2 | 391.2 | 374.7 | 407.7 |
| 25 | 2 | 356.7 | 332.5 | 380.9 |

The benchmark used 300 events per run at three concurrency levels. As with the previous records,
these are measured single-host Compose results rather than production capacity claims.

## Operational-SLO verification — August 2, 2026

The pipeline now exposes separate liveness and readiness probes, partition-labelled consumer lag,
accepted-to-persisted latency, oldest pending outbox age, worker readiness, and graceful-shutdown
counts. Six Prometheus rules enforce the documented latency, lag, DLQ, outbox, worker, and API
dependency thresholds. The Grafana dashboard uses the same metrics and labels.

Local verification:

```text
Ruff: All checks passed
API unit tests: 9 passed
Worker/reliability unit tests: 15 passed, 13 integration tests deselected
Promtool: configuration valid, 1 rule file and 6 rules loaded
Integration tests: 13 passed, 15 unit tests deselected, 85.66 seconds
Graceful-shutdown drill: 50 accepted, 50 persisted after SIGTERM/restart
Prometheus runtime: API and worker targets both up; all 6 rules loaded
Redis readiness drill: /ready returned 503, alert entered pending, recovery in 27 seconds
SLO probe: 400 accepted, 400 persisted, 0 failures, all runs below 5 seconds
```

| Concurrency | Runs | Median persisted events/s | Minimum | Maximum | Slowest run |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 304.1 | 297.4 | 310.9 | 0.336 s |
| 10 | 2 | 395.4 | 386.0 | 404.9 | 0.259 s |

The readiness drill deliberately stopped Redis. `/live` continued to represent the API process,
while `/ready` identified Redis as false and returned 503. Prometheus observed
`TelemetryAPINotReady` in its pending state before Redis was restored. The 27-second measurement
includes container shutdown, the 10-second scrape interval, alert evaluation, restart, and the
successful readiness probe. The alert's two-minute `for` duration intentionally prevented paging
during this short verified recovery.

## Actionable-alert firing and recovery drill — August 3, 2026

This follow-up kept Redis unavailable beyond the alert's two-minute tolerance to demonstrate the
complete operational state machine. The operator first captured healthy `/live`, `/ready`, and
Prometheus alert baselines, stopped only Redis, and checked both probes again. `/live` remained
200 because the API process could still serve traffic; `/ready` returned 503 and identified Redis
as the only failed dependency. No application data or Docker volume was removed.

Measured UTC timeline:

```text
18:23:07  baseline: /live 200, /ready 200, no active alerts
18:23:07  Redis stopped: /live 200, /ready 503, redis=false
18:24:09  TelemetryAPINotReady pending (activeAt 18:24:03)
18:26:18  TelemetryAPINotReady firing
18:27:56  Redis started: /ready 200, all dependencies=true
18:29:15  TelemetryAPINotReady inactive/resolved
```

The intentional dependency outage lasted 4 minutes 49 seconds. Application readiness recovered
in less than one second after Redis started; the alert resolved 79 seconds later because rule
evaluation runs independently from the readiness request. This distinction matters operationally:
service recovery is proved by `/ready`, while alert resolution confirms Prometheus subsequently
observed the recovered metric.

The human recovery sequence was:

```sh
curl -i http://localhost:8000/live
curl -i http://localhost:8000/ready
docker compose -p metringest stop redis
curl -i http://localhost:8000/ready
curl http://localhost:9090/api/v1/alerts
docker compose -p metringest start redis
curl -i http://localhost:8000/ready
curl http://localhost:9090/api/v1/alerts
```

Promtool validated six rules after every alert received explicit `owner`, `runbook`, `dashboard`,
and metric-specific `investigate` annotations. The plain operations dashboard now links its DLQ,
outbox, and dependency indicators directly to the corresponding Prometheus queries. Grafana's
provisioned dashboard has the stable UID `telemetry-pipeline`, so alert links do not depend on a
generated dashboard identifier.

### CI benchmark cold-start correction

The first CI run for the alert branch passed all unit and integration tests but failed the
benchmark smoke step. All 400 measured events persisted without request failures; one concurrency-1
run took 5.298 seconds against the five-second objective, while the other three completed in
1.015, 0.536, and 0.553 seconds. This isolated the failure to first-run service and consumer-group
warm-up rather than event loss or an alerting regression.

The benchmark now persists 10 unmeasured events before starting the matrix. Warm-up failure or
missing persistence still fails the command, but warm-up time is not reported as measured pipeline
latency. The five-second limit remains unchanged for every recorded run. The exact local CI matrix
then accepted and persisted all 410 events (10 warm-up and 400 measured); measured runs completed
in 0.810, 0.565, 0.515, and 0.510 seconds, and both concurrency summaries satisfied the objective.

## Transactional telemetry retention verification — August 3, 2026

Retention was verified against an isolated PostgreSQL 16 container populated with four synthetic
events: two old events with published outbox rows, one old event with a pending outbox row, and one
recent delivered event. The initial dry run reported two eligible events and one protected pending
event without changing either table. Executing one batch selected, rolled up, and deleted exactly
the two delivered old events. The pending old event and recent event remained raw.

A second execution introduced one late event in an hour already represented by a rollup. The
conflict update added its count and used count-weighted averages instead of replacing the previous
aggregate. Final evidence from the disposable database was:

```text
First dry run: eligible_events=2, protected_pending_events=1
First execution: selected_events=2, rollup_rows=2, deleted_events=2
Late execution: selected_events=1, rollup_rows=1, deleted_events=1
Final raw rows: 2 (pending old event and recent event)
Final rollup event count: 3
Final weighted average temperature: 73.3 F
Pending outbox rows: 1
Pending raw event present: 1
Retention unit tests: 2 passed
Repository non-integration tests: 19 passed, 13 deselected
API tests: 10 passed
```

The temporary database container held only synthetic verification data and was removed after the
checks. No persistent Metringest event, outbox row, or Docker volume was altered by this drill.

## Unified historical-query verification — August 3, 2026

The historical endpoint was exercised against the persistent development database after applying
only the idempotent rollup schema migration; raw-event retention was not executed. A 34-day hourly
request returned 2,359,371 events as 186 bounded points in 1.227 seconds and correctly reported
`storage=raw`. The same SQL contract unions retained hourly rows and still-raw rows, then combines
averages by event count, so reads remain continuous while retention batches are running and when
late raw events share an existing rollup hour.

Browser verification loaded a 30-day sensor history on desktop and mobile. During an 11-second
observation, each page issued one historical request while live overview polling issued two
requests. Both returned 200. The 1-day, 7-day, and 30-day controls rendered without horizontal
overflow, both chart canvases retained their dimensions after wheel input, and no browser request
failed. API verification completed with 13 passing tests.
