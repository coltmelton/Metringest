# Metringest engineering record

This file records observed repository history, commands, outputs, failures, and repairs. It is
intended as portfolio evidence and an operational audit trail. It contains no generated imagery
or invented production claims.

## Scope

- Repository: `coltmelton/Metringest`
- Working branch: `feat/metringest-reliability`
- Local environment: Docker Desktop, Python services built from `python:3.12-slim`
- Record date: July 24, 2026

## Implementation commits

| Commit | Recorded change |
| --- | --- |
| `a021ae2` | Added the FastAPI, Kafka, worker, PostgreSQL, Redis, dashboard, and Compose baseline |
| `b04a589` | Added benchmark tooling, black-box integration and recovery tests, CI, and documentation |
| `a1e02f1` | Batched worker storage operations while retaining post-persistence Kafka commits |

## Architecture recorded from the code

1. `POST /v1/metrics` validates a `MetricEvent` with Pydantic.
2. The API waits for Kafka's `send_and_wait` and returns HTTP 202 with the event ID.
3. Kafka retains the event for the `metringest-workers` consumer group.
4. The worker polls up to `WORKER_BATCH_SIZE` events, defaulting to 100.
5. PostgreSQL receives the batch with `executemany` and ignores duplicate event IDs.
6. Redis receives recent-value commands in one non-transactional pipeline.
7. The worker commits Kafka offsets only after both storage operations complete.
8. Query endpoints read durable summaries from PostgreSQL and recent events from Redis.

## Local verification record

Commands:

```sh
.venv/bin/ruff check app tests scripts
PYTHONPATH=. .venv/bin/pytest -m "not integration" -q
PYTHONPATH=. .venv/bin/pytest -m integration -q
docker compose config --quiet
```

Observed results before the first push:

```text
All checks passed!
4 passed, 2 deselected
2 passed, 4 deselected
```

The two integration tests exercised:

- API → Kafka → worker → PostgreSQL and Redis delivery for an exact event ID.
- Worker stop → three accepted Kafka events → worker restart → all three exact IDs recovered.

## Failure-recovery procedure

The automated test follows the same sequence an engineer can execute manually:

1. Start the complete stack and wait for `/health`.
2. Stop only the worker with `docker compose stop worker`.
3. Publish three uniquely identifiable events through the API.
4. Confirm the API returns HTTP 202 for each event.
5. Query the unique metric's Redis-backed recent endpoint and confirm it is still empty.
6. Restart the worker with `docker compose start worker`.
7. Poll until all three event IDs appear.
8. Query PostgreSQL-backed summaries and confirm count, minimum, and maximum.

Recorded automated outcome: all three accepted event IDs appeared after restart and both recovery
assertions passed.

## Benchmark record

Command shape:

```sh
python scripts/benchmark_ingest.py --count 1000 --concurrency 25
```

Both measurements used the local Compose stack, 1,000 requests, concurrency 25, and zero failed
requests.

| Worker implementation | Persisted events | Pipeline time | Pipeline rate |
| --- | ---: | ---: | ---: |
| Per-event PostgreSQL and Redis calls | 1,000 | 4.432 seconds | 225.6 events/second |
| Batched PostgreSQL and Redis calls | 1,000 | 3.858 seconds | 259.2 events/second |

Recorded difference: 33.6 additional events/second, or 14.9% higher end-to-end throughput in this
local run. This is development-machine evidence, not a production capacity guarantee.

## GitHub Actions failure record

Failed workflow:

- Workflow: `Metringest CI`
- Run: `30117659547`
- Commit: `a1e02f1`
- Unit job: `89562203220`
- Integration job: `89562203184`
- Observed outcome: both jobs exited during `pip install -e ".[dev]"`; no tests had started.

GitHub also emitted Node.js 20 deprecation warnings for older action versions. Those warnings were
not the exit-code cause.

### Human investigation process

1. Open the failed workflow summary and identify the first failed step in each job.
2. Notice that both independent jobs failed at the identical editable-install step.
3. Treat packaging as the common cause before changing either test suite.
4. Reproduce the CI environment with the same Python base:

   ```sh
   docker run --rm -v "$PWD:/workspace" -w /workspace python:3.12-slim \
     sh -lc 'python -m pip install -e ".[dev]"'
   ```

5. Read the complete setuptools error:

   ```text
   Multiple top-level packages discovered in a flat-layout:
   ['app', 'lib', 'config', 'guides', 'assets'].
   ```

6. Compare that error with the repository layout. The repository contains an Elixir project and
   the Python `app` package at the same level, so implicit setuptools discovery is ambiguous.
7. Fix the package metadata rather than bypassing installation in CI:
   - Declare `setuptools.build_meta`.
   - Require a current setuptools version.
   - Restrict package discovery to `app*`.
8. Update GitHub action majors to versions that run on Node.js 24, removing the independent
   deprecation warnings.
9. Re-run the clean editable install, then lint, unit tests, integration tests, benchmark smoke,
   and Compose cleanup.
10. Push the fix and inspect the replacement workflow rather than assuming local success equals
    CI success.

### Root cause and fix

Root cause: the Python project did not constrain setuptools package discovery. Editable
installation saw the original Elixir directories and the Python application as multiple possible
top-level packages and stopped safely.

Fix: `pyproject.toml` now explicitly builds only packages matching `app*`. The CI workflow also
uses `actions/checkout@v5`, `actions/setup-python@v6`, and `actions/upload-artifact@v5`.

### Post-fix local CI simulation

The repair was verified with a clean Python 3.12 container and a fresh Compose project named
`metringest-ci-verify`, exposed separately on port 18001.

Observed results:

```text
Editable metringest wheel built successfully
All checks passed!
4 passed, 2 deselected
2 passed, 4 deselected
Benchmark: 100 accepted, 100 persisted, 0 failures
```

The benchmark smoke run completed in 0.338 seconds at 295.8 end-to-end events/second. This
smoke-sized result confirms completion and artifact generation; it is not used as a capacity
claim. Logs were captured before the isolated verification project and only its test volumes were
removed.

## Portfolio-safe statements supported by this record

- Built and tested a containerized telemetry pipeline using FastAPI, Kafka, PostgreSQL, and Redis.
- Added a black-box recovery test that interrupts the worker and verifies all accepted event IDs
  after restart.
- Measured a 14.9% local end-to-end throughput improvement after batching storage operations.
- Diagnosed a CI packaging failure by reproducing the clean Python 3.12 editable install and
  constraining setuptools discovery to the intended application package.

Avoid describing the local benchmark as production throughput or the exercised recovery scenario
as proof against every possible failure mode.
