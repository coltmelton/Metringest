# Operational SLOs and runbooks

These objectives turn pipeline reliability into measurable operating conditions. They apply to
events accepted with HTTP 202; rejected requests remain the producer's responsibility to retry.

| Indicator | Objective | Alert |
| --- | --- | --- |
| Accepted-to-persisted latency | p99 below 5 seconds | 5-minute breach |
| Raw Kafka consumer lag | At most 1,000 messages | 5-minute breach |
| DLQ ratio | Below 0.1% of consumed records | 10-minute breach |
| Oldest pending outbox row | Below 30 seconds | 2-minute breach |
| Worker availability | At least one ready worker | 2-minute absence |
| Dependency readiness | All API dependencies ready | 2-minute degradation |

Prometheus evaluates the executable rules in `infra/prometheus/alerts.yml`. Grafana's Telemetry
Pipeline dashboard shows the same indicators. `/live` only answers whether the API process can
serve requests. `/ready` returns 503 unless PostgreSQL, Redis, Kafka, and Schema Registry are
usable. The backward-compatible `/health` endpoint returns the same details with a 200 response so
operators can inspect a degraded instance.

## Worker unavailable

Check `docker compose ps worker` and worker logs. Start or replace workers, then confirm
`worker_ready == 1` and that `sum(queue_lag)` falls. Accepted Kafka records remain durable while
workers are absent.

## Consumer lag

Compare lag by the `partition` label. Check PostgreSQL/Redis latency and poison-message volume
before scaling workers. Scale only up to the raw topic's partition count; additional consumers are
idle. Confirm lag is decreasing after the change.

## Persistence latency

Inspect API acceptance rate, partition lag, PostgreSQL health, Redis health, and worker batch
latency. The histogram starts at API `received_at` and ends after the durable PostgreSQL write and
Redis cache update. A Redis outage therefore correctly keeps the objective open until replay
repairs the cache.

## DLQ rate

List recent failures with `python /tools/dlq_replay.py list`. Group by reason before replaying.
Correct incompatible envelopes without changing their event ID, dry-run first, and use `--execute`
only after review. See `schema-governance.md` for the audited replay procedure.

## Outbox stall

Check Kafka availability and `outbox_publish_failures_total`. Pending rows are durable in
PostgreSQL. Restore Kafka or restart a stuck dispatcher, then verify both pending count and oldest
age return to zero. Do not delete pending rows.

## API dependency

Use `/ready` to identify the failed dependency and `/live` to distinguish dependency failure from
process failure. Restore the dependency before routing new traffic to that API instance.

## Verification

```sh
docker run --rm --entrypoint promtool -v "$PWD/infra/prometheus:/etc/prometheus:ro" \
  prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
pytest -m integration -v -s
python scripts/benchmark_matrix.py --count 100 --concurrency-levels 1,10 \
  --runs 2 --max-pipeline-seconds 5
```

The CI probe fails if any accepted event is missing or any complete run exceeds five seconds. It
is a deterministic smoke objective, not a substitute for a long-running production SLO window.
