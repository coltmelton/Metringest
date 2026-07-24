# Reliability and performance verification

The integration suite treats the running Compose stack as a black box. It verifies that an
accepted event crosses Kafka, the worker, PostgreSQL, and Redis. Its recovery scenario stops
the worker, publishes three events while the consumer is unavailable, confirms they have not
appeared in storage, starts the worker, and verifies that all three buffered event IDs arrive.

Run the same checks locally:

```sh
docker compose up --build --detach
python -m pytest -m integration
python scripts/benchmark_ingest.py --count 1000 --concurrency 25 \
  --output benchmark-results/local.json
docker compose down --volumes
```

The benchmark reports accepted and durably persisted requests, ingestion and end-to-end pipeline
throughput, and mean/p50/p95/p99 producer latency as JSON. Use `--minimum-rps` to turn a known environment-specific baseline into a
regression gate. CI intentionally uses a smoke-sized run and uploads both its JSON result and
the Compose logs; shared runners are too variable for a universal throughput threshold.

The worker commits Kafka offsets only after PostgreSQL and Redis complete. If the worker exits
before that point, Kafka redelivers the uncommitted messages when it rejoins the consumer group.
PostgreSQL's primary key makes the durable insert idempotent.

For throughput, the worker polls up to `WORKER_BATCH_SIZE` events and sends the group through one
PostgreSQL `executemany` call and one non-transactional Redis pipeline. The default is 100 events
with a maximum poll wait of 250 ms. Both values are configurable so deployments can trade a small
amount of low-traffic latency for fewer storage round trips.

## Optimization evidence

An identical 1,000-event, concurrency-25 run on the development Compose stack measured:

| Worker | Pipeline events/second | Pipeline time | Failures |
| --- | ---: | ---: | ---: |
| Per-event storage calls | 225.6 | 4.432 s | 0 |
| 100-event batched storage calls | 259.2 | 3.858 s | 0 |

That is a 14.9% end-to-end throughput increase in this run. These numbers demonstrate the change,
not a portable capacity promise; hardware and container runtime affect absolute results. Re-run
the command above in the target environment and retain its JSON output for capacity decisions.
