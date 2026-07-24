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

The benchmark reports accepted requests, failures, throughput, and mean/p50/p95/p99 producer
latency as JSON. Use `--minimum-rps` to turn a known environment-specific baseline into a
regression gate. CI intentionally uses a smoke-sized run and uploads both its JSON result and
the Compose logs; shared runners are too variable for a universal throughput threshold.

The worker commits Kafka offsets only after PostgreSQL and Redis complete. If the worker exits
before that point, Kafka redelivers the uncommitted messages when it rejoins the consumer group.
PostgreSQL's primary key makes the durable insert idempotent.
