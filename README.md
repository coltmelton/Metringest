# Distributed Telemetry Pipeline

Production-style MVP for ingesting, validating, queueing, processing, storing, and exposing telemetry from simulated distributed devices.

## Stack

- FastAPI ingestion and query API
- Kafka topics: `raw-telemetry`, `validated-telemetry`, `dead-letter-telemetry`
- Confluent Schema Registry with checked JSON Schema contracts and backward-transitive policy
- Python stream worker with deduplication, transactional outbox delivery, dead-letter routing,
  and metrics
- PostgreSQL time-series-style storage
- React/Vite dashboard
- Prometheus and Grafana observability
- Docker Compose orchestration

## Run

```powershell
docker compose up --build
```

Open:

- API: http://localhost:8000/docs
- Dashboard: http://localhost:5173
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3001, login `admin` / `admin`

## Useful Endpoints

- `POST /telemetry`
- `POST /telemetry/batch`
- `GET /health`
- `GET /metrics`
- `GET /devices`
- `GET /devices/{device_id}/telemetry`
- `GET /devices/{device_id}/latest`
- `GET /regions/{region}/summary`
- `GET /alerts`

The two ingestion endpoints require the development header when using the default Compose setup:

```sh
curl -X POST http://localhost:8000/telemetry \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: development-key' \
  --data @event.json
```

See [docs/admission-control.md](docs/admission-control.md) before using non-development keys or
deploying the API outside a private local environment.

## Example Event

```json
{
  "device_id": "sensor-001",
  "timestamp": "2026-06-24T14:32:10Z",
  "temperature": 72.4,
  "voltage": 12.1,
  "status": "OK",
  "region": "us-east"
}
```

`event_id` is optional at ingestion time. The API adds one if the client does not provide it.

## Tuning The Simulator

Change `simulator` environment variables in `docker-compose.yml`:

- `DEVICE_COUNT`: number of simulated devices
- `EVENTS_PER_SECOND`: publish rate
- `DUPLICATE_RATE`: probability of replaying a previous event
- `FAILURE_RATE`: probability of failed telemetry
- `LATE_EVENT_RATE`: probability of delayed timestamps
- `MALFORMED_RATE`: probability of invalid payloads

## Reset Data

```powershell
docker compose down -v
docker compose up --build
```

## Reliability verification

The clean reliability suite covers poison-message isolation and DLQ routing, mid-batch failure,
PostgreSQL and Redis outages, worker restart recovery, three Kafka partitions, two-worker scaling,
and repeated benchmark matrices.

See [docs/reliability.md](docs/reliability.md) for the consistency boundary and commands, and
[docs/schema-governance.md](docs/schema-governance.md) for schema evolution and controlled DLQ
replay. [docs/records.md](docs/records.md) contains branch provenance and recorded results.
Operational objectives, Prometheus alerts, and response runbooks are in
[docs/operational-slos.md](docs/operational-slos.md).
