# Real-Time Weather Analytics Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestrated-326CE5)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-4.3-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-REST-009688)
![Plotly Dash](https://img.shields.io/badge/Plotly-Dash-purple)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800)

A containerized weather ingestion and analytics platform. Kubernetes manages
Kafka and every Python service. PostgreSQL runs as an external Docker service
with repository-local storage, so its source-of-truth files remain outside the
Kubernetes cluster lifecycle.

## Architecture

```text
Remote weather stations
          │
          ▼
 Live producer ─┐
                ├──► Kafka ──► Consumer ──► PostgreSQL
Backfill job ───┘                            │
                                            ▼
                                      durable watermarks
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                    Aggregator                 FastAPI
                                                   │
                                      ┌────────────┴────────────┐
                                      ▼                         ▼
                                  Dashboard                 Prometheus
                                                                │
                                                                ▼
                                                             Grafana
```

The runtime uses one Kubernetes namespace and one Kafka topic:

| Setting | Value |
| --- | --- |
| Namespace | `weather-python` |
| Kafka topic | `raw_weather_events_python` |
| Consumer group | `weather-postgres-consumer-python` |

All custom Python components share the `weather-platform:0.7.1` image. Each
component runs that image with a different command. PostgreSQL, Kafka, and
Kafka UI use their own specialized images.

## Requirements

- macOS with Docker Desktop
- Docker Desktop Kubernetes enabled
- `docker`
- `kubectl`
- Internet access for images and weather-station source files

Select and verify the local cluster:

```bash
kubectl config use-context docker-desktop
kubectl cluster-info
```

## Start

From the repository root:

```bash
./start
```

Every normal start rebuilds the Python image from the currently checked-out
source. It then:

1. Creates repository-local database storage when needed.
2. Starts the external PostgreSQL container and waits for readiness.
3. Starts Kafka and the database consumer.
4. Publishes a full backfill for a new database, or an incremental repair, to
   Kafka.
5. Waits for Kafka consumer lag to reach zero and validates database rows.
6. Starts the live producer.
7. Starts the aggregator, API, dashboard, and Kafka UI.
8. Enables scheduled reconciliation.
9. Starts Kubernetes state metrics, Prometheus, and Grafana.
10. Opens the local service ports.

First-time setup asks for a PostgreSQL password and saves it in the untracked
`.env` file. Later starts reuse the same credentials and database.

Start without opening local ports:

```bash
./start --no-port-forward
```

## Service addresses

| Service | Address |
| --- | --- |
| Dashboard | <http://127.0.0.1:18050> |
| API | <http://127.0.0.1:18000> |
| API documentation | <http://127.0.0.1:18000/docs> |
| Ingestion status | <http://127.0.0.1:18000/ingestion-status> |
| Kafka UI | <http://127.0.0.1:18080> |
| Prometheus | <http://127.0.0.1:19090> |
| Grafana | <http://127.0.0.1:13000> |

Prometheus collects API health, request rate, response time, ingestion gaps,
database reachability, pod state, restarts, CPU, and memory. Grafana opens with
a provisioned Weather Platform dashboard backed by those Prometheus metrics.

## Report

```bash
./report
```

The report is read-only. It shows the Git version, storage usage, Kubernetes
workloads, latest database timestamp, database-backed ingestion watermarks,
Kafka consumer lag, and reconciliation state.

## Stop

```bash
./stop
```

Shutdown happens in dependency order:

1. Close local port forwards.
2. Suspend reconciliation.
3. Stop the producer.
4. Allow the consumer to drain pending Kafka messages.
5. Stop the consumer, application services, and monitoring.
6. Stop Kafka.
7. Stop PostgreSQL last.

Shutdown preserves database files, Kafka PVC data and offsets, watermarks,
credentials, images, and Kubernetes definitions.

## Persistent storage

The source-of-truth database is kept outside disposable pods:

```text
local-data/
├── postgres/    # Database records and ingestion watermarks
└── run/         # Local port-forward logs and process IDs
```

The PostgreSQL container bind-mounts `local-data/postgres` through Docker
Desktop. Kubernetes workloads connect to it through
`host.docker.internal:5433`. Stopping or recreating Kubernetes does not delete
that folder. Only deleting `local-data/postgres` removes the retained database.

Kafka uses a Kubernetes PersistentVolumeClaim for normal pod restarts. If a
backfill is interrupted, already acknowledged events remain in Kafka. Restarting
publishes an overlapping repair window; the consumer safely upserts duplicate
station/timestamp keys and commits Kafka offsets only after PostgreSQL commits.

## Database watermarks

The `ingestion_state` table records, per station:

- Latest timestamp observed at the source
- Latest timestamp acknowledged by the producer
- Latest timestamp committed by the consumer
- Latest timestamp stored in the database
- Kafka partition and offset
- Last reconciliation time and health status

The consumer updates weather data and its watermark in the same PostgreSQL
transaction. Kafka consumer offsets remain in Kafka because the broker uses
them for delivery coordination.

## Git workflow

One repository folder is sufficient. Git branches separate stable and
in-progress code:

```bash
git switch main
git pull
git switch -c feature/my-change

# Edit files, then rebuild and test the checked-out source.
./stop
./start
./report

git add .
git commit -m "Describe the change"
git switch main
git merge feature/my-change
git push origin main
```

## Repository layout

```text
.
├── src/
│   └── weather_platform/        # Installable-style Python package
│       ├── dashboard/           # Plotly Dash application and pages
│       ├── aggregation.py       # PostgreSQL aggregation worker
│       ├── api.py               # FastAPI service and Prometheus metrics
│       ├── backfill.py          # Historical source-to-Kafka publisher
│       ├── config.py            # Environment-based configuration
│       ├── consumer.py          # Kafka-to-PostgreSQL consumer
│       ├── ingestion_state.py   # Durable ingestion watermark helpers
│       └── producer.py          # Weather source-to-Kafka producer
├── kubernetes/                  # Unified Kubernetes resources
│   ├── kafka/
│   ├── producer/
│   ├── consumer/
│   ├── aggregator/
│   ├── api/
│   ├── dashboard/
│   ├── kafka-ui/
│   ├── monitoring/
│   └── reconciler/
├── local/                       # External PostgreSQL Compose definition
├── scripts/                     # Lifecycle implementation
├── images/                      # Project screenshots
├── previous-version/            # Archived pre-Kubernetes implementation
├── Dockerfile                   # Shared Python application image
├── requirements.txt
├── start                        # Start the complete local platform
├── stop                         # Stop while retaining state
└── report                       # Read-only runtime report
```

For detailed operations, see
[`kubernetes/README.md`](kubernetes/README.md).

## Previous version

The original implementation is retained in
[`previous-version/`](previous-version/README.md) for comparison.

## License

This project is intended for educational and portfolio purposes.
