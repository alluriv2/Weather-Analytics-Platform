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
Kafka and the Python services. Docker runs PostgreSQL with repository-local
storage so the source-of-truth database survives Kubernetes cluster recreation.

## Architecture

```text
Remote weather stations
          │
          ▼
      Producer ──► Kafka ──► Consumer
          │                       │
          │                       ▼
          └── watermarks ──► PostgreSQL ◄── Reconciler
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

All custom Python components share the `weather-platform:0.5.4` image. Each
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
2. Starts PostgreSQL with Docker and waits for database readiness.
3. Deploys Kafka and waits for broker readiness.
4. Performs a full backfill for a new database or incremental reconciliation
   from the retained timestamps.
5. Starts the producer and consumer.
6. Starts the aggregator, API, dashboard, and Kafka UI.
7. Enables scheduled reconciliation.
8. Starts Kubernetes state metrics, Prometheus, and Grafana.
9. Opens the local service ports.

First-time setup asks for a PostgreSQL password and saves it in the untracked
`.env` file. Later starts reuse the same credentials and database.

When upgrading from the former split-environment version, startup stops the
obsolete development PostgreSQL container and reuses the retained
`local-data/postgres` files. It also renames the former `weather_db_dev`
database to `weather_db` without removing its records.

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
credentials, images, and Kubernetes storage definitions.

## Persistent storage

The source-of-truth database is kept outside disposable pods:

```text
local-data/
├── postgres/    # Database records and ingestion watermarks
└── run/         # Local port-forward logs and process IDs
```

`local-data/` and `.env` are excluded from Git. Kubernetes can disappear
without deleting the PostgreSQL files. Only deleting `local-data/postgres`
removes the retained database.

Kafka uses a Kubernetes PersistentVolumeClaim for normal pod restarts.
PostgreSQL remains the long-term source of truth, and startup reconciliation
can reconstruct missing Kafka intervals after complete cluster recreation.

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
├── dashboard/                   # Plotly Dash application
├── kubernetes/                  # Unified Kubernetes resources
│   ├── postgres/
│   ├── kafka/
│   ├── producer/
│   ├── consumer/
│   ├── aggregator/
│   ├── api/
│   ├── dashboard/
│   ├── kafka-ui/
│   ├── monitoring/
│   └── reconciler/
├── scripts/
│   └── bootstrap-local.sh
├── start
├── stop
├── report
├── previous-version/
├── Dockerfile
├── ingestion_state.py
├── inital_backfill.py
├── weather_aggregation_postgres.py
├── weather_api.py
├── weather_kafka_consumer.py
└── weather_kafka_producer.py
```

For detailed operations, see
[`kubernetes/README.md`](kubernetes/README.md).

## Previous version

The original implementation is retained in
[`previous-version/`](previous-version/README.md) for comparison.

## License

This project is intended for educational and portfolio purposes.
