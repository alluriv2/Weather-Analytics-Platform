# Real-Time Weather Analytics Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestrated-326CE5)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-4.3-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-REST-009688)
![Plotly Dash](https://img.shields.io/badge/Plotly-Dash-purple)

A local, real-time weather ingestion and analytics platform. Kubernetes runs
Kafka and the application services, while PostgreSQL and its files remain
outside the Kubernetes cluster so weather history survives cluster shutdown,
deletion, and recreation.

## What the platform runs

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
                                                   ▼
                                             Dashboard
```

- **Producer:** retrieves new station readings and publishes them to Kafka.
- **Kafka:** buffers live weather events inside Kubernetes.
- **Consumer:** writes Kafka events to PostgreSQL and records committed offsets.
- **Reconciler:** compares source and database timestamps every five minutes and
  repairs missing data.
- **Aggregator:** creates time-based summaries for trend analysis.
- **FastAPI:** provides current conditions, history, health, and ingestion state.
- **Dashboard:** displays current conditions and historical trends.
- **PostgreSQL:** stores the durable weather history outside Kubernetes.

## Storage and recovery model

PostgreSQL runs independently from the Kubernetes workloads. Consequently:

- Stopping Kubernetes stops ingestion and the dashboard but does not erase the
  database.
- Deleting the `weather-dev` namespace removes the Kubernetes workloads and
  Kafka data but does not erase the database.
- Starting the platform again reuses the existing database and repairs the
  interval missed while Kubernetes was unavailable.

The `ingestion_state` table tracks source, producer, consumer, and database
timestamps for every station. Kafka offsets are committed only after the
corresponding PostgreSQL transaction succeeds.

## Requirements

- macOS with Docker Desktop
- Docker Desktop Kubernetes enabled
- `docker`
- `kubectl`
- Internet access for image downloads and weather-station source files

Confirm that Docker Desktop shows Kubernetes as running before continuing.

## First run after cloning

Clone the repository and enter it:

```bash
git clone https://github.com/alluriv2/Weather-Analytics-Platform.git
cd Weather-Analytics-Platform
```

Select the Docker Desktop Kubernetes cluster:

```bash
kubectl config use-context docker-desktop
kubectl cluster-info
```

Start the complete platform:

```bash
./scripts/bootstrap-local.sh
```

The first run completes the local database setup. Later starts reuse it
automatically.

The script then performs these operations in order:

1. Builds `weather-platform:0.5.3`.
2. Creates and starts the persistent PostgreSQL database.
3. Creates the `weather-dev` Kubernetes namespace and configuration.
4. Starts Kafka.
5. Detects the empty database and runs the complete historical backfill.
6. Starts the producer, consumer, reconciler, aggregator, API, dashboard, and
   Kafka UI.
7. Waits for every deployment to become ready.
8. Opens the local service addresses.

The first historical backfill takes longer than later starts. Do not close the
terminal until the script reports:

```text
Local Weather Platform bootstrap completed successfully.
```

## Open the services

After a successful bootstrap:

| Service | Address |
| --- | --- |
| Dashboard | <http://127.0.0.1:18050> |
| API | <http://127.0.0.1:18000> |
| API documentation | <http://127.0.0.1:18000/docs> |
| Ingestion status | <http://127.0.0.1:18000/ingestion-status> |
| Kafka UI | <http://127.0.0.1:18080> |

## Starting again after Kubernetes stops

Start Docker Desktop and wait until Kubernetes reports that it is running.
Then, from the repository root:

```bash
kubectl config use-context docker-desktop
./scripts/bootstrap-local.sh
```

The script reuses PostgreSQL, recreates missing Kubernetes resources, compares
timestamps, backfills only missing records, and resumes live processing.

Use the existing image to avoid rebuilding it:

```bash
./scripts/bootstrap-local.sh --skip-build
```

Run without opening local service ports:

```bash
./scripts/bootstrap-local.sh --no-port-forward
```

Both options may be combined:

```bash
./scripts/bootstrap-local.sh --skip-build --no-port-forward
```

## Verify the deployment

Check all Kubernetes workloads:

```bash
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
  --namespace weather-dev
```

Healthy continuous workloads should show `Running`, and deployments should show
their desired replicas as ready. Completed backfill and reconciliation jobs
normally show `Completed`.

Check PostgreSQL:

```bash
docker ps --filter name=weather-postgres-local
```

Its status should include `healthy`.

Check data continuity:

```bash
curl http://127.0.0.1:18000/ingestion-status
```

Check API health:

```bash
curl http://127.0.0.1:18000/health
```

## Normal shutdown

Stopping Kubernetes in Docker Desktop stops the Kubernetes applications. The
independent PostgreSQL database remains available for the next start.

To stop only PostgreSQL:

```bash
docker stop weather-postgres-local
```

It will be started again automatically by the next bootstrap.

To remove the Kubernetes deployment while retaining PostgreSQL:

```bash
kubectl delete namespace weather-dev
```

The next bootstrap recreates the namespace and performs incremental
reconciliation against the retained database.

## Troubleshooting

### `current-context` is not `docker-desktop`

```bash
kubectl config use-context docker-desktop
kubectl cluster-info
```

If `cluster-info` fails, enable Kubernetes in Docker Desktop and wait for it to
finish starting.

### Dashboard does not open

Confirm the dashboard pod is ready:

```bash
kubectl get pods --namespace weather-dev
```

Restart a foreground port-forward if necessary:

```bash
kubectl port-forward \
  --namespace weather-dev \
  service/weather-dashboard \
  18050:8050
```

Keep that terminal open and visit <http://127.0.0.1:18050>.

### Dashboard shows `Unavailable`

Check the API and ingestion status:

```bash
curl http://127.0.0.1:18000/health
curl http://127.0.0.1:18000/ingestion-status
```

Immediately after first startup, a station can briefly show `Unavailable` until
its first live reading is processed.

### A startup reconciliation job fails

The bootstrap prints the failing job logs and exits immediately. Additional
logs are available with:

```bash
kubectl logs job/initial-backfill \
  --namespace weather-dev \
  --all-containers=true
```

### A local port is already in use

The defaults are:

- Dashboard: `18050`
- API: `18000`
- Kafka UI: `18080`

Stop the process already using the required port, or run with
`--no-port-forward` and create your own port-forward mappings.

## Repository layout

```text
.
├── dashboard/                         # Plotly Dash application
├── kubernetes/
│   ├── aggregator/                    # Aggregation deployment
│   ├── api/                           # FastAPI deployment and service
│   ├── consumer/                      # Kafka consumer deployment
│   ├── dashboard/                     # Dashboard deployment and service
│   ├── kafka/                         # Kafka StatefulSet and service
│   ├── kafka-ui/                      # Kafka UI deployment and service
│   ├── producer/                      # Producer and startup backfill
│   └── reconciler/                    # Scheduled gap repair
├── local/
│   └── postgres-compose.yaml          # Cluster-independent PostgreSQL
├── scripts/
│   └── bootstrap-local.sh             # Complete local deployment
├── previous-version/                  # Archived original implementation
├── Dockerfile                         # Shared Python application image
├── ingestion_state.py                 # Watermark and offset management
├── inital_backfill.py                 # Full/incremental reconciliation
├── weather_aggregation_postgres.py    # Aggregate calculations
├── weather_api.py                     # FastAPI service
├── weather_kafka_consumer.py          # Kafka-to-PostgreSQL ingestion
└── weather_kafka_producer.py          # Station-to-Kafka ingestion
```

For detailed Kubernetes operations, see
[`kubernetes/README.md`](kubernetes/README.md).

## Previous version

The original implementation is retained in
[`previous-version/`](previous-version/README.md) for comparison.

## License

This project is intended for educational and portfolio purposes.
