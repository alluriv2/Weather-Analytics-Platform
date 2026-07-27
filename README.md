# Real-Time Weather Analytics Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestrated-326CE5)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-4.3-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-REST-009688)
![Plotly Dash](https://img.shields.io/badge/Plotly-Dash-purple)

The current release is a Kubernetes-based, real-time weather analytics
platform. It collects observations from remote weather stations, streams new
events through Kafka, stores current and historical data in PostgreSQL,
precomputes analytical windows, exposes a FastAPI service, and presents the
results in a Plotly Dash dashboard.

## Current architecture

```text
Weather stations
       │
       ▼
Python producer ──► Apache Kafka ──► Python consumer
                                           │
                                           ▼
                                      PostgreSQL
                                           │
                         ┌─────────────────┴─────────────────┐
                         ▼                                   ▼
                 Aggregation worker                    FastAPI API
                                                            │
                                                            ▼
                                                     Dash dashboard
```

Kubernetes manages the services, health checks, restarts, configuration, and
persistent storage. The Python application containers run as a non-root user
with read-only root filesystems.

## Components

| Component | Purpose |
|---|---|
| Initial backfill Job | Loads the historical station dataset |
| Producer Deployment | Publishes new observations to Kafka |
| Kafka StatefulSet | Provides durable event streaming |
| Consumer Deployment | Writes events to PostgreSQL with committed offsets |
| PostgreSQL StatefulSet | Stores observations and aggregates persistently |
| Aggregator Deployment | Refreshes day, week, month, and year windows |
| FastAPI Deployment | Serves health, latest, and history endpoints |
| Dash Deployment | Provides the interactive weather dashboard |
| Kafka UI Deployment | Displays broker, topic, and consumer information |

## Repository layout

```text
.
├── dashboard/                # Plotly Dash application
├── images/                   # README screenshots
├── kubernetes/               # Current Kubernetes deployment
├── previous-version/         # Archived version 1 (Docker Compose)
├── Dockerfile                # Shared Python application image
├── config.py
├── inital_backfill.py
├── requirements.txt
├── weather_aggregation_postgres.py
├── weather_api.py
├── weather_kafka_consumer.py
└── weather_kafka_producer.py
```

## Run the current release locally

Prerequisites:

- Docker Desktop with Kubernetes enabled
- `docker`
- `kubectl`

Build the application image:

```bash
docker build -t weather-platform:0.4.0 .
```

Create the namespace and configuration:

```bash
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/configmap.yaml
```

Create the database credentials directly in Kubernetes. Do not commit the
password or a generated Secret file:

```bash
read -s "WEATHER_DB_PASSWORD?Enter the PostgreSQL password: "
echo
kubectl create secret generic postgres-credentials \
  --namespace weather-dev \
  --from-literal=POSTGRES_USER=weather_user \
  --from-literal=POSTGRES_PASSWORD="$WEATHER_DB_PASSWORD"
unset WEATHER_DB_PASSWORD
```

Deploy the platform:

```bash
kubectl apply -k kubernetes
```

Check its status:

```bash
kubectl get deployments,statefulsets,pods,pvc -n weather-dev
```

Open the dashboard:

```bash
kubectl port-forward -n weather-dev service/weather-dashboard 18050:8050
```

Then visit <http://127.0.0.1:18050>.

Open Kafka UI in a separate terminal:

```bash
kubectl port-forward -n weather-dev service/kafka-ui 18080:8080
```

Then visit <http://127.0.0.1:18080>.

For first-time deployment order, initial backfill, verification, restart,
recovery, and shutdown procedures, read the
[complete Kubernetes runbook](kubernetes/README.md).

## Previous version

The original Docker Compose implementation is preserved in
[`previous-version/`](previous-version/README.md). It is retained for reference
and is not the current deployment method.

## Data and secrets

The repository intentionally excludes `.env` files, database data, Kafka data,
producer checkpoints, and Kubernetes Secret manifests. Runtime data belongs in
Kubernetes persistent volumes, and credentials must be created locally in the
cluster.

## License

This project is intended for educational and portfolio purposes.
