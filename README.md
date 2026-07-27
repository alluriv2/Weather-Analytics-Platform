# Real-Time Weather Analytics Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestrated-326CE5)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-4.3-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-REST-009688)
![Plotly Dash](https://img.shields.io/badge/Plotly-Dash-purple)

A real-time weather analytics platform with durable, cluster-independent local
PostgreSQL storage. Kubernetes runs Kafka and the application services, while
the database remains available when the Kubernetes cluster or namespace is
stopped, deleted, or recreated.

## Architecture

```text
Remote weather stations
          │
          ▼
Producer ─────► Kafka ─────► Consumer
   │                             │
   │                             ▼
   └── durable watermarks ─► PostgreSQL ◄── Reconciler
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
              Aggregator                  FastAPI API
                                                │
                                                ▼
                                         Dash dashboard
```

PostgreSQL runs separately through Docker Compose and stores its files in
`local-data/postgres/`, which is excluded from Git. Kubernetes pods connect to
it through `host.docker.internal:5433`.

## Recovery model

The `ingestion_state` database table records, per station:

- Latest timestamp available at the remote source
- Latest timestamp published by the producer
- Latest timestamp processed by the consumer
- Latest timestamp stored in PostgreSQL
- Kafka partition and offset
- Reconciliation status and time

The consumer commits a Kafka offset only after its PostgreSQL transaction
succeeds. The `/ingestion-status` API reports timestamp gaps and whether a
backfill is required.

A Kubernetes CronJob runs every five minutes. On an empty database it performs
the complete initial backfill. On an existing database it rechecks only the
recent source files and idempotently restores missing or delayed records.

## Repository layout

```text
.
├── dashboard/                  # Plotly Dash application
├── kubernetes/                 # Kafka and application workloads
├── local/
│   └── postgres-compose.yaml   # Cluster-independent PostgreSQL
├── local-data/                 # Created locally; never committed
├── previous-version/           # Archived version 1
├── Dockerfile
├── ingestion_state.py
├── inital_backfill.py
├── weather_kafka_producer.py
├── weather_kafka_consumer.py
├── weather_aggregation_postgres.py
└── weather_api.py
```

## Local deployment

Prerequisites:

- Docker Desktop with Kubernetes enabled
- `docker`
- `kubectl`

Select and verify the local cluster:

```bash
kubectl config use-context docker-desktop
kubectl cluster-info
```

Build the shared application image:

```bash
docker build -t weather-platform:0.5.2 .
```

Choose the local database password once for this installation:

```bash
read -s "WEATHER_DB_PASSWORD?Enter the PostgreSQL password: "
echo
```

Start the cluster-independent database:

```bash
WEATHER_DB_PASSWORD="$WEATHER_DB_PASSWORD" \
  docker compose -f local/postgres-compose.yaml up -d
```

Create Kubernetes configuration and give the pods the same credential:

```bash
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/configmap.yaml

kubectl create secret generic postgres-credentials \
  --namespace weather-dev \
  --from-literal=POSTGRES_USER=weather_user \
  --from-literal=POSTGRES_PASSWORD="$WEATHER_DB_PASSWORD"

unset WEATHER_DB_PASSWORD
```

Deploy Kafka and wait for it:

```bash
kubectl apply -f kubernetes/kafka
kubectl rollout status statefulset/kafka \
  --namespace weather-dev \
  --timeout=300s
```

Run startup reconciliation:

```bash
kubectl apply -f kubernetes/producer/initial-backfill-job.yaml
kubectl wait \
  --for=condition=complete \
  job/initial-backfill \
  --namespace weather-dev \
  --timeout=1800s
```

For a new clone, this performs the full backfill. If `local-data/postgres`
already contains a database, it performs only incremental reconciliation.

Deploy the continuously running workloads:

```bash
kubectl apply -f kubernetes/reconciler/cronjob.yaml
kubectl apply -f kubernetes/producer/deployment.yaml
kubectl apply -f kubernetes/consumer/deployment.yaml
kubectl apply -f kubernetes/aggregator/deployment.yaml
kubectl apply -f kubernetes/api
kubectl apply -f kubernetes/dashboard
kubectl apply -f kubernetes/kafka-ui
```

Verify:

```bash
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
  --namespace weather-dev
```

Open the dashboard:

```bash
kubectl port-forward \
  --namespace weather-dev \
  service/weather-dashboard \
  18050:8050
```

Visit <http://127.0.0.1:18050>.

Optional local endpoints:

- API: port-forward `service/weather-api 18000:8000`
- API documentation: <http://127.0.0.1:18000/docs>
- Ingestion status: <http://127.0.0.1:18000/ingestion-status>
- Kafka UI: port-forward `service/kafka-ui 18080:8080`

See [the Kubernetes runbook](kubernetes/README.md) for validation, recovery,
restart, and shutdown details.

## Persistence guarantee

Deleting `weather-dev` deletes Kafka and the Kubernetes workloads, but it does
not delete PostgreSQL data in `local-data/postgres/`.

Do not delete `local-data/postgres/` or run Docker Compose with volume/data
removal unless permanent database loss is intended. Independent storage still
requires backups for protection from disk failure or accidental deletion.

## Previous version

The original implementation is retained in
[`previous-version/`](previous-version/README.md) for reference.

## License

This project is intended for educational and portfolio purposes.
