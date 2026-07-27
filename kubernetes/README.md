# Local Kubernetes runbook

This Kubernetes configuration runs Kafka and the Weather Platform application
services. PostgreSQL is deliberately outside Kubernetes and stores its files
under `local-data/postgres/`.

## Ownership boundaries

```text
Docker Compose
└── PostgreSQL + durable host-mounted data

Kubernetes namespace weather-dev
├── Kafka StatefulSet + replaceable local Kafka PVC
├── Initial reconciliation Job
├── Scheduled reconciliation CronJob
├── Producer
├── Consumer
├── Aggregator
├── API
├── Dashboard
└── Kafka UI
```

The database is authoritative. Kafka and the Kubernetes namespace can be
recreated.

## Safety check

```bash
kubectl config current-context
```

For local deployment, this must print `docker-desktop`.

## Automated deployment

The complete ordered workflow can be run from the repository root:

```bash
./scripts/bootstrap-local.sh
```

The script is idempotent. An empty PostgreSQL folder triggers the complete
historical backfill; an existing database triggers incremental reconciliation.
The sections below document the individual operations performed by the script.

## Database lifecycle

Start PostgreSQL from the repository root:

```bash
read -s "WEATHER_DB_PASSWORD?Enter the PostgreSQL password: "
echo

WEATHER_DB_PASSWORD="$WEATHER_DB_PASSWORD" \
  docker compose -f local/postgres-compose.yaml up -d
```

The database listens only on `127.0.0.1:5433`. Docker Desktop Kubernetes pods
reach it using `host.docker.internal:5433`.

Create the matching Kubernetes credential:

```bash
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/configmap.yaml

kubectl create secret generic postgres-credentials \
  --namespace weather-dev \
  --from-literal=POSTGRES_USER=weather_user \
  --from-literal=POSTGRES_PASSWORD="$WEATHER_DB_PASSWORD"

unset WEATHER_DB_PASSWORD
```

## Ordered deployment

Build the immutable local image version:

```bash
docker build -t weather-platform:0.5.3 .
```

Start Kafka:

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
kubectl logs job/initial-backfill --namespace weather-dev
```

The same command supports both cases:

- Empty database: complete historical backfill
- Existing database: recent incremental reconciliation

Start the platform:

```bash
kubectl apply -f kubernetes/reconciler/cronjob.yaml
kubectl apply -f kubernetes/producer/deployment.yaml
kubectl apply -f kubernetes/consumer/deployment.yaml
kubectl apply -f kubernetes/aggregator/deployment.yaml
kubectl apply -f kubernetes/api
kubectl apply -f kubernetes/dashboard
kubectl apply -f kubernetes/kafka-ui
```

## Watermark validation

Expose the API:

```bash
kubectl port-forward \
  --namespace weather-dev \
  service/weather-api \
  18000:8000
```

Open <http://127.0.0.1:18000/ingestion-status>.

For each station, the response includes:

- `source_latest_ts`
- `producer_latest_ts`
- `consumer_latest_ts`
- `database_latest_ts`
- `kafka_partition`
- `kafka_offset`
- `producer_consumer_gap_seconds`
- `source_database_gap_seconds`
- `requires_backfill`

A short producer/consumer difference can occur while Kafka is being consumed.
When Kafka lag returns to zero, the timestamps should converge. If the
database remains behind, the reconciler repairs it.

Check Kafka lag:

```bash
kubectl exec --namespace weather-dev kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group weather-postgres-consumer-dev
```

## Automatic reconciliation

`weather-reconciler` runs every five minutes with
`concurrencyPolicy: Forbid`. It re-reads the last two source days and performs
idempotent upserts. This repairs tail gaps without duplicating rows.

Run it immediately:

```bash
kubectl create job \
  --from=cronjob/weather-reconciler \
  weather-reconcile-manual \
  --namespace weather-dev

kubectl wait \
  --for=condition=complete \
  job/weather-reconcile-manual \
  --namespace weather-dev \
  --timeout=600s
```

Use a new manual Job name each time, or delete the prior Job first.

## Local access

Dashboard:

```bash
kubectl port-forward \
  --namespace weather-dev \
  service/weather-dashboard \
  18050:8050
```

Kafka UI:

```bash
kubectl port-forward \
  --namespace weather-dev \
  service/kafka-ui \
  18080:8080
```

## Cluster or namespace recreation

After Kubernetes is recreated:

1. Start PostgreSQL if its Docker container is stopped.
2. Recreate the namespace, ConfigMap, and Secret.
3. Deploy Kafka.
4. Run the startup reconciliation Job.
5. Deploy the continuous workloads.

The Job detects the existing database and runs incrementally. Do not run a
full historical load unless the database is actually empty.

## Shutdown

Stopping Docker Desktop Kubernetes stops the application but does not remove
the external PostgreSQL files.

Deleting the namespace is safe for PostgreSQL:

```bash
kubectl delete namespace weather-dev
```

It does delete Kafka and all other namespaced resources.

Stop PostgreSQL without deleting its files:

```bash
WEATHER_DB_PASSWORD=unused \
  docker compose -f local/postgres-compose.yaml stop
```

Never delete `local-data/postgres/` unless database loss is intentional.
