# Local Kubernetes runbook

The base manifests in `kubernetes/` are namespace-neutral. Environment overlays
in `deploy/dev/` and `deploy/prod/` assign environment-specific namespaces,
database endpoints, Kafka topics, client IDs, consumer groups, and Kafka
controller addresses.

## Environment boundaries

| Setting | Development | Production |
| --- | --- | --- |
| Namespace | `weather-dev-python` | `weather-prod-python` |
| Kafka topic | `raw_weather_events_python_dev` | `raw_weather_events_python_prod` |
| Consumer group | `weather-postgres-consumer-python-dev` | `weather-postgres-consumer-python-prod` |
| PostgreSQL host port | `5434` | `5433` |
| PostgreSQL database | `weather_db_python_dev` | `weather_db_dev` |
| Dashboard port | `28050` | `18050` |
| API port | `28000` | `18000` |
| Kafka UI port | `28080` | `18080` |
| Prometheus port | `29090` | Not deployed |

Each namespace contains its own Kafka StatefulSet and PVC, producer, consumer,
aggregator, API, dashboard, Kafka UI, startup backfill Job, and scheduled
reconciler.

The development overlay additionally deploys Prometheus. Keeping monitoring in
the development overlay allows metrics and scrape configuration to be tested
without changing production.

## Verify Prometheus in development

After starting development, open <http://127.0.0.1:29090>. The target page at
`/targets` should show the configured application targets as healthy.

## Render and validate manifests

```bash
kubectl kustomize deploy/dev
kubectl kustomize deploy/prod
```

Client-side validation:

```bash
kubectl apply --dry-run=client -k deploy/dev
kubectl apply --dry-run=client -k deploy/prod
```

## Automated deployment

Production:

```bash
./scripts/bootstrap-local.sh --environment prod
```

Development:

```bash
./scripts/bootstrap-local.sh --environment dev
```

The bootstrap:

1. Validates the Docker Desktop context.
2. Starts and authenticates the selected PostgreSQL database.
3. Creates the selected namespace and Secret.
4. Applies the matching Kustomize overlay.
5. Waits for Kafka.
6. Suspends scheduled reconciliation during startup backfill.
7. Runs full or incremental reconciliation.
8. Resumes scheduled reconciliation.
9. Restarts workloads with the current configuration.
10. Waits for readiness and opens environment-specific local ports.

## Verify Kafka

Development:

```bash
kubectl exec --namespace weather-dev-python kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group weather-postgres-consumer-python-dev \
  --describe
```

Production:

```bash
kubectl exec --namespace weather-prod-python kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group weather-postgres-consumer-python-prod \
  --describe
```

Healthy consumers have lag `0`.

## Verify ingestion

Production:

```bash
kubectl port-forward \
  --namespace weather-prod-python \
  service/weather-api \
  18000:8000
```

Development:

```bash
kubectl port-forward \
  --namespace weather-dev-python \
  service/weather-api \
  28000:8000
```

Open the corresponding `/ingestion-status` endpoint and compare the source,
producer, consumer, and database timestamps.

## Namespace recreation

Deleting a namespace removes that environment's Kafka broker, Kafka PVC,
offsets, and application workloads. It does not delete the external PostgreSQL
database.

Recreate only the desired environment by running its bootstrap command. The
startup reconciliation restores Kafka downtime gaps from the retained database
and remote weather source.
