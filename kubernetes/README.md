# Local Kubernetes runbook

The manifests in `kubernetes/` deploy the complete Weather Platform to the
`weather-python` namespace.

## Runtime

| Component | Kubernetes resource | Persistence |
| --- | --- | --- |
| PostgreSQL | Standalone Docker container | `local-data/postgres` |
| Kafka | StatefulSet | Kubernetes PVC |
| Producer | Deployment | Watermark in PostgreSQL |
| Consumer | Deployment | Offset in Kafka and watermark in PostgreSQL |
| Aggregator | Deployment | PostgreSQL |
| API | Deployment and Service | Stateless |
| Dashboard | Deployment and Service | Stateless |
| Kafka UI | Deployment and Service | Stateless |
| Reconciler | CronJob and startup Job | Watermarks in PostgreSQL |
| Prometheus | Deployment and Service | Seven-day in-pod metrics window |
| Grafana | Deployment and Service | Provisioned dashboard |
| kube-state-metrics | Deployment and Service | Stateless |

Docker Desktop Kubernetes runs inside a `kind` node container and cannot
directly mount arbitrary macOS repository folders. PostgreSQL therefore runs
through Docker Compose and mounts `local-data/postgres` directly. Kubernetes
services reach it through `host.docker.internal:5433`.

## Render and validate

```bash
kubectl kustomize kubernetes
kubectl apply --dry-run=client -k kubernetes
```

## Lifecycle

```bash
./start
./report
./stop
```

`./start` always rebuilds `weather-platform:0.5.4`, starts the retained
PostgreSQL container, applies the manifests with application workloads stopped,
then starts components in this order:

```text
PostgreSQL
→ Kafka
→ startup reconciliation
→ producer and consumer
→ aggregator, API, dashboard, and Kafka UI
→ scheduled reconciliation
→ Prometheus and Grafana monitoring
```

`./stop` reverses the dependency order, scales Kafka to zero, and stops
PostgreSQL last. It does not delete the namespace, Kafka PVC, Secret,
PostgreSQL data folder, or images.

## Verify

```bash
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
  --namespace weather-python

docker ps --filter name=weather-postgres-local
```

Monitoring:

```text
Prometheus: http://127.0.0.1:19090
Grafana:    http://127.0.0.1:13000
API metrics: http://127.0.0.1:18000/metrics
```

Prometheus scrapes the weather API, Kubernetes object state, kubelet metrics,
and container metrics. Grafana automatically loads the Weather Platform
dashboard; no manual data-source setup is required.

Kafka consumer lag:

```bash
kubectl exec --namespace weather-python kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group weather-postgres-consumer-python \
  --describe
```

Database watermarks:

```bash
docker exec weather-postgres-local \
  psql --username weather_user --dbname weather_db \
  --command "SELECT * FROM ingestion_state ORDER BY station;"
```

## Recovery

Normal restart:

```text
Retained PostgreSQL and Kafka PVC
→ reconcile timestamps
→ resume live ingestion
```

Kafka storage loss:

```text
Retained PostgreSQL
→ compare database timestamp with source
→ repair the missing interval
→ resume Kafka processing
```

PostgreSQL storage loss:

```text
Initialize a new database
→ run full historical backfill
→ start live ingestion
```

If Kubernetes is completely recreated, Kafka messages and offsets may be lost.
The retained PostgreSQL watermarks let startup reconciliation repair the
missing interval before live ingestion resumes.
