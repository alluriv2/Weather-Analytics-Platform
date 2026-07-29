# Local Kubernetes runbook

The manifests in `kubernetes/` deploy the complete Weather Platform to the
`weather-python` namespace.

## Runtime

| Component | Kubernetes resource | Persistence |
| --- | --- | --- |
| PostgreSQL | StatefulSet | Host-mounted `local-data/postgres` |
| Kafka | StatefulSet | Kubernetes PVC |
| Producer | Deployment | Watermark in PostgreSQL |
| Consumer | Deployment | Offset in Kafka and watermark in PostgreSQL |
| Aggregator | Deployment | PostgreSQL |
| API | Deployment and Service | Stateless |
| Dashboard | Deployment and Service | Stateless |
| Kafka UI | Deployment and Service | Stateless |
| Reconciler | CronJob and startup Job | Publishes repair events to Kafka |
| Prometheus | Deployment and Service | Seven-day in-pod metrics window |
| Grafana | Deployment and Service | Provisioned dashboard |
| kube-state-metrics | Deployment and Service | Stateless |

At startup, the local PostgreSQL StatefulSet is generated from
`local/postgres-statefulset.template.yaml`. Its `hostPath` points through
Docker Desktop's shared host filesystem to this clone's
`local-data/postgres` folder. The pod lifecycle is controlled by Kubernetes;
the database files are controlled by the user.

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

`./start` always rebuilds `weather-platform:0.7.0`, generates the local
PostgreSQL manifest, applies workloads in a stopped state, then starts:

```text
PostgreSQL
→ Kafka
→ consumer
→ backfill publisher
→ wait for zero consumer lag and validate PostgreSQL
→ live producer
→ aggregator, API, dashboard, and Kafka UI
→ scheduled reconciliation
→ Prometheus and Grafana monitoring
```

`./stop` reverses the dependency order and scales PostgreSQL down last. It does
not delete the namespace, Kafka PVC, Secret, PostgreSQL data folder, or images.

## Verify

```bash
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
  --namespace weather-python

kubectl get statefulset/postgres pod/postgres-0 --namespace weather-python
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
kubectl exec --namespace weather-python postgres-0 -- \
  psql --username weather_user --dbname weather_db \
  --command "SELECT * FROM ingestion_state ORDER BY station;"
```

## Recovery

Normal restart:

```text
Retained PostgreSQL and Kafka PVC
→ publish an overlapping repair window to Kafka
→ consume and upsert the repair events
→ resume live ingestion
```

Kafka storage loss:

```text
Retained PostgreSQL
→ compare database timestamp with source
→ publish the missing interval to Kafka
→ consume and upsert it
→ resume Kafka processing
```

PostgreSQL storage loss:

```text
Initialize a new database
→ publish full history to Kafka
→ consume it into PostgreSQL
→ start live ingestion
```

If Kubernetes is completely recreated, Kafka messages and offsets may be lost.
The retained PostgreSQL timestamp lets startup publish an overlapping repair
window before live ingestion resumes.
