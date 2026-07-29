# Local Kubernetes runbook

The manifests in `kubernetes/` deploy the complete Weather Platform to the
`weather-python` namespace.

## Runtime

| Component | Kubernetes resource | Persistence |
| --- | --- | --- |
| PostgreSQL | StatefulSet | `local-data/postgres` |
| Kafka | StatefulSet | `local-data/kafka` |
| Producer | Deployment | Watermark in PostgreSQL |
| Consumer | Deployment | Offset in Kafka and watermark in PostgreSQL |
| Aggregator | Deployment | PostgreSQL |
| API | Deployment and Service | Stateless |
| Dashboard | Deployment and Service | Stateless |
| Kafka UI | Deployment and Service | Stateless |
| Reconciler | CronJob and startup Job | Watermarks in PostgreSQL |

The lifecycle script creates retained PersistentVolumes whose host paths point
to the current clone's `local-data` directories. The paths are generated at
runtime so the repository does not contain a user-specific absolute path.

## Render and validate

```bash
kubectl kustomize kubernetes
kubectl apply --dry-run=client -k kubernetes
```

The persistent volumes are intentionally created by `./start`, not by the
static Kustomize bundle, because their host paths depend on the clone location.

## Lifecycle

```bash
./start
./report
./stop
```

`./start` always rebuilds `weather-platform:0.5.3`, creates or reconnects the
retained volumes, applies the manifests with application workloads stopped,
then starts components in this order:

```text
PostgreSQL
→ Kafka
→ startup reconciliation
→ producer and consumer
→ aggregator, API, dashboard, and Kafka UI
→ scheduled reconciliation
```

`./stop` reverses the dependency order and scales the two StatefulSets to zero.
It does not delete the namespace, PersistentVolumes, PersistentVolumeClaims,
Secrets, local data folders, or images.

## Verify

```bash
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
  --namespace weather-python

kubectl get persistentvolumes \
  weather-python-postgres-data \
  weather-python-kafka-data
```

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
Retained PostgreSQL and Kafka storage
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

If a retained PersistentVolume is in `Released` state after namespace
recreation, `./start` clears its old claim reference before recreating the
claim. The underlying host data is not removed.
