# Real-Time Weather Analytics Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestrated-326CE5)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-4.3-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-REST-009688)
![Plotly Dash](https://img.shields.io/badge/Plotly-Dash-purple)

A real-time weather ingestion and analytics platform with isolated development
and production environments. Kubernetes runs Kafka and the application
services, while PostgreSQL remains outside Kubernetes so weather history
survives cluster shutdown and recreation.

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
                                                   ▼
                                             Dashboard
```

Each environment has its own Kafka broker, topic, consumer group, PostgreSQL
database, Kubernetes resources, and local service addresses.

| Environment | Namespace | Kafka topic |
| --- | --- | --- |
| Development | `weather-dev-python` | `raw_weather_events_python_dev` |
| Production | `weather-prod-python` | `raw_weather_events_python_prod` |

Development and production do not share messages, offsets, databases, secrets,
or persistent volumes.

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

## Start production

```bash
./scripts/bootstrap-local.sh --environment prod
```

Production service addresses:

| Service | Address |
| --- | --- |
| Dashboard | <http://127.0.0.1:18050> |
| API | <http://127.0.0.1:18000> |
| API documentation | <http://127.0.0.1:18000/docs> |
| Ingestion status | <http://127.0.0.1:18000/ingestion-status> |
| Kafka UI | <http://127.0.0.1:18080> |

## Start development

```bash
./scripts/bootstrap-local.sh --environment dev
```

Development service addresses:

| Service | Address |
| --- | --- |
| Dashboard | <http://127.0.0.1:28050> |
| API | <http://127.0.0.1:28000> |
| API documentation | <http://127.0.0.1:28000/docs> |
| Ingestion status | <http://127.0.0.1:28000/ingestion-status> |
| Kafka UI | <http://127.0.0.1:28080> |

The first start creates the environment's independent database and performs a
full historical backfill. Later starts reuse that database and reconcile only
missing records.

## Bootstrap options

Reuse the existing application image:

```bash
./scripts/bootstrap-local.sh --environment dev --skip-build
```

Deploy without opening local service addresses:

```bash
./scripts/bootstrap-local.sh --environment prod --no-port-forward
```

If `--environment` is omitted, the script starts production.

## Verify the environments

Production:

```bash
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
  --namespace weather-prod-python
```

Development:

```bash
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
  --namespace weather-dev-python
```

Check the independent databases:

```bash
docker ps \
  --filter name=weather-postgres-local \
  --filter name=weather-postgres-python-dev
```

Healthy continuous workloads show `Running`; completed backfill and
reconciliation jobs show `Completed`.

## Recovery behavior

Stopping Kubernetes interrupts Kafka and the application services but does not
erase either PostgreSQL database. On restart, run the bootstrap for the desired
environment. It recreates missing Kubernetes resources, compares timestamps,
repairs the missed interval, and resumes live processing.

Deleting one namespace affects only that environment:

```bash
kubectl delete namespace weather-dev-python
```

The production namespace and both external PostgreSQL databases remain
untouched.

## Repository layout

```text
.
├── dashboard/                   # Plotly Dash application
├── deploy/
│   ├── dev/                     # Development namespace overrides
│   └── prod/                    # Production namespace overrides
├── kubernetes/                  # Namespace-neutral Kubernetes resources
├── local/
│   ├── postgres-compose.yaml
│   └── postgres-compose.dev.yaml
├── scripts/
│   └── bootstrap-local.sh
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
