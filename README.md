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
./start
```

The command performs the complete production startup in dependency order:

1. Builds the current application image.
2. Starts the standalone PostgreSQL service and verifies its credentials.
3. Creates or updates the production Kubernetes resources.
4. Starts Kafka and runs the initial or incremental data reconciliation.
5. Starts the producer and consumer.
6. Starts the aggregator, API, dashboard, and Kafka UI.
7. Opens the local service addresses.

On the first run, the startup creates the production database and performs a
full historical backfill. Later runs retain that database and reconcile only
records missing since the last stored timestamp.

Production service addresses:

| Service | Address |
| --- | --- |
| Dashboard | <http://127.0.0.1:18050> |
| API | <http://127.0.0.1:18000> |
| API documentation | <http://127.0.0.1:18000/docs> |
| Ingestion status | <http://127.0.0.1:18000/ingestion-status> |
| Kafka UI | <http://127.0.0.1:18080> |

Check production at any time without changing it:

```bash
./report
```

Stop production:

```bash
./stop
```

Stopping closes the local service addresses, stops the Kubernetes workloads,
and stops PostgreSQL. It retains the production database, Kafka storage,
configuration, and container images so that the next `./start` resumes safely.

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

Development has its own database and Kubernetes namespace and remains available
through the detailed bootstrap command.

## Bootstrap options

Reuse the existing application image:

```bash
./start --skip-build
```

Deploy without opening local service addresses:

```bash
./start --no-port-forward
```

The long-form bootstrap script remains available for development and advanced
operations.

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
├── start                       # Start all production components in order
├── stop                        # Stop production while retaining its data
├── report                      # Read-only production health summary
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
