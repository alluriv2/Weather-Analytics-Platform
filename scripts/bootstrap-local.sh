#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="weather-platform:0.5.3"
NAMESPACE="weather-python"
ENV_FILE="$ROOT_DIR/.env"
POSTGRES_DATA_DIR="$ROOT_DIR/local-data/postgres"
KAFKA_DATA_DIR="$ROOT_DIR/local-data/kafka"
RUN_DIRECTORY="$ROOT_DIR/local-data/run"
POSTGRES_PV="weather-python-postgres-data"
KAFKA_PV="weather-python-kafka-data"
POSTGRES_DB="weather_db"
POSTGRES_USER="weather_user"
KAFKA_TOPIC="raw_weather_events_python"
START_PORT_FORWARDS=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-port-forward)
            START_PORT_FORWARDS=false
            shift
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [--no-port-forward]" >&2
            exit 2
            ;;
    esac
done

cd "$ROOT_DIR"

required_commands=(
    docker
    kubectl
    curl
)

for command_name in "${required_commands[@]}"; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Required command not found: $command_name" >&2
        exit 1
    fi
done

if ! docker info >/dev/null 2>&1; then
    echo "Docker Desktop is not running." >&2
    exit 1
fi

current_context="$(kubectl config current-context 2>/dev/null || true)"

if [[ "$current_context" != "docker-desktop" ]]; then
    echo "Expected Kubernetes context docker-desktop; found: ${current_context:-none}" >&2
    echo "Run: kubectl config use-context docker-desktop" >&2
    exit 1
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "Docker Desktop Kubernetes is unavailable." >&2
    exit 1
fi

postgres_database_exists=false

if [[ -f "$POSTGRES_DATA_DIR/PG_VERSION" ]]; then
    postgres_database_exists=true
fi

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

WEATHER_DB_PASSWORD="${POSTGRES_PASSWORD:-${WEATHER_DB_PASSWORD:-}}"
POSTGRES_DB="weather_db"
POSTGRES_USER="weather_user"

if [[ -z "$WEATHER_DB_PASSWORD" ]]; then
    if [[ "$postgres_database_exists" == true ]]; then
        echo "PostgreSQL data exists, but POSTGRES_PASSWORD is missing from $ENV_FILE." >&2
        echo "Restore the original password; a new password cannot unlock the existing database." >&2
        exit 1
    fi

    if [[ ! -t 0 ]]; then
        echo "First-time setup requires an interactive terminal." >&2
        echo "Run ./start directly in Terminal to choose the database password." >&2
        exit 1
    fi

    read -r -s -p "First-time setup: choose the PostgreSQL password: " WEATHER_DB_PASSWORD
    echo

    if [[ -z "$WEATHER_DB_PASSWORD" ]]; then
        echo "The PostgreSQL password cannot be empty." >&2
        exit 1
    fi

    umask 077
    {
        echo "POSTGRES_DB=$POSTGRES_DB"
        echo "POSTGRES_USER=$POSTGRES_USER"
        printf 'POSTGRES_PASSWORD=%q\n' "$WEATHER_DB_PASSWORD"
    } >>"$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "Saved local database credentials in $ENV_FILE."
fi

mkdir -p \
    "$POSTGRES_DATA_DIR" \
    "$KAFKA_DATA_DIR" \
    "$RUN_DIRECTORY"

echo
echo "==> Building the application image from the current source"
docker build -t "$IMAGE_TAG" .

for legacy_postgres_container in \
    weather-postgres-local \
    weather-postgres-python-dev; do
    if docker inspect "$legacy_postgres_container" >/dev/null 2>&1; then
        legacy_postgres_running="$(
            docker inspect "$legacy_postgres_container" \
                --format '{{.State.Running}}'
        )"

        if [[ "$legacy_postgres_running" == "true" ]]; then
            echo
            echo "==> Stopping legacy container $legacy_postgres_container"
            docker stop "$legacy_postgres_container" >/dev/null
        fi
    fi
done

echo
echo "==> Creating the unified Kubernetes namespace and credentials"
kubectl apply -f "$ROOT_DIR/kubernetes/namespace.yaml"

kubectl create secret generic postgres-credentials \
    --namespace "$NAMESPACE" \
    --from-literal=POSTGRES_USER="$POSTGRES_USER" \
    --from-literal=POSTGRES_PASSWORD="$WEATHER_DB_PASSWORD" \
    --dry-run=client \
    --output yaml \
    | kubectl apply -f -

release_retained_volume() {
    local volume_name="$1"
    local phase

    phase="$(
        kubectl get persistentvolume "$volume_name" \
            --output jsonpath='{.status.phase}' \
            2>/dev/null \
            || true
    )"

    if [[ "$phase" == "Released" ]]; then
        kubectl patch persistentvolume "$volume_name" \
            --type json \
            --patch '[{"op":"remove","path":"/spec/claimRef"}]'
    fi
}

release_retained_volume "$POSTGRES_PV"
release_retained_volume "$KAFKA_PV"

echo
echo "==> Connecting Kubernetes storage to repository-local data"
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: $POSTGRES_PV
  labels:
    app.kubernetes.io/name: postgres
    app.kubernetes.io/part-of: weather-platform
spec:
  capacity:
    storage: 20Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: weather-local
  hostPath:
    path: "$POSTGRES_DATA_DIR"
    type: Directory
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-data
  namespace: $NAMESPACE
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: weather-local
  volumeName: $POSTGRES_PV
  resources:
    requests:
      storage: 20Gi
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: $KAFKA_PV
  labels:
    app.kubernetes.io/name: kafka
    app.kubernetes.io/part-of: weather-platform
spec:
  capacity:
    storage: 20Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: weather-local
  hostPath:
    path: "$KAFKA_DATA_DIR"
    type: Directory
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kafka-data
  namespace: $NAMESPACE
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: weather-local
  volumeName: $KAFKA_PV
  resources:
    requests:
      storage: 20Gi
EOF

echo
echo "==> Applying the unified application configuration"
kubectl apply -k "$ROOT_DIR/kubernetes"

kubectl patch cronjob weather-reconciler \
    --namespace "$NAMESPACE" \
    --type merge \
    --patch '{"spec":{"suspend":true}}'

echo
echo "==> Starting PostgreSQL"
kubectl scale statefulset/postgres \
    --namespace "$NAMESPACE" \
    --replicas=1

kubectl rollout status statefulset/postgres \
    --namespace "$NAMESPACE" \
    --timeout=300s

database_exists="$(
    kubectl exec \
        --namespace "$NAMESPACE" \
        postgres-0 \
        -- \
        env "PGPASSWORD=$WEATHER_DB_PASSWORD" \
        psql \
        --host 127.0.0.1 \
        --username "$POSTGRES_USER" \
        --dbname postgres \
        --tuples-only \
        --no-align \
        --command "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB';" \
        2>/dev/null \
        || true
)"

legacy_database_exists="$(
    kubectl exec \
        --namespace "$NAMESPACE" \
        postgres-0 \
        -- \
        env "PGPASSWORD=$WEATHER_DB_PASSWORD" \
        psql \
        --host 127.0.0.1 \
        --username "$POSTGRES_USER" \
        --dbname postgres \
        --tuples-only \
        --no-align \
        --command "SELECT 1 FROM pg_database WHERE datname = 'weather_db_dev';" \
        2>/dev/null \
        || true
)"

if [[ "$database_exists" != "1" && "$legacy_database_exists" == "1" ]]; then
    echo "==> Renaming the retained database from weather_db_dev to weather_db"
    kubectl exec \
        --namespace "$NAMESPACE" \
        postgres-0 \
        -- \
        env "PGPASSWORD=$WEATHER_DB_PASSWORD" \
        psql \
        --host 127.0.0.1 \
        --username "$POSTGRES_USER" \
        --dbname postgres \
        --command "ALTER DATABASE weather_db_dev RENAME TO weather_db;"
fi

if ! kubectl exec \
    --namespace "$NAMESPACE" \
    postgres-0 \
    -- \
    env "PGPASSWORD=$WEATHER_DB_PASSWORD" \
    psql \
    --host 127.0.0.1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --no-password \
    --command "SELECT 1;" \
    >/dev/null 2>&1; then
    echo "PostgreSQL started, but the configured credentials could not open the database." >&2
    echo "Restore the password originally used to initialize local-data/postgres." >&2
    exit 1
fi

echo "PostgreSQL is ready."

echo
echo "==> Starting Kafka"
kubectl scale statefulset/kafka \
    --namespace "$NAMESPACE" \
    --replicas=1

kubectl rollout status statefulset/kafka \
    --namespace "$NAMESPACE" \
    --timeout=300s

echo
echo "==> Running full or incremental database reconciliation"
kubectl delete job initial-backfill \
    --namespace "$NAMESPACE" \
    --ignore-not-found \
    --wait=true

kubectl create job initial-backfill \
    --namespace "$NAMESPACE" \
    --from=cronjob/weather-reconciler

if ! kubectl wait \
    --namespace "$NAMESPACE" \
    --for=condition=complete \
    job/initial-backfill \
    --timeout=1800s; then
    echo "Startup reconciliation failed or timed out:" >&2
    kubectl logs job/initial-backfill \
        --namespace "$NAMESPACE" \
        --all-containers=true \
        --tail=120 \
        >&2 \
        || true
    exit 1
fi

kubectl logs job/initial-backfill \
    --namespace "$NAMESPACE" \
    --tail=30

echo
echo "==> Starting ingestion"
kubectl scale \
    deployment/weather-producer \
    deployment/weather-consumer \
    --namespace "$NAMESPACE" \
    --replicas=1

for deployment in weather-producer weather-consumer; do
    kubectl rollout status "deployment/$deployment" \
        --namespace "$NAMESPACE" \
        --timeout=180s
done

echo
echo "==> Starting application services"
kubectl scale \
    deployment/weather-aggregator \
    deployment/weather-api \
    deployment/weather-dashboard \
    deployment/kafka-ui \
    --namespace "$NAMESPACE" \
    --replicas=1

for deployment in \
    weather-aggregator \
    weather-api \
    weather-dashboard \
    kafka-ui; do
    kubectl rollout status "deployment/$deployment" \
        --namespace "$NAMESPACE" \
        --timeout=180s
done

echo
echo "==> Enabling scheduled reconciliation"
kubectl patch cronjob weather-reconciler \
    --namespace "$NAMESPACE" \
    --type merge \
    --patch '{"spec":{"suspend":false}}'

start_port_forward() {
    local service_name="$1"
    local local_port="$2"
    local service_port="$3"
    local pid_file="$RUN_DIRECTORY/${service_name}.pid"
    local log_file="$RUN_DIRECTORY/${service_name}.log"

    if [[ -f "$pid_file" ]]; then
        local existing_pid
        existing_pid="$(cat "$pid_file")"

        if kill -0 "$existing_pid" 2>/dev/null; then
            echo "Port-forward for $service_name is already running."
            return 0
        fi

        rm -f "$pid_file"
    fi

    if command -v lsof >/dev/null 2>&1 \
        && lsof -nP -iTCP:"$local_port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "Port $local_port is already in use; not replacing its process." >&2
        return 1
    fi

    nohup kubectl port-forward \
        --namespace "$NAMESPACE" \
        "service/$service_name" \
        "$local_port:$service_port" \
        >"$log_file" 2>&1 &

    local port_forward_pid=$!
    echo "$port_forward_pid" >"$pid_file"

    for _ in {1..30}; do
        if grep -q "Forwarding from" "$log_file" 2>/dev/null; then
            return 0
        fi

        if ! kill -0 "$port_forward_pid" 2>/dev/null; then
            cat "$log_file" >&2
            return 1
        fi

        sleep 1
    done

    echo "Timed out starting port-forward for $service_name." >&2
    cat "$log_file" >&2
    return 1
}

if [[ "$START_PORT_FORWARDS" == true ]]; then
    echo
    echo "==> Opening local services"
    start_port_forward weather-dashboard 18050 8050
    start_port_forward weather-api 18000 8000
    start_port_forward kafka-ui 18080 8080
fi

echo
echo "==> Final status"
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
    --namespace "$NAMESPACE"

echo
echo "Kafka topic:      $KAFKA_TOPIC"

if [[ "$START_PORT_FORWARDS" == true ]]; then
    echo "Dashboard:        http://127.0.0.1:18050"
    echo "API:              http://127.0.0.1:18000"
    echo "Ingestion status: http://127.0.0.1:18000/ingestion-status"
    echo "Kafka UI:         http://127.0.0.1:18080"
fi

echo
echo "Weather Platform startup completed successfully."
