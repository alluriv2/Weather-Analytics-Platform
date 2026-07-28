#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="weather-platform:0.5.3"
ENVIRONMENT="prod"
START_PORT_FORWARDS=true
BUILD_IMAGE=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --environment)
            if [[ $# -lt 2 ]]; then
                echo "Missing value for --environment." >&2
                exit 2
            fi
            ENVIRONMENT="$2"
            shift 2
            ;;
        --environment=*)
            ENVIRONMENT="${1#*=}"
            shift
            ;;
        --no-port-forward)
            START_PORT_FORWARDS=false
            shift
            ;;
        --skip-build)
            BUILD_IMAGE=false
            shift
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [--environment dev|prod] [--skip-build] [--no-port-forward]" >&2
            exit 2
            ;;
    esac
done

cd "$ROOT_DIR"

case "$ENVIRONMENT" in
    prod)
        NAMESPACE="weather-prod-python"
        OVERLAY_DIR="$ROOT_DIR/deploy/prod"
        ENV_FILE="$ROOT_DIR/.env"
        POSTGRES_DATA_DIR="$ROOT_DIR/local-data/postgres"
        POSTGRES_COMPOSE_FILE="$ROOT_DIR/local/postgres-compose.yaml"
        POSTGRES_COMPOSE_PROJECT="local"
        POSTGRES_CONTAINER="weather-postgres-local"
        POSTGRES_PORT="5433"
        POSTGRES_DB="weather_db_dev"
        DASHBOARD_LOCAL_PORT="18050"
        API_LOCAL_PORT="18000"
        KAFKA_UI_LOCAL_PORT="18080"
        PROMETHEUS_ENABLED=false
        ;;
    dev)
        IMAGE_TAG="weather-platform:prometheus-dev"
        NAMESPACE="weather-dev-python"
        OVERLAY_DIR="$ROOT_DIR/deploy/dev"
        ENV_FILE="$ROOT_DIR/.env.dev"
        POSTGRES_DATA_DIR="$ROOT_DIR/local-data/postgres-dev"
        POSTGRES_COMPOSE_FILE="$ROOT_DIR/local/postgres-compose.dev.yaml"
        POSTGRES_COMPOSE_PROJECT="weather-python-dev"
        POSTGRES_CONTAINER="weather-postgres-python-dev"
        POSTGRES_PORT="5434"
        POSTGRES_DB="weather_db_python_dev"
        DASHBOARD_LOCAL_PORT="28050"
        API_LOCAL_PORT="28000"
        KAFKA_UI_LOCAL_PORT="28080"
        PROMETHEUS_LOCAL_PORT="29090"
        PROMETHEUS_ENABLED=true
        ;;
    *)
        echo "Environment must be either dev or prod; received: $ENVIRONMENT" >&2
        exit 2
        ;;
esac

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

current_context="$(kubectl config current-context 2>/dev/null || true)"

if [[ "$current_context" != "docker-desktop" ]]; then
    echo "Expected Kubernetes context docker-desktop; found: ${current_context:-none}" >&2
    echo "Run: kubectl config use-context docker-desktop" >&2
    exit 1
fi

kubectl cluster-info >/dev/null

POSTGRES_DATABASE_EXISTS=false

if [[ -f "$POSTGRES_DATA_DIR/PG_VERSION" ]]; then
    POSTGRES_DATABASE_EXISTS=true
fi

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

WEATHER_DB_PASSWORD="${POSTGRES_PASSWORD:-${WEATHER_DB_PASSWORD:-}}"

if [[ -z "$WEATHER_DB_PASSWORD" ]]; then
    if [[ "$POSTGRES_DATABASE_EXISTS" == true ]]; then
        echo "PostgreSQL data exists, but POSTGRES_PASSWORD is missing from $ENV_FILE." >&2
        echo "Restore the original password in $ENV_FILE; a new password will not unlock the existing database." >&2
        exit 1
    fi

    read -r -s -p "First-time setup: choose the local PostgreSQL password: " WEATHER_DB_PASSWORD
    echo

    if [[ -z "$WEATHER_DB_PASSWORD" ]]; then
        echo "The PostgreSQL password cannot be empty." >&2
        exit 1
    fi

    umask 077
    {
        echo "POSTGRES_HOST=127.0.0.1"
        echo "POSTGRES_PORT=$POSTGRES_PORT"
        echo "POSTGRES_DB=$POSTGRES_DB"
        echo "POSTGRES_USER=weather_user"
        printf 'POSTGRES_PASSWORD=%q\n' "$WEATHER_DB_PASSWORD"
    } >>"$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "Saved the $ENVIRONMENT database configuration in $ENV_FILE."
fi

export WEATHER_DB_PASSWORD

if [[ "$BUILD_IMAGE" == true ]]; then
    echo
    echo "==> Building $IMAGE_TAG"
    docker build -t "$IMAGE_TAG" .
fi

echo
echo "==> Starting $ENVIRONMENT cluster-independent PostgreSQL"
docker compose \
    --project-name "$POSTGRES_COMPOSE_PROJECT" \
    -f "$POSTGRES_COMPOSE_FILE" \
    up -d

postgres_health=""

for _ in {1..60}; do
    postgres_health="$(
        docker inspect "$POSTGRES_CONTAINER" \
            --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
            2>/dev/null \
            || true
    )"

    if [[ "$postgres_health" == "healthy" ]]; then
        break
    fi

    sleep 2
done

if [[ "$postgres_health" != "healthy" ]]; then
    echo "PostgreSQL did not become healthy." >&2
    docker logs "$POSTGRES_CONTAINER" --tail 80 >&2 || true
    exit 1
fi

echo "PostgreSQL is healthy."

if ! docker run \
    --rm \
    -e PGPASSWORD="$WEATHER_DB_PASSWORD" \
    postgres:17 \
    psql \
    --host host.docker.internal \
    --port "$POSTGRES_PORT" \
    --username weather_user \
    --dbname "$POSTGRES_DB" \
    --no-password \
    --command "SELECT 1;" \
    >/dev/null 2>&1; then
    echo "The POSTGRES_PASSWORD in $ENV_FILE does not authenticate to the existing database." >&2
    echo "No Kubernetes workloads were changed. Restore the original password in $ENV_FILE." >&2
    exit 1
fi

echo "PostgreSQL credentials are valid."

echo
echo "==> Creating $ENVIRONMENT Kubernetes configuration"
kubectl apply -f "$OVERLAY_DIR/namespace.yaml"

kubectl create secret generic postgres-credentials \
    --namespace "$NAMESPACE" \
    --from-literal=POSTGRES_USER=weather_user \
    --from-literal=POSTGRES_PASSWORD="$WEATHER_DB_PASSWORD" \
    --dry-run=client \
    -o yaml \
    | kubectl apply -f -

echo
echo "==> Preparing $ENVIRONMENT deployment"
kubectl delete job initial-backfill \
    --namespace "$NAMESPACE" \
    --ignore-not-found \
    --wait=true

kubectl apply -k "$OVERLAY_DIR"
kubectl patch cronjob weather-reconciler \
    --namespace "$NAMESPACE" \
    --type merge \
    --patch '{"spec":{"suspend":true}}'

echo
echo "==> Starting $ENVIRONMENT Kafka"
kubectl rollout status statefulset/kafka \
    --namespace "$NAMESPACE" \
    --timeout=300s

echo
echo "==> Running $ENVIRONMENT startup reconciliation"

reconciliation_finished=false

for _ in {1..900}; do
    succeeded="$(
        kubectl get job initial-backfill \
            --namespace "$NAMESPACE" \
            --output jsonpath='{.status.succeeded}' \
            2>/dev/null \
            || true
    )"
    failed="$(
        kubectl get job initial-backfill \
            --namespace "$NAMESPACE" \
            --output jsonpath='{.status.failed}' \
            2>/dev/null \
            || true
    )"

    if [[ "${succeeded:-0}" -ge 1 ]]; then
        reconciliation_finished=true
        break
    fi

    if [[ "${failed:-0}" -ge 1 ]]; then
        echo "Startup reconciliation failed:" >&2
        kubectl logs job/initial-backfill \
            --namespace "$NAMESPACE" \
            --all-containers=true \
            --tail=100 \
            >&2 \
            || true
        exit 1
    fi

    sleep 2
done

if [[ "$reconciliation_finished" != true ]]; then
    echo "Startup reconciliation timed out after 30 minutes." >&2
    kubectl logs job/initial-backfill \
        --namespace "$NAMESPACE" \
        --all-containers=true \
        --tail=100 \
        >&2 \
        || true
    exit 1
fi

kubectl logs job/initial-backfill \
    --namespace "$NAMESPACE" \
    --tail=30

echo
echo "==> Starting $ENVIRONMENT continuous workloads"
kubectl patch cronjob weather-reconciler \
    --namespace "$NAMESPACE" \
    --type merge \
    --patch '{"spec":{"suspend":false}}'

kubectl scale \
    deployment/weather-producer \
    deployment/weather-consumer \
    --namespace "$NAMESPACE" \
    --replicas=1

deployments=(
    weather-producer
    weather-consumer
    weather-aggregator
    weather-api
    weather-dashboard
    kafka-ui
)

if [[ "$PROMETHEUS_ENABLED" == true ]]; then
    deployments+=(prometheus)
fi

# Environment variables sourced from Secrets and ConfigMaps are captured when a
# pod starts. Restart existing deployments so a recovered or changed credential
# is applied immediately instead of leaving old pods with stale values.
kubectl rollout restart \
    "${deployments[@]/#/deployment/}" \
    --namespace "$NAMESPACE"

for deployment in "${deployments[@]}"; do
    kubectl rollout status "deployment/$deployment" \
        --namespace "$NAMESPACE" \
        --timeout=180s
done

start_port_forward() {
    local service_name="$1"
    local local_port="$2"
    local service_port="$3"
    local run_directory="$ROOT_DIR/local-data/run-$ENVIRONMENT"
    local pid_file="$run_directory/${service_name}.pid"
    local log_file="$run_directory/${service_name}.log"

    mkdir -p "$run_directory"

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
    echo "==> Opening $ENVIRONMENT local services"
    start_port_forward weather-dashboard "$DASHBOARD_LOCAL_PORT" 8050
    start_port_forward weather-api "$API_LOCAL_PORT" 8000
    start_port_forward kafka-ui "$KAFKA_UI_LOCAL_PORT" 8080
    if [[ "$PROMETHEUS_ENABLED" == true ]]; then
        start_port_forward prometheus "$PROMETHEUS_LOCAL_PORT" 9090
    fi
fi

echo
echo "==> Final status"
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
    --namespace "$NAMESPACE"

if [[ "$START_PORT_FORWARDS" == true ]]; then
    echo
    echo "Dashboard:        http://127.0.0.1:$DASHBOARD_LOCAL_PORT"
    echo "API:              http://127.0.0.1:$API_LOCAL_PORT"
    echo "Ingestion status: http://127.0.0.1:$API_LOCAL_PORT/ingestion-status"
    echo "Kafka UI:         http://127.0.0.1:$KAFKA_UI_LOCAL_PORT"
    if [[ "$PROMETHEUS_ENABLED" == true ]]; then
        echo "Prometheus:       http://127.0.0.1:$PROMETHEUS_LOCAL_PORT"
    fi
fi

echo
echo "Local Weather Platform $ENVIRONMENT bootstrap completed successfully."
