#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="weather-platform:0.5.3"
NAMESPACE="weather-dev"
START_PORT_FORWARDS=true
BUILD_IMAGE=true

for argument in "$@"; do
    case "$argument" in
        --no-port-forward)
            START_PORT_FORWARDS=false
            ;;
        --skip-build)
            BUILD_IMAGE=false
            ;;
        *)
            echo "Unknown argument: $argument" >&2
            echo "Usage: $0 [--skip-build] [--no-port-forward]" >&2
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

current_context="$(kubectl config current-context 2>/dev/null || true)"

if [[ "$current_context" != "docker-desktop" ]]; then
    echo "Expected Kubernetes context docker-desktop; found: ${current_context:-none}" >&2
    echo "Run: kubectl config use-context docker-desktop" >&2
    exit 1
fi

kubectl cluster-info >/dev/null

if [[ -z "${WEATHER_DB_PASSWORD:-}" ]]; then
    read -r -s -p "Enter the local PostgreSQL password: " WEATHER_DB_PASSWORD
    echo
fi

if [[ -z "$WEATHER_DB_PASSWORD" ]]; then
    echo "The PostgreSQL password cannot be empty." >&2
    exit 1
fi

export WEATHER_DB_PASSWORD

if [[ "$BUILD_IMAGE" == true ]]; then
    echo
    echo "==> Building $IMAGE_TAG"
    docker build -t "$IMAGE_TAG" .
fi

echo
echo "==> Starting cluster-independent PostgreSQL"
docker compose -f local/postgres-compose.yaml up -d

postgres_health=""

for _ in {1..60}; do
    postgres_health="$(
        docker inspect weather-postgres-local \
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
    docker logs weather-postgres-local --tail 80 >&2 || true
    exit 1
fi

echo "PostgreSQL is healthy."

echo
echo "==> Creating Kubernetes configuration"
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/configmap.yaml

kubectl create secret generic postgres-credentials \
    --namespace "$NAMESPACE" \
    --from-literal=POSTGRES_USER=weather_user \
    --from-literal=POSTGRES_PASSWORD="$WEATHER_DB_PASSWORD" \
    --dry-run=client \
    -o yaml \
    | kubectl apply -f -

echo
echo "==> Starting Kafka"
kubectl apply \
    -f kubernetes/kafka/service.yaml \
    -f kubernetes/kafka/statefulset.yaml

kubectl rollout status statefulset/kafka \
    --namespace "$NAMESPACE" \
    --timeout=300s

echo
echo "==> Running startup reconciliation"
kubectl delete job initial-backfill \
    --namespace "$NAMESPACE" \
    --ignore-not-found \
    --wait=true

kubectl apply -f kubernetes/producer/initial-backfill-job.yaml

kubectl wait \
    --for=condition=complete \
    job/initial-backfill \
    --namespace "$NAMESPACE" \
    --timeout=1800s

kubectl logs job/initial-backfill \
    --namespace "$NAMESPACE" \
    --tail=30

echo
echo "==> Starting continuous workloads"
kubectl apply -f kubernetes/reconciler/cronjob.yaml
kubectl apply \
    -f kubernetes/producer/deployment.yaml \
    -f kubernetes/consumer/deployment.yaml \
    -f kubernetes/aggregator/deployment.yaml
kubectl apply \
    -f kubernetes/api/service.yaml \
    -f kubernetes/api/deployment.yaml
kubectl apply \
    -f kubernetes/dashboard/service.yaml \
    -f kubernetes/dashboard/deployment.yaml
kubectl apply \
    -f kubernetes/kafka-ui/service.yaml \
    -f kubernetes/kafka-ui/deployment.yaml

deployments=(
    weather-producer
    weather-consumer
    weather-aggregator
    weather-api
    weather-dashboard
    kafka-ui
)

for deployment in "${deployments[@]}"; do
    kubectl rollout status "deployment/$deployment" \
        --namespace "$NAMESPACE" \
        --timeout=180s
done

start_port_forward() {
    local service_name="$1"
    local local_port="$2"
    local service_port="$3"
    local run_directory="$ROOT_DIR/local-data/run"
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
    echo "==> Opening local services"
    start_port_forward weather-dashboard 18050 8050
    start_port_forward weather-api 18000 8000
    start_port_forward kafka-ui 18080 8080
fi

echo
echo "==> Final status"
kubectl get deployments,statefulsets,pods,cronjobs,jobs,pvc \
    --namespace "$NAMESPACE"

if [[ "$START_PORT_FORWARDS" == true ]]; then
    echo
    echo "Dashboard:        http://127.0.0.1:18050"
    echo "API:              http://127.0.0.1:18000"
    echo "Ingestion status: http://127.0.0.1:18000/ingestion-status"
    echo "Kafka UI:         http://127.0.0.1:18080"
fi

echo
echo "Local Weather Platform bootstrap completed successfully."
