#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="weather-platform:0.7.0"
NAMESPACE="weather-python"
ENV_FILE="$ROOT_DIR/.env"
POSTGRES_DATA_DIR="$ROOT_DIR/local-data/postgres"
RUN_DIRECTORY="$ROOT_DIR/local-data/run"
POSTGRES_TEMPLATE="$ROOT_DIR/local/postgres-statefulset.template.yaml"
POSTGRES_MANIFEST="$RUN_DIRECTORY/postgres-statefulset.yaml"
POSTGRES_DB="weather_db"
POSTGRES_USER="weather_user"
KAFKA_GROUP="weather-postgres-consumer-python"
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

if [[ -f "$POSTGRES_DATA_DIR/pgdata/PG_VERSION" ]]; then
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
    "$RUN_DIRECTORY"
touch "$POSTGRES_DATA_DIR/.weather-host-volume"

stop_registered_port_forwards() {
    local pid_file

    for pid_file in "$RUN_DIRECTORY"/*.pid; do
        [[ -e "$pid_file" ]] || continue

        local process_id
        process_id="$(cat "$pid_file" 2>/dev/null || true)"

        if [[ "$process_id" =~ ^[0-9]+$ ]] \
            && kill -0 "$process_id" 2>/dev/null; then
            local process_command
            process_command="$(ps -p "$process_id" -o command= 2>/dev/null || true)"

            if [[ "$process_command" == *"kubectl port-forward"* ]] \
                && [[ "$process_command" == *"$NAMESPACE"* ]]; then
                kill "$process_id"
            fi
        fi

        rm -f "$pid_file"
    done
}

echo
echo "==> Building the application image from the current source"
docker build -t "$IMAGE_TAG" .

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

echo
echo "==> Applying the unified application configuration"
kubectl apply -k "$ROOT_DIR/kubernetes"

if [[ "$(uname -s)" == "Darwin" ]]; then
    postgres_node_path="/run/desktop/mnt/host${POSTGRES_DATA_DIR}"
else
    postgres_node_path="$POSTGRES_DATA_DIR"
fi

escaped_postgres_node_path="$(
    printf '%s' "$postgres_node_path" \
        | sed 's/[&|]/\\&/g'
)"
sed \
    "s|__POSTGRES_HOST_PATH__|$escaped_postgres_node_path|" \
    "$POSTGRES_TEMPLATE" \
    >"$POSTGRES_MANIFEST"
kubectl apply -f "$POSTGRES_MANIFEST"

kubectl patch cronjob weather-reconciler \
    --namespace "$NAMESPACE" \
    --type merge \
    --patch '{"spec":{"suspend":true}}'

echo
echo "==> Starting PostgreSQL in Kubernetes"
kubectl scale statefulset/postgres \
    --namespace "$NAMESPACE" \
    --replicas=1

if ! kubectl rollout status statefulset/postgres \
    --namespace "$NAMESPACE" \
    --timeout=300s; then
    kubectl logs postgres-0 \
        --namespace "$NAMESPACE" \
        --tail=100 \
        >&2 \
        || true
    exit 1
fi

echo
echo "==> Starting Kafka"
kubectl scale statefulset/kafka \
    --namespace "$NAMESPACE" \
    --replicas=1

kubectl rollout status statefulset/kafka \
    --namespace "$NAMESPACE" \
    --timeout=300s

kubectl exec \
    --namespace "$NAMESPACE" \
    kafka-0 \
    -- \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$KAFKA_TOPIC" \
    --partitions 1 \
    --replication-factor 1

echo
echo "==> Starting the Kafka-to-PostgreSQL consumer"
kubectl scale deployment/weather-consumer \
    --namespace "$NAMESPACE" \
    --replicas=1

kubectl rollout status deployment/weather-consumer \
    --namespace "$NAMESPACE" \
    --timeout=180s

echo
echo "==> Publishing full or incremental backfill through Kafka"
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
echo "==> Waiting for the consumer to commit every backfill event"
backfill_drained=false

for _ in {1..900}; do
    current_lag="$(
        kubectl exec \
            --namespace "$NAMESPACE" \
            kafka-0 \
            -- \
            /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 \
            --describe \
            --group "$KAFKA_GROUP" \
            2>/dev/null \
            | awk -v topic="$KAFKA_TOPIC" \
                '$2 == topic && $6 ~ /^[0-9]+$/ {lag += $6; found = 1} END {if (found) print lag; else print -1}'
    )"

    if [[ "$current_lag" == "0" ]]; then
        backfill_drained=true
        break
    fi

    sleep 2
done

if [[ "$backfill_drained" != true ]]; then
    echo "The Kafka backfill was published, but the consumer did not drain it." >&2
    kubectl logs deployment/weather-consumer \
        --namespace "$NAMESPACE" \
        --tail=120 \
        >&2 \
        || true
    exit 1
fi

database_rows="$(
    kubectl exec \
        --namespace "$NAMESPACE" \
        postgres-0 \
        -- \
        psql \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" \
        --tuples-only \
        --no-align \
        --command "SELECT COUNT(*) FROM weather;"
)"

if [[ ! "$database_rows" =~ ^[0-9]+$ ]] \
    || [[ "$database_rows" -eq 0 ]]; then
    echo "Backfill validation failed: the weather table is empty." >&2
    exit 1
fi

echo "Kafka backfill consumed successfully: $database_rows database rows."

echo
echo "==> Starting live ingestion"
kubectl scale deployment/weather-producer \
    --namespace "$NAMESPACE" \
    --replicas=1

kubectl rollout status deployment/weather-producer \
    --namespace "$NAMESPACE" \
    --timeout=180s

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

echo
echo "==> Starting monitoring"
kubectl scale \
    deployment/kube-state-metrics \
    deployment/prometheus \
    deployment/grafana \
    --namespace "$NAMESPACE" \
    --replicas=1

for deployment in kube-state-metrics prometheus grafana; do
    kubectl rollout status "deployment/$deployment" \
        --namespace "$NAMESPACE" \
        --timeout=180s
done

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
        </dev/null \
        >"$log_file" 2>&1 &

    local port_forward_pid=$!
    echo "$port_forward_pid" >"$pid_file"
    disown "$port_forward_pid" 2>/dev/null || true

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
    echo "==> Refreshing local service connections"
    stop_registered_port_forwards

    echo
    echo "==> Opening local services"
    port_forward_failures=0

    start_port_forward weather-dashboard 18050 8050 \
        || port_forward_failures=$((port_forward_failures + 1))
    start_port_forward weather-api 18000 8000 \
        || port_forward_failures=$((port_forward_failures + 1))
    start_port_forward kafka-ui 18080 8080 \
        || port_forward_failures=$((port_forward_failures + 1))
    start_port_forward prometheus 19090 9090 \
        || port_forward_failures=$((port_forward_failures + 1))
    start_port_forward grafana 13000 3000 \
        || port_forward_failures=$((port_forward_failures + 1))

    if [[ "$port_forward_failures" -gt 0 ]]; then
        echo "$port_forward_failures local service connection(s) failed to open." >&2
        echo "The Kubernetes services are still running; inspect local-data/run/*.log." >&2
    fi
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
    echo "Prometheus:       http://127.0.0.1:19090"
    echo "Grafana:          http://127.0.0.1:13000"
fi

echo
echo "Weather Platform startup completed successfully."
