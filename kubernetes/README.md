# Weather Platform on local Kubernetes

This directory runs the development Weather Analytics Platform in the
Docker Desktop Kubernetes cluster. It does not replace the Docker Compose
production-like instance in `../prod`.

## Architecture

The application uses one locally built Python image with different commands:

- `weather-producer` publishes new weather observations to Kafka.
- `weather-consumer` writes observations to PostgreSQL and commits Kafka
  consumer-group offsets after each successful database transaction.
- `weather-aggregator` refreshes the day, week, month, and year tables.
- `weather-api` serves FastAPI endpoints on the internal `weather-api:8000`
  Service.
- `weather-dashboard` serves Dash on the internal
  `weather-dashboard:8050` Service.

Kafka and PostgreSQL run as single-node StatefulSets for local development.
Kafka UI is read-only.

## Safety check

Confirm the local context before every deployment:

```bash
kubectl config current-context
```

It must print:

```text
docker-desktop
```

Do not continue if another context is active.

## Build the local application image

From the repository root:

```bash
docker build -t weather-platform:0.4.0 .
```

The manifests use `imagePullPolicy: IfNotPresent`, so Docker Desktop can use
this local image. A remote Kubernetes cluster cannot use the image until it is
pushed to a registry and the manifest image references are updated.

## Create the development Secret

The real password is deliberately absent from Git.

```bash
kubectl apply -f kubernetes/namespace.yaml

read -s "WEATHER_DEV_DB_PASSWORD?Enter the development PostgreSQL password: "
echo

kubectl create secret generic postgres-credentials \
  --namespace weather-dev \
  --from-literal=POSTGRES_USER=weather_user \
  --from-literal=POSTGRES_PASSWORD="$WEATHER_DEV_DB_PASSWORD"

unset WEATHER_DEV_DB_PASSWORD
```

Do not display a Secret with `-o yaml`; Kubernetes Secret data is Base64
encoded rather than concealed.

## First deployment

Apply infrastructure and configuration first:

```bash
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/postgres
kubectl apply -f kubernetes/kafka
kubectl apply -f kubernetes/producer/state-pvc.yaml

kubectl rollout status statefulset/postgres -n weather-dev
kubectl rollout status statefulset/kafka -n weather-dev
```

Run the one-time historical backfill:

```bash
kubectl apply -f kubernetes/producer/initial-backfill-job.yaml
kubectl wait \
  --for=condition=complete \
  job/initial-backfill \
  --namespace weather-dev \
  --timeout=1800s

kubectl logs job/initial-backfill -n weather-dev
```

Then apply the continuously running workloads:

```bash
kubectl apply -f kubernetes/producer/deployment.yaml
kubectl apply -f kubernetes/consumer/deployment.yaml
kubectl apply -f kubernetes/aggregator/deployment.yaml
kubectl apply -f kubernetes/api
kubectl apply -f kubernetes/dashboard
kubectl apply -f kubernetes/kafka-ui
```

The complete desired manifest set can be rendered or applied with:

```bash
kubectl kustomize kubernetes
kubectl apply -k kubernetes
```

For a brand-new cluster, use the ordered first-deployment procedure above.
The combined Kustomize command is most useful after initialization.

## Verify the platform

```bash
kubectl get deployments,statefulsets,pods,services,pvc,jobs \
  --namespace weather-dev

kubectl logs deployment/weather-producer -n weather-dev
kubectl logs deployment/weather-consumer -n weather-dev
kubectl logs deployment/weather-aggregator -n weather-dev
kubectl logs deployment/weather-api -n weather-dev
```

Check the consumer group:

```bash
kubectl exec -n weather-dev kafka-0 -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group weather-postgres-consumer-dev
```

Healthy processing has `CURRENT-OFFSET` equal to `LOG-END-OFFSET` and `LAG`
equal to zero after the consumer catches up.

## Local access

Production-like services continue using ports `8050` and `8080`. Use separate
ports for Kubernetes development:

```bash
kubectl port-forward \
  service/weather-dashboard \
  18050:8050 \
  --namespace weather-dev
```

Open `http://127.0.0.1:18050`.

In another terminal:

```bash
kubectl port-forward \
  service/kafka-ui \
  18080:8080 \
  --namespace weather-dev
```

Open `http://127.0.0.1:18080`.

Optional API access:

```bash
kubectl port-forward \
  service/weather-api \
  18000:8000 \
  --namespace weather-dev
```

Open `http://127.0.0.1:18000/docs`.

## Restart a workload

Delete a Pod only after resolving its exact name:

```bash
kubectl get pods -n weather-dev
kubectl delete pod EXACT_POD_NAME -n weather-dev
```

Its Deployment or StatefulSet recreates it. PostgreSQL, Kafka, and producer
checkpoint data persist through their claims.

## Rerun the initial backfill

The Job name is stable, and a completed Job does not run again when reapplied.
To rerun it intentionally:

```bash
kubectl delete job initial-backfill -n weather-dev
kubectl apply -f kubernetes/producer/initial-backfill-job.yaml
```

The database uses idempotent upserts while `RESET_WEATHER_TABLE` remains
`false`.

## Safe shutdown and removal

Scale application Deployments to zero for a temporary pause:

```bash
kubectl scale deployment \
  weather-producer \
  weather-consumer \
  weather-aggregator \
  weather-api \
  weather-dashboard \
  kafka-ui \
  --replicas=0 \
  --namespace weather-dev
```

Deleting the namespace removes all namespaced workloads and normally removes
dynamically provisioned volumes and their data:

```bash
kubectl delete namespace weather-dev
```

Do not delete the namespace or PVCs unless loss of development PostgreSQL,
Kafka, and checkpoint data is intentional.
