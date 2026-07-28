from time import perf_counter

from fastapi import FastAPI, HTTPException, Query, Request, Response
import psycopg
from psycopg.rows import dict_row
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from config import POSTGRES_CONFIG


app = FastAPI(
    title="Real-Time Weather Analytics API",
    version="1.0",
)

HTTP_REQUESTS = Counter(
    "weather_api_http_requests_total",
    "Total HTTP requests handled by the weather API.",
    ("method", "route", "status"),
)
HTTP_REQUEST_DURATION = Histogram(
    "weather_api_http_request_duration_seconds",
    "Weather API request duration in seconds.",
    ("method", "route"),
)
HTTP_REQUESTS_IN_PROGRESS = Gauge(
    "weather_api_http_requests_in_progress",
    "Weather API requests currently being processed.",
    ("method",),
)
DATABASE_UP = Gauge(
    "weather_database_up",
    "Whether PostgreSQL is reachable from the weather API.",
)
INGESTION_GAP = Gauge(
    "weather_ingestion_gap_seconds",
    "Observed delay between ingestion stages.",
    ("station", "stage"),
)
INGESTION_REQUIRES_BACKFILL = Gauge(
    "weather_ingestion_requires_backfill",
    "Whether a station currently requires reconciliation.",
    ("station",),
)
INGESTION_LATEST_TIMESTAMP = Gauge(
    "weather_ingestion_latest_timestamp_seconds",
    "Latest observed Unix timestamp for each ingestion stage.",
    ("station", "stage"),
)


# Friendly API metric name → PostgreSQL aggregate column
HISTORY_METRIC_MAP = {
    "temperature": "temps_avg_f",
    "wind_speed": "vel_avg_mph",
    "humidity": "hum_avg_pct",
    "pressure": "pres_avg_pa",
    "lux": "lux_avg_lx",
    "rainfall": "rain_inc_in",
}


# Friendly API metric name → weather_latest column
LATEST_METRIC_MAP = {
    "temperature": "temperature",
    "wind_speed": "wind_speed",
    "wind_direction": "wind_direction",
    "humidity": "humidity",
    "pressure": "pressure",
    "lux": "lux",
    "rainfall": "rain_inches",
}


VALID_WINDOWS = {
    "day",
    "week",
    "month",
    "year",
}


def get_connection():
    return psycopg.connect(
        **POSTGRES_CONFIG,
        row_factory=dict_row,
    )


@app.middleware("http")
async def record_http_metrics(request: Request, call_next):
    method = request.method
    started_at = perf_counter()
    status = "500"
    HTTP_REQUESTS_IN_PROGRESS.labels(method=method).inc()

    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    except Exception:
        status = "500"
        raise
    finally:
        route = request.scope.get("route")
        route_path = getattr(route, "path", "unmatched")
        HTTP_REQUESTS.labels(
            method=method,
            route=route_path,
            status=status,
        ).inc()
        HTTP_REQUEST_DURATION.labels(
            method=method,
            route=route_path,
        ).observe(perf_counter() - started_at)
        HTTP_REQUESTS_IN_PROGRESS.labels(method=method).dec()


def refresh_ingestion_metrics():
    query = """
        SELECT
            station,
            source_latest_ts,
            producer_latest_ts,
            consumer_latest_ts,
            database_latest_ts,
            GREATEST(
                EXTRACT(EPOCH FROM (
                    source_latest_ts - database_latest_ts
                )),
                0
            ) AS source_database_gap_seconds,
            GREATEST(
                EXTRACT(EPOCH FROM (
                    producer_latest_ts - consumer_latest_ts
                )),
                0
            ) AS producer_consumer_gap_seconds,
            (
                database_latest_ts IS NULL
                OR database_latest_ts < GREATEST(
                    source_latest_ts,
                    producer_latest_ts,
                    consumer_latest_ts
                )
            ) AS requires_backfill
        FROM ingestion_state;
    """

    INGESTION_GAP.clear()
    INGESTION_REQUIRES_BACKFILL.clear()
    INGESTION_LATEST_TIMESTAMP.clear()

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
        DATABASE_UP.set(1)
    except psycopg.Error:
        DATABASE_UP.set(0)
        return

    timestamp_fields = {
        "source": "source_latest_ts",
        "producer": "producer_latest_ts",
        "consumer": "consumer_latest_ts",
        "database": "database_latest_ts",
    }

    for row in rows:
        station = row["station"]
        INGESTION_GAP.labels(
            station=station,
            stage="source_database",
        ).set(float(row["source_database_gap_seconds"] or 0))
        INGESTION_GAP.labels(
            station=station,
            stage="producer_consumer",
        ).set(float(row["producer_consumer_gap_seconds"] or 0))
        INGESTION_REQUIRES_BACKFILL.labels(station=station).set(
            1 if row["requires_backfill"] else 0
        )

        for stage, field_name in timestamp_fields.items():
            timestamp = row[field_name]
            if timestamp is not None:
                INGESTION_LATEST_TIMESTAMP.labels(
                    station=station,
                    stage=stage,
                ).set(timestamp.timestamp())


@app.get("/")
def landing_page():
    return {
        "message": "Real-Time Weather Analytics API",
        "version": "1.0",
        "available_endpoints": [
            "/latest",
            "/history",
            "/ingestion-status",
            "/health",
            "/metrics",
            "/docs",
        ],
        "latest_metrics": list(LATEST_METRIC_MAP.keys()),
        "history_metrics": list(HISTORY_METRIC_MAP.keys()),
        "windows": sorted(VALID_WINDOWS),
    }


@app.get("/metrics", include_in_schema=False)
def metrics():
    refresh_ingestion_metrics()
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()

        return {
            "status": "healthy",
            "database": "connected",
        }

    except psycopg.Error as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable: {exc}",
        )


@app.get("/ingestion-status")
def ingestion_status():
    """
    Return durable source, producer, consumer, and database watermarks.

    ``requires_backfill`` becomes true when the database is behind any
    known upstream watermark. The reconciler repairs that condition.
    """
    query = """
        SELECT
            station,
            source_latest_ts,
            producer_latest_ts,
            consumer_latest_ts,
            database_latest_ts,
            kafka_partition,
            kafka_offset,
            status,
            last_reconciliation_at,
            GREATEST(
                EXTRACT(EPOCH FROM (
                    source_latest_ts - database_latest_ts
                )),
                0
            )::BIGINT AS source_database_gap_seconds,
            GREATEST(
                EXTRACT(EPOCH FROM (
                    producer_latest_ts - consumer_latest_ts
                )),
                0
            )::BIGINT AS producer_consumer_gap_seconds,
            (
                database_latest_ts IS NULL
                OR database_latest_ts < GREATEST(
                    source_latest_ts,
                    producer_latest_ts,
                    consumer_latest_ts
                )
            ) AS requires_backfill
        FROM ingestion_state
        ORDER BY station;
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

    except psycopg.Error as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Ingestion status query failed: {exc}",
        )

    return {
        "reconciliation_schedule": "every 5 minutes",
        "stations": rows,
    }


@app.get("/latest")
def latest(
    station: str | None = Query(default=None),
    metric: str | None = Query(default=None),
):
    """
    Return the latest weather conditions.

    Examples:
        /latest
        /latest?station=wx_waverly
        /latest?metric=temperature
        /latest?station=wx_east_st&metric=humidity
    """

    if station == "all":
        station = None

    if metric is not None and metric not in LATEST_METRIC_MAP:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Unsupported latest metric: {metric}",
                "valid_metrics": list(LATEST_METRIC_MAP.keys()),
            },
        )

    query = """
        SELECT
            station,
            dt,
            temperature,
            wind_speed,
            wind_direction,
            humidity,
            pressure,
            lux,
            rain_inches,
            updated_at
        FROM weather_latest
    """

    params = []

    if station:
        query += " WHERE station = %s"
        params.append(station)

    query += " ORDER BY station;"

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

    except psycopg.Error as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {exc}",
        )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No latest weather records found.",
        )

    if metric:
        database_column = LATEST_METRIC_MAP[metric]

        return [
            {
                "station": row["station"],
                "dt": row["dt"],
                metric: row[database_column],
            }
            for row in rows
        ]

    return rows


@app.get("/history")
def history(
    window: str,
    metric: str | None = Query(default=None),
    station: str | None = Query(default=None),
):
    """
    Return bucketed historical weather data.

    Examples:
        /history?window=day
        /history?window=week&station=wx_waverly
        /history?window=month&metric=temperature
        /history?window=year&station=wx_east_st&metric=humidity
    """

    if window not in VALID_WINDOWS:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Unsupported window: {window}",
                "valid_windows": sorted(VALID_WINDOWS),
            },
        )

    if metric is not None and metric not in HISTORY_METRIC_MAP:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Unsupported history metric: {metric}",
                "valid_metrics": list(HISTORY_METRIC_MAP.keys()),
            },
        )

    if station == "all":
        station = None

    params = [window]

    if metric:
        database_column = HISTORY_METRIC_MAP[metric]

        query = f"""
            SELECT
                bucket_ts,
                station,
                {database_column} AS {metric}
            FROM weather_aggregates
            WHERE window_name = %s
        """

    else:
        query = """
            SELECT
                window_name,
                bucket_ts,
                station,
                vel_avg_mph,
                temps_avg_f,
                tempb_avg_f,
                hum_avg_pct,
                pres_avg_pa,
                lux_avg_lx,
                rain_inc_in,
                updated_at
            FROM weather_aggregates
            WHERE window_name = %s
        """

    if station:
        query += " AND station = %s"
        params.append(station)

    query += " ORDER BY bucket_ts, station;"

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

    except psycopg.Error as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {exc}",
        )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No historical weather records found.",
        )

    return rows
