from fastapi import FastAPI, HTTPException, Query
import psycopg
from psycopg.rows import dict_row

from config import POSTGRES_CONFIG


app = FastAPI(
    title="Real-Time Weather Analytics API",
    version="1.0",
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
            "/docs",
        ],
        "latest_metrics": list(LATEST_METRIC_MAP.keys()),
        "history_metrics": list(HISTORY_METRIC_MAP.keys()),
        "windows": sorted(VALID_WINDOWS),
    }


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
