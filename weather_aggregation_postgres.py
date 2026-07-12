import psycopg


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

from config import POSTGRES_CONFIG

# ---------------------------------------------------------
# Aggregate table
# ---------------------------------------------------------

def create_aggregate_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_aggregates (
            window_name TEXT NOT NULL,
            bucket_ts TIMESTAMP NOT NULL,
            station TEXT NOT NULL,

            vel_avg_mph DOUBLE PRECISION,
            temps_avg_f DOUBLE PRECISION,
            tempb_avg_f DOUBLE PRECISION,
            hum_avg_pct DOUBLE PRECISION,
            pres_avg_pa DOUBLE PRECISION,
            lux_avg_lx DOUBLE PRECISION,
            rain_inc_in DOUBLE PRECISION,

            updated_at TIMESTAMP NOT NULL
                DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (
                window_name,
                bucket_ts,
                station
            )
        );
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS
            idx_weather_aggregates_lookup
        ON weather_aggregates (
            window_name,
            station,
            bucket_ts
        );
        """)

    conn.commit()


# ---------------------------------------------------------
# Aggregate refresh SQL
# ---------------------------------------------------------

AGGREGATION_SQL = """
WITH latest AS (
    SELECT MAX(dt) AS max_dt
    FROM weather
),

day_data AS (
    SELECT
        'day'::TEXT AS window_name,
        DATE_TRUNC('minute', w.dt) AS bucket_ts,
        w.station,

        AVG(w.vel_avg_mph) AS vel_avg_mph,
        AVG(w.temps_avg_f) AS temps_avg_f,
        AVG(w.tempb_avg_f) AS tempb_avg_f,
        AVG(w.hum_avg_pct) AS hum_avg_pct,
        AVG(w.pres_avg_pa) AS pres_avg_pa,
        AVG(w.lux_avg_lx) AS lux_avg_lx,
        SUM(w.rain_inc_in) AS rain_inc_in,

        CURRENT_TIMESTAMP AS updated_at

    FROM weather AS w
    CROSS JOIN latest AS l

    WHERE w.dt BETWEEN
        l.max_dt - INTERVAL '1 day'
        AND l.max_dt

    GROUP BY
        DATE_TRUNC('minute', w.dt),
        w.station
),

week_data AS (
    SELECT
        'week'::TEXT AS window_name,

        DATE_BIN(
            INTERVAL '5 minutes',
            w.dt,
            TIMESTAMP '2000-01-01 00:00:00'
        ) AS bucket_ts,

        w.station,

        AVG(w.vel_avg_mph) AS vel_avg_mph,
        AVG(w.temps_avg_f) AS temps_avg_f,
        AVG(w.tempb_avg_f) AS tempb_avg_f,
        AVG(w.hum_avg_pct) AS hum_avg_pct,
        AVG(w.pres_avg_pa) AS pres_avg_pa,
        AVG(w.lux_avg_lx) AS lux_avg_lx,
        SUM(w.rain_inc_in) AS rain_inc_in,

        CURRENT_TIMESTAMP AS updated_at

    FROM weather AS w
    CROSS JOIN latest AS l

    WHERE w.dt BETWEEN
        l.max_dt - INTERVAL '7 days'
        AND l.max_dt

    GROUP BY
        DATE_BIN(
            INTERVAL '5 minutes',
            w.dt,
            TIMESTAMP '2000-01-01 00:00:00'
        ),
        w.station
),

month_data AS (
    SELECT
        'month'::TEXT AS window_name,

        DATE_BIN(
            INTERVAL '15 minutes',
            w.dt,
            TIMESTAMP '2000-01-01 00:00:00'
        ) AS bucket_ts,

        w.station,

        AVG(w.vel_avg_mph) AS vel_avg_mph,
        AVG(w.temps_avg_f) AS temps_avg_f,
        AVG(w.tempb_avg_f) AS tempb_avg_f,
        AVG(w.hum_avg_pct) AS hum_avg_pct,
        AVG(w.pres_avg_pa) AS pres_avg_pa,
        AVG(w.lux_avg_lx) AS lux_avg_lx,
        SUM(w.rain_inc_in) AS rain_inc_in,

        CURRENT_TIMESTAMP AS updated_at

    FROM weather AS w
    CROSS JOIN latest AS l

    WHERE w.dt BETWEEN
        l.max_dt - INTERVAL '30 days'
        AND l.max_dt

    GROUP BY
        DATE_BIN(
            INTERVAL '15 minutes',
            w.dt,
            TIMESTAMP '2000-01-01 00:00:00'
        ),
        w.station
),

year_data AS (
    SELECT
        'year'::TEXT AS window_name,
        DATE_TRUNC('hour', w.dt) AS bucket_ts,
        w.station,

        AVG(w.vel_avg_mph) AS vel_avg_mph,
        AVG(w.temps_avg_f) AS temps_avg_f,
        AVG(w.tempb_avg_f) AS tempb_avg_f,
        AVG(w.hum_avg_pct) AS hum_avg_pct,
        AVG(w.pres_avg_pa) AS pres_avg_pa,
        AVG(w.lux_avg_lx) AS lux_avg_lx,
        SUM(w.rain_inc_in) AS rain_inc_in,

        CURRENT_TIMESTAMP AS updated_at

    FROM weather AS w
    CROSS JOIN latest AS l

    WHERE w.dt BETWEEN
        l.max_dt - INTERVAL '365 days'
        AND l.max_dt

    GROUP BY
        DATE_TRUNC('hour', w.dt),
        w.station
)

INSERT INTO weather_aggregates (
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
)

SELECT * FROM day_data

UNION ALL

SELECT * FROM week_data

UNION ALL

SELECT * FROM month_data

UNION ALL

SELECT * FROM year_data;
"""


# ---------------------------------------------------------
# Aggregate refresh
# ---------------------------------------------------------

def refresh_aggregates(conn):
    """
    The delete and insert occur in one PostgreSQL transaction.

    Other readers continue seeing the previous committed aggregate data
    until the new refresh completes successfully.
    """

    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM weather_aggregates;
            """)

            cur.execute(AGGREGATION_SQL)

        conn.commit()

    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------
# Validation
# ---------------------------------------------------------

def print_validation(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT
            window_name,
            station,
            COUNT(*) AS row_count,
            MIN(bucket_ts) AS earliest_bucket,
            MAX(bucket_ts) AS latest_bucket,
            MAX(updated_at) AS refreshed_at
        FROM weather_aggregates
        GROUP BY
            window_name,
            station
        ORDER BY
            window_name,
            station;
        """)

        rows = cur.fetchall()

    print("\nAggregation validation:")

    for (
        window_name,
        station,
        row_count,
        earliest_bucket,
        latest_bucket,
        refreshed_at,
    ) in rows:
        print(
            f"{window_name:<6} | "
            f"{station:<15} | "
            f"{row_count:>8,} rows | "
            f"{earliest_bucket} → {latest_bucket} | "
            f"refreshed {refreshed_at}"
        )


# ---------------------------------------------------------
# Sample aggregate values
# ---------------------------------------------------------

def print_sample_rows(conn):
    with conn.cursor() as cur:
        cur.execute("""
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
            rain_inc_in
        FROM weather_aggregates
        ORDER BY bucket_ts DESC, station
        LIMIT 10;
        """)

        rows = cur.fetchall()

    print("\nMost recent aggregate rows:")

    for row in rows:
        print(row)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

from datetime import datetime


def main():
    start = datetime.now()

    print("Starting PostgreSQL aggregate refresh...")

    with psycopg.connect(**POSTGRES_CONFIG) as conn:
        create_aggregate_table(conn)
        refresh_aggregates(conn)
        print_validation(conn)
        print_sample_rows(conn)

    duration = datetime.now() - start

    print(
        f"\nPostgreSQL aggregate refresh completed "
        f"in {duration}."
    )


if __name__ == "__main__":
    main()