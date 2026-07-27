"""Durable ingestion watermarks stored in PostgreSQL."""

from datetime import datetime


CREATE_INGESTION_STATE_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_state (
    station TEXT PRIMARY KEY,
    source_latest_ts TIMESTAMP,
    producer_latest_ts TIMESTAMP,
    consumer_latest_ts TIMESTAMP,
    database_latest_ts TIMESTAMP,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    status TEXT NOT NULL DEFAULT 'unknown',
    last_reconciliation_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def create_ingestion_state_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_INGESTION_STATE_SQL)
    conn.commit()


def database_latest(conn, station):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(dt) FROM weather WHERE station = %s;",
            (station,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def load_watermarks(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                station,
                source_latest_ts,
                producer_latest_ts,
                consumer_latest_ts,
                database_latest_ts,
                kafka_partition,
                kafka_offset,
                status,
                last_reconciliation_at
            FROM ingestion_state
            ORDER BY station;
        """)
        rows = cur.fetchall()

    return {
        row[0]: {
            "source_latest_ts": row[1],
            "producer_latest_ts": row[2],
            "consumer_latest_ts": row[3],
            "database_latest_ts": row[4],
            "kafka_partition": row[5],
            "kafka_offset": row[6],
            "status": row[7],
            "last_reconciliation_at": row[8],
        }
        for row in rows
    }


def record_producer_watermark(conn, station, timestamp):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingestion_state (
                station,
                producer_latest_ts,
                updated_at
            )
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (station)
            DO UPDATE SET
                producer_latest_ts = GREATEST(
                    ingestion_state.producer_latest_ts,
                    EXCLUDED.producer_latest_ts
                ),
                updated_at = CURRENT_TIMESTAMP;
        """, (station, timestamp))
    conn.commit()


def record_consumer_watermark(
    cur,
    station,
    timestamp,
    partition,
    offset,
):
    cur.execute("""
        INSERT INTO ingestion_state (
            station,
            consumer_latest_ts,
            database_latest_ts,
            kafka_partition,
            kafka_offset,
            status,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            'streaming',
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (station)
        DO UPDATE SET
            consumer_latest_ts = GREATEST(
                ingestion_state.consumer_latest_ts,
                EXCLUDED.consumer_latest_ts
            ),
            database_latest_ts = GREATEST(
                ingestion_state.database_latest_ts,
                EXCLUDED.database_latest_ts
            ),
            kafka_partition = EXCLUDED.kafka_partition,
            kafka_offset = EXCLUDED.kafka_offset,
            status = 'streaming',
            updated_at = CURRENT_TIMESTAMP;
    """, (
        station,
        timestamp,
        timestamp,
        partition,
        offset,
    ))


def reconciliation_status(
    source_latest,
    producer_latest,
    consumer_latest,
    database_latest_ts,
):
    if database_latest_ts is None:
        return "database_empty"

    known_upstream = [
        value
        for value in (
            source_latest,
            producer_latest,
            consumer_latest,
        )
        if value is not None
    ]

    if known_upstream and database_latest_ts < max(known_upstream):
        return "database_gap_detected"

    if source_latest and database_latest_ts < source_latest:
        return "source_gap_detected"

    return "healthy"


def record_reconciliation(
    conn,
    station,
    source_latest,
    database_latest_ts,
    producer_latest=None,
    consumer_latest=None,
):
    status = reconciliation_status(
        source_latest=source_latest,
        producer_latest=producer_latest,
        consumer_latest=consumer_latest,
        database_latest_ts=database_latest_ts,
    )

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingestion_state (
                station,
                source_latest_ts,
                producer_latest_ts,
                consumer_latest_ts,
                database_latest_ts,
                status,
                last_reconciliation_at,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (station)
            DO UPDATE SET
                source_latest_ts = EXCLUDED.source_latest_ts,
                producer_latest_ts = COALESCE(
                    EXCLUDED.producer_latest_ts,
                    ingestion_state.producer_latest_ts
                ),
                consumer_latest_ts = COALESCE(
                    EXCLUDED.consumer_latest_ts,
                    ingestion_state.consumer_latest_ts
                ),
                database_latest_ts = EXCLUDED.database_latest_ts,
                status = EXCLUDED.status,
                last_reconciliation_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP;
        """, (
            station,
            source_latest,
            producer_latest,
            consumer_latest,
            database_latest_ts,
            status,
        ))
    conn.commit()
    return status
