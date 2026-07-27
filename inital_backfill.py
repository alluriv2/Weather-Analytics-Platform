import json

from datetime import datetime, timedelta
from urllib.parse import urljoin

import psycopg

import requests

from bs4 import BeautifulSoup


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

from config import (
    BACKFILL_BATCH_SIZE,
    POSTGRES_CONFIG,
    RESET_WEATHER_TABLE,
    RECONCILIATION_LOOKBACK_DAYS,
    STATION_URLS,
)
from ingestion_state import (
    create_ingestion_state_table,
    database_latest,
    load_watermarks,
    record_reconciliation,
)

# ---------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------

def parse_dt(raw_dt):
    if not raw_dt:
        return None

    # Convert offsets such as -0400 into -04:00.
    if (
        len(raw_dt) > 5
        and raw_dt[-5] in ["+", "-"]
        and raw_dt[-3] != ":"
    ):
        raw_dt = raw_dt[:-2] + ":" + raw_dt[-2:]

    try:
        # Keep the same behavior as your DuckDB version:
        # parse the timezone, then store a timezone-naive timestamp.
        return datetime.fromisoformat(raw_dt).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------
# Remote file listing
# ---------------------------------------------------------

def list_server_files(url):
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    files = [
        anchor.get("href")
        for anchor in soup.find_all("a")
        if anchor.get("href")
        and anchor.get("href").endswith(".txt")
        and anchor.get("href")[:8].isdigit()
    ]

    return sorted(files)


def latest_source_timestamp(url, files):
    if not files:
        return None

    response = requests.get(
        urljoin(url, files[-1]),
        timeout=120,
    )
    response.raise_for_status()

    latest_dt = None

    for line in response.text.splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        row_dt = parse_dt(obj.get("dt"))

        if row_dt is not None and (
            latest_dt is None
            or row_dt > latest_dt
        ):
            latest_dt = row_dt

    return latest_dt


# ---------------------------------------------------------
# PostgreSQL schema
# ---------------------------------------------------------

def create_weather_table(conn):
    with conn.cursor() as cur:
        if RESET_WEATHER_TABLE:
            print(
                "RESET_WEATHER_TABLE=true: "
                "dropping the existing weather table."
            )

            cur.execute(
                "DROP TABLE IF EXISTS weather CASCADE;"
            )

        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            station TEXT NOT NULL,
            dt TIMESTAMP NOT NULL,

            vel_avg_mph DOUBLE PRECISION,
            vel_min_mph DOUBLE PRECISION,
            vel_max_mph DOUBLE PRECISION,
            dir_avg_deg BIGINT,

            temps_avg_f DOUBLE PRECISION,
            temps_min_f DOUBLE PRECISION,
            temps_max_f DOUBLE PRECISION,

            tempb_avg_f DOUBLE PRECISION,
            tempb_min_f DOUBLE PRECISION,
            tempb_max_f DOUBLE PRECISION,

            hum_avg_pct DOUBLE PRECISION,
            hum_min_pct DOUBLE PRECISION,
            hum_max_pct DOUBLE PRECISION,

            pres_avg_pa BIGINT,
            pres_min_pa BIGINT,
            pres_max_pa BIGINT,

            lux_avg_lx BIGINT,
            lux_min_lx BIGINT,
            lux_max_lx BIGINT,

            rain_inc_count BIGINT,
            rain_inc_in DOUBLE PRECISION,

            uptime_seconds BIGINT,
            millis BIGINT,

            node_ip TEXT,
            wifi_ssid TEXT,
            hostname TEXT,

            server_rmt_ip TEXT,
            server_svr_dt TIMESTAMP,

            PRIMARY KEY (station, dt)
        );
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_dt
        ON weather (dt);
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_station_dt
        ON weather (station, dt DESC);
        """)

    conn.commit()
    print("PostgreSQL weather table is ready.")


# ---------------------------------------------------------
# JSON record extraction
# ---------------------------------------------------------

def extract_record(obj, station_name):
    node = obj.get("node", {})
    server = obj.get("server", {})

    dt = parse_dt(obj.get("dt"))
    if dt is None:
        return None

    hostname = node.get("hostname") or station_name

    return {
        "station": hostname,
        "dt": dt,

        "vel_avg_mph": obj.get("vel_avg_mph"),
        "vel_min_mph": obj.get("vel_min_mph"),
        "vel_max_mph": obj.get("vel_max_mph"),
        "dir_avg_deg": obj.get("dir_avg_deg"),

        "temps_avg_f": obj.get("temps_avg_f"),
        "temps_min_f": obj.get("temps_min_f"),
        "temps_max_f": obj.get("temps_max_f"),

        "tempb_avg_f": obj.get("tempb_avg_f"),
        "tempb_min_f": obj.get("tempb_min_f"),
        "tempb_max_f": obj.get("tempb_max_f"),

        "hum_avg_pct": obj.get("hum_avg_pct"),
        "hum_min_pct": obj.get("hum_min_pct"),
        "hum_max_pct": obj.get("hum_max_pct"),

        "pres_avg_pa": obj.get("pres_avg_pa"),
        "pres_min_pa": obj.get("pres_min_pa"),
        "pres_max_pa": obj.get("pres_max_pa"),

        "lux_avg_lx": obj.get("lux_avg_lx"),
        "lux_min_lx": obj.get("lux_min_lx"),
        "lux_max_lx": obj.get("lux_max_lx"),

        "rain_inc_count": obj.get("rain_inc_count"),
        "rain_inc_in": obj.get("rain_inc_in"),

        "uptime_seconds": node.get("uptime_seconds"),
        "millis": node.get("millis"),
        "node_ip": node.get("ip"),
        "wifi_ssid": node.get("wifi_ssid"),
        "hostname": hostname,

        "server_rmt_ip": server.get("rmt_ip"),
        "server_svr_dt": parse_dt(server.get("svr_dt")),
    }


# ---------------------------------------------------------
# PostgreSQL upsert
# ---------------------------------------------------------

UPSERT_SQL = """
INSERT INTO weather (
    station,
    dt,

    vel_avg_mph,
    vel_min_mph,
    vel_max_mph,
    dir_avg_deg,

    temps_avg_f,
    temps_min_f,
    temps_max_f,

    tempb_avg_f,
    tempb_min_f,
    tempb_max_f,

    hum_avg_pct,
    hum_min_pct,
    hum_max_pct,

    pres_avg_pa,
    pres_min_pa,
    pres_max_pa,

    lux_avg_lx,
    lux_min_lx,
    lux_max_lx,

    rain_inc_count,
    rain_inc_in,

    uptime_seconds,
    millis,
    node_ip,
    wifi_ssid,
    hostname,

    server_rmt_ip,
    server_svr_dt
)
VALUES (
    %(station)s,
    %(dt)s,

    %(vel_avg_mph)s,
    %(vel_min_mph)s,
    %(vel_max_mph)s,
    %(dir_avg_deg)s,

    %(temps_avg_f)s,
    %(temps_min_f)s,
    %(temps_max_f)s,

    %(tempb_avg_f)s,
    %(tempb_min_f)s,
    %(tempb_max_f)s,

    %(hum_avg_pct)s,
    %(hum_min_pct)s,
    %(hum_max_pct)s,

    %(pres_avg_pa)s,
    %(pres_min_pa)s,
    %(pres_max_pa)s,

    %(lux_avg_lx)s,
    %(lux_min_lx)s,
    %(lux_max_lx)s,

    %(rain_inc_count)s,
    %(rain_inc_in)s,

    %(uptime_seconds)s,
    %(millis)s,
    %(node_ip)s,
    %(wifi_ssid)s,
    %(hostname)s,

    %(server_rmt_ip)s,
    %(server_svr_dt)s
)
ON CONFLICT (station, dt)
DO UPDATE SET
    vel_avg_mph = EXCLUDED.vel_avg_mph,
    vel_min_mph = EXCLUDED.vel_min_mph,
    vel_max_mph = EXCLUDED.vel_max_mph,
    dir_avg_deg = EXCLUDED.dir_avg_deg,

    temps_avg_f = EXCLUDED.temps_avg_f,
    temps_min_f = EXCLUDED.temps_min_f,
    temps_max_f = EXCLUDED.temps_max_f,

    tempb_avg_f = EXCLUDED.tempb_avg_f,
    tempb_min_f = EXCLUDED.tempb_min_f,
    tempb_max_f = EXCLUDED.tempb_max_f,

    hum_avg_pct = EXCLUDED.hum_avg_pct,
    hum_min_pct = EXCLUDED.hum_min_pct,
    hum_max_pct = EXCLUDED.hum_max_pct,

    pres_avg_pa = EXCLUDED.pres_avg_pa,
    pres_min_pa = EXCLUDED.pres_min_pa,
    pres_max_pa = EXCLUDED.pres_max_pa,

    lux_avg_lx = EXCLUDED.lux_avg_lx,
    lux_min_lx = EXCLUDED.lux_min_lx,
    lux_max_lx = EXCLUDED.lux_max_lx,

    rain_inc_count = EXCLUDED.rain_inc_count,
    rain_inc_in = EXCLUDED.rain_inc_in,

    uptime_seconds = EXCLUDED.uptime_seconds,
    millis = EXCLUDED.millis,
    node_ip = EXCLUDED.node_ip,
    wifi_ssid = EXCLUDED.wifi_ssid,
    hostname = EXCLUDED.hostname,

    server_rmt_ip = EXCLUDED.server_rmt_ip,
    server_svr_dt = EXCLUDED.server_svr_dt;
"""


def upsert_batch(conn, records):
    if not records:
        return

    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, records)

    conn.commit()


# ---------------------------------------------------------
# Station backfill
# ---------------------------------------------------------

def backfill_station(
    conn,
    station_name,
    url,
    existing_latest,
    known_upstream_latest,
):
    print(f"\n[{station_name}] Listing files...")

    files = list_server_files(url)

    print(f"[{station_name}] Found {len(files)} files.")

    source_latest = latest_source_timestamp(url, files)

    known_latest_values = [
        value
        for value in (
            source_latest,
            known_upstream_latest,
        )
        if value is not None
    ]

    if (
        existing_latest is not None
        and known_latest_values
        and existing_latest >= max(known_latest_values)
    ):
        print(
            f"[{station_name}] No gap: database "
            f"{existing_latest} is current with source "
            f"{source_latest} and pipeline "
            f"{known_upstream_latest}."
        )
        return source_latest

    if existing_latest is not None:
        start_date = (
            existing_latest
            - timedelta(days=RECONCILIATION_LOOKBACK_DAYS)
        ).strftime("%Y%m%d")
        files = [
            filename
            for filename in files
            if filename[:8] >= start_date
        ]
        print(
            f"[{station_name}] Gap detected; reconciling from "
            f"{start_date}; checking {len(files)} files."
        )
    else:
        print(
            f"[{station_name}] Database is empty; "
            "running the complete initial backfill."
        )

    processed_count = 0
    batch = []

    for filename in files:
        file_url = urljoin(url, filename)

        print(f"[{station_name}] Reading {filename}")

        response = requests.get(file_url, timeout=120)
        response.raise_for_status()

        for line in response.text.splitlines():
            line = line.strip()

            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            record = extract_record(obj, station_name)

            if record is None:
                continue

            if (
                source_latest is None
                or record["dt"] > source_latest
            ):
                source_latest = record["dt"]

            batch.append(record)
            processed_count += 1

            if len(batch) >= BACKFILL_BATCH_SIZE:
                try:
                    upsert_batch(conn, batch)
                except Exception:
                    conn.rollback()
                    raise

                print(
                    f"[{station_name}] Committed "
                    f"{processed_count:,} records"
                )

                batch.clear()

    # Insert the final partial batch.
    if batch:
        try:
            upsert_batch(conn, batch)
        except Exception:
            conn.rollback()
            raise

    print(
        f"[{station_name}] Backfill complete. "
        f"Records processed: {processed_count:,}"
    )
    print(
        f"[{station_name}] Source latest timestamp: "
        f"{source_latest}"
    )

    return source_latest


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    try:
        with psycopg.connect(**POSTGRES_CONFIG) as conn:
            print(
                "Connected to PostgreSQL "
                f"{POSTGRES_CONFIG['dbname']} "
                f"on port {POSTGRES_CONFIG['port']}."
            )

            create_weather_table(conn)
            create_ingestion_state_table(conn)
            watermarks = load_watermarks(conn)

            for station_name, url in STATION_URLS.items():
                existing_latest = database_latest(
                    conn,
                    station_name,
                )
                station_state = watermarks.get(
                    station_name,
                    {},
                )
                upstream_values = [
                    value
                    for value in (
                        station_state.get(
                            "producer_latest_ts"
                        ),
                        station_state.get(
                            "consumer_latest_ts"
                        ),
                    )
                    if value is not None
                ]
                known_upstream_latest = (
                    max(upstream_values)
                    if upstream_values
                    else None
                )

                source_latest = backfill_station(
                    conn=conn,
                    station_name=station_name,
                    url=url,
                    existing_latest=existing_latest,
                    known_upstream_latest=(
                        known_upstream_latest
                    ),
                )

                stored_latest = database_latest(
                    conn,
                    station_name,
                )
                status = record_reconciliation(
                    conn=conn,
                    station=station_name,
                    source_latest=source_latest,
                    database_latest_ts=stored_latest,
                    producer_latest=station_state.get(
                        "producer_latest_ts"
                    ),
                    consumer_latest=station_state.get(
                        "consumer_latest_ts"
                    ),
                )
                print(
                    f"[{station_name}] Reconciliation status: "
                    f"{status}; database latest: {stored_latest}"
                )

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        station,
                        COUNT(*) AS row_count,
                        MIN(dt) AS earliest_dt,
                        MAX(dt) AS latest_dt
                    FROM weather
                    GROUP BY station
                    ORDER BY station;
                """)

                validation_rows = cur.fetchall()

            print("\nPostgreSQL backfill validation:")

            for station, row_count, earliest_dt, latest_dt in validation_rows:
                print(
                    f"{station}: "
                    f"{row_count:,} rows, "
                    f"{earliest_dt} → {latest_dt}"
                )

    except psycopg.OperationalError as exc:
        print("\nCould not connect to PostgreSQL.")
        print("Confirm that PostgreSQL is reachable at " f"{POSTGRES_CONFIG['host']}:" f"{POSTGRES_CONFIG['port']}.")
        raise exc

    except KeyboardInterrupt:
        print("\nBackfill interrupted by user.")
        raise

    print("\nInitial PostgreSQL backfill completed successfully.")


if __name__ == "__main__":
    main()
