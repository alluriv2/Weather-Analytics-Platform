import json

import os

from datetime import datetime

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
    STATION_URLS,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STATE_PATH = os.path.join(
    BASE_DIR,
    "producer_state.json",
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

def backfill_station(conn, station_name, url):
    print(f"\n[{station_name}] Listing files...")

    files = list_server_files(url)

    print(f"[{station_name}] Found {len(files)} files.")

    latest_dt = None
    processed_count = 0
    batch = []

    for filename in files:
        file_url = url + filename

        print(f"[{station_name}] Reading {filename}")

        response = requests.get(file_url, timeout=30)
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

            batch.append(record)
            processed_count += 1

            if latest_dt is None or record["dt"] > latest_dt:
                latest_dt = record["dt"]

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
    print(f"[{station_name}] Latest timestamp: {latest_dt}")

    return latest_dt


# ---------------------------------------------------------
# Producer checkpoint
# ---------------------------------------------------------

def save_producer_state(state):
    serializable_state = {
        station: timestamp.isoformat() if timestamp else None
        for station, timestamp in state.items()
    }

    temporary_path = STATE_PATH + ".tmp"

    # Write to a temporary file first so a failed write does not leave
    # producer_state.json partially corrupted.
    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(serializable_state, file, indent=2)

    os.replace(temporary_path, STATE_PATH)

    print(f"\nSaved producer checkpoint to {STATE_PATH}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    producer_state = {}

    try:
        with psycopg.connect(**POSTGRES_CONFIG) as conn:
            print(
                "Connected to PostgreSQL "
                f"{POSTGRES_CONFIG['dbname']} "
                f"on port {POSTGRES_CONFIG['port']}."
            )

            create_weather_table(conn)

            for station_name, url in STATION_URLS.items():
                latest_dt = backfill_station(
                    conn=conn,
                    station_name=station_name,
                    url=url,
                )

                producer_state[station_name] = latest_dt

            save_producer_state(producer_state)

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