import json
from datetime import datetime

import psycopg
from confluent_kafka import Consumer, KafkaException


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

from config import (
    POSTGRES_CONFIG,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
)
from ingestion_state import (
    create_ingestion_state_table,
    record_consumer_watermark,
)


# ---------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------

def parse_dt(raw_dt):
    if not raw_dt:
        return None

    # Convert timezone offset such as -0400 to -04:00.
    if (
        len(raw_dt) > 5
        and raw_dt[-5] in ["+", "-"]
        and raw_dt[-3] != ":"
    ):
        raw_dt = raw_dt[:-2] + ":" + raw_dt[-2:]

    try:
        return datetime.fromisoformat(raw_dt).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------
# PostgreSQL latest table creation
# ---------------------------------------------------------

def create_latest_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_latest (
            station TEXT PRIMARY KEY,
            dt TIMESTAMP NOT NULL,

            temperature DOUBLE PRECISION,
            wind_speed DOUBLE PRECISION,
            wind_direction DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            lux DOUBLE PRECISION,
            rain_inches DOUBLE PRECISION,

            updated_at TIMESTAMP NOT NULL
                DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Add these columns if weather_latest already existed
        # before humidity and pressure were introduced.
        cur.execute("""
        ALTER TABLE weather_latest
        ADD COLUMN IF NOT EXISTS humidity DOUBLE PRECISION;
        """)

        cur.execute("""
        ALTER TABLE weather_latest
        ADD COLUMN IF NOT EXISTS pressure DOUBLE PRECISION;
        """)

    conn.commit()


# ---------------------------------------------------------
# Kafka JSON extraction
# ---------------------------------------------------------

def extract_record(obj):
    node = obj.get("node", {})
    server = obj.get("server", {})

    dt = parse_dt(obj.get("dt"))

    if dt is None:
        return None

    station = obj.get("station") or node.get("hostname")

    if not station:
        return None

    return {
        "station": station,
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
        "hostname": node.get("hostname") or station,

        "server_rmt_ip": server.get("rmt_ip"),
        "server_svr_dt": parse_dt(server.get("svr_dt")),
    }


# ---------------------------------------------------------
# Raw weather upsert
# ---------------------------------------------------------

WEATHER_UPSERT_SQL = """
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


# ---------------------------------------------------------
# Latest-state upsert
# ---------------------------------------------------------

LATEST_UPSERT_SQL = """
INSERT INTO weather_latest (
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
)
VALUES (
    %(station)s,
    %(dt)s,
    %(temps_avg_f)s,
    %(vel_avg_mph)s,
    %(dir_avg_deg)s,
    %(hum_avg_pct)s,
    %(pressure_hpa)s,
    %(lux_avg_lx)s,
    %(rain_inc_in)s,
    CURRENT_TIMESTAMP
)
ON CONFLICT (station)
DO UPDATE SET
    dt = EXCLUDED.dt,
    temperature = EXCLUDED.temperature,
    wind_speed = EXCLUDED.wind_speed,
    wind_direction = EXCLUDED.wind_direction,
    humidity = EXCLUDED.humidity,
    pressure = EXCLUDED.pressure,
    lux = EXCLUDED.lux,
    rain_inches = EXCLUDED.rain_inches,
    updated_at = CURRENT_TIMESTAMP
WHERE EXCLUDED.dt >= weather_latest.dt;
"""


def write_weather_record(
    conn,
    record,
    partition,
    offset,
):
    """
    Write the raw observation and latest-state row in one PostgreSQL
    transaction.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(WEATHER_UPSERT_SQL, record)
            cur.execute(LATEST_UPSERT_SQL, record)
            record_consumer_watermark(
                cur=cur,
                station=record["station"],
                timestamp=record["dt"],
                partition=partition,
                offset=offset,
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------
# Main consumer
# ---------------------------------------------------------

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC])

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {KAFKA_CONSUMER_GROUP}")
    print("Press Ctrl+C to stop.\n")

    try:
        with psycopg.connect(**POSTGRES_CONFIG) as conn:
            create_latest_table(conn)
            create_ingestion_state_table(conn)

            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    raise KafkaException(msg.error())

                try:
                    obj = json.loads(
                        msg.value().decode("utf-8")
                    )

                    record = extract_record(obj)

                    # Check before accessing any fields in record.
                    if record is None:
                        print(
                            "Skipping malformed message "
                            f"at offset {msg.offset()}"
                        )
                        consumer.commit(
                            message=msg,
                            asynchronous=False,
                        )
                        continue

                    # Keep raw pressure in Pa in the weather table.
                    # Store the latest/dashboard pressure in hPa.
                    if record["pres_avg_pa"] is not None:
                        record["pressure_hpa"] = (
                            record["pres_avg_pa"] / 100
                        )
                    else:
                        record["pressure_hpa"] = None

                    # Raw and latest writes are committed together.
                    write_weather_record(
                        conn=conn,
                        record=record,
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )

                    # Commit the Kafka offset only after PostgreSQL commits.
                    # A crash between these commits can replay a message, so
                    # the database writes remain idempotent upserts.
                    consumer.commit(
                        message=msg,
                        asynchronous=False,
                    )

                    print(
                        f"Stored {record['station']} "
                        f"{record['dt']} "
                        f"from partition {msg.partition()} "
                        f"offset {msg.offset()}"
                    )

                except Exception as exc:
                    print(
                        f"Error processing partition "
                        f"{msg.partition()} offset "
                        f"{msg.offset()}: {exc}"
                    )
                    print(
                        "Database transaction was rolled back."
                    )
                    break

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
