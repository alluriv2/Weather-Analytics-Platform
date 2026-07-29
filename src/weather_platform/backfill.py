"""Publish historical weather observations to Kafka for durable ingestion."""

import json
from datetime import timedelta
from urllib.parse import urljoin

import psycopg
import requests
from confluent_kafka import KafkaException, Producer

from weather_platform.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_TOPIC,
    POSTGRES_CONFIG,
    RECONCILIATION_LOOKBACK_DAYS,
    STATION_URLS,
)
from weather_platform.ingestion_state import database_latest
from weather_platform.producer import list_server_files, parse_dt


def latest_source_timestamp(url, files):
    if not files:
        return None

    response = requests.get(urljoin(url, files[-1]), timeout=120)
    response.raise_for_status()
    latest = None

    for line in response.text.splitlines():
        try:
            row_dt = parse_dt(json.loads(line).get("dt"))
        except json.JSONDecodeError:
            continue

        if row_dt is not None and (latest is None or row_dt > latest):
            latest = row_dt

    return latest


def publish_event(
    producer,
    station_name,
    obj,
    row_dt,
    delivery_failures,
):
    node = obj.get("node", {})
    obj["station"] = node.get("hostname") or station_name
    obj["ingestion_mode"] = "backfill"
    message_key = f"{obj['station']}|{row_dt.isoformat()}"

    while True:
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=message_key.encode("utf-8"),
                value=json.dumps(obj).encode("utf-8"),
                on_delivery=lambda error, message: (
                    delivery_failures.append(
                        f"{message.topic()} partition "
                        f"{message.partition()}: {error}"
                    )
                    if error is not None
                    else None
                ),
            )
            return
        except BufferError:
            producer.poll(1.0)


def publish_station_backfill(producer, station_name, url, stored_latest):
    files = list_server_files(url)
    source_latest = latest_source_timestamp(url, files)

    if (
        stored_latest is not None
        and source_latest is not None
        and stored_latest >= source_latest
    ):
        print(
            f"[{station_name}] No gap: database timestamp "
            f"{stored_latest} is current with source {source_latest}."
        )
        return

    if stored_latest is None:
        candidate_files = files
        cutoff = None
        print(f"[{station_name}] Database is empty; publishing full history.")
    else:
        cutoff = stored_latest - timedelta(
            days=RECONCILIATION_LOOKBACK_DAYS
        )
        start_date = cutoff.strftime("%Y%m%d")
        candidate_files = [
            filename
            for filename in files
            if filename[:8] >= start_date
        ]
        print(
            f"[{station_name}] Publishing reconciliation window "
            f"from {cutoff}."
        )

    published_count = 0
    delivery_failures = []
    for filename in candidate_files:
        file_url = urljoin(url, filename)
        print(f"[{station_name}] Reading {filename}")
        response = requests.get(file_url, timeout=120)
        response.raise_for_status()

        for line in response.text.splitlines():
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            row_dt = parse_dt(obj.get("dt"))
            if row_dt is None:
                continue

            if cutoff is not None and row_dt < cutoff:
                continue

            publish_event(
                producer,
                station_name,
                obj,
                row_dt,
                delivery_failures,
            )
            producer.poll(0)
            published_count += 1

    remaining = producer.flush(timeout=120)
    if remaining:
        raise KafkaException(
            f"{remaining} backfill messages were not delivered to Kafka."
        )
    if delivery_failures:
        raise KafkaException(
            "Kafka rejected backfill messages: "
            + "; ".join(delivery_failures[:5])
        )

    print(
        f"[{station_name}] Published {published_count:,} events; "
        f"source latest timestamp is {source_latest}."
    )


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": f"{KAFKA_CLIENT_ID}-backfill",
        "enable.idempotence": True,
        "acks": "all",
    })

    with psycopg.connect(**POSTGRES_CONFIG) as conn:
        for station_name, url in STATION_URLS.items():
            try:
                stored_latest = database_latest(conn, station_name)
            except psycopg.errors.UndefinedTable:
                conn.rollback()
                stored_latest = None

            publish_station_backfill(
                producer=producer,
                station_name=station_name,
                url=url,
                stored_latest=stored_latest,
            )

    print("Kafka backfill publishing completed successfully.")


if __name__ == "__main__":
    main()
