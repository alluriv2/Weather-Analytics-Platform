import json
import os
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from confluent_kafka import KafkaException, Producer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_TOPIC,
    PRODUCER_POLL_SECONDS,
    STATION_URLS,
)


# ---------------------------------------------------------
# Local state
# ---------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(BASE_DIR, "producer_state.json")


# ---------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------

def parse_dt(raw_dt):
    if not raw_dt:
        return None

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
# Producer checkpoint
# ---------------------------------------------------------

def load_state():
    if not os.path.exists(STATE_PATH):
        raise FileNotFoundError(
            f"{STATE_PATH} not found. "
            "Run initial_backfill.py first."
        )

    with open(
        STATE_PATH,
        "r",
        encoding="utf-8",
    ) as file:
        raw_state = json.load(file)

    return {
        station: parse_dt(timestamp)
        if timestamp
        else None
        for station, timestamp in raw_state.items()
    }


def save_state(state):
    serializable_state = {
        station: timestamp.isoformat()
        if timestamp
        else None
        for station, timestamp in state.items()
    }

    temporary_path = STATE_PATH + ".tmp"

    with open(
        temporary_path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            serializable_state,
            file,
            indent=2,
        )

    os.replace(
        temporary_path,
        STATE_PATH,
    )


# ---------------------------------------------------------
# Remote weather files
# ---------------------------------------------------------

def list_server_files(url):
    response = requests.get(
        url,
        timeout=30,
    )

    response.raise_for_status()

    soup = BeautifulSoup(
        response.text,
        "html.parser",
    )

    files = [
        anchor.get("href")
        for anchor in soup.find_all("a")
        if anchor.get("href")
        and anchor.get("href").endswith(".txt")
        and anchor.get("href")[:8].isdigit()
    ]

    return sorted(files)


# ---------------------------------------------------------
# Kafka delivery callback
# ---------------------------------------------------------

def delivery_report(error, message):
    if error is not None:
        print(
            f"Kafka delivery failed: {error}"
        )
        return

    print(
        f"Delivered to {message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


# ---------------------------------------------------------
# Station publishing
# ---------------------------------------------------------

def publish_station_updates(
    producer,
    station_name,
    url,
    last_ts,
):
    if last_ts is None:
        raise ValueError(
            f"No checkpoint found for {station_name}. "
            "Run the initial backfill first."
        )

    start_date = last_ts.strftime("%Y%m%d")
    files = list_server_files(url)

    candidate_files = [
        filename
        for filename in files
        if filename[:8] >= start_date
    ]

    print(
        f"\n[{station_name}] "
        f"Last checkpoint: {last_ts}"
    )

    print(
        f"[{station_name}] "
        f"Files to check: {len(candidate_files)}"
    )

    newest_published_ts = last_ts
    published_count = 0

    for filename in candidate_files:
        file_url = urljoin(
            url,
            filename,
        )

        print(
            f"[{station_name}] "
            f"Checking {filename}"
        )

        response = requests.get(
            file_url,
            timeout=30,
        )

        response.raise_for_status()

        for line in response.text.splitlines():
            line = line.strip()

            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            row_dt = parse_dt(
                obj.get("dt")
            )

            if row_dt is None:
                continue

            if row_dt <= last_ts:
                continue

            node = obj.get(
                "node",
                {},
            )

            obj["station"] = (
                node.get("hostname")
                or station_name
            )

            message_key = (
                f"{obj['station']}|"
                f"{row_dt.isoformat()}"
            )

            # If the local producer queue is full,
            # poll and retry instead of losing the record.
            while True:
                try:
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=message_key.encode(
                            "utf-8"
                        ),
                        value=json.dumps(
                            obj
                        ).encode("utf-8"),
                        callback=delivery_report,
                    )
                    break

                except BufferError:
                    producer.poll(1.0)

            producer.poll(0)

            published_count += 1

            if row_dt > newest_published_ts:
                newest_published_ts = row_dt

    remaining_messages = producer.flush(
        timeout=30
    )

    if remaining_messages != 0:
        raise KafkaException(
            f"{remaining_messages} Kafka messages "
            "were not delivered."
        )

    print(
        f"[{station_name}] "
        f"Published {published_count} new events."
    )

    print(
        f"[{station_name}] "
        f"New checkpoint: {newest_published_ts}"
    )

    return newest_published_ts


# ---------------------------------------------------------
# One producer cycle
# ---------------------------------------------------------

def run_once():
    state = load_state()

    producer = Producer({
        "bootstrap.servers":
            KAFKA_BOOTSTRAP_SERVERS,

        "client.id":
            KAFKA_CLIENT_ID,
    })

    state_changed = False

    for station_name, url in STATION_URLS.items():
        last_ts = state.get(
            station_name
        )

        newest_ts = publish_station_updates(
            producer=producer,
            station_name=station_name,
            url=url,
            last_ts=last_ts,
        )

        if (
            last_ts is None
            or newest_ts > last_ts
        ):
            state[station_name] = newest_ts
            state_changed = True

    if state_changed:
        save_state(state)

        print(
            f"\nSaved updated producer "
            f"checkpoint to {STATE_PATH}"
        )
    else:
        print(
            "\nNo new events. "
            "Producer checkpoint unchanged."
        )

    print(
        "\nKafka producer cycle completed."
    )


# ---------------------------------------------------------
# Main loop
# ---------------------------------------------------------

def main():
    while True:
        try:
            run_once()

        except KeyboardInterrupt:
            print(
                "\nProducer stopped by user."
            )
            break

        except (
            requests.RequestException,
            KafkaException,
            ValueError,
            FileNotFoundError,
            json.JSONDecodeError,
        ) as exc:
            print(
                f"\nProducer cycle failed: {exc}"
            )

        print(
            f"Sleeping for "
            f"{PRODUCER_POLL_SECONDS} seconds..."
        )

        time.sleep(
            PRODUCER_POLL_SECONDS
        )


if __name__ == "__main__":
    main()