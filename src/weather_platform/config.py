import os

from dotenv import load_dotenv


# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------

load_dotenv()


# ---------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------

POSTGRES_CONFIG = {
    "host": os.getenv(
        "POSTGRES_HOST",
        "localhost",
    ),
    "port": int(
        os.getenv(
            "POSTGRES_PORT",
            "5432",
        )
    ),
    "dbname": os.getenv(
        "POSTGRES_DB",
        "weather_db",
    ),
    "user": os.getenv(
        "POSTGRES_USER",
        "weather_user",
    ),
    "password": os.getenv(
        "POSTGRES_PASSWORD",
        "weather_password",
    ),
}


# ---------------------------------------------------------
# Weather Stations
# ---------------------------------------------------------

WAVERLY_STATION = os.getenv(
    "WAVERLY_STATION",
    "wx_waverly",
)

EAST_STREET_STATION = os.getenv(
    "EAST_STREET_STATION",
    "wx_east_st",
)


STATION_URLS = {
    WAVERLY_STATION: os.getenv(
        "WAVERLY_STATION_URL",
        "https://tylerconlon.com/wx/logs/wx_waverly/",
    ),
    EAST_STREET_STATION: os.getenv(
        "EAST_STREET_STATION_URL",
        "https://tylerconlon.com/wx/logs/wx_east_st/",
    ),
}


# ---------------------------------------------------------
# FastAPI
# ---------------------------------------------------------

API_HOST = os.getenv(
    "API_HOST",
    "127.0.0.1",
)

API_PORT = int(
    os.getenv(
        "API_PORT",
        "8000",
    )
)

API_BASE_URL = os.getenv(
    "API_BASE_URL",
    f"http://{API_HOST}:{API_PORT}",
)


# ---------------------------------------------------------
# Dash
# ---------------------------------------------------------

DASH_HOST = os.getenv(
    "DASH_HOST",
    "127.0.0.1",
)

DASH_PORT = int(
    os.getenv(
        "DASH_PORT",
        "8050",
    )
)


# ---------------------------------------------------------
# Kafka
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "raw_weather_events_python",
)

KAFKA_CLIENT_ID = os.getenv(
    "KAFKA_CLIENT_ID",
    "weather-producer",
)

KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "weather-postgres-consumer",
)

KAFKA_PARTITION = int(
    os.getenv(
        "KAFKA_PARTITION",
        "0",
    )
)

KAFKA_START_OFFSET = int(
    os.getenv(
        "KAFKA_START_OFFSET",
        "0",
    )
)

PRODUCER_POLL_SECONDS = int(
    os.getenv(
        "PRODUCER_POLL_SECONDS",
        "120",
    )
)

RECONCILIATION_LOOKBACK_DAYS = int(
    os.getenv(
        "RECONCILIATION_LOOKBACK_DAYS",
        "2",
    )
)
