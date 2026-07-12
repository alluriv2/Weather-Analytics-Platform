# Real-Time Weather Analytics Platform

A real-time weather analytics platform that ingests live weather observations from remote weather stations, streams new observations through Apache Kafka, stores both historical and latest-state data in PostgreSQL, precomputes historical aggregates, exposes REST APIs using FastAPI, and visualizes live and historical weather data through an interactive Dash dashboard.

---

## Overview

This project demonstrates the design and implementation of an end-to-end real-time data engineering pipeline using modern streaming and analytics technologies.

The platform continuously monitors remote weather stations, detects newly available observations, publishes them into Kafka, consumes the events into PostgreSQL, maintains both raw and latest weather tables, periodically generates analytical aggregates, and exposes the processed data through REST APIs for an interactive dashboard.

The architecture follows a typical modern streaming analytics workflow:

```
Weather Stations
        │
        ▼
Initial Backfill
        │
        ▼
Kafka Producer
        │
        ▼
Apache Kafka
        │
        ▼
Kafka Consumer
        │
        ▼
PostgreSQL
        │
        ├──────────────► weather
        │
        ├──────────────► weather_latest
        │
        └──────────────► weather_aggregates
                              │
                              ▼
                        FastAPI REST API
                              │
                              ▼
                     Plotly Dash Dashboard
```

---

# Features

## Real-Time Streaming

- Continuous polling of remote weather station log files
- Incremental event detection using producer checkpoints
- Apache Kafka event streaming
- Idempotent message publishing
- Automatic recovery after restart

---

## Historical Data Storage

Stores every weather observation in PostgreSQL.

Captured weather metrics include:

- Temperature
- Wind Speed
- Wind Direction
- Humidity
- Pressure
- Rainfall
- Lux
- Node metadata
- Server metadata

---

## Latest State Table

Maintains a continuously updated table containing the most recent observation from every weather station.

Optimized for dashboard queries.

---

## Historical Aggregation

Automatically generates precomputed analytical datasets for multiple time windows.

Current aggregation windows:

- 24 Hours
- 1 Week
- 1 Month
- 1 Year

Aggregations include:

- Average Temperature
- Average Wind Speed
- Average Humidity
- Average Pressure
- Average Lux
- Total Rainfall

---

## REST API

FastAPI exposes two endpoints.

### Latest Weather

```
GET /latest
```

Example

```
/latest?station=wx_waverly
```

Returns the most recent observation.

---

### Historical Weather

```
GET /history
```

Example

```
/history?window=week&metric=temperature
```

Supports

- Station filtering
- Time window selection
- Individual weather metrics

---

## Interactive Dashboard

Built using Plotly Dash.

Features include

- Current weather conditions
- Historical trend visualization
- Interactive station selection
- Multiple aggregation windows
- Clickable metric cards
- Responsive interface

---

# Technology Stack

| Component | Technology |
|------------|------------|
| Programming Language | Python |
| Streaming Platform | Apache Kafka |
| Database | PostgreSQL |
| REST API | FastAPI |
| Dashboard | Plotly Dash |
| Visualization | Plotly |
| Data Processing | Pandas |
| HTTP Client | Requests, HTTPX |
| Web Scraping | BeautifulSoup |
| Containerization | Docker Compose |

---

# Project Structure

```
WEATHER_ANALYTICS_PLATFORM/

│
├── dashboard/
│   ├── app.py
│   ├── __init__.py
│   └── pages/
│       ├── __init__.py
│       ├── landing.py
│       └── trends.py
│
├── kafka-storage/
├── spark/
│
├── .env
├── .gitignore
├── requirements.txt
├── config.py
├── docker-compose.yml
│
├── initial_backfill.py
├── weather_kafka_producer.py
├── weather_kafka_consumer.py
├── weather_aggregation_postgres.py
├── weather_api.py
│
├── producer_state.json
│
└── README.md
```

---

# PostgreSQL Tables

## weather

Stores every historical weather observation.

Primary Key

```
(station, dt)
```

---

## weather_latest

Stores only the newest observation per station.

Used by the dashboard for live weather cards.

---

## weather_aggregates

Stores precomputed aggregates for historical trend visualization.

Supports

- day
- week
- month
- year

---

# Data Pipeline

## 1. Initial Backfill

Downloads all historical weather files.

Creates

- PostgreSQL weather table
- producer checkpoint

Run

```bash
python initial_backfill.py
```

---

## 2. Kafka Producer

Continuously checks remote weather stations for newly published observations.

Publishes only unseen records.

Run

```bash
python weather_kafka_producer.py
```

---

## 3. Kafka Consumer

Consumes Kafka events.

Updates

- weather
- weather_latest

Run

```bash
python weather_kafka_consumer.py
```

---

## 4. Aggregation Job

Refreshes historical aggregates.

Updates

```
weather_aggregates
```

Run

```bash
python weather_aggregation_postgres.py
```

---

## 5. FastAPI

Starts REST API.

Run

```bash
uvicorn weather_api:app --reload
```

---

## 6. Dashboard

Starts interactive dashboard.

Run

```bash
python dashboard/app.py
```

---

# Configuration

Application settings are managed through

```
.env
```

Configuration includes

- PostgreSQL connection
- Kafka configuration
- FastAPI settings
- Dash settings
- Weather station URLs
- Producer polling interval

---

# Installation

Clone repository

```bash
git clone https://github.com/<your_username>/Weather_Analytics_Platform.git
```

Navigate into project

```bash
cd Weather_Analytics_Platform
```

Install dependencies

```bash
pip install -r requirements.txt
```

Start Docker services

```bash
docker-compose up -d
```

Run initial backfill

```bash
python initial_backfill.py
```

Start producer

```bash
python weather_kafka_producer.py
```

Start consumer

```bash
python weather_kafka_consumer.py
```

Generate aggregates

```bash
python weather_aggregation_postgres.py
```

Start API

```bash
uvicorn weather_api:app --reload
```

Start dashboard

```bash
python dashboard/app.py
```

Dashboard

```
http://127.0.0.1:8050
```

API

```
http://127.0.0.1:8000/docs
```

---

# Future Improvements

- Kubernetes deployment
- Azure cloud deployment
- Automatic aggregation scheduling using Airflow
- Incremental aggregate refresh
- Historical "All Time" aggregation
- Authentication and API rate limiting
- Dockerized microservices
- CI/CD using GitHub Actions
- Monitoring using Prometheus and Grafana

---

# Learning Objectives

This project demonstrates practical experience with

- Event-driven architecture
- Real-time streaming pipelines
- Apache Kafka
- PostgreSQL
- Data modeling
- Incremental ETL
- REST API development
- Dashboard development
- Docker
- Configuration management
- End-to-end data engineering workflows

---

# License

This project is intended for educational and portfolio purposes.