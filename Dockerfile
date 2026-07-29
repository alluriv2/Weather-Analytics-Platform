FROM python:3.12.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt \
    && addgroup --system --gid 10001 weather \
    && adduser --system --uid 10001 --ingroup weather --home /app weather

COPY --chown=weather:weather . .

USER 10001:10001

CMD ["python", "-m", "weather_platform.producer"]
