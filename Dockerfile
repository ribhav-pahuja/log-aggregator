# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Runtime libs for psycopg; curl for optional health probes
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM base AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --prefix=/install .

FROM base AS runtime

COPY --from=builder /install /usr/local
COPY config/alerts.yaml /config/alerts.yaml
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Non-root user
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app
USER appuser

ENV ALERT_CONFIG_PATH=/config/alerts.yaml \
    KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
    KAFKA_INPUT_TOPIC=logs \
    KAFKA_CONSUMER_GROUP=alert-pipeline \
    DATABASE_URL=postgresql+psycopg://alerts:alerts@postgres:5432/alerts \
    LOG_LEVEL=INFO

ENTRYPOINT ["/entrypoint.sh"]
CMD ["alert-pipeline"]
