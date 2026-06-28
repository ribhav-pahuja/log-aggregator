"""Publish synthetic ERROR logs to Kafka for local demos.

Usage:
    log-producer
    # or: python -m alert_pipeline.sample_producer
"""

from __future__ import annotations

import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from alert_pipeline.config import get_settings

logger = logging.getLogger(__name__)

SERVICES = ["payments-api", "checkout", "inventory", "auth-service"]
MESSAGES = [
    "connection refused while calling postgres",
    "payment gateway timeout after 30s",
    "null pointer in OrderService.confirm",
    "rate limit exceeded for upstream /charge",
    "disk usage above 95% on data volume",
]
ERROR_CODES = ["DB_CONN", "GW_TIMEOUT", "NPE", "RATE_LIMIT", "DISK_FULL", None]


def main() -> None:
    settings = get_settings()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topic = settings.kafka_input_topic
    logger.info("Producing to %s on %s (Ctrl+C to stop)", topic, settings.kafka_bootstrap_servers)

    i = 0
    try:
        while True:
            i += 1
            # Bias toward repeats so dedup is visible
            msg = MESSAGES[0] if i % 3 == 0 else random.choice(MESSAGES)
            code = random.choice(ERROR_CODES)
            service = SERVICES[0] if i % 4 == 0 else random.choice(SERVICES)
            level = "ERROR" if random.random() > 0.15 else "INFO"
            event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "service": service,
                "host": f"pod-{random.randint(1, 3)}",
                "message": msg,
                "error_code": code,
                "trace_id": str(uuid.uuid4()),
                "labels": {"env": "local", "team": "platform"},
            }
            producer.produce(
                topic,
                key=service.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            producer.poll(0)
            logger.info("produced #%s level=%s service=%s msg=%s", i, level, service, msg[:50])
            time.sleep(0.8)
    except KeyboardInterrupt:
        logger.info("Stopping producer…")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
