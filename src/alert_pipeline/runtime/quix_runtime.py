"""Quix Streams runtime adapter."""

from __future__ import annotations

import logging
from typing import Any

from alert_pipeline.config import Settings
from alert_pipeline.processing.handler import AlertProcessor

logger = logging.getLogger(__name__)


class QuixStreamRuntime:
    """Kafka → Quix dataframe → AlertProcessor (default runtime)."""

    name = "quix"

    def run(self, settings: Settings) -> None:
        from quixstreams import Application

        processor = AlertProcessor(settings, reload_yaml=True)

        app = Application(
            broker_address=settings.kafka_bootstrap_servers,
            consumer_group=settings.kafka_consumer_group,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            auto_create_topics=True,
        )
        logs = app.topic(name=settings.kafka_input_topic, value_deserializer="json")
        sdf = app.dataframe(topic=logs)

        def handle_message(payload: dict[str, Any]) -> dict[str, Any] | None:
            return processor.handle_payload(payload).to_dict()

        sdf = sdf.apply(handle_message, expand=False)
        sdf = sdf.filter(lambda x: x is not None)

        logger.info(
            "Quix runtime topic=%s group=%s",
            settings.kafka_input_topic,
            settings.kafka_consumer_group,
        )
        app.run()
