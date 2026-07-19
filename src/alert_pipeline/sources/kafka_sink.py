"""Final adapter: publish normalized log events to the Kafka ``logs`` topic."""

from __future__ import annotations

import json
import logging
from typing import Literal

from alert_pipeline.config import Settings
from alert_pipeline.sources.base import NormalizedLog
from alert_pipeline.types import JsonObject

logger = logging.getLogger(__name__)

ProducerKind = Literal["confluent", "kafka-python"]


class KafkaLogSink:
    """Final adapter — the only place sources need to reach Kafka.

    Implements the :class:`~alert_pipeline.sources.base.LogSink` protocol.
    All ingress sources (Grafana Loki, Alerting webhook, future systems)
    inject this sink via :class:`~alert_pipeline.sources.base.BaseLogSource`.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._kind: ProducerKind
        self._producer: object
        self._kind, self._producer = _make_producer(settings.kafka_bootstrap_servers)
        self.topic = settings.kafka_input_topic
        logger.info(
            "KafkaLogSink (final adapter) topic=%s bootstrap=%s producer=%s",
            self.topic,
            settings.kafka_bootstrap_servers,
            self._kind,
        )

    def publish(self, event: NormalizedLog | JsonObject, *, key: str | None = None) -> None:
        payload = dict(event)
        body = json.dumps(payload, default=str).encode("utf-8")
        key_bytes = (key or str(payload.get("service") or "unknown")).encode("utf-8")
        if self._kind == "confluent":
            self._producer.produce(self.topic, key=key_bytes, value=body)  # type: ignore[union-attr]
            self._producer.poll(0)  # type: ignore[union-attr]
        else:
            self._producer.send(self.topic, key=key_bytes, value=body)  # type: ignore[union-attr]

    def flush(self, timeout: float = 10.0) -> None:
        try:
            if self._kind == "confluent":
                self._producer.flush(timeout)  # type: ignore[union-attr]
            else:
                self._producer.flush()  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001
            logger.warning("KafkaLogSink flush error: %s", exc)

    def close(self) -> None:
        self.flush()
        if self._kind == "kafka-python":
            try:
                self._producer.close()  # type: ignore[union-attr]
            except Exception:  # noqa: BLE001
                pass


def _make_producer(bootstrap: str) -> tuple[ProducerKind, object]:
    try:
        from confluent_kafka import Producer

        return "confluent", Producer({"bootstrap.servers": bootstrap})
    except ImportError:
        pass
    try:
        from kafka import KafkaProducer  # type: ignore

        return (
            "kafka-python",
            KafkaProducer(
                bootstrap_servers=bootstrap.split(","),
                value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else v,
                key_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else v,
            ),
        )
    except ImportError as exc:
        raise ImportError(
            "Kafka producer requires confluent-kafka (via quixstreams / pipeline extra) "
            "or kafka-python. Install: pip install 'alert-pipeline[pipeline]'"
        ) from exc
