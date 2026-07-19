"""Quix Streams runtime — Kafka consume + **Quix keyed state** for dedup.

Dedup lives in Quix per-fingerprint state (via ``group_by`` + ``stateful=True``).
Redis is not used for dedup; the pipeline only touches Redis to invalidate the
UI read cache after writes.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Literal, cast

from alert_pipeline.config import Settings
from alert_pipeline.dedup.quix_state import (
    StateLike,
    alert_from_wire,
    build_enrichment,
    process_enriched_with_state,
)
from alert_pipeline.processing.handler import AlertProcessor, parse_log_payload
from alert_pipeline.schemas import LEVEL_RANK, LogEvent
from alert_pipeline.types import (
    DedupEmitRow,
    EnrichmentRow,
    JsonObject,
    JsonValue,
    ProcessResultDict,
)

logger = logging.getLogger(__name__)

DlqProducerKind = Literal["confluent", "kafka-python"]
DlqProducerBundle = tuple[DlqProducerKind, object]


class _SafeJsonDeserializer:
    """Quix Deserializer that never raises — bad payloads become DLQ markers."""

    @property
    def split_values(self) -> bool:
        return False

    def __call__(self, value: bytes | bytearray | str | None, ctx: object = None) -> JsonObject:
        if value is None:
            return {"__unparseable__": True, "raw": None}
        try:
            if isinstance(value, (bytes, bytearray)):
                text = value.decode("utf-8")
            else:
                text = str(value)
            parsed: object = json.loads(text)
            if isinstance(parsed, dict):
                return cast(JsonObject, parsed)
            return {"__unparseable__": True, "raw": cast(JsonValue, parsed)}
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            try:
                raw_repr = (
                    value.decode("utf-8", errors="replace")
                    if isinstance(value, (bytes, bytearray))
                    else repr(value)
                )
            except Exception:  # noqa: BLE001
                raw_repr = repr(value)[:2000]
            return {"__unparseable__": True, "raw": raw_repr[:4000]}


try:
    from quixstreams.models.serializers import Deserializer as _QuixDeserializer

    class SafeJsonDeserializer(_QuixDeserializer):  # type: ignore[misc, valid-type]
        @property
        def split_values(self) -> bool:
            return False

        def __call__(
            self, value: bytes | bytearray | str | None, ctx: object = None
        ) -> Mapping[str, JsonValue]:
            return _SafeJsonDeserializer()(value, ctx)

except ImportError:  # pragma: no cover
    SafeJsonDeserializer = _SafeJsonDeserializer  # type: ignore[misc, assignment]


class QuixStreamRuntime:
    """Kafka → enrich → group_by(fingerprint) → Quix state dedup → Postgres/dispatch."""

    name = "quix"

    def run(self, settings: Settings) -> None:
        from quixstreams import Application

        # Dedup is owned by Quix state — processor only persists + dispatches.
        processor = AlertProcessor(
            settings,
            reload_yaml=True,
            external_dedup=True,
        )
        dlq_producer = _maybe_dlq_producer(settings)
        alert_config = processor.alert_config

        app = Application(
            broker_address=settings.kafka_bootstrap_servers,
            consumer_group=settings.kafka_consumer_group,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            # group_by creates repartition__* topics via Admin API (broker auto-create
            # stays off). Required for keyed co-location of fingerprints.
            auto_create_topics=True,
        )
        logs = app.topic(
            name=settings.kafka_input_topic,
            value_deserializer=SafeJsonDeserializer(),
        )
        sdf = app.dataframe(topic=logs)

        def enrich(payload: object) -> EnrichmentRow | None:
            """Parse + min-level gate; attach fingerprint for group_by."""
            if isinstance(payload, dict) and payload.get("__unparseable__") is True:
                _publish_dlq(
                    dlq_producer,
                    settings.kafka_dlq_topic,
                    reason="unparseable",
                    payload=payload.get("raw", payload),
                )
                return None

            raw = parse_log_payload(payload)
            if raw is None:
                _publish_dlq(
                    dlq_producer,
                    settings.kafka_dlq_topic,
                    reason="unparseable",
                    payload=payload,
                )
                return None

            event = LogEvent.from_kafka_value(raw)
            cfg = alert_config.resolve_for(event)
            if LEVEL_RANK.get(event.level, 0) < LEVEL_RANK[cfg.min_level_enum]:
                return None

            return build_enrichment(
                event,
                window_seconds=cfg.dedup_window_seconds,
                refire_interval_seconds=cfg.refire_interval_seconds,
                suppress_dispatch_while_acknowledged=cfg.suppress_dispatch_while_acknowledged,
                allow_reopen_after_resolve=cfg.allow_reopen_after_resolve,
                dedup_fields=list(cfg.dedup_fields),
            )

        def dedup_stateful(
            row: EnrichmentRow | JsonObject, state: StateLike
        ) -> DedupEmitRow | None:
            return process_enriched_with_state(row, state)

        def sink(row: DedupEmitRow | JsonObject) -> ProcessResultDict | None:
            alert_raw = row["alert"]
            if not isinstance(alert_raw, dict):
                return None
            alert = alert_from_wire(cast(JsonObject, alert_raw))
            result = processor.emit_alert(
                alert,
                suppress_while_acked=bool(row.get("suppress_dispatch_while_acknowledged", True)),
                allow_reopen_after_resolve=bool(row.get("allow_reopen_after_resolve", True)),
            )
            return result.to_dict()

        sdf = sdf.apply(enrich, expand=False)
        sdf = sdf.filter(lambda x: x is not None)
        # Co-locate all events for the same fingerprint on one key (repartition topic)
        sdf = sdf.group_by("fingerprint", name="alert-fingerprint")
        sdf = sdf.apply(dedup_stateful, stateful=True, expand=False)
        sdf = sdf.filter(lambda x: x is not None)
        sdf = sdf.apply(sink, expand=False)
        sdf = sdf.filter(lambda x: x is not None)

        logger.info(
            "Quix runtime topic=%s group=%s dedup=quix-state "
            "auto_create_topics=True (repartition) dlq=%s",
            settings.kafka_input_topic,
            settings.kafka_consumer_group,
            settings.kafka_dlq_topic if settings.kafka_dlq_enabled else "(disabled)",
        )
        try:
            app.run()
        finally:
            if dlq_producer is not None:
                try:
                    dlq_producer[1].flush(5)  # type: ignore[union-attr]
                    dlq_producer[1].close()  # type: ignore[union-attr]
                except Exception:  # noqa: BLE001
                    pass


def _maybe_dlq_producer(settings: Settings) -> DlqProducerBundle | None:
    if not settings.kafka_dlq_enabled or not settings.kafka_dlq_topic:
        return None
    try:
        from confluent_kafka import Producer
    except ImportError:
        try:
            from kafka import KafkaProducer  # type: ignore

            return (
                "kafka-python",
                KafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                ),
            )
        except ImportError:
            logger.warning("No Kafka producer library for DLQ; unparseable messages logged only")
            return None
    return ("confluent", Producer({"bootstrap.servers": settings.kafka_bootstrap_servers}))


def _publish_dlq(
    producer_bundle: DlqProducerBundle | None,
    topic: str,
    *,
    reason: str,
    payload: object,
) -> None:
    if not topic:
        return
    body: JsonObject = {"reason": reason, "payload": _safe_dlq_payload(payload)}
    logger.warning("Routing message to DLQ topic=%s reason=%s", topic, reason)
    if producer_bundle is None:
        return
    kind, producer = producer_bundle
    try:
        if kind == "confluent":
            producer.produce(topic, json.dumps(body).encode("utf-8"))  # type: ignore[union-attr]
            producer.poll(0)  # type: ignore[union-attr]
        else:
            producer.send(topic, body)  # type: ignore[union-attr]
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to publish to DLQ: %s", exc)


def _safe_dlq_payload(raw: object) -> JsonValue:
    if isinstance(raw, dict) and raw.get("__unparseable__"):
        return _safe_dlq_payload(raw.get("raw"))
    if isinstance(raw, (dict, list, str, int, float, bool)) or raw is None:
        return cast(JsonValue, raw)
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8", errors="replace")
    return repr(raw)[:2000]
