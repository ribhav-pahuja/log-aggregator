"""Portable incident processor — no stream-runtime imports.

The Quix runtime owns dedup (keyed state) and calls ``emit_alert``.
``handle_payload`` / ``handle_event`` use in-process memory for unit tests.

Redis is never used for dedup here — only optional UI cache invalidation.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from alert_pipeline.alert_config import AlertYamlConfig, get_alert_config, reload_alert_config
from alert_pipeline.config import Settings, get_settings
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.dedup.engine import DedupEngine
from alert_pipeline.dedup.store import MemoryDedupStore, build_dedup_store
from alert_pipeline.dispatchers.registry import DispatchFanout, build_dispatchers
from alert_pipeline.schemas import LEVEL_RANK, AlertEvent, LogEvent, LogLevel
from alert_pipeline.ui_cache_invalidate import invalidate_ui_snapshot

logger = logging.getLogger(__name__)


def parse_log_payload(value: Any) -> dict[str, Any] | None:
    """Normalize heterogeneous Kafka message values to a dict.

    Returns None for unparseable payloads (callers may route to DLQ).
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.warning("Skipping non-JSON bytes payload")
            return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            logger.warning("Skipping non-JSON string payload")
            return None
    logger.warning("Unsupported payload type: %s", type(value))
    return None


@dataclass
class ProcessResult:
    """Outcome of processing one log (for metrics / optional sink topics)."""

    emitted: bool
    alert_id: str | None = None
    fingerprint: str | None = None
    is_new: bool | None = None
    occurrence_count: int | None = None
    service: str | None = None
    severity: str | None = None
    dispatch_suppressed: bool = False
    skipped_reason: str | None = None
    raw_for_dlq: Any = None

    def to_dict(self) -> dict[str, Any] | None:
        if not self.emitted:
            return None
        return {
            "alert_id": self.alert_id,
            "fingerprint": self.fingerprint,
            "is_new": self.is_new,
            "occurrence_count": self.occurrence_count,
            "service": self.service,
            "severity": self.severity,
            "dispatch_suppressed": self.dispatch_suppressed,
        }


class AlertProcessor:
    """
    Persist + dispatch for incidents. Optional in-process DedupEngine for
    tests / non-Quix runtimes.

    When ``external_dedup=True`` (Quix path), callers run dedup in the stream
    engine and only invoke ``emit_alert``.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        alert_config: AlertYamlConfig | None = None,
        reload_yaml: bool = False,
        external_dedup: bool = False,
    ) -> None:
        self.settings = settings or get_settings()
        self.external_dedup = external_dedup
        if alert_config is not None:
            self.alert_config = alert_config
        elif reload_yaml:
            self.alert_config = reload_alert_config()
        else:
            self.alert_config = get_alert_config()

        min_level = LogLevel.normalize(
            self.alert_config.defaults.min_level or self.settings.alert_min_level
        )

        # Never use Redis for dedup. Memory engine is for unit tests only.
        if external_dedup:
            store = MemoryDedupStore()
            backend_label = "external (quix-state)"
        else:
            backend = self.settings.dedup_backend
            if backend == "redis":
                logger.warning(
                    "DEDUP_BACKEND=redis is deprecated; using in-process memory. "
                    "Quix runtime uses Quix keyed state. Redis is for UI cache only."
                )
                store = MemoryDedupStore()
                backend_label = "memory (redis-dedup-disabled)"
            elif backend in ("quix", "external"):
                store = MemoryDedupStore()
                backend_label = "memory (use Quix runtime for quix-state)"
            else:
                store = build_dedup_store("memory")
                backend_label = "memory"

        self.engine = DedupEngine(
            alert_config=self.alert_config,
            store=store,
            window_seconds=self.settings.dedup_window_seconds,
            update_interval_seconds=self.settings.dedup_update_interval_seconds,
            min_level=min_level,
        )
        self.repo = AlertRepository(self.settings.database_url)
        self.fanout = DispatchFanout(build_dispatchers(self.settings), repo=self.repo)
        logger.info(
            "AlertProcessor ready dedup=%s fields=%s window=%ss refire=%ss min_level=%s",
            backend_label,
            self.alert_config.defaults.dedup_fields,
            self.alert_config.defaults.dedup_window_seconds,
            self.alert_config.defaults.refire_interval_seconds,
            self.alert_config.defaults.min_level,
        )

    def handle_payload(self, payload: Any) -> ProcessResult:
        if isinstance(payload, dict) and payload.get("__unparseable__") is True:
            return ProcessResult(
                emitted=False,
                skipped_reason="unparseable",
                raw_for_dlq=payload.get("raw", payload),
            )
        raw = parse_log_payload(payload)
        if raw is None:
            return ProcessResult(
                emitted=False, skipped_reason="unparseable", raw_for_dlq=payload
            )
        return self.handle_event(LogEvent.from_kafka_value(raw))

    def handle_event(self, event: LogEvent) -> ProcessResult:
        """In-process dedup path (unit tests). Quix runtime uses emit_alert instead."""
        if self.external_dedup:
            raise RuntimeError(
                "AlertProcessor was constructed with external_dedup=True; "
                "use emit_alert() after Quix state dedup, not handle_event()"
            )
        cfg = self.engine.settings_for(event)
        if LEVEL_RANK.get(event.level, 0) < LEVEL_RANK[cfg.min_level_enum]:
            return ProcessResult(emitted=False, skipped_reason="below_min_level")

        alert = self.engine.process(event)
        if alert is None:
            return ProcessResult(emitted=False, skipped_reason="dedup_suppressed")

        return self.emit_alert(alert, suppress_while_acked=cfg.suppress_dispatch_while_acknowledged)

    def emit_alert(
        self, alert: AlertEvent, *, suppress_while_acked: bool = True
    ) -> ProcessResult:
        """Persist incident and fan-out notifications (dedup already decided)."""
        return self._persist_and_maybe_dispatch(alert, suppress_while_acked)

    def _persist_and_maybe_dispatch(
        self, alert: AlertEvent, suppress_while_acked: bool
    ) -> ProcessResult:
        record = self.repo.upsert_alert(alert)
        # Redis: UI snapshot only
        if self.settings.ui_cache_invalidate_on_write:
            invalidate_ui_snapshot(
                self.settings.redis_url,
                key_prefix=self.settings.ui_cache_key_prefix,
            )

        dispatch_suppressed = False
        if (
            not alert.is_new
            and suppress_while_acked
            and record.status == "acknowledged"
        ):
            dispatch_suppressed = True
            logger.info(
                "Skip dispatch for acked incident %s (refire suppressed by YAML)",
                alert.id,
            )
        else:
            self.fanout.dispatch(alert)

        return ProcessResult(
            emitted=True,
            alert_id=alert.id,
            fingerprint=alert.fingerprint,
            is_new=alert.is_new,
            occurrence_count=alert.occurrence_count,
            service=alert.service,
            severity=alert.severity.value,
            dispatch_suppressed=dispatch_suppressed,
        )
