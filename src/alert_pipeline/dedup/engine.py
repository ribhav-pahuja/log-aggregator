"""In-process deduplication state with YAML-driven windows / refire intervals."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from alert_pipeline.alert_config import AlertYamlConfig, RefireSettings, get_alert_config
from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
from alert_pipeline.schemas import (
    LEVEL_RANK,
    AlertEvent,
    AlertStatus,
    LogEvent,
    LogLevel,
)

logger = logging.getLogger(__name__)

ResolveFn = Callable[[LogEvent], RefireSettings]


@dataclass
class IncidentState:
    alert_id: str
    fingerprint: str
    first_seen: datetime
    last_seen: datetime
    occurrence_count: int = 1
    last_emitted_at: float = field(default_factory=time.time)
    severity: LogLevel = LogLevel.ERROR
    service: str = "unknown"
    host: str = "unknown"
    title: str = ""
    sample_message: str = ""
    error_code: str | None = None
    trace_id: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    window_seconds: int = 300


class DedupEngine:
    def __init__(
        self,
        *,
        alert_config: AlertYamlConfig | None = None,
        resolve_settings: ResolveFn | None = None,
        # Fallbacks if YAML not used
        window_seconds: int = 300,
        update_interval_seconds: int = 60,
        min_level: LogLevel = LogLevel.ERROR,
    ) -> None:
        self._config = alert_config or get_alert_config()
        self._resolve = resolve_settings or (lambda ev: self._config.resolve_for(ev))
        self._fallback = RefireSettings(
            min_level=min_level.value,
            dedup_window_seconds=window_seconds,
            refire_interval_seconds=update_interval_seconds,
        )
        self._state: dict[str, IncidentState] = {}

    def settings_for(self, event: LogEvent) -> RefireSettings:
        try:
            return self._resolve(event)
        except Exception:  # noqa: BLE001
            logger.exception("Failed resolving alert config; using fallback")
            return self._fallback

    def _expire_stale(self, now: float) -> None:
        expired = [
            fp
            for fp, st in self._state.items()
            if now - st.last_seen.timestamp() > st.window_seconds
        ]
        for fp in expired:
            del self._state[fp]

    def process(self, event: LogEvent) -> AlertEvent | None:
        cfg = self.settings_for(event)
        min_rank = LEVEL_RANK[cfg.min_level_enum]
        if LEVEL_RANK.get(event.level, 0) < min_rank:
            return None

        now_ts = time.time()
        self._expire_stale(now_ts)

        fingerprint = compute_fingerprint(event, cfg.dedup_fields)
        existing = self._state.get(fingerprint)

        if existing is None:
            alert = AlertEvent(
                fingerprint=fingerprint,
                title=build_title(event),
                description=event.message,
                severity=event.level,
                service=event.service,
                host=event.host,
                status=AlertStatus.OPEN,
                occurrence_count=1,
                first_seen=event.timestamp,
                last_seen=event.timestamp,
                error_code=event.error_code,
                trace_id=event.trace_id,
                labels=event.labels,
                sample_message=event.message,
                is_new=True,
            )
            self._state[fingerprint] = IncidentState(
                alert_id=alert.id,
                fingerprint=fingerprint,
                first_seen=event.timestamp,
                last_seen=event.timestamp,
                occurrence_count=1,
                last_emitted_at=now_ts,
                severity=event.level,
                service=event.service,
                host=event.host,
                title=alert.title,
                sample_message=event.message,
                error_code=event.error_code,
                trace_id=event.trace_id,
                labels=dict(event.labels),
                window_seconds=cfg.dedup_window_seconds,
            )
            logger.info(
                "New incident fingerprint=%s service=%s window=%ss refire=%ss",
                fingerprint,
                event.service,
                cfg.dedup_window_seconds,
                cfg.refire_interval_seconds,
            )
            return alert

        existing.occurrence_count += 1
        existing.last_seen = event.timestamp
        existing.sample_message = event.message
        existing.window_seconds = cfg.dedup_window_seconds
        if event.trace_id:
            existing.trace_id = event.trace_id
        if LEVEL_RANK[event.level] > LEVEL_RANK[existing.severity]:
            existing.severity = event.level

        should_emit_update = (now_ts - existing.last_emitted_at) >= cfg.refire_interval_seconds
        if not should_emit_update:
            logger.debug(
                "Suppressed duplicate fingerprint=%s count=%s (refire in %ss)",
                fingerprint,
                existing.occurrence_count,
                cfg.refire_interval_seconds,
            )
            return None

        existing.last_emitted_at = now_ts
        return AlertEvent(
            id=existing.alert_id,
            fingerprint=fingerprint,
            title=existing.title,
            description=event.message,
            severity=existing.severity,
            service=existing.service,
            host=existing.host,
            status=AlertStatus.UPDATED,
            occurrence_count=existing.occurrence_count,
            first_seen=existing.first_seen,
            last_seen=existing.last_seen,
            error_code=existing.error_code,
            trace_id=existing.trace_id,
            labels=existing.labels,
            sample_message=existing.sample_message,
            is_new=False,
        )

    def stats(self) -> dict[str, int]:
        return {"open_incidents": len(self._state)}
