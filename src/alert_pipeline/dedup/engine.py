"""Deduplication engine with pluggable store (memory or shared Redis)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from uuid import uuid4

from alert_pipeline.alert_config import AlertYamlConfig, RefireSettings, get_alert_config
from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
from alert_pipeline.dedup.store import (
    DedupStore,
    IncidentState,
    MemoryDedupStore,
    RedisDedupStore,
)
from alert_pipeline.schemas import (
    LEVEL_RANK,
    AlertEvent,
    AlertStatus,
    LogEvent,
    LogLevel,
)

logger = logging.getLogger(__name__)

ResolveFn = Callable[[LogEvent], RefireSettings]


class DedupEngine:
    def __init__(
        self,
        *,
        alert_config: AlertYamlConfig | None = None,
        resolve_settings: ResolveFn | None = None,
        store: DedupStore | None = None,
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
        self._store: DedupStore = store or MemoryDedupStore()

    @property
    def store(self) -> DedupStore:
        return self._store

    def settings_for(self, event: LogEvent) -> RefireSettings:
        try:
            return self._resolve(event)
        except Exception:  # noqa: BLE001
            logger.exception("Failed resolving alert config; using fallback")
            return self._fallback

    def process(self, event: LogEvent) -> AlertEvent | None:
        cfg = self.settings_for(event)
        min_rank = LEVEL_RANK[cfg.min_level_enum]
        if LEVEL_RANK.get(event.level, 0) < min_rank:
            return None

        # Event-time for window/refire/expiry (not wall-clock processing time).
        now_ts = event.timestamp.timestamp() if event.timestamp else time.time()
        wall = time.time()
        if now_ts > wall + 300:
            now_ts = wall
        self._store.expire_stale(now_ts)

        fingerprint = compute_fingerprint(event, cfg.dedup_fields)
        title = build_title(event)

        # Fast path: Redis Lua does create/update/suppress atomically
        if isinstance(self._store, RedisDedupStore):
            return self._process_redis(event, cfg, fingerprint, title, now_ts)

        return self._process_memory(event, cfg, fingerprint, title, now_ts)

    def _process_redis(
        self,
        event: LogEvent,
        cfg: RefireSettings,
        fingerprint: str,
        title: str,
        now_ts: float,
    ) -> AlertEvent | None:
        assert isinstance(self._store, RedisDedupStore)
        create_state = IncidentState(
            alert_id=str(uuid4()),
            fingerprint=fingerprint,
            first_seen=event.timestamp,
            last_seen=event.timestamp,
            occurrence_count=1,
            last_emitted_at=now_ts,
            severity=event.level,
            service=event.service,
            host=event.host,
            title=title,
            sample_message=event.message,
            error_code=event.error_code,
            trace_id=event.trace_id,
            labels=dict(event.labels),
            window_seconds=cfg.dedup_window_seconds,
        )
        action, state = self._store.process_event(
            fingerprint=fingerprint,
            window_seconds=cfg.dedup_window_seconds,
            refire_interval_seconds=cfg.refire_interval_seconds,
            create_state=create_state,
            event_severity=event.level.value,
            event_last_seen=event.timestamp,
            event_sample_message=event.message,
            event_trace_id=event.trace_id,
        )
        if action == "new":
            logger.info(
                "New incident fingerprint=%s service=%s window=%ss backend=redis",
                fingerprint,
                event.service,
                cfg.dedup_window_seconds,
            )
            return self._to_alert(
                state, is_new=True, status=AlertStatus.OPEN, description=event.message
            )
        if action == "suppress":
            logger.debug(
                "Suppressed duplicate fingerprint=%s count=%s (redis)",
                fingerprint,
                state.occurrence_count,
            )
            return None
        # update
        return self._to_alert(
            state, is_new=False, status=AlertStatus.UPDATED, description=event.message
        )

    def _process_memory(
        self,
        event: LogEvent,
        cfg: RefireSettings,
        fingerprint: str,
        title: str,
        now_ts: float,
    ) -> AlertEvent | None:
        existing = self._store.get(fingerprint)

        if existing is None:
            alert_id = str(uuid4())
            state = IncidentState(
                alert_id=alert_id,
                fingerprint=fingerprint,
                first_seen=event.timestamp,
                last_seen=event.timestamp,
                occurrence_count=1,
                last_emitted_at=now_ts,
                severity=event.level,
                service=event.service,
                host=event.host,
                title=title,
                sample_message=event.message,
                error_code=event.error_code,
                trace_id=event.trace_id,
                labels=dict(event.labels),
                window_seconds=cfg.dedup_window_seconds,
            )
            created = self._store.try_create(state, ttl_seconds=cfg.dedup_window_seconds)
            if not created:
                # Race with another thread — treat as existing
                existing = self._store.get(fingerprint)
                if existing is None:
                    return None
            else:
                logger.info(
                    "New incident fingerprint=%s service=%s window=%ss refire=%ss",
                    fingerprint,
                    event.service,
                    cfg.dedup_window_seconds,
                    cfg.refire_interval_seconds,
                )
                return AlertEvent(
                    id=alert_id,
                    fingerprint=fingerprint,
                    title=title,
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
            self._store.put(existing, ttl_seconds=cfg.dedup_window_seconds)
            logger.debug(
                "Suppressed duplicate fingerprint=%s count=%s (refire in %ss)",
                fingerprint,
                existing.occurrence_count,
                cfg.refire_interval_seconds,
            )
            return None

        existing.last_emitted_at = now_ts
        self._store.put(existing, ttl_seconds=cfg.dedup_window_seconds)
        return self._to_alert(
            existing, is_new=False, status=AlertStatus.UPDATED, description=event.message
        )

    @staticmethod
    def _to_alert(
        state: IncidentState,
        *,
        is_new: bool,
        status: AlertStatus,
        description: str,
    ) -> AlertEvent:
        return AlertEvent(
            id=state.alert_id,
            fingerprint=state.fingerprint,
            title=state.title,
            description=description,
            severity=state.severity
            if isinstance(state.severity, LogLevel)
            else LogLevel.normalize(str(state.severity)),
            service=state.service,
            host=state.host,
            status=status,
            occurrence_count=state.occurrence_count,
            first_seen=state.first_seen,
            last_seen=state.last_seen,
            error_code=state.error_code,
            trace_id=state.trace_id,
            labels=state.labels,
            sample_message=state.sample_message,
            is_new=is_new,
        )

    def stats(self) -> dict[str, int]:
        return {"open_incidents": self._store.count()}
