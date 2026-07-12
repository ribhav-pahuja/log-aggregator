"""In-process deduplication engine (unit tests / non-Quix paths).

Rules live in ``transition.apply_dedup_transition``. This class only owns
config resolution and the memory store adapter.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from alert_pipeline.alert_config import AlertYamlConfig, RefireSettings, get_alert_config
from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
from alert_pipeline.dedup.store import DedupStore, IncidentState, MemoryDedupStore
from alert_pipeline.dedup.transition import (
    alert_event_from_state,
    apply_dedup_transition,
    dict_to_incident_fields,
    event_clock,
    incident_state_to_dict,
)
from alert_pipeline.schemas import LEVEL_RANK, AlertEvent, LogEvent, LogLevel

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

        event_ts = event_clock(event)
        self._store.expire_stale(event_ts)

        fingerprint = compute_fingerprint(event, cfg.dedup_fields)
        title = build_title(event)
        existing_obj = self._store.get(fingerprint)
        existing = incident_state_to_dict(existing_obj) if existing_obj is not None else None

        result = apply_dedup_transition(
            existing=existing,
            event=event,
            fingerprint=fingerprint,
            window_seconds=cfg.dedup_window_seconds,
            refire_interval_seconds=cfg.refire_interval_seconds,
            title=title,
            event_ts=event_ts,
        )
        fields = dict_to_incident_fields(result.state)
        incident = IncidentState(**fields)

        if result.action == "new":
            created = self._store.try_create(incident, ttl_seconds=cfg.dedup_window_seconds)
            if not created:
                # Race with another thread — re-apply against the winner's state
                winner = self._store.get(fingerprint)
                if winner is None:
                    return None
                result = apply_dedup_transition(
                    existing=incident_state_to_dict(winner),
                    event=event,
                    fingerprint=fingerprint,
                    window_seconds=cfg.dedup_window_seconds,
                    refire_interval_seconds=cfg.refire_interval_seconds,
                    title=title,
                    event_ts=event_ts,
                )
                incident = IncidentState(**dict_to_incident_fields(result.state))
                self._store.put(incident, ttl_seconds=cfg.dedup_window_seconds)
                if result.action == "suppress":
                    return None
                return alert_event_from_state(result.state, is_new=False, description=event.message)

            logger.info(
                "New incident fingerprint=%s service=%s window=%ss refire=%ss",
                fingerprint,
                event.service,
                cfg.dedup_window_seconds,
                cfg.refire_interval_seconds,
            )
            return alert_event_from_state(result.state, is_new=True, description=event.message)

        self._store.put(incident, ttl_seconds=cfg.dedup_window_seconds)
        if result.action == "suppress":
            logger.debug(
                "Suppressed duplicate fingerprint=%s count=%s (refire in %ss)",
                fingerprint,
                result.state.get("occurrence_count"),
                cfg.refire_interval_seconds,
            )
            return None

        return alert_event_from_state(result.state, is_new=False, description=event.message)

    def stats(self) -> dict[str, int]:
        return {"open_incidents": self._store.count()}
