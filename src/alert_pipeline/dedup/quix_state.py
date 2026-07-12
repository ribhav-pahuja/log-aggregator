"""Quix-native dedup adapter (per-key State).

Rules live in ``transition.apply_dedup_transition``. This module only
serializes wire rows and reads/writes Quix State.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
from alert_pipeline.dedup.transition import (
    alert_wire_from_state,
    apply_dedup_transition,
    event_clock,
    window_expired,
)
from alert_pipeline.schemas import AlertEvent, LogEvent

logger = logging.getLogger(__name__)

_STATE_KEY = "incident"


class StateLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any: ...
    def set(self, key: str, value: Any) -> None: ...
    def delete(self, key: str) -> None: ...


def log_event_to_wire(event: LogEvent) -> dict[str, Any]:
    return event.model_dump(mode="json")


def log_event_from_wire(data: dict[str, Any]) -> LogEvent:
    return LogEvent.model_validate(data)


def build_enrichment(
    event: LogEvent,
    *,
    window_seconds: int,
    refire_interval_seconds: int,
    suppress_dispatch_while_acknowledged: bool,
    allow_reopen_after_resolve: bool = True,
    dedup_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Serializable row for Quix (must include fingerprint for group_by)."""
    fp = compute_fingerprint(event, dedup_fields)
    return {
        "fingerprint": fp,
        "event": log_event_to_wire(event),
        "window_seconds": int(window_seconds),
        "refire_interval_seconds": int(refire_interval_seconds),
        "suppress_dispatch_while_acknowledged": bool(suppress_dispatch_while_acknowledged),
        "allow_reopen_after_resolve": bool(allow_reopen_after_resolve),
        "title": build_title(event),
    }


def process_enriched_with_state(
    row: dict[str, Any],
    state: StateLike,
    *,
    now: float | None = None,
) -> dict[str, Any] | None:
    """
    Apply window / refire rules using Quix per-key state.

    Windows and refire use **event time** (``event.timestamp``), not processing
    wall-clock. Pass ``now`` only in tests to inject a clock.

    Returns a wire dict for the sink step, or None if suppressed.
    """
    event = log_event_from_wire(row["event"])
    event_ts = event_clock(event, now=now)
    fingerprint = row["fingerprint"]
    window = max(1, int(row.get("window_seconds") or 300))
    refire = max(0, int(row.get("refire_interval_seconds") or 60))
    title = row.get("title") or build_title(event)
    suppress_ack = bool(row.get("suppress_dispatch_while_acknowledged", True))
    allow_reopen = bool(row.get("allow_reopen_after_resolve", True))

    raw = state.get(_STATE_KEY)
    existing: dict[str, Any] | None = raw if isinstance(raw, dict) else None

    # Drop expired blob before transition so Quix state stays clean
    if existing is not None and window_expired(
        existing, event_ts=event_ts, window_seconds=window
    ):
        try:
            state.delete(_STATE_KEY)
        except Exception:  # noqa: BLE001
            pass
        existing = None

    result = apply_dedup_transition(
        existing=existing,
        event=event,
        fingerprint=fingerprint,
        window_seconds=window,
        refire_interval_seconds=refire,
        title=title,
        event_ts=event_ts,
    )
    state.set(_STATE_KEY, result.state)

    if result.action == "suppress":
        logger.debug(
            "Suppressed duplicate fingerprint=%s count=%s (quix state, event-time)",
            fingerprint,
            result.state.get("occurrence_count"),
        )
        return None

    if result.action == "new":
        logger.info(
            "New incident fingerprint=%s service=%s window=%ss backend=quix event_time",
            fingerprint,
            event.service,
            window,
        )
        return {
            "alert": alert_wire_from_state(
                result.state, is_new=True, description=event.message
            ),
            "suppress_dispatch_while_acknowledged": suppress_ack,
            "allow_reopen_after_resolve": allow_reopen,
        }

    return {
        "alert": alert_wire_from_state(
            result.state, is_new=False, description=event.message
        ),
        "suppress_dispatch_while_acknowledged": suppress_ack,
        "allow_reopen_after_resolve": allow_reopen,
    }


def alert_from_wire(data: dict[str, Any]) -> AlertEvent:
    return AlertEvent.model_validate(data)
