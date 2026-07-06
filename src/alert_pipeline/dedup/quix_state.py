"""Quix-native dedup transitions (per-key State).

Used by the Quix runtime after ``group_by(fingerprint)``. State lives in
Quix's state store (keyed by fingerprint), not Redis.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from alert_pipeline.dedup.fingerprint import build_title, compute_fingerprint
from alert_pipeline.schemas import LEVEL_RANK, AlertEvent, AlertStatus, LogEvent, LogLevel

logger = logging.getLogger(__name__)

_STATE_KEY = "incident"


class StateLike(Protocol):
    def get(self, key: str, default: Any = None) -> Any: ...
    def set(self, key: str, value: Any) -> None: ...
    def delete(self, key: str) -> None: ...


def _dt_to_iso(v: datetime) -> str:
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.isoformat()


def _iso_to_dt(v: str) -> datetime:
    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


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
    dedup_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Serializable row for Quix (must include fingerprint for group_by)."""
    fp = compute_fingerprint(event, dedup_fields)
    return {
        "fingerprint": fp,
        "event": log_event_to_wire(event),
        "window_seconds": int(window_seconds),
        "refire_interval_seconds": int(refire_interval_seconds),
        "suppress_dispatch_while_acknowledged": bool(
            suppress_dispatch_while_acknowledged
        ),
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

    Returns a wire dict for the sink step, or None if suppressed.
    """
    now_ts = time.time() if now is None else now
    event = log_event_from_wire(row["event"])
    fingerprint = row["fingerprint"]
    window = max(1, int(row.get("window_seconds") or 300))
    refire = max(0, int(row.get("refire_interval_seconds") or 60))
    title = row.get("title") or build_title(event)
    suppress_ack = bool(row.get("suppress_dispatch_while_acknowledged", True))

    raw = state.get(_STATE_KEY)
    existing: dict[str, Any] | None = raw if isinstance(raw, dict) else None

    if existing is not None:
        last_seen = _iso_to_dt(existing["last_seen"])
        if now_ts - last_seen.timestamp() > float(existing.get("window_seconds") or window):
            # Window expired — treat as brand-new incident
            existing = None
            try:
                state.delete(_STATE_KEY)
            except Exception:  # noqa: BLE001
                pass

    if existing is None:
        alert_id = str(uuid4())
        st = {
            "alert_id": alert_id,
            "fingerprint": fingerprint,
            "first_seen": _dt_to_iso(event.timestamp),
            "last_seen": _dt_to_iso(event.timestamp),
            "occurrence_count": 1,
            "last_emitted_at": now_ts,
            "severity": event.level.value,
            "service": event.service,
            "host": event.host,
            "title": title,
            "sample_message": event.message,
            "error_code": event.error_code,
            "trace_id": event.trace_id,
            "labels": dict(event.labels or {}),
            "window_seconds": window,
        }
        state.set(_STATE_KEY, st)
        logger.info(
            "New incident fingerprint=%s service=%s window=%ss backend=quix",
            fingerprint,
            event.service,
            window,
        )
        return {
            "alert": _alert_wire_from_state(st, is_new=True, description=event.message),
            "suppress_dispatch_while_acknowledged": suppress_ack,
        }

    # Update existing window
    existing["occurrence_count"] = int(existing.get("occurrence_count") or 1) + 1
    existing["last_seen"] = _dt_to_iso(event.timestamp)
    existing["sample_message"] = event.message
    existing["window_seconds"] = window
    if event.trace_id:
        existing["trace_id"] = event.trace_id

    old_sev = existing.get("severity") or "ERROR"
    try:
        old_level = LogLevel(old_sev)
    except ValueError:
        old_level = LogLevel.normalize(str(old_sev))
    if LEVEL_RANK.get(event.level, 0) > LEVEL_RANK.get(old_level, 0):
        existing["severity"] = event.level.value

    last_em = float(existing.get("last_emitted_at") or 0)
    if (now_ts - last_em) < refire:
        state.set(_STATE_KEY, existing)
        logger.debug(
            "Suppressed duplicate fingerprint=%s count=%s (quix state)",
            fingerprint,
            existing["occurrence_count"],
        )
        return None

    existing["last_emitted_at"] = now_ts
    state.set(_STATE_KEY, existing)
    return {
        "alert": _alert_wire_from_state(
            existing, is_new=False, description=event.message
        ),
        "suppress_dispatch_while_acknowledged": suppress_ack,
    }


def _alert_wire_from_state(
    st: dict[str, Any], *, is_new: bool, description: str
) -> dict[str, Any]:
    sev = st.get("severity") or "ERROR"
    try:
        severity = LogLevel(sev)
    except ValueError:
        severity = LogLevel.normalize(str(sev))
    return {
        "id": st["alert_id"],
        "fingerprint": st["fingerprint"],
        "title": st.get("title") or "",
        "description": description,
        "severity": severity.value,
        "service": st.get("service") or "unknown",
        "host": st.get("host") or "unknown",
        "status": AlertStatus.OPEN.value if is_new else AlertStatus.UPDATED.value,
        "occurrence_count": int(st.get("occurrence_count") or 1),
        "first_seen": st["first_seen"],
        "last_seen": st["last_seen"],
        "error_code": st.get("error_code"),
        "trace_id": st.get("trace_id"),
        "labels": dict(st.get("labels") or {}),
        "sample_message": st.get("sample_message") or "",
        "is_new": is_new,
    }


def alert_from_wire(data: dict[str, Any]) -> AlertEvent:
    return AlertEvent.model_validate(data)
