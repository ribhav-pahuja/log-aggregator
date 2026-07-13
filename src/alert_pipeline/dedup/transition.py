"""Pure dedup window / refire transition — single source of truth.

Both the Quix keyed-state path (``quix_state.py``) and the in-process
``DedupEngine`` call :func:`apply_dedup_transition`. Adapters only handle
I/O (Quix State / MemoryDedupStore) and serialization.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol, cast
from uuid import uuid4

from alert_pipeline.schemas import LEVEL_RANK, AlertEvent, AlertStatus, LogEvent, LogLevel
from alert_pipeline.types import IncidentStateDict, JsonObject

logger = logging.getLogger(__name__)

# Max how far ahead of wall-clock an event timestamp may be before we clamp.
MAX_FUTURE_SKEW_SECONDS = 300.0

DedupAction = Literal["new", "update", "suppress"]


class IncidentStateLike(Protocol):
    """Structural type for ``IncidentState`` (avoids circular imports)."""

    alert_id: str
    fingerprint: str
    first_seen: datetime
    last_seen: datetime
    occurrence_count: int
    last_emitted_at: float
    severity: LogLevel | str
    service: str
    host: str
    title: str
    sample_message: str
    error_code: str | None
    trace_id: str | None
    labels: dict[str, str]
    window_seconds: int


@dataclass(frozen=True)
class TransitionResult:
    """Outcome of applying one log event against optional existing incident state."""

    action: DedupAction
    state: IncidentStateDict
    """Canonical state blob to persist (dict form shared by Quix + memory)."""


def event_clock(event: LogEvent, *, now: float | None = None) -> float:
    """Event-time for window/refire. ``now`` overrides (tests). Clamps far-future skew."""
    if now is not None:
        return float(now)
    et = event.timestamp.timestamp() if event.timestamp else time.time()
    wall = time.time()
    if et > wall + MAX_FUTURE_SKEW_SECONDS:
        logger.warning(
            "Event timestamp far in the future (%.0fs); clamping to wall-clock",
            et - wall,
        )
        return wall
    return et


def dt_to_iso(v: datetime) -> str:
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.isoformat()


def iso_to_dt(v: str) -> datetime:
    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _severity_rank(sev: object) -> int:
    if isinstance(sev, LogLevel):
        return LEVEL_RANK.get(sev, 0)
    try:
        return LEVEL_RANK.get(LogLevel(str(sev)), 0)
    except ValueError:
        return LEVEL_RANK.get(LogLevel.normalize(str(sev)), 0)


def _coerce_level(sev: object) -> LogLevel:
    if isinstance(sev, LogLevel):
        return sev
    try:
        return LogLevel(str(sev))
    except ValueError:
        return LogLevel.normalize(str(sev))


def window_expired(
    existing: IncidentStateDict,
    *,
    event_ts: float,
    window_seconds: int,
) -> bool:
    """True if active window ended (event-time) relative to this event."""
    last_seen_raw = existing.get("last_seen")
    if last_seen_raw is None:
        return True
    if isinstance(last_seen_raw, datetime):
        last_ts = last_seen_raw.timestamp()
    else:
        last_ts = iso_to_dt(str(last_seen_raw)).timestamp()
    win = float(existing.get("window_seconds") or window_seconds)
    return event_ts - last_ts > win


def apply_dedup_transition(
    *,
    existing: IncidentStateDict | None,
    event: LogEvent,
    fingerprint: str,
    window_seconds: int,
    refire_interval_seconds: int,
    title: str,
    event_ts: float,
    alert_id: str | None = None,
) -> TransitionResult:
    """Apply create / suppress / refire rules for one event.

    Parameters
    ----------
    existing:
        Prior incident state for this fingerprint, or None. Values may use ISO
        strings or datetimes for timestamps; severity as str or LogLevel.
    event_ts:
        Event-time clock (use :func:`event_clock`).
    alert_id:
        Optional id when creating a new incident (tests); otherwise a new UUID.
    """
    window = max(1, int(window_seconds))
    refire = max(0, int(refire_interval_seconds))

    active = existing
    if active is not None and window_expired(active, event_ts=event_ts, window_seconds=window):
        active = None

    if active is None:
        new_id = alert_id or str(uuid4())
        st: IncidentStateDict = {
            "alert_id": new_id,
            "fingerprint": fingerprint,
            "first_seen": dt_to_iso(event.timestamp),
            "last_seen": dt_to_iso(event.timestamp),
            "occurrence_count": 1,
            "last_emitted_at": float(event_ts),
            "severity": event.level.value
            if isinstance(event.level, LogLevel)
            else str(event.level),
            "service": event.service,
            "host": event.host,
            "title": title,
            "sample_message": event.message,
            "error_code": event.error_code,
            "trace_id": event.trace_id,
            "labels": dict(event.labels or {}),
            "window_seconds": window,
        }
        return TransitionResult(action="new", state=st)

    st = cast(IncidentStateDict, dict(active))
    st["occurrence_count"] = int(st.get("occurrence_count") or 1) + 1
    st["last_seen"] = dt_to_iso(event.timestamp)
    st["sample_message"] = event.message
    st["window_seconds"] = window
    if event.trace_id:
        st["trace_id"] = event.trace_id

    if LEVEL_RANK.get(event.level, 0) > _severity_rank(st.get("severity") or "ERROR"):
        st["severity"] = (
            event.level.value if isinstance(event.level, LogLevel) else str(event.level)
        )

    last_em = float(st.get("last_emitted_at") or 0)
    if (event_ts - last_em) < refire:
        return TransitionResult(action="suppress", state=st)

    st["last_emitted_at"] = float(event_ts)
    return TransitionResult(action="update", state=st)


def alert_event_from_state(
    st: IncidentStateDict,
    *,
    is_new: bool,
    description: str,
) -> AlertEvent:
    """Build an AlertEvent from a canonical state blob."""
    first: datetime | str = st["first_seen"]
    last: datetime | str = st["last_seen"]
    if not isinstance(first, datetime):
        first = iso_to_dt(str(first))
    if not isinstance(last, datetime):
        last = iso_to_dt(str(last))
    return AlertEvent(
        id=str(st["alert_id"]),
        fingerprint=str(st["fingerprint"]),
        title=str(st.get("title") or ""),
        description=description,
        severity=_coerce_level(st.get("severity") or "ERROR"),
        service=str(st.get("service") or "unknown"),
        host=str(st.get("host") or "unknown"),
        status=AlertStatus.OPEN if is_new else AlertStatus.UPDATED,
        occurrence_count=int(st.get("occurrence_count") or 1),
        first_seen=first,
        last_seen=last,
        error_code=st.get("error_code"),
        trace_id=st.get("trace_id"),
        labels=dict(st.get("labels") or {}),
        sample_message=str(st.get("sample_message") or ""),
        is_new=is_new,
    )


def alert_wire_from_state(
    st: IncidentStateDict,
    *,
    is_new: bool,
    description: str,
) -> JsonObject:
    """JSON-friendly alert dict for the Quix sink path."""
    return cast(
        JsonObject,
        alert_event_from_state(st, is_new=is_new, description=description).model_dump(mode="json"),
    )


def incident_state_to_dict(state: IncidentStateLike) -> IncidentStateDict:
    """Convert ``IncidentState`` (or compatible) to the canonical dict blob."""
    sev = state.severity
    sev_s = sev.value if isinstance(sev, LogLevel) else str(sev)
    first = state.first_seen
    last = state.last_seen
    return {
        "alert_id": state.alert_id,
        "fingerprint": state.fingerprint,
        "first_seen": dt_to_iso(first) if isinstance(first, datetime) else str(first),
        "last_seen": dt_to_iso(last) if isinstance(last, datetime) else str(last),
        "occurrence_count": int(state.occurrence_count),
        "last_emitted_at": float(state.last_emitted_at),
        "severity": sev_s,
        "service": state.service,
        "host": state.host,
        "title": state.title,
        "sample_message": state.sample_message,
        "error_code": state.error_code,
        "trace_id": state.trace_id,
        "labels": dict(state.labels or {}),
        "window_seconds": int(state.window_seconds),
    }


def dict_to_incident_fields(st: IncidentStateDict) -> dict[str, object]:
    """Keyword args suitable for constructing ``IncidentState``."""
    first: datetime | str = st["first_seen"]
    last: datetime | str = st["last_seen"]
    if not isinstance(first, datetime):
        first = iso_to_dt(str(first))
    if not isinstance(last, datetime):
        last = iso_to_dt(str(last))
    return {
        "alert_id": str(st["alert_id"]),
        "fingerprint": str(st["fingerprint"]),
        "first_seen": first,
        "last_seen": last,
        "occurrence_count": int(st.get("occurrence_count") or 1),
        "last_emitted_at": float(st.get("last_emitted_at") or 0),
        "severity": _coerce_level(st.get("severity") or "ERROR"),
        "service": str(st.get("service") or "unknown"),
        "host": str(st.get("host") or "unknown"),
        "title": str(st.get("title") or ""),
        "sample_message": str(st.get("sample_message") or ""),
        "error_code": st.get("error_code"),
        "trace_id": st.get("trace_id"),
        "labels": dict(st.get("labels") or {}),
        "window_seconds": int(st.get("window_seconds") or 300),
    }
