"""TTA / TTR helpers.

TTA (Time To Acknowledge) = acknowledged_at - first_seen  (seconds)
TTR (Time To Resolve)     = resolved_at - first_seen      (seconds)

Persisted on the alert row when the operator transitions status in the UI (or API).
"""

from __future__ import annotations

from datetime import datetime, timezone

from alert_pipeline.db.models import AlertRecord


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _seconds_between(start: datetime, end: datetime) -> int:
    delta = _aware(end) - _aware(start)
    return max(0, int(delta.total_seconds()))


def apply_status_timestamps(
    row: AlertRecord, new_status: str, *, now: datetime | None = None
) -> None:
    """Mutate row with status + TTA/TTR timestamps and durations."""
    now = now or datetime.now(timezone.utc)
    prev = row.status
    row.status = new_status
    row.updated_at = now

    if new_status == "acknowledged" and prev in ("open", "updated"):
        row.acknowledged_at = now
        row.tta_seconds = _seconds_between(row.first_seen, now)
        # Clearing a previous resolve is not expected here; leave resolved_* alone

    elif new_status == "resolved" and prev != "resolved":
        row.resolved_at = now
        row.ttr_seconds = _seconds_between(row.first_seen, now)
        # Implicit ack at resolve time if operator never acked (common for fast fixes)
        if row.acknowledged_at is None:
            row.acknowledged_at = now
            row.tta_seconds = row.ttr_seconds

    elif new_status == "open" and prev == "acknowledged":
        # Un-ack: wipe ack metrics so a later ack recomputes TTA
        row.acknowledged_at = None
        row.tta_seconds = None

    elif new_status in ("open", "updated") and prev == "resolved":
        # Full reopen of a closed incident
        row.resolved_at = None
        row.ttr_seconds = None
        row.acknowledged_at = None
        row.tta_seconds = None
