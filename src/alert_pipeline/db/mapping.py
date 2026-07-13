"""Single place to convert between AlertEvent, AlertRecord, and AlertView.

Keeps the three layers field-aligned: pipeline write model, ORM row, and
operator read model. Prefer these helpers over ad-hoc field copies.
"""

from __future__ import annotations

import json

from alert_pipeline.db.models import AlertRecord, DispatchLog
from alert_pipeline.schemas import AlertEvent, AlertStatus, AlertView, DispatchView


def parse_labels_json(raw: str | None) -> dict[str, str]:
    try:
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            return {}
        return {str(k): str(v) for k, v in data.items()}
    except json.JSONDecodeError:
        return {}


def alert_record_from_event(alert: AlertEvent) -> AlertRecord:
    """Build a new ORM row from a pipeline emit (insert path)."""
    return AlertRecord(
        id=alert.id,
        fingerprint=alert.fingerprint,
        title=alert.title,
        description=alert.description,
        severity=alert.severity.value if hasattr(alert.severity, "value") else str(alert.severity),
        service=alert.service,
        host=alert.host,
        status=alert.status.value if hasattr(alert.status, "value") else str(alert.status),
        occurrence_count=alert.occurrence_count,
        first_seen=alert.first_seen,
        last_seen=alert.last_seen,
        error_code=alert.error_code,
        trace_id=alert.trace_id,
        labels_json=json.dumps(alert.labels or {}),
        sample_message=alert.sample_message,
    )


def apply_event_to_record(record: AlertRecord, alert: AlertEvent) -> None:
    """Refresh an existing active row from a pipeline emit (update path).

    Occurrence count never decreases. Acknowledged incidents stay acknowledged
    (status not forced to ``updated``).
    """
    if alert.is_new or alert.occurrence_count <= record.occurrence_count:
        record.occurrence_count = record.occurrence_count + 1
    else:
        record.occurrence_count = alert.occurrence_count

    record.last_seen = alert.last_seen
    if record.status != AlertStatus.ACKNOWLEDGED.value:
        record.status = AlertStatus.UPDATED.value
    record.severity = (
        alert.severity.value if hasattr(alert.severity, "value") else str(alert.severity)
    )
    record.sample_message = alert.sample_message or record.sample_message
    if alert.trace_id:
        record.trace_id = alert.trace_id


def alert_view_from_record(
    row: AlertRecord,
    *,
    dispatch_success: int = 0,
    dispatch_failed: int = 0,
) -> AlertView:
    """ORM row → operator read model (optional dispatch aggregates)."""
    return AlertView(
        id=row.id,
        fingerprint=row.fingerprint,
        title=row.title,
        description=row.description or "",
        severity=row.severity,
        service=row.service,
        host=row.host or "unknown",
        status=row.status,
        occurrence_count=row.occurrence_count,
        first_seen=row.first_seen,
        last_seen=row.last_seen,
        error_code=row.error_code,
        trace_id=row.trace_id,
        labels=parse_labels_json(row.labels_json),
        sample_message=row.sample_message or "",
        acknowledged_at=row.acknowledged_at,
        resolved_at=row.resolved_at,
        tta_seconds=row.tta_seconds,
        ttr_seconds=row.ttr_seconds,
        created_at=row.created_at,
        updated_at=row.updated_at,
        dispatch_success=dispatch_success,
        dispatch_failed=dispatch_failed,
    )


def dispatch_view_from_log(row: DispatchLog) -> DispatchView:
    return DispatchView(
        id=row.id,
        alert_id=row.alert_id,
        channel=row.channel,
        success=bool(row.success),
        status_code=row.status_code,
        error_message=row.error_message,
        created_at=row.created_at,
    )
