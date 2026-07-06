"""Persistence helpers for alerts and dispatch audit records."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from sqlalchemy import create_engine, delete, select, text
import uuid
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.db.models import AlertRecord, Base, DispatchLog, WidgetRecord
from alert_pipeline.metrics import apply_status_timestamps
from alert_pipeline.schemas import ACTIVE_ALERT_STATUSES, AlertEvent

logger = logging.getLogger(__name__)

_ACTIVE = tuple(ACTIVE_ALERT_STATUSES)

# Partial unique index: at most one active incident per fingerprint.
# Status list must match ACTIVE_ALERT_STATUSES.
_ACTIVE_FP_INDEX = "uq_alerts_active_fingerprint"
_ACTIVE_FP_WHERE = "status IN ('open', 'updated', 'acknowledged')"


class AlertRepository:
    def __init__(self, database_url: str) -> None:
        connect_args = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        self._engine = create_engine(database_url, future=True, connect_args=connect_args)
        self._session_factory = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)
        # Bootstrap for tests / first boot. Production should prefer Alembic
        # (`alembic upgrade head`); create_all is idempotent and safe alongside it.
        Base.metadata.create_all(self._engine)
        self._ensure_active_fingerprint_index(database_url)
        logger.info(
            "Database ready: %s",
            database_url.split("@")[-1] if "@" in database_url else database_url,
        )

    def _ensure_active_fingerprint_index(self, database_url: str) -> None:
        """Enforce one active row per fingerprint (multi-worker safety)."""
        is_sqlite = database_url.startswith("sqlite")
        with self._engine.begin() as conn:
            if is_sqlite:
                conn.execute(
                    text(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS {_ACTIVE_FP_INDEX} "
                        f"ON alerts (fingerprint) WHERE {_ACTIVE_FP_WHERE}"
                    )
                )
            else:
                conn.execute(
                    text(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS {_ACTIVE_FP_INDEX} "
                        f"ON alerts (fingerprint) WHERE {_ACTIVE_FP_WHERE}"
                    )
                )

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_alert(self, alert: AlertEvent) -> AlertRecord:
        """Insert a new incident or refresh occurrence_count / last_seen for an open one.

        Occurrence count never decreases: if the engine lost state and emits
        ``is_new`` with count=1 while an active row exists, we bump the DB count
        rather than resetting it.
        """
        try:
            return self._upsert_once(alert)
        except IntegrityError:
            # Concurrent insert of same active fingerprint — retry as update
            logger.info(
                "Active fingerprint race for %s; retrying as update", alert.fingerprint
            )
            alert.is_new = False
            return self._upsert_once(alert)

    def _upsert_once(self, alert: AlertEvent) -> AlertRecord:
        with self.session() as session:
            existing = session.scalar(
                select(AlertRecord).where(
                    AlertRecord.fingerprint == alert.fingerprint,
                    AlertRecord.status.in_(_ACTIVE),
                )
            )
            if existing is None:
                record = AlertRecord(
                    id=alert.id,
                    fingerprint=alert.fingerprint,
                    title=alert.title,
                    description=alert.description,
                    severity=alert.severity.value,
                    service=alert.service,
                    host=alert.host,
                    status=alert.status.value,
                    occurrence_count=alert.occurrence_count,
                    first_seen=alert.first_seen,
                    last_seen=alert.last_seen,
                    error_code=alert.error_code,
                    trace_id=alert.trace_id,
                    labels_json=json.dumps(alert.labels),
                    sample_message=alert.sample_message,
                )
                session.add(record)
                session.flush()
                session.expunge(record)
                return record

            # Never decrease counts (restart / multi-worker safety)
            if alert.is_new or alert.occurrence_count <= existing.occurrence_count:
                existing.occurrence_count = existing.occurrence_count + 1
            else:
                existing.occurrence_count = alert.occurrence_count

            existing.last_seen = alert.last_seen
            if existing.status != "acknowledged":
                existing.status = "updated"
            existing.severity = alert.severity.value
            existing.sample_message = alert.sample_message or existing.sample_message
            if alert.trace_id:
                existing.trace_id = alert.trace_id
            session.flush()
            alert.id = existing.id
            alert.is_new = False
            alert.occurrence_count = existing.occurrence_count
            session.expunge(existing)
            return existing

    def get_status(self, alert_id: str) -> str | None:
        with self.session() as session:
            row = session.get(AlertRecord, alert_id)
            return row.status if row else None

    def set_alert_status(self, alert_id: str, status: str) -> AlertRecord | None:
        """Operator actions: acknowledge / resolve / reopen — also persists TTA/TTR."""
        allowed = {"open", "updated", "acknowledged", "resolved"}
        if status not in allowed:
            raise ValueError(f"invalid status {status!r}")
        with self.session() as session:
            row = session.get(AlertRecord, alert_id)
            if row is None:
                return None
            apply_status_timestamps(row, status, now=datetime.now(timezone.utc))
            session.flush()
            session.refresh(row)
            session.expunge(row)
            return row

    def resolve_alert(self, fingerprint: str) -> None:
        with self.session() as session:
            rows = session.scalars(
                select(AlertRecord).where(
                    AlertRecord.fingerprint == fingerprint,
                    AlertRecord.status.in_(_ACTIVE),
                )
            ).all()
            now = datetime.now(timezone.utc)
            for row in rows:
                apply_status_timestamps(row, "resolved", now=now)

    def clear_all(self) -> dict[str, int]:
        """Wipe alerts + dispatch audit (demo / empty slate)."""
        with self.session() as session:
            d1 = session.execute(delete(DispatchLog))
            d2 = session.execute(delete(AlertRecord))
            return {
                "dispatch_log_deleted": int(d1.rowcount or 0),
                "alerts_deleted": int(d2.rowcount or 0),
            }

    def log_dispatch(
        self,
        *,
        alert_id: str,
        channel: str,
        success: bool,
        status_code: int | None = None,
        response_body: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self.session() as session:
            session.add(
                DispatchLog(
                    alert_id=alert_id,
                    channel=channel,
                    success=1 if success else 0,
                    status_code=status_code,
                    response_body=(response_body or "")[:4000] or None,
                    error_message=(error_message or "")[:2000] or None,
                )
            )

    # --- shared dashboard widgets -------------------------------------------------

    def list_widgets(self) -> list[WidgetRecord]:
        with self.session() as session:
            rows = session.scalars(
                select(WidgetRecord).order_by(WidgetRecord.sort_order, WidgetRecord.title)
            ).all()
            for r in rows:
                session.expunge(r)
            return list(rows)

    def get_widget(self, widget_id: str) -> WidgetRecord | None:
        with self.session() as session:
            row = session.get(WidgetRecord, widget_id)
            if row:
                session.expunge(row)
            return row

    def upsert_widget(
        self,
        *,
        widget_id: str | None,
        title: str,
        labels: list[dict],
        status_filter: str = "",
        sort_order: int = 0,
    ) -> WidgetRecord:
        import json as _json

        wid = widget_id or str(uuid.uuid4())
        payload = _json.dumps(labels or [])
        with self.session() as session:
            row = session.get(WidgetRecord, wid)
            if row is None:
                row = WidgetRecord(
                    id=wid,
                    title=title,
                    labels_json=payload,
                    status_filter=status_filter or "",
                    sort_order=sort_order,
                )
                session.add(row)
            else:
                row.title = title
                row.labels_json = payload
                row.status_filter = status_filter or ""
                row.sort_order = sort_order
            session.flush()
            session.refresh(row)
            session.expunge(row)
            return row

    def delete_widget(self, widget_id: str) -> bool:
        with self.session() as session:
            row = session.get(WidgetRecord, widget_id)
            if row is None:
                return False
            session.delete(row)
            return True
