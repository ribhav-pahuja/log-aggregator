"""Persistence helpers for alerts and dispatch audit records."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from sqlalchemy import create_engine, delete, select, text
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.db.models import AlertRecord, Base, DispatchLog
from alert_pipeline.metrics import apply_status_timestamps
from alert_pipeline.schemas import ACTIVE_ALERT_STATUSES, AlertEvent

logger = logging.getLogger(__name__)

_ACTIVE = tuple(ACTIVE_ALERT_STATUSES)

# Lightweight additive migration for existing DBs created before TTA/TTR columns.
_EXTRA_COLUMNS: list[tuple[str, str]] = [
    ("acknowledged_at", "TIMESTAMP WITH TIME ZONE"),
    ("resolved_at", "TIMESTAMP WITH TIME ZONE"),
    ("tta_seconds", "INTEGER"),
    ("ttr_seconds", "INTEGER"),
]


class AlertRepository:
    def __init__(self, database_url: str) -> None:
        connect_args = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        self._engine = create_engine(database_url, future=True, connect_args=connect_args)
        self._session_factory = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)
        Base.metadata.create_all(self._engine)
        self._migrate_extra_columns(database_url)
        logger.info(
            "Database ready: %s",
            database_url.split("@")[-1] if "@" in database_url else database_url,
        )

    def _migrate_extra_columns(self, database_url: str) -> None:
        is_sqlite = database_url.startswith("sqlite")
        with self._engine.begin() as conn:
            if is_sqlite:
                existing = {
                    row[1]
                    for row in conn.execute(text("PRAGMA table_info(alerts)")).fetchall()
                }
                for col, _typ in _EXTRA_COLUMNS:
                    if col not in existing:
                        # SQLite: use generic types
                        sql_type = "DATETIME" if "at" in col else "INTEGER"
                        conn.execute(text(f"ALTER TABLE alerts ADD COLUMN {col} {sql_type}"))
                        logger.info("Added column alerts.%s", col)
            else:
                for col, sql_type in _EXTRA_COLUMNS:
                    conn.execute(
                        text(
                            f"ALTER TABLE alerts ADD COLUMN IF NOT EXISTS {col} {sql_type}"
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
        """Insert a new incident or refresh occurrence_count / last_seen for an open one."""
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
