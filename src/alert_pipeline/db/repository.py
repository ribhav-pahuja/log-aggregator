"""Persistence helpers for alerts and dispatch audit records."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator

from sqlalchemy import create_engine, delete, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.db.models import (
    AlertRecord,
    DispatchLog,
    DispatchOutbox,
    WidgetRecord,
)
from alert_pipeline.metrics import apply_status_timestamps
from alert_pipeline.schemas import ACTIVE_ALERT_STATUSES, AlertEvent

logger = logging.getLogger(__name__)

_ACTIVE = tuple(ACTIVE_ALERT_STATUSES)
_OUTBOX_OPEN = ("pending", "processing", "failed")

# Partial unique index: at most one active incident per fingerprint.
# Status list must match ACTIVE_ALERT_STATUSES.
_ACTIVE_FP_INDEX = "uq_alerts_active_fingerprint"
_ACTIVE_FP_WHERE = "status IN ('open', 'updated', 'acknowledged')"


class AlertRepository:
    def __init__(self, database_url: str) -> None:
        if "sqlite" in (database_url or "").lower():
            raise ValueError(
                "SQLite is not supported. Use PostgreSQL "
                "(postgresql+psycopg://user:pass@host:5432/db)."
            )
        if not (database_url or "").lower().startswith("postgresql"):
            raise ValueError(f"DATABASE_URL must be PostgreSQL, got {database_url!r}")
        self._engine = create_engine(database_url, future=True, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)
        # Schema is owned by Alembic (`alembic upgrade head` on boot / in tests).
        # Ensure partial unique index exists on older DBs that pre-date migrations.
        self._ensure_active_fingerprint_index()
        logger.info(
            "Database ready: %s",
            database_url.split("@")[-1] if "@" in database_url else database_url,
        )

    def _ensure_active_fingerprint_index(self) -> None:
        """Enforce one active row per fingerprint (multi-worker safety)."""
        with self._engine.begin() as conn:
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
            with self.session() as session:
                return self._upsert_in_session(session, alert)
        except IntegrityError:
            # Concurrent insert of same active fingerprint — retry as update
            logger.info("Active fingerprint race for %s; retrying as update", alert.fingerprint)
            alert.is_new = False
            with self.session() as session:
                return self._upsert_in_session(session, alert)

    def upsert_and_maybe_enqueue(
        self,
        alert: AlertEvent,
        channels: list[str],
        *,
        should_enqueue: Callable[[AlertRecord, AlertEvent], bool] | None = None,
    ) -> tuple[AlertRecord, list[str]]:
        """Upsert incident and optionally enqueue outbox rows in **one transaction**.

        Prevents the failure mode where an incident is committed but outbox rows
        never land (crash between separate upsert / enqueue commits).

        ``should_enqueue`` is evaluated **after** upsert (so it can read DB status,
        e.g. suppress dispatch while acknowledged). When it returns False or
        ``channels`` is empty, only the upsert is committed.
        """
        try:
            return self._upsert_and_maybe_enqueue_once(
                alert, channels, should_enqueue=should_enqueue
            )
        except IntegrityError:
            logger.info(
                "Active fingerprint race for %s during emit; retrying as update",
                alert.fingerprint,
            )
            alert.is_new = False
            return self._upsert_and_maybe_enqueue_once(
                alert, channels, should_enqueue=should_enqueue
            )

    def _upsert_and_maybe_enqueue_once(
        self,
        alert: AlertEvent,
        channels: list[str],
        *,
        should_enqueue: Callable[[AlertRecord, AlertEvent], bool] | None,
    ) -> tuple[AlertRecord, list[str]]:
        with self.session() as session:
            record = self._upsert_in_session(session, alert)
            keys: list[str] = []
            do_enqueue = bool(channels) and (
                should_enqueue is None or should_enqueue(record, alert)
            )
            if do_enqueue:
                keys = self._enqueue_in_session(session, alert, channels)
            return record, keys

    def has_active_fingerprint(self, fingerprint: str) -> bool:
        with self.session() as session:
            row = session.scalar(
                select(AlertRecord.id).where(
                    AlertRecord.fingerprint == fingerprint,
                    AlertRecord.status.in_(_ACTIVE),
                )
            )
            return row is not None

    def has_resolved_fingerprint(self, fingerprint: str) -> bool:
        """True if a resolved row exists (regardless of active)."""
        with self.session() as session:
            row = session.scalar(
                select(AlertRecord.id).where(
                    AlertRecord.fingerprint == fingerprint,
                    AlertRecord.status == "resolved",
                )
            )
            return row is not None

    def _upsert_in_session(self, session: Session, alert: AlertEvent) -> AlertRecord:
        existing = session.scalar(
            select(AlertRecord).where(
                AlertRecord.fingerprint == alert.fingerprint,
                AlertRecord.status.in_(_ACTIVE),
            )
        )
        if existing is None:
            # Reopen after resolve may reuse Quix state's alert_id; PK would collide.
            by_id = session.get(AlertRecord, alert.id)
            if by_id is not None:
                alert.id = str(uuid.uuid4())
                alert.is_new = True
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
        """Wipe alerts + dispatch audit + outbox (demo / empty slate)."""
        with self.session() as session:
            d0 = session.execute(delete(DispatchOutbox))
            d1 = session.execute(delete(DispatchLog))
            d2 = session.execute(delete(AlertRecord))
            return {
                "outbox_deleted": int(d0.rowcount or 0),
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
        idempotency_key: str | None = None,
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
                    idempotency_key=idempotency_key,
                )
            )

    # --- dispatch outbox --------------------------------------------------------

    @staticmethod
    def make_idempotency_key(alert_id: str, channel: str, occurrence_count: int) -> str:
        return f"{alert_id}:{channel}:{int(occurrence_count)}"

    def enqueue_dispatch(
        self,
        alert: AlertEvent,
        channels: list[str],
    ) -> list[str]:
        """Insert pending outbox rows (one per channel). Returns new idempotency keys.

        Duplicate keys (reprocessing) are ignored via unique constraint.
        Prefer :meth:`upsert_and_maybe_enqueue` on the emit path so upsert and
        enqueue share one transaction.
        """
        if not channels:
            return []
        with self.session() as session:
            return self._enqueue_in_session(session, alert, channels)

    def _enqueue_in_session(
        self,
        session: Session,
        alert: AlertEvent,
        channels: list[str],
    ) -> list[str]:
        if not channels:
            return []
        payload = json.dumps(alert.model_dump(mode="json"))
        now = datetime.now(timezone.utc)
        created: list[str] = []
        for channel in channels:
            key = self.make_idempotency_key(alert.id, channel, alert.occurrence_count)
            exists = session.scalar(
                select(DispatchOutbox.id).where(DispatchOutbox.idempotency_key == key)
            )
            if exists is not None:
                continue
            session.add(
                DispatchOutbox(
                    idempotency_key=key,
                    alert_id=alert.id,
                    channel=channel,
                    payload_json=payload,
                    status="pending",
                    attempts=0,
                    next_attempt_at=now,
                )
            )
            created.append(key)
        session.flush()
        return created

    def claim_outbox_batch(
        self,
        *,
        batch_size: int = 50,
        stale_processing_seconds: int = 120,
    ) -> list[DispatchOutbox]:
        """Mark a batch of due rows as processing and return them.

        Multi-worker safe (PostgreSQL):
        * ``SELECT … FOR UPDATE SKIP LOCKED`` so concurrent workers co-claim
          distinct rows without blocking each other.
        * Compare-and-swap ``UPDATE … WHERE status IN ('pending','failed')`` so a
          second worker never double-processes a row already claimed.
        """
        now = datetime.now(timezone.utc)
        stale_before = now - timedelta(seconds=stale_processing_seconds)
        with self.session() as session:
            # Recover stale processing rows (worker crash / kill -9 mid-send)
            session.execute(
                update(DispatchOutbox)
                .where(
                    DispatchOutbox.status == "processing",
                    DispatchOutbox.updated_at < stale_before,
                )
                .values(status="pending", next_attempt_at=now)
            )

            candidate_q = (
                select(DispatchOutbox.id)
                .where(
                    DispatchOutbox.status.in_(("pending", "failed")),
                    DispatchOutbox.next_attempt_at <= now,
                )
                .order_by(DispatchOutbox.next_attempt_at, DispatchOutbox.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )

            candidate_ids = list(session.scalars(candidate_q).all())
            if not candidate_ids:
                return []

            claimed: list[DispatchOutbox] = []
            for oid in candidate_ids:
                # Atomic claim: only transition if still claimable. Concurrent
                # workers that selected the same id lose the race (rowcount=0).
                result = session.execute(
                    update(DispatchOutbox)
                    .where(
                        DispatchOutbox.id == oid,
                        DispatchOutbox.status.in_(("pending", "failed")),
                        DispatchOutbox.next_attempt_at <= now,
                    )
                    .values(
                        status="processing",
                        attempts=DispatchOutbox.attempts + 1,
                        updated_at=now,
                    )
                )
                if result.rowcount != 1:
                    continue
                row = session.get(DispatchOutbox, oid)
                if row is None:
                    continue
                session.expunge(row)
                claimed.append(row)
            return claimed

    def mark_outbox_sent(self, outbox_id: int) -> None:
        now = datetime.now(timezone.utc)
        with self.session() as session:
            row = session.get(DispatchOutbox, outbox_id)
            if row is None:
                return
            row.status = "sent"
            row.last_error = None
            row.updated_at = now

    def mark_outbox_result(
        self,
        outbox_id: int,
        *,
        success: bool,
        error: str | None,
        max_attempts: int,
        backoff_base_seconds: float = 2.0,
    ) -> str:
        """Return final status: sent | failed | dead."""
        now = datetime.now(timezone.utc)
        with self.session() as session:
            row = session.get(DispatchOutbox, outbox_id)
            if row is None:
                return "missing"
            if success:
                row.status = "sent"
                row.last_error = None
                row.updated_at = now
                return "sent"
            attempts = int(row.attempts or 0)
            row.last_error = (error or "")[:2000] or None
            if attempts >= max_attempts:
                row.status = "dead"
                row.updated_at = now
                return "dead"
            # Exponential backoff: base^attempts seconds (capped)
            delay = min(300.0, backoff_base_seconds ** max(1, attempts))
            row.status = "failed"
            row.next_attempt_at = now + timedelta(seconds=delay)
            row.updated_at = now
            return "failed"

    def dispatch_idempotency_succeeded(self, idempotency_key: str) -> bool:
        with self.session() as session:
            row = session.scalar(
                select(DispatchLog.id).where(
                    DispatchLog.idempotency_key == idempotency_key,
                    DispatchLog.success == 1,
                )
            )
            return row is not None

    def count_outbox_open(self) -> int:
        with self.session() as session:
            from sqlalchemy import func

            n = session.scalar(
                select(func.count())
                .select_from(DispatchOutbox)
                .where(DispatchOutbox.status.in_(_OUTBOX_OPEN))
            )
            return int(n or 0)

    def count_outbox(self, status: str | None = None) -> int:
        """Count outbox rows; optional exact status filter (e.g. ``dead``)."""
        with self.session() as session:
            from sqlalchemy import func

            q = select(func.count()).select_from(DispatchOutbox)
            if status:
                q = q.where(DispatchOutbox.status == status)
            return int(session.scalar(q) or 0)

    def outbox_status_counts(self) -> dict[str, int]:
        """Counts keyed by status for operator dashboards."""
        with self.session() as session:
            from sqlalchemy import func

            rows = session.execute(
                select(DispatchOutbox.status, func.count()).group_by(DispatchOutbox.status)
            ).all()
            return {str(status): int(n) for status, n in rows}

    def list_outbox(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[DispatchOutbox]:
        """List outbox rows newest-first (operator view / dead-letter)."""
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self.session() as session:
            q = select(DispatchOutbox).order_by(
                DispatchOutbox.updated_at.desc(), DispatchOutbox.id.desc()
            )
            if status:
                q = q.where(DispatchOutbox.status == status)
            rows = list(session.scalars(q.offset(offset).limit(limit)).all())
            for r in rows:
                session.expunge(r)
            return rows

    def redrive_outbox(
        self,
        *,
        ids: list[int] | None = None,
        status: str = "dead",
        all_matching: bool = False,
    ) -> int:
        """Reset outbox rows to ``pending`` for another worker attempt.

        Either pass explicit ``ids`` (must be ``dead`` or ``failed``) or
        ``all_matching=True`` with a ``status`` filter (default ``dead``).
        Resets ``attempts`` so max_attempts applies fully again.
        """
        if not all_matching and not ids:
            return 0
        now = datetime.now(timezone.utc)
        with self.session() as session:
            q = select(DispatchOutbox)
            if all_matching:
                q = q.where(DispatchOutbox.status == status)
            else:
                q = q.where(
                    DispatchOutbox.id.in_(list(ids or [])),
                    DispatchOutbox.status.in_(("dead", "failed")),
                )
            rows = list(session.scalars(q).all())
            for row in rows:
                row.status = "pending"
                row.attempts = 0
                row.next_attempt_at = now
                row.last_error = None
                row.updated_at = now
            session.flush()
            return len(rows)

    def delete_outbox(
        self,
        *,
        ids: list[int] | None = None,
        status: str = "dead",
        all_matching: bool = False,
    ) -> int:
        """Permanently delete outbox rows (typically dead-letter discard)."""
        if not all_matching and not ids:
            return 0
        with self.session() as session:
            if all_matching:
                result = session.execute(
                    delete(DispatchOutbox).where(DispatchOutbox.status == status)
                )
            else:
                result = session.execute(
                    delete(DispatchOutbox).where(DispatchOutbox.id.in_(list(ids or [])))
                )
            return int(result.rowcount or 0)

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
