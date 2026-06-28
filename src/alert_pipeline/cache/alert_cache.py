"""In-process TTL cache for UI reads.

The stream pipeline and UI mutations write to Postgres. The UI **reads** from
this cache so list/stats endpoints do not hit the DB on every poll. A background
refresher reloads from the DB on an interval (and callers can ``invalidate()``
after writes).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from sqlalchemy import desc, func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.db.models import AlertRecord, DispatchLog

logger = logging.getLogger(__name__)


@dataclass
class CachedAlert:
    id: str
    fingerprint: str
    title: str
    description: str
    severity: str
    service: str
    host: str
    status: str
    occurrence_count: int
    first_seen: datetime
    last_seen: datetime
    error_code: str | None
    trace_id: str | None
    labels: dict[str, str]
    sample_message: str
    acknowledged_at: datetime | None
    resolved_at: datetime | None
    tta_seconds: int | None
    ttr_seconds: int | None
    created_at: datetime | None
    updated_at: datetime | None
    dispatch_success: int = 0
    dispatch_failed: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "fingerprint": self.fingerprint,
            "title": self.title,
            "description": self.description,
            "severity": self.severity,
            "service": self.service,
            "host": self.host,
            "status": self.status,
            "occurrence_count": self.occurrence_count,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "error_code": self.error_code,
            "trace_id": self.trace_id,
            "labels": self.labels,
            "sample_message": self.sample_message,
            "acknowledged_at": self.acknowledged_at,
            "resolved_at": self.resolved_at,
            "tta_seconds": self.tta_seconds,
            "ttr_seconds": self.ttr_seconds,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "dispatch_success": self.dispatch_success,
            "dispatch_failed": self.dispatch_failed,
        }


@dataclass
class CachedDispatch:
    id: int
    alert_id: str
    channel: str
    success: bool
    status_code: int | None
    error_message: str | None
    created_at: datetime


@dataclass
class CachedStats:
    total: int = 0
    open: int = 0
    updated: int = 0
    acknowledged: int = 0
    resolved: int = 0
    critical_or_error: int = 0
    services: int = 0
    dispatches_ok: int = 0
    dispatches_fail: int = 0
    last_alert_at: datetime | None = None


@dataclass
class _Snapshot:
    alerts: dict[str, CachedAlert] = field(default_factory=dict)
    # alert_id -> dispatches (newest first, capped)
    dispatches: dict[str, list[CachedDispatch]] = field(default_factory=dict)
    recent_dispatches: list[CachedDispatch] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    stats: CachedStats = field(default_factory=CachedStats)
    loaded_at: float = 0.0
    generation: int = 0


def _row_to_cached(
    row: AlertRecord, ok: int = 0, fail: int = 0
) -> CachedAlert:
    try:
        labels = json.loads(row.labels_json or "{}")
        if not isinstance(labels, dict):
            labels = {}
        labels = {str(k): str(v) for k, v in labels.items()}
    except json.JSONDecodeError:
        labels = {}
    return CachedAlert(
        id=row.id,
        fingerprint=row.fingerprint,
        title=row.title,
        description=row.description,
        severity=row.severity,
        service=row.service,
        host=row.host,
        status=row.status,
        occurrence_count=row.occurrence_count,
        first_seen=row.first_seen,
        last_seen=row.last_seen,
        error_code=row.error_code,
        trace_id=row.trace_id,
        labels=labels,
        sample_message=row.sample_message,
        acknowledged_at=getattr(row, "acknowledged_at", None),
        resolved_at=getattr(row, "resolved_at", None),
        tta_seconds=getattr(row, "tta_seconds", None),
        ttr_seconds=getattr(row, "ttr_seconds", None),
        created_at=row.created_at,
        updated_at=row.updated_at,
        dispatch_success=ok,
        dispatch_failed=fail,
    )


class AlertReadCache:
    """Thread-safe in-memory snapshot refreshed from Postgres on a TTL."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        ttl_seconds: float = 2.0,
        max_alerts: int = 2000,
        max_dispatches_per_alert: int = 50,
        max_recent_dispatches: int = 100,
        background_refresh: bool = True,
    ) -> None:
        self._session_factory = session_factory
        self.ttl_seconds = max(0.2, float(ttl_seconds))
        self.max_alerts = max_alerts
        self.max_dispatches_per_alert = max_dispatches_per_alert
        self.max_recent_dispatches = max_recent_dispatches
        self._lock = threading.RLock()
        self._snap = _Snapshot()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        if background_refresh:
            self._thread = threading.Thread(
                target=self._loop, name="alert-read-cache", daemon=True
            )
            self._thread.start()

    def start(self) -> None:
        self.refresh(force=True)

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _loop(self) -> None:
        while not self._stop.wait(self.ttl_seconds):
            try:
                self.refresh(force=True)
            except Exception:  # noqa: BLE001
                logger.exception("Cache background refresh failed")

    def invalidate(self) -> None:
        """Mark snapshot stale so the next read triggers a reload (or wait for BG)."""
        with self._lock:
            self._snap.loaded_at = 0.0

    def refresh(self, *, force: bool = False) -> None:
        with self._lock:
            age = time.monotonic() - self._snap.loaded_at
            if not force and self._snap.loaded_at and age < self.ttl_seconds:
                return

        snap = self._load_from_db()
        with self._lock:
            snap.generation = self._snap.generation + 1
            self._snap = snap
        logger.debug(
            "AlertReadCache refreshed generation=%s alerts=%s",
            snap.generation,
            len(snap.alerts),
        )

    def _load_from_db(self) -> _Snapshot:
        snap = _Snapshot(loaded_at=time.monotonic())
        with self._session_factory() as session:
            rows = session.scalars(
                select(AlertRecord)
                .order_by(desc(AlertRecord.last_seen))
                .limit(self.max_alerts)
            ).all()
            ids = [r.id for r in rows]
            ok_map: dict[str, int] = {}
            fail_map: dict[str, int] = {}
            if ids:
                for alert_id, success, cnt in session.execute(
                    select(
                        DispatchLog.alert_id,
                        DispatchLog.success,
                        func.count().label("cnt"),
                    )
                    .where(DispatchLog.alert_id.in_(ids))
                    .group_by(DispatchLog.alert_id, DispatchLog.success)
                ).all():
                    if success:
                        ok_map[alert_id] = int(cnt)
                    else:
                        fail_map[alert_id] = int(cnt)

            for r in rows:
                snap.alerts[r.id] = _row_to_cached(
                    r, ok_map.get(r.id, 0), fail_map.get(r.id, 0)
                )

            # Per-alert dispatch history (batch load, then bucket)
            if ids:
                drows = session.scalars(
                    select(DispatchLog)
                    .where(DispatchLog.alert_id.in_(ids))
                    .order_by(desc(DispatchLog.created_at))
                    .limit(self.max_alerts * self.max_dispatches_per_alert)
                ).all()
                for d in drows:
                    bucket = snap.dispatches.setdefault(d.alert_id, [])
                    if len(bucket) >= self.max_dispatches_per_alert:
                        continue
                    bucket.append(
                        CachedDispatch(
                            id=d.id,
                            alert_id=d.alert_id,
                            channel=d.channel,
                            success=bool(d.success),
                            status_code=d.status_code,
                            error_message=d.error_message,
                            created_at=d.created_at,
                        )
                    )

            recent = session.scalars(
                select(DispatchLog)
                .order_by(desc(DispatchLog.created_at))
                .limit(self.max_recent_dispatches)
            ).all()
            snap.recent_dispatches = [
                CachedDispatch(
                    id=d.id,
                    alert_id=d.alert_id,
                    channel=d.channel,
                    success=bool(d.success),
                    status_code=d.status_code,
                    error_message=d.error_message,
                    created_at=d.created_at,
                )
                for d in recent
            ]

            services = session.scalars(
                select(AlertRecord.service).distinct().order_by(AlertRecord.service)
            ).all()
            snap.services = list(services)

            def _count(status: str) -> int:
                return int(
                    session.scalar(
                        select(func.count())
                        .select_from(AlertRecord)
                        .where(AlertRecord.status == status)
                    )
                    or 0
                )

            total = int(session.scalar(select(func.count()).select_from(AlertRecord)) or 0)
            crit = int(
                session.scalar(
                    select(func.count())
                    .select_from(AlertRecord)
                    .where(AlertRecord.severity.in_(("ERROR", "CRITICAL", "FATAL")))
                )
                or 0
            )
            ok = int(
                session.scalar(
                    select(func.count()).select_from(DispatchLog).where(DispatchLog.success == 1)
                )
                or 0
            )
            fail = int(
                session.scalar(
                    select(func.count()).select_from(DispatchLog).where(DispatchLog.success == 0)
                )
                or 0
            )
            last_at = session.scalar(select(func.max(AlertRecord.last_seen)))
            snap.stats = CachedStats(
                total=total,
                open=_count("open"),
                updated=_count("updated"),
                acknowledged=_count("acknowledged"),
                resolved=_count("resolved"),
                critical_or_error=crit,
                services=len(snap.services),
                dispatches_ok=ok,
                dispatches_fail=fail,
                last_alert_at=last_at,
            )
        return snap

    def _ensure_fresh(self) -> _Snapshot:
        with self._lock:
            age = time.monotonic() - self._snap.loaded_at
            stale = not self._snap.loaded_at or age >= self.ttl_seconds
            if not stale:
                return self._snap
        self.refresh(force=True)
        with self._lock:
            return self._snap

    def stats(self) -> CachedStats:
        return self._ensure_fresh().stats

    def list_services(self) -> list[str]:
        return list(self._ensure_fresh().services)

    def get_alert(self, alert_id: str) -> CachedAlert | None:
        snap = self._ensure_fresh()
        return snap.alerts.get(alert_id)

    def list_alerts(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        service: str | None = None,
        q: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[CachedAlert]:
        snap = self._ensure_fresh()
        items = list(snap.alerts.values())
        # Already ordered by last_seen desc when loaded; re-sort for safety
        items.sort(key=lambda a: a.last_seen or a.first_seen, reverse=True)

        if status:
            statuses = {s.strip() for s in status.split(",") if s.strip()}
            if statuses:
                items = [a for a in items if a.status in statuses]
        if severity:
            sevs = {s.strip().upper() for s in severity.split(",") if s.strip()}
            if sevs:
                items = [a for a in items if a.severity.upper() in sevs]
        if service:
            items = [a for a in items if a.service == service]
        if q:
            needle = q.lower()
            def match(a: CachedAlert) -> bool:
                blob = " ".join(
                    [
                        a.title or "",
                        a.sample_message or "",
                        a.fingerprint or "",
                        a.error_code or "",
                        a.service or "",
                    ]
                ).lower()
                return needle in blob

            items = [a for a in items if match(a)]

        return items[offset : offset + limit]

    def alert_dispatches(self, alert_id: str, limit: int = 50) -> list[CachedDispatch]:
        snap = self._ensure_fresh()
        rows = snap.dispatches.get(alert_id, [])
        return rows[:limit]

    def recent_dispatches(self, limit: int = 30) -> list[CachedDispatch]:
        snap = self._ensure_fresh()
        return snap.recent_dispatches[:limit]

    def meta(self) -> dict[str, Any]:
        with self._lock:
            return {
                "source": "memory_cache",
                "generation": self._snap.generation,
                "loaded_at_mono": self._snap.loaded_at,
                "ttl_seconds": self.ttl_seconds,
                "alert_count": len(self._snap.alerts),
            }
