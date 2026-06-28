"""Shared Redis read cache for the operator UI (multi-instance safe).

* Snapshot lives in Redis under ``alert_ui:snapshot`` with a **10s TTL** by default.
* Cache stampede is prevented with a Redis lock (``SET NX EX``): only one server
  reloads from Postgres when the snapshot is missing/expired; others wait and
  re-read Redis (or fall back to a single DB read if the lock holder is slow).
* Logical ``expires_at`` inside the payload enables soft-TTL / early refresh
  while the lock is held by another instance.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.db.models import AlertRecord, DispatchLog

logger = logging.getLogger(__name__)

REDIS_SNAPSHOT_KEY = "alert_ui:snapshot"
REDIS_LOCK_KEY = "alert_ui:snapshot:lock"


def _dt_to_iso(v: datetime | None) -> str | None:
    if v is None:
        return None
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.isoformat()


def _iso_to_dt(v: str | None) -> datetime | None:
    if not v:
        return None
    return datetime.fromisoformat(v.replace("Z", "+00:00"))


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

    def to_jsonable(self) -> dict[str, Any]:
        d = self.as_dict()
        for k in (
            "first_seen",
            "last_seen",
            "acknowledged_at",
            "resolved_at",
            "created_at",
            "updated_at",
        ):
            d[k] = _dt_to_iso(d[k])
        return d

    @classmethod
    def from_jsonable(cls, d: dict[str, Any]) -> "CachedAlert":
        return cls(
            id=d["id"],
            fingerprint=d["fingerprint"],
            title=d["title"],
            description=d.get("description") or "",
            severity=d["severity"],
            service=d["service"],
            host=d.get("host") or "unknown",
            status=d["status"],
            occurrence_count=int(d.get("occurrence_count") or 1),
            first_seen=_iso_to_dt(d.get("first_seen")) or datetime.now(timezone.utc),
            last_seen=_iso_to_dt(d.get("last_seen")) or datetime.now(timezone.utc),
            error_code=d.get("error_code"),
            trace_id=d.get("trace_id"),
            labels=dict(d.get("labels") or {}),
            sample_message=d.get("sample_message") or "",
            acknowledged_at=_iso_to_dt(d.get("acknowledged_at")),
            resolved_at=_iso_to_dt(d.get("resolved_at")),
            tta_seconds=d.get("tta_seconds"),
            ttr_seconds=d.get("ttr_seconds"),
            created_at=_iso_to_dt(d.get("created_at")),
            updated_at=_iso_to_dt(d.get("updated_at")),
            dispatch_success=int(d.get("dispatch_success") or 0),
            dispatch_failed=int(d.get("dispatch_failed") or 0),
        )


@dataclass
class CachedDispatch:
    id: int
    alert_id: str
    channel: str
    success: bool
    status_code: int | None
    error_message: str | None
    created_at: datetime

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "alert_id": self.alert_id,
            "channel": self.channel,
            "success": self.success,
            "status_code": self.status_code,
            "error_message": self.error_message,
            "created_at": _dt_to_iso(self.created_at),
        }

    @classmethod
    def from_jsonable(cls, d: dict[str, Any]) -> "CachedDispatch":
        return cls(
            id=int(d["id"]),
            alert_id=d["alert_id"],
            channel=d["channel"],
            success=bool(d["success"]),
            status_code=d.get("status_code"),
            error_message=d.get("error_message"),
            created_at=_iso_to_dt(d.get("created_at")) or datetime.now(timezone.utc),
        )


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

    def to_jsonable(self) -> dict[str, Any]:
        d = asdict(self)
        d["last_alert_at"] = _dt_to_iso(self.last_alert_at)
        return d

    @classmethod
    def from_jsonable(cls, d: dict[str, Any]) -> "CachedStats":
        return cls(
            total=int(d.get("total") or 0),
            open=int(d.get("open") or 0),
            updated=int(d.get("updated") or 0),
            acknowledged=int(d.get("acknowledged") or 0),
            resolved=int(d.get("resolved") or 0),
            critical_or_error=int(d.get("critical_or_error") or 0),
            services=int(d.get("services") or 0),
            dispatches_ok=int(d.get("dispatches_ok") or 0),
            dispatches_fail=int(d.get("dispatches_fail") or 0),
            last_alert_at=_iso_to_dt(d.get("last_alert_at")),
        )


@dataclass
class Page:
    """Paginated list result."""

    items: list[Any]
    total: int
    page: int
    page_size: int

    @property
    def pages(self) -> int:
        if self.page_size <= 0:
            return 0
        return max(1, (self.total + self.page_size - 1) // self.page_size) if self.total else 0

    @property
    def has_next(self) -> bool:
        return self.page * self.page_size < self.total

    @property
    def has_prev(self) -> bool:
        return self.page > 1


@dataclass
class _Snapshot:
    alerts: dict[str, CachedAlert] = field(default_factory=dict)
    dispatches: dict[str, list[CachedDispatch]] = field(default_factory=dict)
    recent_dispatches: list[CachedDispatch] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    stats: CachedStats = field(default_factory=CachedStats)
    loaded_at_unix: float = 0.0
    expires_at_unix: float = 0.0
    generation: int = 0

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "alerts": {k: v.to_jsonable() for k, v in self.alerts.items()},
            "dispatches": {
                k: [d.to_jsonable() for d in v] for k, v in self.dispatches.items()
            },
            "recent_dispatches": [d.to_jsonable() for d in self.recent_dispatches],
            "services": self.services,
            "stats": self.stats.to_jsonable(),
            "loaded_at_unix": self.loaded_at_unix,
            "expires_at_unix": self.expires_at_unix,
            "generation": self.generation,
        }

    @classmethod
    def from_jsonable(cls, data: dict[str, Any]) -> "_Snapshot":
        alerts = {
            k: CachedAlert.from_jsonable(v) for k, v in (data.get("alerts") or {}).items()
        }
        dispatches = {
            k: [CachedDispatch.from_jsonable(x) for x in v]
            for k, v in (data.get("dispatches") or {}).items()
        }
        recent = [
            CachedDispatch.from_jsonable(x) for x in (data.get("recent_dispatches") or [])
        ]
        return cls(
            alerts=alerts,
            dispatches=dispatches,
            recent_dispatches=recent,
            services=list(data.get("services") or []),
            stats=CachedStats.from_jsonable(data.get("stats") or {}),
            loaded_at_unix=float(data.get("loaded_at_unix") or 0),
            expires_at_unix=float(data.get("expires_at_unix") or 0),
            generation=int(data.get("generation") or 0),
        )


def _row_to_cached(row: AlertRecord, ok: int = 0, fail: int = 0) -> CachedAlert:
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
    """Redis-backed snapshot cache with stampede lock + optional local fallback."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        redis_url: str = "redis://localhost:6379/0",
        ttl_seconds: float = 10.0,
        lock_ttl_seconds: float = 5.0,
        lock_wait_seconds: float = 2.0,
        max_alerts: int = 2000,
        max_dispatches_per_alert: int = 50,
        max_recent_dispatches: int = 100,
        key_prefix: str = "alert_ui",
        # Probabilistic early expiry (beta) to spread refresh load before hard TTL
        early_expire_beta: float = 1.0,
    ) -> None:
        self._session_factory = session_factory
        self.ttl_seconds = max(1.0, float(ttl_seconds))
        self.lock_ttl_seconds = max(1.0, float(lock_ttl_seconds))
        self.lock_wait_seconds = max(0.1, float(lock_wait_seconds))
        self.max_alerts = max_alerts
        self.max_dispatches_per_alert = max_dispatches_per_alert
        self.max_recent_dispatches = max_recent_dispatches
        self.early_expire_beta = max(0.0, float(early_expire_beta))
        self._snapshot_key = f"{key_prefix}:snapshot"
        self._lock_key = f"{key_prefix}:snapshot:lock"
        self._token = str(uuid.uuid4())
        self._local = _Snapshot()
        self._local_lock = threading.RLock()
        self._db_fetch_count = 0
        self._db_fetch_lock = threading.Lock()

        try:
            import redis

            self._redis = redis.Redis.from_url(redis_url, decode_responses=True)
            self._redis.ping()
            self._backend = "redis"
            logger.info("AlertReadCache using Redis at %s ttl=%ss", redis_url, self.ttl_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Redis unavailable (%s); falling back to in-process cache (not multi-instance safe)",
                exc,
            )
            self._redis = None
            self._backend = "memory"

    def start(self) -> None:
        self.refresh(force=True)

    def stop(self) -> None:
        return

    # --- stampede-safe load -------------------------------------------------

    def _should_refresh_early(self, snap: _Snapshot) -> bool:
        """XFetch-style probabilistic early expiration to avoid synchronized expiry."""
        if not snap.expires_at_unix or self.early_expire_beta <= 0:
            return False
        now = time.time()
        if now >= snap.expires_at_unix:
            return True
        # remaining = expires - now; refresh early with probability rising as TTL ends
        ttl = max(0.001, snap.expires_at_unix - snap.loaded_at_unix)
        delta = snap.expires_at_unix - now
        # expiryFactor = delta - beta * ttl * log(random)
        expiry_factor = delta - self.early_expire_beta * ttl * (-1.0 * _safe_log_random())
        return expiry_factor < 0

    def _redis_error(self, op: str, exc: BaseException) -> None:
        """On Redis failures, degrade to in-process cache so the UI never 500s."""
        logger.warning(
            "Redis %s failed (%s); degrading to in-process cache for this process",
            op,
            exc,
        )
        self._redis = None
        self._backend = "memory"

    def _read_redis_snapshot(self) -> _Snapshot | None:
        if not self._redis:
            return None
        try:
            raw = self._redis.get(self._snapshot_key)
        except Exception as exc:  # noqa: BLE001
            self._redis_error("GET snapshot", exc)
            return None
        if not raw:
            return None
        try:
            return _Snapshot.from_jsonable(json.loads(raw))
        except (json.JSONDecodeError, TypeError, KeyError):
            logger.warning("Corrupt cache snapshot; ignoring")
            return None

    def _write_redis_snapshot(self, snap: _Snapshot) -> None:
        if not self._redis:
            return
        # Hard TTL slightly above logical TTL so readers can still use soft-stale during lock
        hard_ttl = int(self.ttl_seconds) + int(self.lock_ttl_seconds) + 2
        payload = json.dumps(snap.to_jsonable(), separators=(",", ":"))
        try:
            self._redis.set(self._snapshot_key, payload, ex=hard_ttl)
        except Exception as exc:  # noqa: BLE001
            self._redis_error("SET snapshot", exc)

    def _acquire_lock(self) -> bool:
        if not self._redis:
            return True
        try:
            return bool(
                self._redis.set(
                    self._lock_key,
                    self._token,
                    nx=True,
                    ex=int(self.lock_ttl_seconds),
                )
            )
        except Exception as exc:  # noqa: BLE001
            self._redis_error("SET lock", exc)
            return True  # proceed as sole loader in memory mode

    def _release_lock(self) -> None:
        if not self._redis:
            return
        # Release only if we own the lock (simple token compare)
        try:
            pipe = self._redis.pipeline(True)
            while True:
                try:
                    pipe.watch(self._lock_key)
                    current = pipe.get(self._lock_key)
                    if current == self._token:
                        pipe.multi()
                        pipe.delete(self._lock_key)
                        pipe.execute()
                    else:
                        pipe.unwatch()
                    return
                except Exception:  # noqa: BLE001 — redis WatchError etc.
                    continue
        except Exception as exc:  # noqa: BLE001
            self._redis_error("release lock", exc)

    def _load_from_db(self, *, reason: str = "unspecified") -> _Snapshot:
        """Load full alert snapshot from Postgres. Always logs — use for cache-miss alerts."""
        with self._db_fetch_lock:
            self._db_fetch_count += 1
            fetch_n = self._db_fetch_count

        # Distinct marker line for log-based alerts / metrics scrapers:
        #   ALERT_DB_FETCH  or  event=alert_ui_db_fetch
        logger.warning(
            "ALERT_DB_FETCH event=alert_ui_db_fetch reason=%s fetch_count=%s "
            "backend=%s ttl_seconds=%s — UI/cache is hitting Postgres to load alerts "
            "(not served purely from Redis cache)",
            reason,
            fetch_n,
            self._backend,
            self.ttl_seconds,
        )
        logger.info(
            "Loading alerts from database reason=%s fetch_count=%s",
            reason,
            fetch_n,
        )

        now = time.time()
        snap = _Snapshot(
            loaded_at_unix=now,
            expires_at_unix=now + self.ttl_seconds,
        )
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

    def refresh(self, *, force: bool = False) -> None:
        """Reload snapshot from DB under distributed lock (stampede-safe)."""
        if not force:
            snap = self._read_redis_snapshot()
            if snap and time.time() < snap.expires_at_unix and not self._should_refresh_early(snap):
                with self._local_lock:
                    self._local = snap
                return

        got_lock = self._acquire_lock()
        if not got_lock:
            # Another instance is rebuilding — wait for Redis key
            deadline = time.time() + self.lock_wait_seconds
            while time.time() < deadline:
                snap = self._read_redis_snapshot()
                if snap:
                    with self._local_lock:
                        self._local = snap
                    return
                time.sleep(0.05)
            # Timed out waiting — last resort single DB read (rare)
            logger.warning("Cache lock wait timed out; loading DB without lock")
            snap = self._load_from_db(reason="lock_wait_timeout")
            with self._local_lock:
                snap.generation = self._local.generation + 1
                self._local = snap
            return

        try:
            # Double-check after lock (another writer may have finished)
            if not force:
                existing = self._read_redis_snapshot()
                if (
                    existing
                    and time.time() < existing.expires_at_unix
                    and not self._should_refresh_early(existing)
                ):
                    with self._local_lock:
                        self._local = existing
                    return

            reason = "force_refresh" if force else "ttl_expired_or_missing"
            snap = self._load_from_db(reason=reason)
            with self._local_lock:
                snap.generation = self._local.generation + 1
                self._local = snap
            self._write_redis_snapshot(snap)
            logger.info(
                "Redis snapshot refreshed generation=%s alerts=%s ttl=%ss reason=%s",
                snap.generation,
                len(snap.alerts),
                self.ttl_seconds,
                reason,
            )
        finally:
            self._release_lock()

    def invalidate(self) -> None:
        """Drop shared snapshot so the next read rebuilds (stampede-locked)."""
        if self._redis:
            try:
                self._redis.delete(self._snapshot_key)
            except Exception as exc:  # noqa: BLE001
                self._redis_error("DELETE snapshot", exc)
        with self._local_lock:
            self._local = _Snapshot()

    def _ensure_fresh(self) -> _Snapshot:
        snap = self._read_redis_snapshot()
        now = time.time()
        if snap is None or now >= snap.expires_at_unix or self._should_refresh_early(snap):
            self.refresh(force=snap is None)
            snap = self._read_redis_snapshot()
        if snap is None:
            with self._local_lock:
                if self._local.alerts or self._local.loaded_at_unix:
                    return self._local
            # Memory fallback path (no Redis or empty after failed refresh)
            snap = self._load_from_db(reason="ensure_fresh_empty_fallback")
            with self._local_lock:
                self._local = snap
            return snap
        with self._local_lock:
            self._local = snap
        return snap

    # --- query API (all from snapshot; pagination in-process on cached set) -

    def stats(self) -> CachedStats:
        return self._ensure_fresh().stats

    def list_services(self) -> list[str]:
        return list(self._ensure_fresh().services)

    def get_alert(self, alert_id: str) -> CachedAlert | None:
        return self._ensure_fresh().alerts.get(alert_id)

    def _filtered_alerts(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        service: str | None = None,
        q: str | None = None,
    ) -> list[CachedAlert]:
        snap = self._ensure_fresh()
        items = list(snap.alerts.values())
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
        return items

    def list_alerts_page(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        service: str | None = None,
        q: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> Page:
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        items = self._filtered_alerts(
            status=status, severity=severity, service=service, q=q
        )
        total = len(items)
        start = (page - 1) * page_size
        slice_ = items[start : start + page_size]
        return Page(items=slice_, total=total, page=page, page_size=page_size)

    def alert_dispatches_page(
        self, alert_id: str, *, page: int = 1, page_size: int = 50
    ) -> Page:
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        snap = self._ensure_fresh()
        rows = list(snap.dispatches.get(alert_id, []))
        total = len(rows)
        start = (page - 1) * page_size
        return Page(
            items=rows[start : start + page_size],
            total=total,
            page=page,
            page_size=page_size,
        )

    def recent_dispatches_page(self, *, page: int = 1, page_size: int = 30) -> Page:
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        rows = list(self._ensure_fresh().recent_dispatches)
        total = len(rows)
        start = (page - 1) * page_size
        return Page(
            items=rows[start : start + page_size],
            total=total,
            page=page,
            page_size=page_size,
        )

    # Back-compat helpers used by older call sites
    def list_alerts(self, **kwargs: Any) -> list[CachedAlert]:
        page = int(kwargs.pop("page", 1) or 1)
        # legacy limit/offset
        if "limit" in kwargs or "offset" in kwargs:
            limit = int(kwargs.pop("limit", 100))
            offset = int(kwargs.pop("offset", 0))
            page_size = limit
            page = (offset // page_size) + 1 if page_size else 1
            kwargs["page"] = page
            kwargs["page_size"] = page_size
        return self.list_alerts_page(**kwargs).items

    def alert_dispatches(self, alert_id: str, limit: int = 50) -> list[CachedDispatch]:
        return self.alert_dispatches_page(alert_id, page=1, page_size=limit).items

    def recent_dispatches(self, limit: int = 30) -> list[CachedDispatch]:
        return self.recent_dispatches_page(page=1, page_size=limit).items

    def meta(self) -> dict[str, Any]:
        snap = self._read_redis_snapshot()
        with self._local_lock:
            local_gen = self._local.generation
            local_n = len(self._local.alerts)
        with self._db_fetch_lock:
            db_fetches = self._db_fetch_count
        return {
            "source": self._backend,
            "snapshot_key": self._snapshot_key,
            "ttl_seconds": self.ttl_seconds,
            "lock_ttl_seconds": self.lock_ttl_seconds,
            "stampede_protection": "redis_nx_lock+probabilistic_early_expire",
            "redis_generation": snap.generation if snap else None,
            "redis_expires_at_unix": snap.expires_at_unix if snap else None,
            "local_generation": local_gen,
            "local_alert_count": local_n,
            "redis_alert_count": len(snap.alerts) if snap else 0,
            "db_fetch_count": db_fetches,
            "db_fetch_log_marker": "ALERT_DB_FETCH",
        }


def _safe_log_random() -> float:
    r = random.random()
    if r <= 0.0:
        r = 1e-12
    import math

    return math.log(r)
