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
from dataclasses import dataclass, field
from typing import Generic, TypeVar, cast

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, sessionmaker

from alert_pipeline.db.mapping import alert_view_from_record, dispatch_view_from_log
from alert_pipeline.db.models import AlertRecord, DispatchLog
from alert_pipeline.schemas import (
    AlertStatus,
    AlertView,
    CachedAlert,
    CachedDispatch,
    CachedStats,
    DispatchView,
    StatsView,
)
from alert_pipeline.types import CacheMeta, JsonObject

logger = logging.getLogger(__name__)

REDIS_SNAPSHOT_KEY = "alert_ui:snapshot"
REDIS_LOCK_KEY = "alert_ui:snapshot:lock"

# Re-export shared view types so existing imports keep working.
__all__ = [
    "AlertReadCache",
    "AlertView",
    "CachedAlert",
    "CachedDispatch",
    "CachedStats",
    "DispatchView",
    "Page",
    "StatsView",
]

T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    """Paginated list result."""

    items: list[T]
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
    alerts: dict[str, AlertView] = field(default_factory=dict)
    dispatches: dict[str, list[DispatchView]] = field(default_factory=dict)
    recent_dispatches: list[DispatchView] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    stats: StatsView = field(default_factory=StatsView)
    loaded_at_unix: float = 0.0
    expires_at_unix: float = 0.0
    generation: int = 0

    def to_jsonable(self) -> JsonObject:
        return {
            "alerts": {k: v.to_jsonable() for k, v in self.alerts.items()},
            "dispatches": {k: [d.to_jsonable() for d in v] for k, v in self.dispatches.items()},
            "recent_dispatches": [d.to_jsonable() for d in self.recent_dispatches],
            "services": list(self.services),
            "stats": self.stats.to_jsonable(),
            "loaded_at_unix": self.loaded_at_unix,
            "expires_at_unix": self.expires_at_unix,
            "generation": self.generation,
        }

    @classmethod
    def from_jsonable(cls, data: JsonObject) -> "_Snapshot":
        alerts_raw = data.get("alerts") or {}
        dispatches_raw = data.get("dispatches") or {}
        recent_raw = data.get("recent_dispatches") or []
        services_raw = data.get("services") or []
        stats_raw = data.get("stats") or {}

        alerts: dict[str, AlertView] = {}
        if isinstance(alerts_raw, dict):
            for k, v in alerts_raw.items():
                if isinstance(v, dict):
                    alerts[str(k)] = AlertView.from_jsonable(cast(JsonObject, v))

        dispatches: dict[str, list[DispatchView]] = {}
        if isinstance(dispatches_raw, dict):
            for k, v in dispatches_raw.items():
                if isinstance(v, list):
                    dispatches[str(k)] = [
                        DispatchView.from_jsonable(cast(JsonObject, x))
                        for x in v
                        if isinstance(x, dict)
                    ]

        recent: list[DispatchView] = []
        if isinstance(recent_raw, list):
            recent = [
                DispatchView.from_jsonable(cast(JsonObject, x))
                for x in recent_raw
                if isinstance(x, dict)
            ]

        services = [str(s) for s in services_raw] if isinstance(services_raw, list) else []
        stats = (
            StatsView.from_jsonable(cast(JsonObject, stats_raw))
            if isinstance(stats_raw, dict)
            else StatsView()
        )
        return cls(
            alerts=alerts,
            dispatches=dispatches,
            recent_dispatches=recent,
            services=services,
            stats=stats,
            loaded_at_unix=float(data.get("loaded_at_unix") or 0),
            expires_at_unix=float(data.get("expires_at_unix") or 0),
            generation=int(data.get("generation") or 0),
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
            parsed: object = json.loads(raw)
            if not isinstance(parsed, dict):
                return None
            return _Snapshot.from_jsonable(cast(JsonObject, parsed))
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
                select(AlertRecord).order_by(desc(AlertRecord.last_seen)).limit(self.max_alerts)
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
                snap.alerts[r.id] = alert_view_from_record(
                    r,
                    dispatch_success=ok_map.get(r.id, 0),
                    dispatch_failed=fail_map.get(r.id, 0),
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
                    bucket.append(dispatch_view_from_log(d))

            recent = session.scalars(
                select(DispatchLog)
                .order_by(desc(DispatchLog.created_at))
                .limit(self.max_recent_dispatches)
            ).all()
            snap.recent_dispatches = [dispatch_view_from_log(d) for d in recent]

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
            snap.stats = StatsView(
                total=total,
                open=_count(AlertStatus.OPEN.value),
                updated=_count(AlertStatus.UPDATED.value),
                acknowledged=_count(AlertStatus.ACKNOWLEDGED.value),
                resolved=_count(AlertStatus.RESOLVED.value),
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

    def stats(self) -> StatsView:
        return self._ensure_fresh().stats

    def list_services(self) -> list[str]:
        return list(self._ensure_fresh().services)

    def get_alert(self, alert_id: str) -> AlertView | None:
        return self._ensure_fresh().alerts.get(alert_id)

    def _filtered_alerts(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        service: str | None = None,
        q: str | None = None,
        label_key: str | None = None,
        label_value: str | None = None,
        labels: list[dict[str, str]] | None = None,
    ) -> list[AlertView]:
        snap = self._ensure_fresh()
        items = list(snap.alerts.values())
        # Newest activity first (last_seen desc; ties use first_seen)
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

        # Multi-label AND: every {key,value} must match (empty value = any for that key)
        label_specs: list[tuple[str, str]] = []
        if labels:
            for spec in labels:
                if not isinstance(spec, dict):
                    continue
                k = str(spec.get("key") or "").strip().lower()
                if not k:
                    continue
                v = str(spec.get("value") or "").strip().lower()
                label_specs.append((k, v))
        elif label_key:
            label_specs.append((label_key.strip().lower(), (label_value or "").strip().lower()))

        if label_specs:

            def all_labels_match(a: AlertView) -> bool:
                al = {str(k).lower(): str(v).lower() for k, v in (a.labels or {}).items()}
                for lk, lv in label_specs:
                    if lk not in al:
                        return False
                    if lv and al[lk] != lv:
                        return False
                return True

            items = [a for a in items if all_labels_match(a)]
        if q:
            needle = q.lower()

            def match(a: AlertView) -> bool:
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
        label_key: str | None = None,
        label_value: str | None = None,
        labels: list[dict[str, str]] | None = None,
        page: int = 1,
        page_size: int = 10,
    ) -> Page[AlertView]:
        page = max(1, int(page))
        page_size = min(200, max(1, int(page_size)))
        items = self._filtered_alerts(
            status=status,
            severity=severity,
            service=service,
            q=q,
            label_key=label_key,
            label_value=label_value,
            labels=labels,
        )
        total = len(items)
        start = (page - 1) * page_size
        slice_ = items[start : start + page_size]
        return Page(items=slice_, total=total, page=page, page_size=page_size)

    def alert_dispatches_page(
        self, alert_id: str, *, page: int = 1, page_size: int = 50
    ) -> Page[DispatchView]:
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

    def recent_dispatches_page(self, *, page: int = 1, page_size: int = 30) -> Page[DispatchView]:
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
    def list_alerts(
        self,
        *,
        status: str | None = None,
        severity: str | None = None,
        service: str | None = None,
        q: str | None = None,
        label_key: str | None = None,
        label_value: str | None = None,
        labels: list[dict[str, str]] | None = None,
        page: int = 1,
        page_size: int = 10,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[AlertView]:
        # legacy limit/offset
        if limit is not None or offset is not None:
            page_size = int(limit if limit is not None else page_size)
            off = int(offset or 0)
            page = (off // page_size) + 1 if page_size else 1
        return self.list_alerts_page(
            status=status,
            severity=severity,
            service=service,
            q=q,
            label_key=label_key,
            label_value=label_value,
            labels=labels,
            page=page,
            page_size=page_size,
        ).items

    def alert_dispatches(self, alert_id: str, limit: int = 50) -> list[DispatchView]:
        return self.alert_dispatches_page(alert_id, page=1, page_size=limit).items

    def recent_dispatches(self, limit: int = 30) -> list[DispatchView]:
        return self.recent_dispatches_page(page=1, page_size=limit).items

    def meta(self) -> CacheMeta:
        snap = self._read_redis_snapshot()
        with self._local_lock:
            local_gen = self._local.generation
            local_n = len(self._local.alerts)
        with self._db_fetch_lock:
            db_fetches = self._db_fetch_count
        return {
            "source": self._backend,
            "snapshot_key": self._snapshot_key,
            "ttl_seconds": float(self.ttl_seconds),
            "lock_ttl_seconds": float(self.lock_ttl_seconds),
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
