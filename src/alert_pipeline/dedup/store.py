"""Pluggable dedup state backends: in-process memory or shared Redis.

Redis key layout (prefix defaults to ``alert_dedup``)::

    {prefix}:fp:{fingerprint}  → JSON IncidentState  TTL = window_seconds
"""

from __future__ import annotations

import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from alert_pipeline.schemas import LogLevel

logger = logging.getLogger(__name__)


def _dt_to_iso(v: datetime) -> str:
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.isoformat()


def _iso_to_dt(v: str) -> datetime:
    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@dataclass
class IncidentState:
    alert_id: str
    fingerprint: str
    first_seen: datetime
    last_seen: datetime
    occurrence_count: int = 1
    last_emitted_at: float = field(default_factory=time.time)
    severity: LogLevel = LogLevel.ERROR
    service: str = "unknown"
    host: str = "unknown"
    title: str = ""
    sample_message: str = ""
    error_code: str | None = None
    trace_id: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    window_seconds: int = 300

    def to_json(self) -> str:
        payload = {
            "alert_id": self.alert_id,
            "fingerprint": self.fingerprint,
            "first_seen": _dt_to_iso(self.first_seen),
            "last_seen": _dt_to_iso(self.last_seen),
            "occurrence_count": self.occurrence_count,
            "last_emitted_at": self.last_emitted_at,
            "severity": self.severity.value
            if isinstance(self.severity, LogLevel)
            else str(self.severity),
            "service": self.service,
            "host": self.host,
            "title": self.title,
            "sample_message": self.sample_message,
            "error_code": self.error_code,
            "trace_id": self.trace_id,
            "labels": self.labels,
            "window_seconds": self.window_seconds,
        }
        return json.dumps(payload)

    @classmethod
    def from_json(cls, raw: str | bytes) -> "IncidentState":
        data = json.loads(raw)
        sev = data.get("severity") or "ERROR"
        try:
            severity = LogLevel(sev) if not isinstance(sev, LogLevel) else sev
        except ValueError:
            severity = LogLevel.normalize(str(sev))
        return cls(
            alert_id=data["alert_id"],
            fingerprint=data["fingerprint"],
            first_seen=_iso_to_dt(data["first_seen"]),
            last_seen=_iso_to_dt(data["last_seen"]),
            occurrence_count=int(data.get("occurrence_count") or 1),
            last_emitted_at=float(data.get("last_emitted_at") or time.time()),
            severity=severity,
            service=data.get("service") or "unknown",
            host=data.get("host") or "unknown",
            title=data.get("title") or "",
            sample_message=data.get("sample_message") or "",
            error_code=data.get("error_code"),
            trace_id=data.get("trace_id"),
            labels=dict(data.get("labels") or {}),
            window_seconds=int(data.get("window_seconds") or 300),
        )


class DedupStore(ABC):
    """Shared interface for active-incident window state."""

    @abstractmethod
    def get(self, fingerprint: str) -> IncidentState | None:
        raise NotImplementedError

    @abstractmethod
    def put(self, state: IncidentState, *, ttl_seconds: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def try_create(self, state: IncidentState, *, ttl_seconds: int) -> bool:
        """Atomically create if missing. Returns True if this caller created it."""
        raise NotImplementedError

    @abstractmethod
    def delete(self, fingerprint: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def expire_stale(self, now: float) -> None:
        """Drop expired entries (no-op for Redis which uses key TTL)."""
        raise NotImplementedError

    def count(self) -> int:
        return 0

    def close(self) -> None:
        return None


class MemoryDedupStore(DedupStore):
    """Process-local map — fine for tests and single-consumer demos."""

    def __init__(self) -> None:
        self._state: dict[str, IncidentState] = {}
        self._lock = threading.RLock()

    def get(self, fingerprint: str) -> IncidentState | None:
        with self._lock:
            return self._state.get(fingerprint)

    def put(self, state: IncidentState, *, ttl_seconds: int) -> None:
        with self._lock:
            state.window_seconds = ttl_seconds
            self._state[state.fingerprint] = state

    def try_create(self, state: IncidentState, *, ttl_seconds: int) -> bool:
        with self._lock:
            if state.fingerprint in self._state:
                return False
            state.window_seconds = ttl_seconds
            self._state[state.fingerprint] = state
            return True

    def delete(self, fingerprint: str) -> None:
        with self._lock:
            self._state.pop(fingerprint, None)

    def expire_stale(self, now: float) -> None:
        with self._lock:
            expired = [
                fp
                for fp, st in self._state.items()
                if now - st.last_seen.timestamp() > st.window_seconds
            ]
            for fp in expired:
                del self._state[fp]

    def count(self) -> int:
        with self._lock:
            return len(self._state)


_SEVERITY_RANK = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
    "FATAL": 50,
}


class RedisDedupStore(DedupStore):
    """Shared active-window state for multi-instance / multi-task pipelines.

    Uses per-fingerprint short locks (SET NX) so concurrent workers serialize
    create/update/suppress decisions. Key TTL equals the dedup window.
    """

    def __init__(
        self,
        redis_url: str,
        *,
        key_prefix: str = "alert_dedup",
        client: Any | None = None,
    ) -> None:
        self._prefix = key_prefix.rstrip(":")
        if client is not None:
            self._client = client
        else:
            try:
                import redis
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("redis package required for DEDUP_BACKEND=redis") from exc

            self._client = redis.Redis.from_url(redis_url, decode_responses=True)
            self._client.ping()
            logger.info(
                "RedisDedupStore ready prefix=%s url=%s",
                self._prefix,
                _redact_url(redis_url),
            )

    def _key(self, fingerprint: str) -> str:
        return f"{self._prefix}:fp:{fingerprint}"

    def _lock_key(self, fingerprint: str) -> str:
        return f"{self._prefix}:lock:{fingerprint}"

    def get(self, fingerprint: str) -> IncidentState | None:
        raw = self._client.get(self._key(fingerprint))
        if not raw:
            return None
        return IncidentState.from_json(raw)

    def put(self, state: IncidentState, *, ttl_seconds: int) -> None:
        ttl = max(1, int(ttl_seconds))
        self._client.set(self._key(state.fingerprint), state.to_json(), ex=ttl)

    def try_create(self, state: IncidentState, *, ttl_seconds: int) -> bool:
        ttl = max(1, int(ttl_seconds))
        # SET NX EX — only one worker creates the active window
        ok = self._client.set(self._key(state.fingerprint), state.to_json(), nx=True, ex=ttl)
        return bool(ok)

    def delete(self, fingerprint: str) -> None:
        self._client.delete(self._key(fingerprint))

    def expire_stale(self, now: float) -> None:
        # Key TTL owns expiry
        return None

    def process_event(
        self,
        *,
        fingerprint: str,
        window_seconds: int,
        refire_interval_seconds: int,
        create_state: IncidentState,
        event_severity: str,
        event_last_seen: datetime,
        event_sample_message: str,
        event_trace_id: str | None,
    ) -> tuple[str, IncidentState]:
        """Create / update / suppress for one event under a short per-fp lock."""
        window = max(1, int(window_seconds))
        refire = max(0, int(refire_interval_seconds))
        now = time.time()
        lock_key = self._lock_key(fingerprint)
        token = f"{time.time_ns()}"
        acquired = False
        for _ in range(100):
            if self._client.set(lock_key, token, nx=True, ex=3):
                acquired = True
                break
            time.sleep(0.01)
        try:
            return self._process_locked(
                fingerprint=fingerprint,
                window=window,
                refire=refire,
                now=now,
                create_state=create_state,
                event_severity=event_severity,
                event_last_seen=event_last_seen,
                event_sample_message=event_sample_message,
                event_trace_id=event_trace_id,
            )
        finally:
            if acquired:
                # Only delete if we still own the lock
                try:
                    if self._client.get(lock_key) == token:
                        self._client.delete(lock_key)
                except Exception:  # noqa: BLE001
                    pass

    def _process_locked(
        self,
        *,
        fingerprint: str,
        window: int,
        refire: int,
        now: float,
        create_state: IncidentState,
        event_severity: str,
        event_last_seen: datetime,
        event_sample_message: str,
        event_trace_id: str | None,
    ) -> tuple[str, IncidentState]:
        key = self._key(fingerprint)
        raw = self._client.get(key)
        if not raw:
            create_state.last_emitted_at = now
            create_state.window_seconds = window
            created = self._client.set(key, create_state.to_json(), nx=True, ex=window)
            if created:
                return "new", create_state
            raw = self._client.get(key)
            if not raw:
                # Extremely unlikely; treat as new without NX
                self._client.set(key, create_state.to_json(), ex=window)
                return "new", create_state

        state = IncidentState.from_json(raw)
        state.occurrence_count += 1
        state.last_seen = event_last_seen
        state.sample_message = event_sample_message
        state.window_seconds = window
        if event_trace_id:
            state.trace_id = event_trace_id
        old_r = _SEVERITY_RANK.get(
            state.severity.value if isinstance(state.severity, LogLevel) else str(state.severity),
            0,
        )
        new_r = _SEVERITY_RANK.get(event_severity, 0)
        if new_r > old_r:
            try:
                state.severity = LogLevel(event_severity)
            except ValueError:
                state.severity = LogLevel.normalize(event_severity)

        action = "suppress"
        if (now - state.last_emitted_at) >= refire:
            state.last_emitted_at = now
            action = "update"

        self._client.set(key, state.to_json(), ex=window)
        return action, state

    def count(self) -> int:
        # SCAN is approximate; used only for debug stats
        n = 0
        for _ in self._client.scan_iter(match=f"{self._prefix}:fp:*", count=100):
            n += 1
            if n >= 10_000:
                break
        return n

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


def _redact_url(url: str) -> str:
    if "@" in url:
        return url.split("@", 1)[-1]
    return url


def build_dedup_store(
    backend: str,
    *,
    redis_url: str | None = None,
    key_prefix: str = "alert_dedup",
) -> DedupStore:
    """Build an in-process store.

    Production Quix path uses Quix keyed state (see ``dedup/quix_state.py``),
    not this helper. Redis is **not** used for dedup (UI cache only).
    """
    name = (backend or "memory").strip().lower()
    if name in ("memory", "local", "inprocess", "in-process", "quix", "external"):
        logger.info("Dedup store: in-process memory (Quix uses its own keyed state)")
        return MemoryDedupStore()
    if name == "redis":
        logger.warning(
            "DEDUP_BACKEND=redis is no longer used for pipeline dedup; "
            "using memory. Prefer Quix runtime keyed state. Redis remains for UI cache."
        )
        return MemoryDedupStore()
    raise ValueError(f"Unknown DEDUP_BACKEND {backend!r}; use 'quix' or 'memory'")
