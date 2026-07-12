"""In-process dedup state for unit tests and non-Quix paths.

Production uses Quix keyed state (``dedup/quix_state.py``). Redis is only
used for the operator UI read cache — never for dedup.
"""

from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from alert_pipeline.schemas import LogLevel

logger = logging.getLogger(__name__)


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


class DedupStore(ABC):
    """Shared interface for active-incident window state (in-process only)."""

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
        """Drop entries whose last_seen is older than window_seconds."""
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


def build_memory_dedup_store() -> MemoryDedupStore:
    """In-process store for unit tests / ``DedupEngine`` only.

    Production never calls this — Quix keyed state owns live windows.
    """
    return MemoryDedupStore()
