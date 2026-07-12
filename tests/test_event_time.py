"""Event-time window / refire semantics."""

from datetime import datetime, timedelta, timezone

from alert_pipeline.dedup.engine import DedupEngine
from alert_pipeline.dedup.quix_state import build_enrichment, process_enriched_with_state
from alert_pipeline.dedup.store import MemoryDedupStore
from alert_pipeline.schemas import LogEvent, LogLevel


class _DictState:
    def __init__(self) -> None:
        self._d: dict = {}

    def get(self, key, default=None):
        return self._d.get(key, default)

    def set(self, key, value):
        self._d[key] = value

    def delete(self, key):
        self._d.pop(key, None)


def test_quix_window_uses_event_time_not_wall_clock():
    """Two events 5s apart in event time stay in the same window even if processed later."""
    st = _DictState()
    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    e1 = LogEvent(
        timestamp=t0,
        level=LogLevel.ERROR,
        service="svc",
        message="same",
        labels={"env": "t"},
    )
    e2 = LogEvent(
        timestamp=t0 + timedelta(seconds=5),
        level=LogLevel.ERROR,
        service="svc",
        message="same",
        labels={"env": "t"},
    )
    row1 = build_enrichment(
        e1,
        window_seconds=300,
        refire_interval_seconds=60,
        suppress_dispatch_while_acknowledged=True,
    )
    row2 = build_enrichment(
        e2,
        window_seconds=300,
        refire_interval_seconds=60,
        suppress_dispatch_while_acknowledged=True,
    )
    # Inject processing "now" far in the future — window must still use event timestamps
    first = process_enriched_with_state(row1, st, now=t0.timestamp())
    second = process_enriched_with_state(row2, st, now=t0.timestamp() + 5)
    assert first is not None and first["alert"]["is_new"] is True
    assert second is None  # within refire of 60s event-time


def test_memory_engine_event_time_refire():
    store = MemoryDedupStore()
    engine = DedupEngine(
        store=store,
        window_seconds=300,
        update_interval_seconds=60,
        min_level=LogLevel.ERROR,
    )
    t0 = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    base = dict(
        level=LogLevel.ERROR,
        service="svc",
        message="m",
        labels={"env": "x"},
    )
    a1 = engine.process(LogEvent(timestamp=t0, **base))
    a2 = engine.process(LogEvent(timestamp=t0 + timedelta(seconds=10), **base))
    a3 = engine.process(LogEvent(timestamp=t0 + timedelta(seconds=70), **base))
    assert a1 is not None and a1.is_new
    assert a2 is None
    assert a3 is not None and not a3.is_new
