"""Redis-backed dedup (uses fakeredis)."""

from datetime import datetime, timezone

import fakeredis
import pytest

from alert_pipeline.dedup.engine import DedupEngine
from alert_pipeline.dedup.store import RedisDedupStore
from alert_pipeline.schemas import LogEvent, LogLevel


@pytest.fixture
def redis_store():
    server = fakeredis.FakeServer()
    fake = fakeredis.FakeRedis(server=server, decode_responses=True)
    return RedisDedupStore("redis://unused", client=fake)


def _err(msg: str = "redis backend boom") -> LogEvent:
    return LogEvent(
        timestamp=datetime.now(timezone.utc),
        level=LogLevel.ERROR,
        service="payments-api",
        host="pod-1",
        message=msg,
        labels={"env": "test"},
    )


def test_redis_dedup_emits_then_suppresses(redis_store):
    engine = DedupEngine(window_seconds=300, update_interval_seconds=60, store=redis_store)
    first = engine.process(_err())
    second = engine.process(_err())
    assert first is not None and first.is_new is True
    assert second is None


def test_redis_dedup_shared_across_engines(redis_store):
    """Two processors sharing Redis see the same active window."""
    e1 = DedupEngine(window_seconds=300, update_interval_seconds=60, store=redis_store)
    e2 = DedupEngine(window_seconds=300, update_interval_seconds=60, store=redis_store)
    a = e1.process(_err("shared window"))
    b = e2.process(_err("shared window"))
    assert a is not None and a.is_new
    assert b is None  # suppressed by shared state
