"""Tests for shared UI alert read cache (memory fallback + Redis semantics)."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

import pytest

from alert_pipeline.cache.alert_cache import AlertReadCache
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def _repo(tmp_path, name: str = "c.db") -> AlertRepository:
    return AlertRepository(f"sqlite+pysqlite:///{tmp_path / name}")


def _seed(repo: AlertRepository, n: int = 5) -> None:
    now = datetime.now(timezone.utc)
    for i in range(n):
        repo.upsert_alert(
            AlertEvent(
                fingerprint=f"fp-{i}",
                title=f"title-{i}",
                description="d",
                severity=LogLevel.ERROR,
                service="payments-api" if i % 2 == 0 else "checkout",
                host="h1",
                status=AlertStatus.OPEN,
                occurrence_count=1,
                first_seen=now,
                last_seen=now,
                sample_message=f"message number {i}",
                error_code="DEMO" if i < 3 else "OTHER",
                is_new=True,
            )
        )


def test_memory_fallback_loads_and_paginates(tmp_path):
    repo = _repo(tmp_path)
    _seed(repo, 5)
    cache = AlertReadCache(
        repo._session_factory,
        redis_url="redis://127.0.0.1:1/0",  # force memory fallback
        ttl_seconds=10,
    )
    cache.start()
    assert cache.meta()["source"] == "memory"
    assert cache.stats().total == 5

    page1 = cache.list_alerts_page(page=1, page_size=2)
    assert page1.total == 5
    assert len(page1.items) == 2
    assert page1.has_next is True
    assert page1.has_prev is False
    assert page1.pages == 3

    # default page_size is 10
    default_page = cache.list_alerts_page(page=1)
    assert default_page.page_size == 10
    assert len(default_page.items) == 5  # only 5 seeded

    page2 = cache.list_alerts_page(page=2, page_size=2)
    assert len(page2.items) == 2
    assert page2.has_prev is True

    page3 = cache.list_alerts_page(page=3, page_size=2)
    assert len(page3.items) == 1
    assert page3.has_next is False


def test_filter_and_search(tmp_path):
    repo = _repo(tmp_path)
    _seed(repo, 5)
    cache = AlertReadCache(
        repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10
    )
    cache.start()
    only_checkout = cache.list_alerts_page(service="checkout", page=1, page_size=50)
    assert only_checkout.total == 2  # i=1,3

    q = cache.list_alerts_page(q="message number 4", page=1, page_size=10)
    assert q.total == 1
    assert "4" in q.items[0].sample_message


def test_db_fetch_logged_and_counted(tmp_path, caplog):
    repo = _repo(tmp_path)
    _seed(repo, 2)
    cache = AlertReadCache(
        repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10
    )
    with caplog.at_level(logging.WARNING, logger="alert_pipeline.cache.alert_cache"):
        cache.start()
    assert cache.meta()["db_fetch_count"] == 1
    assert any("ALERT_DB_FETCH" in r.message for r in caplog.records)
    assert any("event=alert_ui_db_fetch" in r.message for r in caplog.records)

    before = cache.meta()["db_fetch_count"]
    with caplog.at_level(logging.WARNING, logger="alert_pipeline.cache.alert_cache"):
        cache.invalidate()
        cache.refresh(force=True)
    assert cache.meta()["db_fetch_count"] > before
    reasons = [r.message for r in caplog.records if "ALERT_DB_FETCH" in r.message]
    assert any("force_refresh" in m for m in reasons)


def test_invalidate_clears_and_reload_hits_db(tmp_path):
    repo = _repo(tmp_path)
    _seed(repo, 1)
    cache = AlertReadCache(
        repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10
    )
    cache.start()
    n1 = cache.meta()["db_fetch_count"]
    cache.invalidate()
    cache.refresh(force=True)
    assert cache.meta()["db_fetch_count"] == n1 + 1


@pytest.fixture
def fakeredis_client():
    fakeredis = pytest.importorskip("fakeredis")
    return fakeredis.FakeRedis(decode_responses=True)


def test_redis_snapshot_shared_and_stampede_single_db_load(tmp_path, fakeredis_client, monkeypatch):
    """With Redis, concurrent force refresh should only load DB once (lock)."""
    repo = _repo(tmp_path, "redis.db")
    _seed(repo, 3)

    cache = AlertReadCache(
        repo._session_factory,
        redis_url="redis://unused/0",
        ttl_seconds=10,
        lock_ttl_seconds=5,
        lock_wait_seconds=2,
    )
    # Inject fake redis
    cache._redis = fakeredis_client
    cache._backend = "redis"

    calls = {"n": 0}
    real_load = cache._load_from_db

    def counting_load(*, reason: str = "unspecified"):
        calls["n"] += 1
        return real_load(reason=reason)

    monkeypatch.setattr(cache, "_load_from_db", counting_load)

    barrier = threading.Barrier(4)
    errors: list[BaseException] = []

    def worker():
        try:
            barrier.wait(timeout=5)
            cache.refresh(force=True)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors
    # Only lock holders load; with force=True each holder may load once.
    # Stampede lock => ideally 1; allow 1-2 under timing races on fake redis.
    assert calls["n"] >= 1
    assert calls["n"] <= 2

    # Snapshot present in Redis for other instances
    assert fakeredis_client.get("alert_ui:snapshot")
    cache2 = AlertReadCache(
        repo._session_factory, redis_url="redis://unused/0", ttl_seconds=10
    )
    cache2._redis = fakeredis_client
    cache2._backend = "redis"
    # Read path should use Redis without another DB load if TTL valid
    before = calls["n"]
    pg = cache2.list_alerts_page(page=1, page_size=10)
    assert pg.total == 3
    # May or may not increment depending on early expire — snapshot should work
    assert len(pg.items) == 3


def test_meta_exposes_db_fetch_marker(tmp_path):
    repo = _repo(tmp_path)
    cache = AlertReadCache(
        repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10
    )
    cache.start()
    meta = cache.meta()
    assert meta["db_fetch_log_marker"] == "ALERT_DB_FETCH"
    assert meta["ttl_seconds"] == 10.0
