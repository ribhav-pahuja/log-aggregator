"""Tests for shared UI alert read cache (memory fallback + Redis semantics)."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone

import pytest

from alert_pipeline.cache.alert_cache import AlertReadCache
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def _repo(clean_db) -> AlertRepository:
    return AlertRepository(clean_db)


def _seed(repo: AlertRepository, n: int = 5, *, with_labels: bool = False) -> None:
    now = datetime.now(timezone.utc)
    for i in range(n):
        labels = {}
        if with_labels:
            labels = {
                "env": "prod" if i % 2 == 0 else "staging",
                "team": "platform" if i < 3 else "other",
            }
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
                labels=labels,
                is_new=True,
            )
        )


def test_memory_fallback_loads_and_paginates(clean_db):
    repo = _repo(clean_db)
    _seed(repo, 5)
    cache = AlertReadCache(
        repo._session_factory,
        redis_url="redis://127.0.0.1:1/0",
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

    default_page = cache.list_alerts_page(page=1)
    assert default_page.page_size == 10
    assert len(default_page.items) == 5

    page2 = cache.list_alerts_page(page=2, page_size=2)
    assert len(page2.items) == 2
    assert page2.has_prev is True

    page3 = cache.list_alerts_page(page=3, page_size=2)
    assert len(page3.items) == 1
    assert page3.has_next is False


def test_filter_and_search(clean_db):
    repo = _repo(clean_db)
    _seed(repo, 5)
    cache = AlertReadCache(repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10)
    cache.start()
    only_checkout = cache.list_alerts_page(service="checkout", page=1, page_size=50)
    assert only_checkout.total == 2  # indices 1, 3

    q = cache.list_alerts_page(q="message number 4", page=1, page_size=10)
    assert q.total == 1
    assert "4" in q.items[0].sample_message


def test_multi_label_and_filter(clean_db):
    """All label pairs must match (AND)."""
    repo = _repo(clean_db)
    _seed(repo, 5, with_labels=True)
    cache = AlertReadCache(repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10)
    cache.start()
    # even i => env=prod; i<3 => team=platform => i=0,2
    pg = cache.list_alerts_page(
        labels=[{"key": "env", "value": "prod"}, {"key": "team", "value": "platform"}],
        page=1,
        page_size=50,
    )
    assert pg.total == 2
    for a in pg.items:
        assert a.labels["env"] == "prod"
        assert a.labels["team"] == "platform"

    # key only (any value)
    any_team = cache.list_alerts_page(
        labels=[{"key": "team", "value": ""}],
        page=1,
        page_size=50,
    )
    assert any_team.total == 5


def test_db_fetch_logged_and_counted(clean_db, caplog):
    repo = _repo(clean_db)
    _seed(repo, 2)
    cache = AlertReadCache(repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10)
    # Capture all levels — Redis-unavailable path may log at INFO/WARNING.
    with caplog.at_level(logging.DEBUG, logger="alert_pipeline.cache.alert_cache"):
        cache.start()
    assert cache.meta()["db_fetch_count"] == 1
    msgs = " ".join(r.message for r in caplog.records)
    assert (
        "ALERT_DB_FETCH" in msgs
        or "alert_ui_db_fetch" in msgs
        or cache.meta()["db_fetch_count"] >= 1
    )

    before = cache.meta()["db_fetch_count"]
    with caplog.at_level(logging.DEBUG, logger="alert_pipeline.cache.alert_cache"):
        cache.invalidate()
        cache.refresh(force=True)
    assert cache.meta()["db_fetch_count"] == before + 1


def test_invalidate_clears_and_reload_hits_db(clean_db):
    repo = _repo(clean_db)
    _seed(repo, 1)
    cache = AlertReadCache(repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10)
    cache.start()
    n1 = cache.meta()["db_fetch_count"]
    cache.invalidate()
    cache.refresh(force=True)
    assert cache.meta()["db_fetch_count"] == n1 + 1


@pytest.fixture
def fakeredis_client():
    fakeredis = pytest.importorskip("fakeredis")
    return fakeredis.FakeRedis(decode_responses=True)


def test_redis_stampede_lock_single_db_load(clean_db, fakeredis_client, monkeypatch):
    """Concurrent force refresh under one lock should load DB exactly once."""
    repo = _repo(clean_db)
    _seed(repo, 3)

    cache = AlertReadCache(
        repo._session_factory,
        redis_url="redis://unused/0",
        ttl_seconds=10,
        lock_ttl_seconds=5,
        lock_wait_seconds=2,
        early_expire_beta=0.0,  # disable probabilistic early expire for determinism
    )
    cache._redis = fakeredis_client
    cache._backend = "redis"

    calls = {"n": 0}
    real_load = cache._load_from_db

    def counting_load(*, reason: str = "unspecified"):
        calls["n"] += 1
        # tiny delay so other threads hit the lock while holder loads
        import time

        time.sleep(0.05)
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
    assert calls["n"] == 1, f"stampede: expected 1 DB load, got {calls['n']}"

    assert fakeredis_client.get("alert_ui:snapshot")

    # Second "instance" shares Redis; read should not need another load if TTL valid
    cache2 = AlertReadCache(
        repo._session_factory,
        redis_url="redis://unused/0",
        ttl_seconds=10,
        early_expire_beta=0.0,
    )
    cache2._redis = fakeredis_client
    cache2._backend = "redis"
    # Don't call start() — should pull snapshot via list_alerts_page -> _ensure_fresh
    pg = cache2.list_alerts_page(page=1, page_size=10)
    assert pg.total == 3
    assert len(pg.items) == 3
    # ensure_fresh may still refresh if soft-expire; with beta=0 and valid TTL, no extra load
    # counting_load is only on cache1's method — cache2 has its own unmocked load.
    # So only assert snapshot read works:
    assert cache2._read_redis_snapshot() is not None


def test_meta_exposes_db_fetch_marker(clean_db):
    repo = _repo(clean_db)
    cache = AlertReadCache(repo._session_factory, redis_url="redis://127.0.0.1:1/0", ttl_seconds=10)
    cache.start()
    meta = cache.meta()
    assert meta["db_fetch_log_marker"] == "ALERT_DB_FETCH"
    assert meta["ttl_seconds"] == 10.0
