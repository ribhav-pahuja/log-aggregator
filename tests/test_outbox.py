"""Outbox enqueue + worker drain with idempotency."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import MagicMock

from sqlalchemy import select

from alert_pipeline.config import Settings
from alert_pipeline.db.models import DispatchLog, DispatchOutbox
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.dispatchers.base import DispatchResult
from alert_pipeline.dispatchers.outbox_worker import process_batch
from alert_pipeline.dispatchers.registry import DispatchFanout
from alert_pipeline.processing.handler import AlertProcessor
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def _alert(**kwargs) -> AlertEvent:
    now = datetime.now(timezone.utc)
    base = dict(
        fingerprint="fp-outbox",
        title="t",
        description="d",
        severity=LogLevel.ERROR,
        service="svc",
        host="h",
        status=AlertStatus.OPEN,
        occurrence_count=1,
        first_seen=now,
        last_seen=now,
        sample_message="boom",
        is_new=True,
    )
    base.update(kwargs)
    return AlertEvent(**base)


def test_enqueue_idempotent(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/o.db")
    a = _alert()
    keys1 = repo.enqueue_dispatch(a, ["webhook", "teams"])
    keys2 = repo.enqueue_dispatch(a, ["webhook", "teams"])
    assert len(keys1) == 2
    assert keys2 == []
    with repo.session() as session:
        n = len(session.scalars(select(DispatchOutbox)).all())
        assert n == 2


def test_worker_dispatches_and_marks_sent(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/w.db")
    a = _alert(id="aid-1", occurrence_count=3)
    repo.enqueue_dispatch(a, ["webhook"])

    mock_d = MagicMock()
    mock_d.name = "webhook"
    mock_d.send.return_value = DispatchResult(channel="webhook", success=True, status_code=200)
    fanout = DispatchFanout([mock_d], repo=repo)
    settings = Settings(
        database_url=f"sqlite+pysqlite:///{tmp_path}/w.db",
        dispatch_outbox_batch_size=10,
        dispatch_outbox_max_attempts=5,
    )
    n = process_batch(repo, fanout, settings)
    assert n == 1
    mock_d.send.assert_called_once()
    with repo.session() as session:
        row = session.scalar(select(DispatchOutbox))
        assert row.status == "sent"
        log = session.scalar(select(DispatchLog))
        assert log is not None and log.success == 1
        assert log.idempotency_key == "aid-1:webhook:3"


def test_worker_skips_duplicate_successful_audit(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/dup.db")
    a = _alert(id="aid-2", occurrence_count=1)
    key = repo.make_idempotency_key(a.id, "webhook", 1)
    repo.log_dispatch(alert_id=a.id, channel="webhook", success=True, idempotency_key=key)
    repo.enqueue_dispatch(a, ["webhook"])

    mock_d = MagicMock()
    mock_d.name = "webhook"
    mock_d.send.return_value = DispatchResult(channel="webhook", success=True)
    fanout = DispatchFanout([mock_d], repo=repo)
    settings = Settings(
        database_url=f"sqlite+pysqlite:///{tmp_path}/dup.db",
        dispatch_outbox_max_attempts=5,
    )
    process_batch(repo, fanout, settings)
    mock_d.send.assert_not_called()
    with repo.session() as session:
        assert session.scalar(select(DispatchOutbox)).status == "sent"


def test_processor_enqueues_outbox_not_inline(tmp_path):
    settings = Settings(
        database_url=f"sqlite+pysqlite:///{tmp_path}/p.db",
        dispatch_enabled=True,
        dispatch_mode="outbox",
        dispatch_webhook_enabled=True,
        webhook_url="http://example.invalid/hook",
        ui_cache_invalidate_on_write=False,
        alert_config_path="config/alerts.yaml",
    )
    proc = AlertProcessor(settings, reload_yaml=True)
    r = proc.handle_payload(
        {
            "level": "ERROR",
            "service": "svc",
            "message": "outbox path",
            "labels": {"env": "t"},
        }
    )
    assert r.emitted is True
    with proc.repo.session() as session:
        rows = list(session.scalars(select(DispatchOutbox)).all())
        assert len(rows) == 1
        assert rows[0].channel == "webhook"
        assert rows[0].status == "pending"


def test_reopen_disallowed_skips_emit(tmp_path):
    settings = Settings(
        database_url=f"sqlite+pysqlite:///{tmp_path}/r.db",
        dispatch_enabled=False,
        ui_cache_invalidate_on_write=False,
        alert_config_path="config/alerts.yaml",
    )
    proc = AlertProcessor(settings, reload_yaml=True)
    a = _alert(fingerprint="fp-reopen", is_new=True)
    proc.repo.upsert_alert(a)
    proc.repo.set_alert_status(a.id, "resolved")

    a2 = _alert(fingerprint="fp-reopen", is_new=True, id="new-id")
    result = proc.emit_alert(a2, allow_reopen_after_resolve=False)
    assert result.emitted is False
    assert result.skipped_reason == "reopen_disallowed"


def test_claim_outbox_batch_is_exclusive(tmp_path):
    """Two sequential claims never return the same row."""
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/claim.db")
    for i in range(5):
        repo.enqueue_dispatch(_alert(id=f"aid-{i}", occurrence_count=i + 1), ["webhook"])

    first = repo.claim_outbox_batch(batch_size=3)
    second = repo.claim_outbox_batch(batch_size=3)

    first_ids = {r.id for r in first}
    second_ids = {r.id for r in second}
    assert len(first) == 3
    assert len(second) == 2
    assert first_ids.isdisjoint(second_ids)
    assert all(r.status == "processing" for r in first + second)
    assert all(r.attempts == 1 for r in first + second)


def test_concurrent_claim_outbox_no_double_claim(tmp_path):
    """Parallel workers must not claim the same outbox row (CAS)."""
    db_url = f"sqlite+pysqlite:///{tmp_path}/concurrent.db"
    repo = AlertRepository(db_url)
    n_rows = 20
    for i in range(n_rows):
        repo.enqueue_dispatch(
            _alert(id=f"c-{i}", occurrence_count=1, fingerprint=f"fp-{i}"),
            ["webhook"],
        )

    # Separate repository instances (separate sessions) mimic multi-worker
    workers = [AlertRepository(db_url) for _ in range(4)]

    def claim_all(r: AlertRepository) -> list[int]:
        claimed_ids: list[int] = []
        while True:
            batch = r.claim_outbox_batch(batch_size=3)
            if not batch:
                break
            claimed_ids.extend(row.id for row in batch)
        return claimed_ids

    with ThreadPoolExecutor(max_workers=len(workers)) as pool:
        results = list(pool.map(claim_all, workers))

    all_ids = [oid for part in results for oid in part]
    assert len(all_ids) == n_rows
    assert len(set(all_ids)) == n_rows  # no duplicates

    with repo.session() as session:
        rows = list(session.scalars(select(DispatchOutbox)).all())
        assert len(rows) == n_rows
        assert all(r.status == "processing" for r in rows)
        assert all(r.attempts == 1 for r in rows)


def test_claim_increments_attempts_on_retry(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/retry.db")
    repo.enqueue_dispatch(_alert(id="retry-1"), ["webhook"])
    claimed = repo.claim_outbox_batch(batch_size=1)
    assert len(claimed) == 1 and claimed[0].attempts == 1

    # Simulate failed attempt → back to failed/pending path
    repo.mark_outbox_result(
        claimed[0].id,
        success=False,
        error="boom",
        max_attempts=5,
        backoff_base_seconds=0.0,
    )
    # next_attempt_at may be in the future with backoff 0 still near-now
    with repo.session() as session:
        row = session.get(DispatchOutbox, claimed[0].id)
        assert row is not None
        row.next_attempt_at = datetime.now(timezone.utc)
        row.status = "failed"

    claimed2 = repo.claim_outbox_batch(batch_size=1)
    assert len(claimed2) == 1
    assert claimed2[0].id == claimed[0].id
    assert claimed2[0].attempts == 2
