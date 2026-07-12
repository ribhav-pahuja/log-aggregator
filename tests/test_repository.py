"""AlertRepository persistence tests (PostgreSQL)."""

from datetime import datetime, timezone

from sqlalchemy import select

from alert_pipeline.db.models import AlertRecord
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def _alert(**kwargs) -> AlertEvent:
    now = datetime.now(timezone.utc)
    base = dict(
        fingerprint="fp-repo",
        title="t",
        description="d",
        severity=LogLevel.ERROR,
        service="svc",
        host="h",
        status=AlertStatus.OPEN,
        occurrence_count=1,
        first_seen=now,
        last_seen=now,
        sample_message="m",
        is_new=True,
    )
    base.update(kwargs)
    return AlertEvent(**base)


def test_upsert_creates_and_updates(repo: AlertRepository):
    r1 = repo.upsert_alert(_alert())
    assert r1.occurrence_count == 1
    r2 = repo.upsert_alert(_alert(occurrence_count=5, is_new=False, status=AlertStatus.UPDATED))
    assert r2.id == r1.id
    assert r2.occurrence_count == 5


def test_upsert_never_resets_count_on_stale_new(repo: AlertRepository):
    r1 = repo.upsert_alert(_alert(occurrence_count=1))
    r2 = repo.upsert_alert(_alert(occurrence_count=10, is_new=False, status=AlertStatus.UPDATED))
    assert r2.occurrence_count == 10
    r3 = repo.upsert_alert(_alert(occurrence_count=1, is_new=True))
    assert r3.id == r1.id
    assert r3.occurrence_count == 11


def test_active_fingerprint_unique_index(repo: AlertRepository):
    repo.upsert_alert(_alert(fingerprint="same-fp", id="id-1"))
    a2 = _alert(fingerprint="same-fp", id="id-2", is_new=True)
    r = repo.upsert_alert(a2)
    assert r.fingerprint == "same-fp"
    with repo.session() as session:
        rows = list(
            session.scalars(select(AlertRecord).where(AlertRecord.fingerprint == "same-fp")).all()
        )
        assert len(rows) == 1
