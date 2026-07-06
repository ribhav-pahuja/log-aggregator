from datetime import datetime, timezone

from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def _alert(**kwargs) -> AlertEvent:
    now = datetime.now(timezone.utc)
    base = dict(
        fingerprint="abc123",
        title="test",
        description="desc",
        severity=LogLevel.ERROR,
        service="svc",
        host="h1",
        status=AlertStatus.OPEN,
        occurrence_count=1,
        first_seen=now,
        last_seen=now,
        sample_message="boom",
        is_new=True,
    )
    base.update(kwargs)
    return AlertEvent(**base)


def test_upsert_creates_and_updates(tmp_path):
    db = tmp_path / "alerts.db"
    repo = AlertRepository(f"sqlite+pysqlite:///{db}")
    r1 = repo.upsert_alert(_alert())
    assert r1.occurrence_count == 1

    r2 = repo.upsert_alert(
        _alert(occurrence_count=5, is_new=False, status=AlertStatus.UPDATED)
    )
    assert r2.id == r1.id
    assert r2.occurrence_count == 5


def test_upsert_never_resets_count_on_stale_new(tmp_path):
    """Restart / memory loss: engine emits is_new count=1 but DB has higher count."""
    db = tmp_path / "alerts2.db"
    repo = AlertRepository(f"sqlite+pysqlite:///{db}")
    r1 = repo.upsert_alert(_alert(occurrence_count=1))
    assert r1.occurrence_count == 1

    r2 = repo.upsert_alert(
        _alert(occurrence_count=10, is_new=False, status=AlertStatus.UPDATED)
    )
    assert r2.occurrence_count == 10

    # Memory lost: engine thinks new with count=1
    r3 = repo.upsert_alert(_alert(occurrence_count=1, is_new=True))
    assert r3.id == r1.id
    assert r3.occurrence_count == 11  # bumped, not reset to 1


def test_active_fingerprint_unique_index(tmp_path):
    db = tmp_path / "alerts3.db"
    repo = AlertRepository(f"sqlite+pysqlite:///{db}")
    repo.upsert_alert(_alert(fingerprint="same-fp", id="id-1"))

    a2 = _alert(fingerprint="same-fp", id="id-2", is_new=True, occurrence_count=1)
    r = repo.upsert_alert(a2)
    assert r.fingerprint == "same-fp"
    assert r.occurrence_count >= 2
