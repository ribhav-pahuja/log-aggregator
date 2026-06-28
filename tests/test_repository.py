from datetime import datetime, timezone

from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def test_upsert_creates_and_updates(tmp_path):
    db = tmp_path / "alerts.db"
    repo = AlertRepository(f"sqlite+pysqlite:///{db}")
    now = datetime.now(timezone.utc)
    alert = AlertEvent(
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
    r1 = repo.upsert_alert(alert)
    assert r1.occurrence_count == 1

    alert.occurrence_count = 5
    alert.is_new = False
    alert.status = AlertStatus.UPDATED
    r2 = repo.upsert_alert(alert)
    assert r2.id == r1.id
    assert r2.occurrence_count == 5
