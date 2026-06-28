from datetime import datetime, timedelta, timezone
from pathlib import Path

from alert_pipeline.alert_config import load_alert_config
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogEvent, LogLevel


def test_tta_ttr_persisted(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/a.db")
    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    alert = AlertEvent(
        fingerprint="fp",
        title="t",
        description="d",
        severity=LogLevel.ERROR,
        service="svc",
        host="h",
        status=AlertStatus.OPEN,
        occurrence_count=1,
        first_seen=t0,
        last_seen=t0,
        sample_message="m",
        is_new=True,
    )
    row = repo.upsert_alert(alert)
    aid = row.id

    # Simulate ack 90s later via repository
    from alert_pipeline.db.models import AlertRecord
    from alert_pipeline.metrics import apply_status_timestamps
    from sqlalchemy import select

    with repo.session() as session:
        r = session.get(AlertRecord, aid)
        apply_status_timestamps(r, "acknowledged", now=t0 + timedelta(seconds=90))
    with repo.session() as session:
        r = session.get(AlertRecord, aid)
        assert r.status == "acknowledged"
        assert r.tta_seconds == 90
        assert r.acknowledged_at is not None
        apply_status_timestamps(r, "resolved", now=t0 + timedelta(seconds=300))
    with repo.session() as session:
        r = session.get(AlertRecord, aid)
        assert r.status == "resolved"
        assert r.ttr_seconds == 300
        assert r.tta_seconds == 90


def test_yaml_service_override():
    root = Path(__file__).resolve().parents[1]
    cfg = load_alert_config(root / "config" / "alerts.yaml")
    ev = LogEvent(service="payments-api", message="x", level=LogLevel.ERROR)
    s = cfg.resolve_for(ev)
    assert s.dedup_window_seconds == 600
    assert s.refire_interval_seconds == 120

    inv = cfg.resolve_for(LogEvent(service="inventory", message="x", level=LogLevel.ERROR))
    assert inv.min_level == "CRITICAL"
