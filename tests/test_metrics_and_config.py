from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from alert_pipeline.alert_config import load_alert_config
from alert_pipeline.config import Settings
from alert_pipeline.db.models import AlertRecord
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.metrics import apply_status_timestamps
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


def test_set_alert_status_sets_tta(tmp_path):
    repo = AlertRepository(f"sqlite+pysqlite:///{tmp_path}/b.db")
    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    row = repo.upsert_alert(
        AlertEvent(
            fingerprint="fp2",
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
    )
    # repository set_alert_status uses "now" — just assert status flips
    updated = repo.set_alert_status(row.id, "acknowledged")
    assert updated is not None
    assert updated.status == "acknowledged"
    assert updated.tta_seconds is not None
    assert updated.acknowledged_at is not None


def test_yaml_service_override():
    root = Path(__file__).resolve().parents[1]
    cfg = load_alert_config(root / "config" / "alerts.yaml")
    ev = LogEvent(service="payments-api", message="x", level=LogLevel.ERROR)
    s = cfg.resolve_for(ev)
    assert s.dedup_window_seconds == 600
    assert s.refire_interval_seconds == 120

    inv = cfg.resolve_for(LogEvent(service="inventory", message="x", level=LogLevel.ERROR))
    assert inv.min_level == "CRITICAL"


def test_dedup_backend_setting_removed():
    """DEDUP_BACKEND is not a Settings field — dedup path is fixed by runtime."""
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert not hasattr(s, "dedup_backend")


def test_dedup_backend_redis_env_rejected(monkeypatch):
    monkeypatch.setenv("DEDUP_BACKEND", "redis")
    with pytest.raises((ValidationError, ValueError), match="redis was removed"):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_dedup_backend_quix_env_ignored(monkeypatch):
    """Legacy DEDUP_BACKEND=quix must not select a store or crash boot."""
    monkeypatch.setenv("DEDUP_BACKEND", "quix")
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert s.pipeline_runtime in ("quix", "quixstreams")
