"""Core processor works without any stream runtime."""

from sqlalchemy import select

from alert_pipeline.config import Settings
from alert_pipeline.db.models import AlertRecord
from alert_pipeline.processing.handler import AlertProcessor
from alert_pipeline.runtime.factory import get_runtime


def test_processor_emits_then_dedups(tmp_path):
    settings = Settings(
        database_url=f"sqlite+pysqlite:///{tmp_path}/p.db",
        dispatch_enabled=False,
        alert_config_path="config/alerts.yaml",
    )
    proc = AlertProcessor(settings, reload_yaml=True)
    payload = {
        "level": "ERROR",
        "service": "svc",
        "message": "portable core test",
        "labels": {"env": "test"},
    }
    r1 = proc.handle_payload(payload)
    assert r1.emitted is True and r1.is_new is True
    r2 = proc.handle_payload(payload)
    # Within refire window: no new alert event (in-memory dedup suppress)
    assert r2.emitted is False
    assert r2.skipped_reason == "dedup_suppressed"

    # Important behavioral note: suppressed events do NOT upsert DB again
    with proc.repo.session() as session:
        row = session.scalar(select(AlertRecord).where(AlertRecord.id == r1.alert_id))
        assert row is not None
        assert row.occurrence_count == 1

    r3 = proc.handle_payload({**payload, "message": "different text"})
    assert r3.emitted is True and r3.is_new is True
    assert r3.fingerprint != r1.fingerprint


def test_factory_resolves_names():
    assert get_runtime("quix").name == "quix"
    assert get_runtime("flink").name == "flink"


def test_factory_unknown_raises():
    import pytest

    with pytest.raises(ValueError, match="Unknown pipeline runtime"):
        get_runtime("spark")
