"""Core processor works without any stream runtime."""

from alert_pipeline.config import Settings
from alert_pipeline.processing.handler import AlertProcessor


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
    assert r2.emitted is False
    r3 = proc.handle_payload({**payload, "message": "different text"})
    assert r3.emitted is True and r3.is_new is True
    assert r3.fingerprint != r1.fingerprint


def test_factory_resolves_names():
    from alert_pipeline.runtime.factory import get_runtime

    assert get_runtime("quix").name == "quix"
    assert get_runtime("flink").name == "flink"
