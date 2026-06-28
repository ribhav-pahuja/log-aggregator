from datetime import datetime, timezone
from pathlib import Path

from alert_pipeline.alert_config import load_alert_config
from alert_pipeline.dedup.fingerprint import compute_fingerprint
from alert_pipeline.schemas import LogEvent, LogLevel


def _ev(**kwargs) -> LogEvent:
    base = dict(
        timestamp=datetime.now(timezone.utc),
        level=LogLevel.ERROR,
        service="payments-api",
        host="pod-1",
        message="boom",
        labels={"env": "prod", "team": "a"},
        error_code="X",
    )
    base.update(kwargs)
    return LogEvent(**base)


def test_message_only_ignores_labels():
    fields = ["message"]
    a = _ev(labels={"env": "prod"})
    b = _ev(labels={"env": "staging"})
    assert compute_fingerprint(a, fields) == compute_fingerprint(b, fields)


def test_labels_and_message_differ_on_label_change():
    fields = ["labels", "message"]
    a = _ev(labels={"env": "prod"})
    b = _ev(labels={"env": "staging"})
    assert compute_fingerprint(a, fields) != compute_fingerprint(b, fields)


def test_single_label_key():
    fields = ["label:env", "message"]
    a = _ev(labels={"env": "prod", "team": "a"})
    b = _ev(labels={"env": "prod", "team": "b"})
    # team ignored
    assert compute_fingerprint(a, fields) == compute_fingerprint(b, fields)
    c = _ev(labels={"env": "staging", "team": "a"})
    assert compute_fingerprint(a, fields) != compute_fingerprint(c, fields)


def test_yaml_loads_dedup_fields():
    root = Path(__file__).resolve().parents[1]
    cfg = load_alert_config(root / "config" / "alerts.yaml")
    assert "message" in cfg.defaults.dedup_fields
    assert "labels" in cfg.defaults.dedup_fields
