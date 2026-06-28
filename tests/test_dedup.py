from datetime import datetime, timezone

from alert_pipeline.dedup.engine import DedupEngine
from alert_pipeline.dedup.fingerprint import compute_fingerprint
from alert_pipeline.schemas import LogEvent, LogLevel


def _err(msg: str = "connection refused while calling postgres", **kwargs) -> LogEvent:
    base = dict(
        timestamp=datetime.now(timezone.utc),
        level=LogLevel.ERROR,
        service="payments-api",
        host="pod-1",
        message=msg,
        labels={"env": "local", "team": "platform"},
    )
    base.update(kwargs)
    return LogEvent(**base)


def test_fingerprint_stable_across_hosts_same_message_and_labels():
    a = _err("timeout after retry", host="pod-1")
    b = _err("timeout after retry", host="pod-9")
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_fingerprint_differs_by_message():
    a = _err("connection refused while calling postgres")
    b = _err("payment gateway timeout after 30s")
    assert compute_fingerprint(a) != compute_fingerprint(b)


def test_fingerprint_differs_by_labels():
    a = _err(labels={"env": "prod", "region": "us"})
    b = _err(labels={"env": "staging", "region": "us"})
    assert compute_fingerprint(a) != compute_fingerprint(b)


def test_fingerprint_same_labels_different_order():
    a = _err(labels={"b": "2", "a": "1"})
    b = _err(labels={"a": "1", "b": "2"})
    assert compute_fingerprint(a) == compute_fingerprint(b)


def test_fingerprint_differs_by_service():
    a = _err(service="payments-api")
    b = _err(service="checkout")
    assert compute_fingerprint(a) != compute_fingerprint(b)


def test_same_error_code_different_message_are_different_groups():
    """Regression: error_code alone must not collapse different messages."""
    a = _err(message="totally different text", error_code="DB_CONN")
    b = _err(message="another different text", error_code="DB_CONN")
    assert compute_fingerprint(a) != compute_fingerprint(b)


def test_dedup_emits_first_then_suppresses():
    engine = DedupEngine(window_seconds=300, update_interval_seconds=60)
    first = engine.process(_err())
    second = engine.process(_err())
    assert first is not None and first.is_new is True
    assert first.occurrence_count == 1
    assert second is None  # within update interval


def test_different_message_opens_new_incident():
    engine = DedupEngine(window_seconds=300, update_interval_seconds=60)
    first = engine.process(_err("message one"))
    second = engine.process(_err("message two"))
    assert first is not None and second is not None
    assert first.is_new and second.is_new
    assert first.fingerprint != second.fingerprint
    assert first.id != second.id


def test_dedup_emits_update_after_interval(monkeypatch):
    engine = DedupEngine(window_seconds=300, update_interval_seconds=1)
    first = engine.process(_err())
    assert first is not None

    fp = first.fingerprint
    engine._state[fp].last_emitted_at = 0

    upd = engine.process(_err())
    assert upd is not None
    assert upd.is_new is False
    assert upd.occurrence_count >= 2
    assert upd.id == first.id


def test_info_logs_ignored():
    engine = DedupEngine(min_level=LogLevel.ERROR)
    assert engine.process(_err(level=LogLevel.INFO, message="all good")) is None
