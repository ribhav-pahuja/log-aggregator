"""Unit tests for Quix-state dedup transitions (no Quix runtime required)."""

from datetime import datetime, timezone

from alert_pipeline.dedup.quix_state import build_enrichment, process_enriched_with_state
from alert_pipeline.schemas import LogEvent, LogLevel


class _DictState:
    def __init__(self) -> None:
        self._d: dict = {}

    def get(self, key, default=None):
        return self._d.get(key, default)

    def set(self, key, value):
        self._d[key] = value

    def delete(self, key):
        self._d.pop(key, None)


def _err(msg: str = "boom") -> LogEvent:
    return LogEvent(
        timestamp=datetime.now(timezone.utc),
        level=LogLevel.ERROR,
        service="payments-api",
        host="pod-1",
        message=msg,
        labels={"env": "test"},
    )


def test_quix_state_emits_then_suppresses():
    st = _DictState()
    row = build_enrichment(
        _err(),
        window_seconds=300,
        refire_interval_seconds=60,
        suppress_dispatch_while_acknowledged=True,
    )
    first = process_enriched_with_state(row, st)
    second = process_enriched_with_state(row, st)
    assert first is not None and first["alert"]["is_new"] is True
    assert second is None


def test_quix_state_refire_after_interval():
    st = _DictState()
    row = build_enrichment(
        _err("same"),
        window_seconds=300,
        refire_interval_seconds=1,
        suppress_dispatch_while_acknowledged=True,
    )
    first = process_enriched_with_state(row, st, now=1000.0)
    assert first is not None
    # Force last_emitted_at into the past via state
    inc = st.get("incident")
    inc["last_emitted_at"] = 0
    st.set("incident", inc)
    upd = process_enriched_with_state(row, st, now=2000.0)
    assert upd is not None
    assert upd["alert"]["is_new"] is False
    assert upd["alert"]["occurrence_count"] >= 2
    assert upd["alert"]["id"] == first["alert"]["id"]


def test_window_expiry_opens_new_incident():
    st = _DictState()
    row = build_enrichment(
        _err("expired window"),
        window_seconds=10,
        refire_interval_seconds=60,
        suppress_dispatch_while_acknowledged=True,
    )
    first = process_enriched_with_state(row, st, now=1_700_000_000.0)
    assert first is not None
    # last_seen older than window relative to the next `now`
    inc = st.get("incident")
    from datetime import datetime, timezone

    inc["last_seen"] = datetime.fromtimestamp(
        1_700_000_000.0 - 100, tz=timezone.utc
    ).isoformat()
    inc["window_seconds"] = 10
    st.set("incident", inc)
    again = process_enriched_with_state(row, st, now=1_700_000_100.0)
    assert again is not None and again["alert"]["is_new"] is True
    assert again["alert"]["id"] != first["alert"]["id"]
