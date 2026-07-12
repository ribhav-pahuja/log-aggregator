"""Parity: memory DedupEngine and Quix-state adapter must agree on emit decisions.

Both paths call ``apply_dedup_transition``; this suite locks the adapters to
the same observable outcomes for shared event sequences.

Quix co-locates state per fingerprint via ``group_by``; the test harness
mirrors that with a per-fingerprint state bag.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from alert_pipeline.alert_config import RefireSettings
from alert_pipeline.dedup.engine import DedupEngine
from alert_pipeline.dedup.quix_state import build_enrichment, process_enriched_with_state
from alert_pipeline.dedup.store import MemoryDedupStore
from alert_pipeline.schemas import LogEvent, LogLevel

# Explicit fields so memory YAML defaults cannot diverge from Quix enrichment.
_DEDUP_FIELDS = ["service", "level", "labels", "message"]


class _PerFingerprintState:
    """Mimic Quix keyed state: one isolated bag per fingerprint."""

    def __init__(self) -> None:
        self._bags: dict[str, dict] = {}
        self._fp: str | None = None

    def bind(self, fingerprint: str) -> None:
        self._fp = fingerprint
        self._bags.setdefault(fingerprint, {})

    def get(self, key, default=None):
        assert self._fp is not None
        return self._bags[self._fp].get(key, default)

    def set(self, key, value):
        assert self._fp is not None
        self._bags[self._fp][key] = value

    def delete(self, key):
        assert self._fp is not None
        self._bags[self._fp].pop(key, None)


def _ev(
    ts: datetime,
    msg: str = "same boom",
    *,
    level: LogLevel = LogLevel.ERROR,
    service: str = "svc",
    labels: dict | None = None,
    trace_id: str | None = None,
) -> LogEvent:
    return LogEvent(
        timestamp=ts,
        level=level,
        service=service,
        host="pod-1",
        message=msg,
        labels=labels or {"env": "t"},
        trace_id=trace_id,
    )


def _run_memory(
    events: list[LogEvent],
    *,
    window: int = 300,
    refire: int = 60,
) -> list[dict | None]:
    def resolve(ev: LogEvent) -> RefireSettings:
        return RefireSettings(
            min_level="ERROR",
            dedup_window_seconds=window,
            refire_interval_seconds=refire,
            dedup_fields=list(_DEDUP_FIELDS),
        )

    engine = DedupEngine(
        store=MemoryDedupStore(),
        resolve_settings=resolve,
        window_seconds=window,
        update_interval_seconds=refire,
        min_level=LogLevel.ERROR,
    )
    out: list[dict | None] = []
    for ev in events:
        alert = engine.process(ev)
        if alert is None:
            out.append(None)
        else:
            out.append(
                {
                    "is_new": alert.is_new,
                    "occurrence_count": alert.occurrence_count,
                    "id": alert.id,
                    "fingerprint": alert.fingerprint,
                    "severity": alert.severity.value,
                    "trace_id": alert.trace_id,
                }
            )
    return out


def _run_quix(
    events: list[LogEvent],
    *,
    window: int = 300,
    refire: int = 60,
) -> list[dict | None]:
    st = _PerFingerprintState()
    out: list[dict | None] = []
    for ev in events:
        row = build_enrichment(
            ev,
            window_seconds=window,
            refire_interval_seconds=refire,
            suppress_dispatch_while_acknowledged=True,
            dedup_fields=list(_DEDUP_FIELDS),
        )
        st.bind(row["fingerprint"])
        # Inject event timestamp so both paths share event-time
        result = process_enriched_with_state(row, st, now=ev.timestamp.timestamp())
        if result is None:
            out.append(None)
        else:
            a = result["alert"]
            out.append(
                {
                    "is_new": a["is_new"],
                    "occurrence_count": a["occurrence_count"],
                    "id": a["id"],
                    "fingerprint": a["fingerprint"],
                    "severity": a["severity"],
                    "trace_id": a.get("trace_id"),
                }
            )
    return out


def _assert_parity(mem: list[dict | None], quix: list[dict | None]) -> None:
    assert len(mem) == len(quix)
    for i, (m, q) in enumerate(zip(mem, quix, strict=True)):
        if m is None or q is None:
            assert m is None and q is None, f"step {i}: mem={m} quix={q}"
            continue
        # alert ids are random per path — compare decision shape only
        assert m["is_new"] == q["is_new"], f"step {i} is_new mem={m} quix={q}"
        assert m["occurrence_count"] == q["occurrence_count"], f"step {i} count"
        assert m["fingerprint"] == q["fingerprint"], f"step {i} fingerprint"
        assert m["severity"] == q["severity"], f"step {i} severity"
        assert m["trace_id"] == q["trace_id"], f"step {i} trace_id"


def test_parity_emit_suppress_refire():
    t0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        _ev(t0),
        _ev(t0 + timedelta(seconds=10)),  # suppress
        _ev(t0 + timedelta(seconds=70)),  # refire update
        _ev(t0 + timedelta(seconds=80)),  # suppress again
    ]
    mem = _run_memory(events, window=300, refire=60)
    quix = _run_quix(events, window=300, refire=60)
    assert mem[0] is not None and mem[0]["is_new"] is True
    assert mem[1] is None
    assert mem[2] is not None and mem[2]["is_new"] is False
    assert mem[2]["occurrence_count"] == 3
    assert mem[3] is None
    _assert_parity(mem, quix)
    assert mem[2]["id"] == mem[0]["id"]
    assert quix[2]["id"] == quix[0]["id"]


def test_parity_window_expiry_new_incident():
    t0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        _ev(t0),
        _ev(t0 + timedelta(seconds=5)),  # suppress
        _ev(t0 + timedelta(seconds=400)),  # window 300 expired → new
    ]
    mem = _run_memory(events, window=300, refire=60)
    quix = _run_quix(events, window=300, refire=60)
    assert mem[0] is not None and mem[0]["is_new"]
    assert mem[1] is None
    assert mem[2] is not None and mem[2]["is_new"]
    assert mem[2]["id"] != mem[0]["id"]
    _assert_parity(mem, quix)
    assert quix[2]["id"] != quix[0]["id"]


def test_parity_severity_escalation_same_fingerprint():
    """When level is not in dedup_fields, severity can escalate on the same incident."""
    t0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    fields = ["service", "labels", "message"]  # no level

    def resolve(ev: LogEvent) -> RefireSettings:
        return RefireSettings(
            min_level="ERROR",
            dedup_window_seconds=300,
            refire_interval_seconds=60,
            dedup_fields=list(fields),
        )

    engine = DedupEngine(
        store=MemoryDedupStore(),
        resolve_settings=resolve,
        min_level=LogLevel.ERROR,
    )
    st = _PerFingerprintState()
    e1 = _ev(t0, level=LogLevel.ERROR)
    e2 = _ev(t0 + timedelta(seconds=70), level=LogLevel.CRITICAL)

    m1 = engine.process(e1)
    m2 = engine.process(e2)
    assert m1 is not None and m1.is_new
    assert m2 is not None and not m2.is_new
    assert m2.severity == LogLevel.CRITICAL
    assert m2.id == m1.id

    for ev in (e1, e2):
        row = build_enrichment(
            ev,
            window_seconds=300,
            refire_interval_seconds=60,
            suppress_dispatch_while_acknowledged=True,
            dedup_fields=list(fields),
        )
        st.bind(row["fingerprint"])
        r = process_enriched_with_state(row, st, now=ev.timestamp.timestamp())
        if ev is e1:
            assert r is not None and r["alert"]["is_new"] is True
            qid = r["alert"]["id"]
        else:
            assert r is not None and r["alert"]["is_new"] is False
            assert r["alert"]["severity"] == "CRITICAL"
            assert r["alert"]["id"] == qid


def test_parity_trace_id_update_on_suppress():
    t0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        _ev(t0, trace_id="t1"),
        _ev(t0 + timedelta(seconds=5), trace_id="t2"),  # suppress but keep latest trace
        _ev(t0 + timedelta(seconds=70), trace_id="t3"),  # update emits with t3
    ]
    mem = _run_memory(events, window=300, refire=60)
    quix = _run_quix(events, window=300, refire=60)
    assert mem[2] is not None and mem[2]["trace_id"] == "t3"
    _assert_parity(mem, quix)


def test_parity_different_message_different_fingerprint():
    t0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        _ev(t0, "message one"),
        _ev(t0 + timedelta(seconds=1), "message two"),
    ]
    mem = _run_memory(events)
    quix = _run_quix(events)
    assert mem[0] is not None and mem[1] is not None
    assert mem[0]["fingerprint"] != mem[1]["fingerprint"]
    assert mem[0]["is_new"] and mem[1]["is_new"]
    _assert_parity(mem, quix)
