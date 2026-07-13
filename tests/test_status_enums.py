"""Pure unit tests for alert/outbox status enums (no Postgres)."""

from alert_pipeline.schemas import (
    ACTIVE_ALERT_STATUS_SQL,
    ACTIVE_ALERT_STATUS_VALUES,
    ACTIVE_ALERT_STATUSES,
    OPERATOR_ALERT_STATUS_VALUES,
    OUTBOX_CLAIMABLE_STATUS_VALUES,
    OUTBOX_OPEN_STATUS_VALUES,
    OUTBOX_REDRIVE_STATUS_VALUES,
    AlertStatus,
    OutboxStatus,
)


def test_active_sql_matches_active_set():
    for value in ACTIVE_ALERT_STATUS_VALUES:
        assert f"'{value}'" in ACTIVE_ALERT_STATUS_SQL
    assert ACTIVE_ALERT_STATUS_SQL.startswith("status IN (")
    assert set(ACTIVE_ALERT_STATUS_VALUES) == {s.value for s in ACTIVE_ALERT_STATUSES}


def test_operator_excludes_suppressed():
    assert AlertStatus.SUPPRESSED.value not in OPERATOR_ALERT_STATUS_VALUES
    assert AlertStatus.OPEN.value in OPERATOR_ALERT_STATUS_VALUES


def test_outbox_sets():
    assert OutboxStatus.PENDING.value in OUTBOX_CLAIMABLE_STATUS_VALUES
    assert OutboxStatus.FAILED.value in OUTBOX_CLAIMABLE_STATUS_VALUES
    assert OutboxStatus.PROCESSING.value not in OUTBOX_CLAIMABLE_STATUS_VALUES
    assert OutboxStatus.PROCESSING.value in OUTBOX_OPEN_STATUS_VALUES
    assert OutboxStatus.DEAD.value in OUTBOX_REDRIVE_STATUS_VALUES
    assert OutboxStatus.SENT.value not in OUTBOX_OPEN_STATUS_VALUES


def test_parse_roundtrip():
    assert AlertStatus.parse("acknowledged") is AlertStatus.ACKNOWLEDGED
    assert AlertStatus.parse(AlertStatus.RESOLVED) is AlertStatus.RESOLVED
    assert OutboxStatus.parse("dead") is OutboxStatus.DEAD


def test_str_enum_equals_value():
    assert AlertStatus.OPEN == "open"
    assert OutboxStatus.PENDING == "pending"
