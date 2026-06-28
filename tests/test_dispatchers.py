from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from alert_pipeline.dispatchers.teams import TeamsDispatcher
from alert_pipeline.dispatchers.webhook import WebhookDispatcher
from alert_pipeline.dispatchers.zenduty import ZendutyDispatcher
from alert_pipeline.schemas import AlertEvent, AlertStatus, LogLevel


def _alert() -> AlertEvent:
    now = datetime.now(timezone.utc)
    return AlertEvent(
        fingerprint="fp1",
        title="[svc] failure",
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


def test_zenduty_payload_and_success():
    d = ZendutyDispatcher(integration_key="key123")
    mock_resp = MagicMock(status_code=200, text="ok")
    with patch.object(d, "_post", return_value=mock_resp):
        result = d.send(_alert())
    assert result.success is True
    assert result.channel == "zenduty"


def test_teams_success():
    d = TeamsDispatcher(webhook_url="https://example.com/webhook")
    mock_resp = MagicMock(status_code=200, text="1")
    with patch.object(d, "_post", return_value=mock_resp):
        result = d.send(_alert())
    assert result.success is True


def test_webhook_success():
    d = WebhookDispatcher(url="https://example.com/hooks/alerts")
    mock_resp = MagicMock(status_code=202, text="accepted")
    with patch.object(d, "_post", return_value=mock_resp):
        result = d.send(_alert())
    assert result.success is True
