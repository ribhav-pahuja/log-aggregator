from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import httpx

from alert_pipeline.dispatchers.http import (
    close_dispatch_http_client,
    create_dispatch_http_client,
    get_dispatch_http_client,
)
from alert_pipeline.dispatchers.registry import build_dispatchers, enabled_channel_names
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


def test_shared_http_client_reused_across_posts():
    """Dispatchers must POST via the injected client (no per-request Client())."""
    close_dispatch_http_client()
    client = create_dispatch_http_client()
    mock_resp = MagicMock(status_code=200, text="ok")
    client.post = MagicMock(return_value=mock_resp)  # type: ignore[method-assign]

    d = WebhookDispatcher(url="https://example.com/hooks", http_client=client)
    assert d.send(_alert()).success is True
    assert d.send(_alert()).success is True
    assert client.post.call_count == 2
    client.close()


def test_process_client_is_singleton():
    close_dispatch_http_client()
    a = get_dispatch_http_client()
    b = get_dispatch_http_client()
    assert a is b
    close_dispatch_http_client()


def test_enabled_channel_names_no_http_client(monkeypatch):
    """enabled_channel_names must not construct httpx clients."""
    from alert_pipeline.config import Settings

    created: list[object] = []
    real_client = httpx.Client

    class TrackingClient(real_client):  # type: ignore[misc, valid-type]
        def __init__(self, *args, **kwargs):
            created.append(1)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", TrackingClient)
    settings = Settings(
        dispatch_enabled=True,
        dispatch_zenduty_enabled=True,
        zenduty_integration_key="k",
        dispatch_teams_enabled=True,
        teams_webhook_url="https://example.com/t",
        dispatch_webhook_enabled=True,
        webhook_url="https://example.com/w",
        database_url="postgresql+psycopg://alerts:alerts@localhost:5432/alerts",
    )
    names = enabled_channel_names(settings)
    assert names == ["zenduty", "microsoft_teams", "webhook"]
    assert created == []


def test_build_dispatchers_injects_shared_client():
    from alert_pipeline.config import Settings

    client = create_dispatch_http_client()
    settings = Settings(
        dispatch_enabled=True,
        dispatch_webhook_enabled=True,
        webhook_url="https://example.com/w",
        database_url="postgresql+psycopg://alerts:alerts@localhost:5432/alerts",
    )
    ds = build_dispatchers(settings, http_client=client)
    assert len(ds) == 1
    assert ds[0]._http_client is client  # noqa: SLF001
    client.close()
