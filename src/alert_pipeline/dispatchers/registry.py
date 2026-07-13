"""Build the set of enabled dispatchers and fan-out alerts to all of them."""

from __future__ import annotations

import logging

import httpx

from alert_pipeline.config import Settings
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.dispatchers.base import AlertDispatcher, DispatchResult
from alert_pipeline.dispatchers.teams import TeamsDispatcher
from alert_pipeline.dispatchers.webhook import WebhookDispatcher, parse_headers_json
from alert_pipeline.dispatchers.zenduty import ZendutyDispatcher
from alert_pipeline.schemas import AlertEvent

logger = logging.getLogger(__name__)


def enabled_channel_names(settings: Settings) -> list[str]:
    """Channel names that would receive a dispatch for the current config.

    Pure config inspection — does not construct HTTP clients or dispatchers.
    """
    if not settings.dispatch_enabled:
        return []
    names: list[str] = []
    if settings.dispatch_zenduty_enabled:
        names.append(ZendutyDispatcher.name)
    if settings.dispatch_teams_enabled:
        names.append(TeamsDispatcher.name)
    if settings.dispatch_webhook_enabled:
        names.append(WebhookDispatcher.name)
    return names


def build_dispatchers(
    settings: Settings,
    *,
    http_client: httpx.Client | None = None,
) -> list[AlertDispatcher]:
    """Construct enabled channel dispatchers.

    Pass ``http_client`` to share one connection pool (outbox worker). When
    omitted, dispatchers use the process-wide client from
    :mod:`alert_pipeline.dispatchers.http`.
    """
    if not settings.dispatch_enabled:
        logger.info("Dispatch globally disabled")
        return []

    dispatchers: list[AlertDispatcher] = []
    if settings.dispatch_zenduty_enabled:
        dispatchers.append(
            ZendutyDispatcher(
                integration_key=settings.zenduty_integration_key,
                api_url=settings.zenduty_api_url,
                http_client=http_client,
            )
        )
    if settings.dispatch_teams_enabled:
        dispatchers.append(
            TeamsDispatcher(
                webhook_url=settings.teams_webhook_url,
                http_client=http_client,
            )
        )
    if settings.dispatch_webhook_enabled:
        dispatchers.append(
            WebhookDispatcher(
                url=settings.webhook_url,
                headers=parse_headers_json(settings.webhook_headers_json),
                http_client=http_client,
            )
        )
    logger.info("Enabled dispatchers: %s", [d.name for d in dispatchers] or ["(none)"])
    return dispatchers


class DispatchFanout:
    """Send each alert to every configured channel and audit the results."""

    def __init__(
        self, dispatchers: list[AlertDispatcher], repo: AlertRepository | None = None
    ) -> None:
        self.dispatchers = dispatchers
        self.repo = repo
        self._by_name = {d.name: d for d in dispatchers}

    def dispatch(
        self,
        alert: AlertEvent,
        *,
        idempotency_key: str | None = None,
    ) -> list[DispatchResult]:
        results: list[DispatchResult] = []
        for dispatcher in self.dispatchers:
            key = idempotency_key
            if key is None and self.repo is not None:
                key = self.repo.make_idempotency_key(
                    alert.id, dispatcher.name, alert.occurrence_count
                )
            results.append(self.dispatch_one(alert, channel=dispatcher.name, idempotency_key=key))
        return results

    def dispatch_one(
        self,
        alert: AlertEvent,
        *,
        channel: str,
        idempotency_key: str | None = None,
    ) -> DispatchResult:
        dispatcher = self._by_name.get(channel)
        if dispatcher is None:
            result = DispatchResult(
                channel=channel,
                success=False,
                error_message=f"unknown channel {channel!r}",
            )
        else:
            result = dispatcher.send(alert)
        if self.repo is not None:
            self.repo.log_dispatch(
                alert_id=alert.id,
                channel=result.channel,
                success=result.success,
                status_code=result.status_code,
                response_body=result.response_body,
                error_message=result.error_message,
                idempotency_key=idempotency_key,
            )
        if result.success:
            logger.info("Dispatched alert %s via %s", alert.id, result.channel)
        else:
            logger.error(
                "Failed dispatch alert %s via %s: %s",
                alert.id,
                result.channel,
                result.error_message or result.response_body,
            )
        return result
