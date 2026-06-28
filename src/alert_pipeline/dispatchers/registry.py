"""Build the set of enabled dispatchers and fan-out alerts to all of them."""

from __future__ import annotations

import logging

from alert_pipeline.config import Settings
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.dispatchers.base import AlertDispatcher, DispatchResult
from alert_pipeline.dispatchers.teams import TeamsDispatcher
from alert_pipeline.dispatchers.webhook import WebhookDispatcher, parse_headers_json
from alert_pipeline.dispatchers.zenduty import ZendutyDispatcher
from alert_pipeline.schemas import AlertEvent

logger = logging.getLogger(__name__)


def build_dispatchers(settings: Settings) -> list[AlertDispatcher]:
    if not settings.dispatch_enabled:
        logger.info("Dispatch globally disabled")
        return []

    dispatchers: list[AlertDispatcher] = []
    if settings.dispatch_zenduty_enabled:
        dispatchers.append(
            ZendutyDispatcher(
                integration_key=settings.zenduty_integration_key,
                api_url=settings.zenduty_api_url,
            )
        )
    if settings.dispatch_teams_enabled:
        dispatchers.append(TeamsDispatcher(webhook_url=settings.teams_webhook_url))
    if settings.dispatch_webhook_enabled:
        dispatchers.append(
            WebhookDispatcher(
                url=settings.webhook_url,
                headers=parse_headers_json(settings.webhook_headers_json),
            )
        )
    logger.info("Enabled dispatchers: %s", [d.name for d in dispatchers] or ["(none)"])
    return dispatchers


class DispatchFanout:
    """Send each alert to every configured channel and audit the results."""

    def __init__(self, dispatchers: list[AlertDispatcher], repo: AlertRepository | None = None) -> None:
        self.dispatchers = dispatchers
        self.repo = repo

    def dispatch(self, alert: AlertEvent) -> list[DispatchResult]:
        results: list[DispatchResult] = []
        for dispatcher in self.dispatchers:
            result = dispatcher.send(alert)
            results.append(result)
            if self.repo is not None:
                self.repo.log_dispatch(
                    alert_id=alert.id,
                    channel=result.channel,
                    success=result.success,
                    status_code=result.status_code,
                    response_body=result.response_body,
                    error_message=result.error_message,
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
        return results
