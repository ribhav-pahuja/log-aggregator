"""Microsoft Teams incoming webhook dispatcher (Adaptive Card style MessageCard)."""

from __future__ import annotations

import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from alert_pipeline.dispatchers.base import AlertDispatcher, DispatchResult
from alert_pipeline.schemas import AlertEvent

logger = logging.getLogger(__name__)

_THEME = {
    "CRITICAL": "FF0000",
    "FATAL": "FF0000",
    "ERROR": "E81123",
    "WARN": "FFB900",
    "WARNING": "FFB900",
    "INFO": "0078D4",
    "DEBUG": "69797E",
}


class TeamsDispatcher(AlertDispatcher):
    name = "microsoft_teams"

    def __init__(self, webhook_url: str) -> None:
        if not webhook_url:
            raise ValueError("teams_webhook_url is required when Teams is enabled")
        self.webhook_url = webhook_url

    def _payload(self, alert: AlertEvent) -> dict:
        color = _THEME.get(alert.severity.value, "E81123")
        status_label = "NEW" if alert.is_new else alert.status.value.upper()
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": alert.title,
            "title": f"[{status_label}] {alert.title}",
            "sections": [
                {
                    "facts": [
                        {"name": "Service", "value": alert.service},
                        {"name": "Host", "value": alert.host},
                        {"name": "Severity", "value": alert.severity.value},
                        {"name": "Occurrences", "value": str(alert.occurrence_count)},
                        {"name": "Fingerprint", "value": alert.fingerprint},
                        {"name": "Error code", "value": alert.error_code or "—"},
                        {"name": "Trace ID", "value": alert.trace_id or "—"},
                    ],
                    "text": alert.sample_message or alert.description,
                }
            ],
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        reraise=True,
    )
    def _post(self, body: dict) -> httpx.Response:
        with httpx.Client(timeout=15.0) as client:
            return client.post(self.webhook_url, json=body)

    def send(self, alert: AlertEvent) -> DispatchResult:
        try:
            resp = self._post(self._payload(alert))
            ok = 200 <= resp.status_code < 300
            if not ok:
                logger.warning("Teams non-2xx: %s %s", resp.status_code, resp.text[:300])
            return DispatchResult(
                channel=self.name,
                success=ok,
                status_code=resp.status_code,
                response_body=resp.text,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Teams dispatch failed")
            return DispatchResult(channel=self.name, success=False, error_message=str(exc))
