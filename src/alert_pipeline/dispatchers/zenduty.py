"""Zenduty Events API dispatcher.

Docs: https://docs.zenduty.com/docs/events
Uses the integration key as the routing key for trigger events.
"""

from __future__ import annotations

import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from alert_pipeline.dispatchers.base import AlertDispatcher, DispatchResult
from alert_pipeline.schemas import AlertEvent, AlertStatus
from alert_pipeline.types import JsonObject

logger = logging.getLogger(__name__)


class ZendutyDispatcher(AlertDispatcher):
    name = "zenduty"

    def __init__(
        self,
        integration_key: str,
        api_url: str = "https://www.zenduty.com/api/events",
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        super().__init__(http_client=http_client)
        if not integration_key:
            raise ValueError("zenduty_integration_key is required when Zenduty is enabled")
        self.integration_key = integration_key
        self.api_url = api_url.rstrip("/")

    def _payload(self, alert: AlertEvent) -> JsonObject:
        event_type = "resolve" if alert.status == AlertStatus.RESOLVED else "trigger"
        severity = alert.severity.value.lower()
        if severity in ("warn", "warning"):
            severity = "warning"
        elif severity in ("fatal", "critical"):
            severity = "critical"
        elif severity == "error":
            severity = "error"
        else:
            severity = "info"

        return {
            "message": alert.title,
            "summary": alert.sample_message or alert.description,
            "alert_type": event_type,
            "severity": severity,
            "entity_id": alert.fingerprint,
            "payload": {
                "alert_id": alert.id,
                "service": alert.service,
                "host": alert.host,
                "occurrence_count": alert.occurrence_count,
                "error_code": alert.error_code,
                "trace_id": alert.trace_id,
                "labels": alert.labels,
            },
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        reraise=True,
    )
    def _post(self, body: JsonObject) -> httpx.Response:
        url = f"{self.api_url}/{self.integration_key}/"
        return self._http().post(url, json=body)

    def send(self, alert: AlertEvent) -> DispatchResult:
        try:
            resp = self._post(self._payload(alert))
            ok = 200 <= resp.status_code < 300
            if not ok:
                logger.warning("Zenduty non-2xx: %s %s", resp.status_code, resp.text[:300])
            return DispatchResult(
                channel=self.name,
                success=ok,
                status_code=resp.status_code,
                response_body=resp.text,
            )
        except Exception as exc:  # noqa: BLE001 — surface as dispatch failure
            logger.exception("Zenduty dispatch failed")
            return DispatchResult(channel=self.name, success=False, error_message=str(exc))
