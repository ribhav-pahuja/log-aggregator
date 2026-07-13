"""Generic JSON webhook dispatcher for custom APIs / Slack-compatible endpoints."""

from __future__ import annotations

import json
import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from alert_pipeline.dispatchers.base import AlertDispatcher, DispatchResult
from alert_pipeline.schemas import AlertEvent
from alert_pipeline.types import JsonObject

logger = logging.getLogger(__name__)


class WebhookDispatcher(AlertDispatcher):
    name = "webhook"

    def __init__(self, url: str, headers: dict[str, str] | None = None) -> None:
        if not url:
            raise ValueError("webhook_url is required when generic webhook is enabled")
        self.url = url
        self.headers = headers or {"Content-Type": "application/json"}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        reraise=True,
    )
    def _post(self, body: JsonObject) -> httpx.Response:
        with httpx.Client(timeout=15.0) as client:
            return client.post(self.url, json=body, headers=self.headers)

    def send(self, alert: AlertEvent) -> DispatchResult:
        try:
            resp = self._post(alert.to_dispatch_dict())
            ok = 200 <= resp.status_code < 300
            return DispatchResult(
                channel=self.name,
                success=ok,
                status_code=resp.status_code,
                response_body=resp.text,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Webhook dispatch failed")
            return DispatchResult(channel=self.name, success=False, error_message=str(exc))


def parse_headers_json(raw: str) -> dict[str, str]:
    try:
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            return {}
        return {str(k): str(v) for k, v in data.items()}
    except json.JSONDecodeError:
        logger.warning("Invalid WEBHOOK_HEADERS_JSON; ignoring")
        return {}
