"""Dispatcher interface and result type."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import httpx

from alert_pipeline.dispatchers.http import resolve_http_client
from alert_pipeline.schemas import AlertEvent


@dataclass
class DispatchResult:
    channel: str
    success: bool
    status_code: int | None = None
    response_body: str | None = None
    error_message: str | None = None


class AlertDispatcher(ABC):
    """Pluggable destination for alerts (Zenduty, Teams, PagerDuty, Slack, …)."""

    name: str = "base"

    def __init__(self, *, http_client: httpx.Client | None = None) -> None:
        # Shared / process client — never open a new Client per request.
        self._http_client = http_client

    def _http(self) -> httpx.Client:
        return resolve_http_client(self._http_client)

    @abstractmethod
    def send(self, alert: AlertEvent) -> DispatchResult:
        raise NotImplementedError
