"""Dispatcher interface and result type."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

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

    @abstractmethod
    def send(self, alert: AlertEvent) -> DispatchResult:
        raise NotImplementedError
