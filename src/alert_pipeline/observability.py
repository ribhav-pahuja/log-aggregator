"""Lightweight Prometheus metrics (optional if prometheus_client missing)."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

    _PROM = True
except ImportError:  # pragma: no cover
    _PROM = False
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"

    def generate_latest() -> bytes:  # type: ignore[misc]
        return b"# prometheus_client not installed\n"


if _PROM:
    ALERTS_EMITTED = Counter(
        "alert_pipeline_alerts_emitted_total",
        "Incidents persisted (new or refire)",
        ["is_new"],
    )
    ALERTS_SKIPPED = Counter(
        "alert_pipeline_alerts_skipped_total",
        "Log events / emits skipped",
        ["reason"],
    )
    OUTBOX_ENQUEUED = Counter(
        "alert_pipeline_outbox_enqueued_total",
        "Outbox rows enqueued",
        ["channel"],
    )
    OUTBOX_PROCESSED = Counter(
        "alert_pipeline_outbox_processed_total",
        "Outbox rows finished",
        ["channel", "result"],
    )
    DISPATCH_ATTEMPTS = Counter(
        "alert_pipeline_dispatch_attempts_total",
        "Channel dispatch attempts",
        ["channel", "success"],
    )
    OUTBOX_PENDING = Gauge(
        "alert_pipeline_outbox_pending",
        "Outbox rows in pending/processing/failed (open work)",
    )
    OUTBOX_DEAD = Gauge(
        "alert_pipeline_outbox_dead",
        "Outbox rows in dead status (max attempts exhausted)",
    )
else:  # pragma: no cover

    class _Noop:
        def labels(self, *args: object, **kwargs: object) -> "_Noop":
            return self

        def inc(self, amount: float = 1) -> None:
            return None

        def set(self, value: float) -> None:
            return None

    ALERTS_EMITTED = _Noop()  # type: ignore[assignment]
    ALERTS_SKIPPED = _Noop()  # type: ignore[assignment]
    OUTBOX_ENQUEUED = _Noop()  # type: ignore[assignment]
    OUTBOX_PROCESSED = _Noop()  # type: ignore[assignment]
    DISPATCH_ATTEMPTS = _Noop()  # type: ignore[assignment]
    OUTBOX_PENDING = _Noop()  # type: ignore[assignment]
    OUTBOX_DEAD = _Noop()  # type: ignore[assignment]


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
