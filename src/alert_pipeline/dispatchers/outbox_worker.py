"""Drain ``dispatch_outbox`` and call channel dispatchers with retries.

Run as a separate process so HTTP side-effects never block Quix consume::

    alert-dispatch-worker
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time

from alert_pipeline.config import Settings, get_settings
from alert_pipeline.db.repository import AlertRepository
from alert_pipeline.dispatchers.http import (
    close_dispatch_http_client,
    create_dispatch_http_client,
)
from alert_pipeline.dispatchers.registry import DispatchFanout, build_dispatchers
from alert_pipeline.observability import (
    DISPATCH_ATTEMPTS,
    OUTBOX_DEAD,
    OUTBOX_PENDING,
    OUTBOX_PROCESSED,
)
from alert_pipeline.schemas import AlertEvent

logger = logging.getLogger(__name__)


def _refresh_outbox_gauges(repo: AlertRepository) -> None:
    OUTBOX_PENDING.set(repo.count_outbox_open())
    OUTBOX_DEAD.set(repo.count_outbox("dead"))


def process_batch(
    repo: AlertRepository,
    fanout: DispatchFanout,
    settings: Settings,
) -> int:
    """Claim and process up to batch_size pending outbox rows. Returns count handled."""
    rows = repo.claim_outbox_batch(
        batch_size=settings.dispatch_outbox_batch_size,
        stale_processing_seconds=settings.dispatch_outbox_stale_processing_seconds,
    )
    if not rows:
        _refresh_outbox_gauges(repo)
        return 0

    handled = 0
    for row in rows:
        handled += 1
        try:
            alert = AlertEvent.model_validate(json.loads(row.payload_json))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Invalid outbox payload id=%s", row.id)
            repo.mark_outbox_result(
                row.id,
                success=False,
                error=f"invalid payload: {exc}",
                max_attempts=settings.dispatch_outbox_max_attempts,
                backoff_base_seconds=2.0,
            )
            OUTBOX_PROCESSED.labels(channel=row.channel, result="dead").inc()
            continue

        # Idempotent: skip if this key already succeeded in audit log
        if repo.dispatch_idempotency_succeeded(row.idempotency_key):
            repo.mark_outbox_sent(row.id)
            OUTBOX_PROCESSED.labels(channel=row.channel, result="duplicate_skip").inc()
            continue

        result = fanout.dispatch_one(
            alert,
            channel=row.channel,
            idempotency_key=row.idempotency_key,
        )
        ok = bool(result and result.success)
        DISPATCH_ATTEMPTS.labels(channel=row.channel, success="true" if ok else "false").inc()
        if ok:
            repo.mark_outbox_sent(row.id)
            OUTBOX_PROCESSED.labels(channel=row.channel, result="sent").inc()
        else:
            err = (result.error_message if result else None) or "dispatch failed"
            final = repo.mark_outbox_result(
                row.id,
                success=False,
                error=err,
                max_attempts=settings.dispatch_outbox_max_attempts,
                backoff_base_seconds=2.0,
            )
            OUTBOX_PROCESSED.labels(
                channel=row.channel, result="dead" if final == "dead" else "failed"
            ).inc()

    _refresh_outbox_gauges(repo)
    return handled


def run_worker(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    repo = AlertRepository(settings.database_url)

    stop = False

    def _stop(*_args: object) -> None:
        nonlocal stop
        stop = True
        logger.info("Dispatch worker shutting down…")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    # One pooled client for the worker lifetime — all channels share keep-alive.
    with create_dispatch_http_client() as http_client:
        fanout = DispatchFanout(
            build_dispatchers(settings, http_client=http_client),
            repo=repo,
        )
        logger.info(
            "Dispatch worker started mode=outbox poll=%ss batch=%s max_attempts=%s",
            settings.dispatch_outbox_poll_seconds,
            settings.dispatch_outbox_batch_size,
            settings.dispatch_outbox_max_attempts,
        )
        try:
            while not stop:
                n = process_batch(repo, fanout, settings)
                if n == 0:
                    time.sleep(settings.dispatch_outbox_poll_seconds)
                # else immediately poll again while there is work
        finally:
            # Drop process singleton too in case any path used it.
            close_dispatch_http_client()


def main() -> None:
    run_worker()


if __name__ == "__main__":
    main()
