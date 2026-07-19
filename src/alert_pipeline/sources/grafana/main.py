"""CLI: grafana-source — Grafana sources + final Kafka adapter.

Usage::

    # Poll Loki and publish to KAFKA_INPUT_TOPIC
    GRAFANA_LOKI_URL=http://localhost:3100 grafana-source

    # Receive Grafana Alerting webhooks only
    GRAFANA_SOURCE_MODE=webhook grafana-source

    # Both (shared KafkaLogSink final adapter)
    GRAFANA_SOURCE_MODE=both GRAFANA_LOKI_URL=http://loki:3100 grafana-source
"""

from __future__ import annotations

import logging
import sys

from alert_pipeline.config import get_settings
from alert_pipeline.sources.base import BaseLogSource, run_sources
from alert_pipeline.sources.grafana.loki import LokiSource
from alert_pipeline.sources.grafana.webhook import GrafanaWebhookSource
from alert_pipeline.sources.kafka_sink import KafkaLogSink


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    mode = (settings.grafana_source_mode or "loki").strip().lower()
    if mode not in ("loki", "webhook", "both"):
        logging.error("GRAFANA_SOURCE_MODE must be loki|webhook|both, got %r", mode)
        sys.exit(2)

    logging.info(
        "grafana-source mode=%s kafka=%s topic=%s (final adapter=KafkaLogSink)",
        mode,
        settings.kafka_bootstrap_servers,
        settings.kafka_input_topic,
    )

    want_loki = mode in ("loki", "both")
    want_webhook = mode in ("webhook", "both")
    has_loki_url = bool((settings.grafana_loki_url or "").strip())

    if want_loki and not has_loki_url:
        if mode == "loki":
            logging.error(
                "GRAFANA_LOKI_URL is required for GRAFANA_SOURCE_MODE=loki "
                "(e.g. http://loki:3100). Use MODE=webhook for Alerting only."
            )
            sys.exit(2)
        logging.warning("GRAFANA_LOKI_URL unset with mode=both — starting webhook only")
        want_loki = False
        if not want_webhook:
            sys.exit(2)

    # One final adapter shared by all sources
    sink = KafkaLogSink(settings)
    sources: list[BaseLogSource] = []
    background: list[str] = []

    if want_webhook:
        sources.append(GrafanaWebhookSource(settings, sink))
        if want_loki:
            # Webhook is push-based; run it in the background while Loki polls
            background.append(GrafanaWebhookSource.name)

    if want_loki:
        sources.append(LokiSource(settings, sink))

    run_sources(sources, sink=sink, background=background)


if __name__ == "__main__":
    main()
