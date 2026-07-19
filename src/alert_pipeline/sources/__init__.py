"""Log ingress sources.

Kafka remains the primary pipeline transport (Quix). Optional bridges implement
:class:`~alert_pipeline.sources.base.BaseLogSource`, normalize to
:class:`~alert_pipeline.sources.base.NormalizedLog`, and emit through a shared
final adapter (:class:`~alert_pipeline.sources.kafka_sink.KafkaLogSink`).

Supported:
* **Kafka** — native consume path (``runtime/quix_runtime``)
* **Grafana Loki** — :class:`~alert_pipeline.sources.grafana.loki.LokiSource`
* **Grafana Alerting** — :class:`~alert_pipeline.sources.grafana.webhook.GrafanaWebhookSource`
"""

from alert_pipeline.sources.base import (
    BaseLogSource,
    LogSink,
    LogSource,
    NormalizedLog,
    run_sources,
)
from alert_pipeline.sources.kafka_sink import KafkaLogSink

__all__ = [
    "BaseLogSource",
    "KafkaLogSink",
    "LogSink",
    "LogSource",
    "NormalizedLog",
    "run_sources",
]
