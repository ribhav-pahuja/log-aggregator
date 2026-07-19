"""Grafana / Loki ingress: normalize, BaseLogSource, final Kafka adapter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from alert_pipeline.config import Settings
from alert_pipeline.schemas import LogEvent
from alert_pipeline.sources.base import BaseLogSource, NormalizedLog, run_sources
from alert_pipeline.sources.grafana.loki import LokiSource
from alert_pipeline.sources.grafana.normalize import (
    SOURCE_ALERTING,
    SOURCE_LOKI,
    normalize_grafana_alert_webhook,
    normalize_loki_query_response,
    normalize_loki_stream_entry,
)
from alert_pipeline.sources.grafana.webhook import GrafanaWebhookSource


def _settings(**kwargs: Any) -> Settings:
    base = dict(
        database_url="postgresql+psycopg://alerts:alerts@localhost:5432/alerts",
        grafana_loki_url="http://loki:3100",
        kafka_bootstrap_servers="localhost:9092",
        kafka_input_topic="logs",
        ui_cache_invalidate_on_write=False,
    )
    base.update(kwargs)
    return Settings(**base)


def test_normalize_loki_plain_line():
    ts_ns = str(int(datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9))
    event = normalize_loki_stream_entry(
        {"app": "payments-api", "level": "error", "pod": "pay-1"},
        ts_ns,
        "connection refused to postgres",
    )
    assert event["service"] == "payments-api"
    assert event["level"] == "ERROR"
    assert event["host"] == "pay-1"
    assert event["message"] == "connection refused to postgres"
    assert event["labels"]["source"] == SOURCE_LOKI
    assert event["source"] == SOURCE_LOKI

    le = LogEvent.from_kafka_value(dict(event))
    assert le.service == "payments-api"
    assert le.level.value == "ERROR"
    assert "connection refused" in le.message


def test_normalize_loki_json_line():
    ts_ns = "1717243200000000000"
    line = (
        '{"level":"ERROR","service":"checkout","message":"gateway timeout",'
        '"error_code":"GW_TIMEOUT","trace_id":"abc","labels":{"env":"prod"}}'
    )
    event = normalize_loki_stream_entry({"job": "varlogs"}, ts_ns, line)
    assert event["service"] == "checkout"
    assert event["level"] == "ERROR"
    assert event["message"] == "gateway timeout"
    assert event["error_code"] == "GW_TIMEOUT"
    assert event["trace_id"] == "abc"
    assert event["labels"]["env"] == "prod"
    assert event["labels"]["job"] == "varlogs"


def test_normalize_loki_query_response_orders_and_parses():
    payload = {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"service": "auth", "level": "ERROR"},
                    "values": [
                        ["1717243201000000000", "second"],
                        ["1717243200000000000", "first"],
                    ],
                }
            ],
        },
    }
    events = normalize_loki_query_response(payload)
    assert len(events) == 2
    assert events[0]["message"] == "first"
    assert events[1]["message"] == "second"
    assert events[0]["service"] == "auth"


def test_normalize_grafana_alerting_webhook():
    payload = {
        "receiver": "alert-pipeline",
        "status": "firing",
        "groupLabels": {"alertname": "HighErrorRate"},
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "HighErrorRate",
                    "severity": "critical",
                    "service": "payments-api",
                    "env": "prod",
                },
                "annotations": {
                    "summary": "Error rate above 5%",
                    "description": "p99 errors elevated",
                },
                "startsAt": "2024-06-01T12:00:00Z",
                "fingerprint": "fp-1",
            },
            {
                "status": "resolved",
                "labels": {"alertname": "DiskFull", "service": "inventory"},
                "annotations": {"summary": "Disk was full"},
                "startsAt": "2024-06-01T11:00:00Z",
                "fingerprint": "fp-2",
            },
        ],
    }
    events = normalize_grafana_alert_webhook(payload)
    assert len(events) == 2
    assert events[0]["service"] == "payments-api"
    assert events[0]["level"] == "CRITICAL"
    assert events[0]["message"] == "Error rate above 5%"
    assert events[0]["labels"]["source"] == SOURCE_ALERTING
    assert events[0]["labels"]["grafana_fingerprint"] == "fp-1"
    assert events[0]["labels"]["grafana_receiver"] == "alert-pipeline"
    assert events[1]["message"].startswith("[resolved]")
    assert events[1]["level"] == "INFO"

    le = LogEvent.from_kafka_value(dict(events[0]))
    assert le.service == "payments-api"
    assert le.level.value == "CRITICAL"


def test_normalize_grafana_test_notification_without_alerts_array():
    payload = {
        "receiver": "test",
        "state": "alerting",
        "title": "TestAlert",
        "alertname": "TestAlert",
        "message": "This is a test notification",
    }
    events = normalize_grafana_alert_webhook(payload)
    assert len(events) == 1
    assert events[0]["source"] == SOURCE_ALERTING
    assert "TestAlert" in events[0]["message"] or events[0]["service"]


def test_loki_source_dedupes_and_emits_via_sink():
    sink = MagicMock()
    client = MagicMock()
    client.query_range.return_value = {
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"service": "svc", "level": "ERROR"},
                    "values": [
                        ["1717243200000000000", "boom"],
                        ["1717243200000000000", "boom"],
                    ],
                }
            ],
        },
    }
    source = LokiSource(_settings(), sink, client=client)
    assert isinstance(source, BaseLogSource)
    published = source.poll_once(now=datetime(2024, 6, 1, 12, 1, 0, tzinfo=timezone.utc))
    assert len(published) == 1
    assert sink.publish.call_count == 1
    published2 = source.poll_once(now=datetime(2024, 6, 1, 12, 1, 15, tzinfo=timezone.utc))
    assert published2 == []
    assert sink.publish.call_count == 1


def test_loki_source_requires_url():
    with pytest.raises(ValueError, match="GRAFANA_LOKI_URL"):
        LokiSource(_settings(grafana_loki_url=""), MagicMock())


def test_webhook_source_emits_via_sink():
    sink = MagicMock()
    server = GrafanaWebhookSource(_settings(), sink)
    assert isinstance(server, BaseLogSource)
    n = server.handle_payload(
        {
            "alerts": [
                {
                    "status": "firing",
                    "labels": {"alertname": "X", "service": "s"},
                    "annotations": {"summary": "y"},
                    "startsAt": "2024-06-01T00:00:00Z",
                }
            ]
        }
    )
    assert n == 1
    sink.publish.assert_called_once()
    sink.flush.assert_called()


def test_custom_source_uses_final_adapter_pattern():
    """New systems only implement BaseLogSource; sink is the final adapter."""

    class FakeSource(BaseLogSource):
        name = "fake"

        def __init__(self, sink: Any, events: list[NormalizedLog]) -> None:
            super().__init__(sink)
            self._events = events
            self.ran = False

        def run(self) -> None:
            self.ran = True
            self.emit_many(self._events, flush=True)

    sink = MagicMock()
    events: list[NormalizedLog] = [
        {
            "level": "ERROR",
            "service": "demo",
            "message": "from custom source",
            "source": "fake",
        }
    ]
    src = FakeSource(sink, events)
    run_sources([src], sink=sink)
    assert src.ran is True
    sink.publish.assert_called_once()
    sink.close.assert_called_once()
