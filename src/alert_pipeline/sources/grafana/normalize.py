"""Normalize Grafana Loki streams and Alerting webhooks to Kafka log shape."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, cast

from alert_pipeline.sources.base import NormalizedLog
from alert_pipeline.types import JsonObject

logger = logging.getLogger(__name__)

SOURCE_LOKI = "grafana-loki"
SOURCE_ALERTING = "grafana-alerting"

# Common stream / alert label keys → LogEvent fields
_SERVICE_KEYS = ("service", "app", "application", "job", "container", "name", "k8s_app")
_HOST_KEYS = ("host", "hostname", "pod", "instance", "node", "k8s_pod_name")
_LEVEL_KEYS = ("level", "severity", "log_level", "lvl")
_TRACE_KEYS = ("trace_id", "traceId", "traceid", "tid")
_ERROR_CODE_KEYS = ("error_code", "code", "errorCode")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ns_to_iso(ts_ns: str | int | float) -> str:
    try:
        ns = int(ts_ns)
        # Loki uses nanoseconds; also accept seconds/ms if small
        if ns < 1e12:
            # seconds
            sec = float(ns)
        elif ns < 1e14:
            # milliseconds
            sec = ns / 1_000.0
        else:
            sec = ns / 1_000_000_000.0
        return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return _utc_now_iso()


def _str_map(labels: dict[str, Any] | None) -> dict[str, str]:
    if not labels:
        return {}
    out: dict[str, str] = {}
    for k, v in labels.items():
        if v is None:
            continue
        out[str(k)] = str(v)
    return out


def _pick(labels: dict[str, str], keys: tuple[str, ...], default: str = "unknown") -> str:
    for key in keys:
        if key in labels and labels[key]:
            return labels[key]
        # case-insensitive fallback
        low = {k.lower(): v for k, v in labels.items()}
        if key.lower() in low and low[key.lower()]:
            return low[key.lower()]
    return default


def _try_parse_json_line(line: str) -> dict[str, Any] | None:
    text = (line or "").strip()
    if not text or text[0] not in "{[":
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def normalize_loki_stream_entry(
    stream_labels: dict[str, Any] | None,
    timestamp_ns: str | int | float,
    line: str,
    *,
    source: str = SOURCE_LOKI,
) -> NormalizedLog:
    """Convert one Loki ``[ts_ns, line]`` pair into a normalized log dict."""
    labels = _str_map(stream_labels)
    nested = _try_parse_json_line(line)

    level = "INFO"
    service = _pick(labels, _SERVICE_KEYS)
    host = _pick(labels, _HOST_KEYS)
    message = line or ""
    error_code: str | None = None
    trace_id: str | None = None
    extra_labels: dict[str, str] = dict(labels)
    timestamp = _ns_to_iso(timestamp_ns)

    if nested:
        # Nested JSON log line — prefer structured fields
        nested_labels_raw = nested.get("labels")
        if isinstance(nested_labels_raw, dict):
            for k, v in nested_labels_raw.items():
                if v is not None:
                    extra_labels[str(k)] = str(v)

        level_val = None
        for k in _LEVEL_KEYS:
            if nested.get(k) is not None:
                level_val = nested.get(k)
                break
        if level_val is None:
            level_val = _pick(labels, _LEVEL_KEYS, default="")
        level = str(level_val or "INFO").upper()

        for k in _SERVICE_KEYS:
            if nested.get(k):
                service = str(nested[k])
                break

        for k in _HOST_KEYS:
            if nested.get(k):
                host = str(nested[k])
                break

        message = str(
            nested.get("message")
            or nested.get("msg")
            or nested.get("error")
            or nested.get("text")
            or nested.get("log")
            or line
            or ""
        )

        for k in _ERROR_CODE_KEYS:
            if nested.get(k) is not None:
                error_code = str(nested[k])
                break

        for k in _TRACE_KEYS:
            if nested.get(k) is not None:
                trace_id = str(nested[k])
                break

        ts_raw = nested.get("timestamp") or nested.get("@timestamp") or nested.get("time")
        if ts_raw is not None:
            # Prefer structured timestamp when present
            try:
                from alert_pipeline.schemas import LogEvent

                # Reuse LogEvent coercion via a throwaway instance
                event = LogEvent.from_kafka_value(
                    cast(JsonObject, {"timestamp": ts_raw, "message": message, "level": level})
                )
                timestamp = event.timestamp.isoformat()
            except Exception:  # noqa: BLE001
                pass
    else:
        level = _pick(labels, _LEVEL_KEYS, default="INFO").upper()
        error_code = None
        for k in _ERROR_CODE_KEYS:
            if k in labels:
                error_code = labels[k]
                break
        for k in _TRACE_KEYS:
            if k in labels:
                trace_id = labels[k]
                break

    extra_labels.setdefault("source", source)
    # Keep stream job/level visible as labels even when mapped to top-level fields
    result: NormalizedLog = {
        "timestamp": timestamp,
        "level": level,
        "service": service,
        "host": host,
        "message": message,
        "error_code": error_code,
        "trace_id": trace_id,
        "labels": extra_labels,
        "source": source,
        "raw": cast(
            JsonObject,
            {
                "stream": labels,
                "line": line,
                "timestamp_ns": str(timestamp_ns),
            },
        ),
    }
    return result


def normalize_loki_query_response(payload: dict[str, Any] | JsonObject) -> list[NormalizedLog]:
    """Parse Loki ``/loki/api/v1/query_range`` (or query) JSON into log events.

    Expected shape::

        {"status": "success", "data": {"resultType": "streams", "result": [
            {"stream": {...}, "values": [["ts_ns", "line"], ...]}
        ]}}
    """
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return []
    result = data.get("result")
    if not isinstance(result, list):
        return []

    events: list[NormalizedLog] = []
    for stream_row in result:
        if not isinstance(stream_row, dict):
            continue
        stream_labels = stream_row.get("stream") if isinstance(stream_row.get("stream"), dict) else {}
        values = stream_row.get("values")
        if not isinstance(values, list):
            continue
        for pair in values:
            if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                continue
            ts_ns, line = pair[0], pair[1]
            events.append(
                normalize_loki_stream_entry(
                    cast(dict[str, Any], stream_labels),
                    cast(str | int | float, ts_ns),
                    str(line),
                )
            )
    # Loki returns newest-first within a stream; chronological order is nicer for Kafka
    events.sort(key=lambda e: e.get("timestamp") or "")
    return events


def normalize_grafana_alert(
    alert: dict[str, Any],
    *,
    receiver: str | None = None,
    group_labels: dict[str, Any] | None = None,
) -> NormalizedLog:
    """Map one Grafana Unified Alerting alert object to a log event."""
    labels = _str_map(alert.get("labels") if isinstance(alert.get("labels"), dict) else {})
    annotations = _str_map(
        alert.get("annotations") if isinstance(alert.get("annotations"), dict) else {}
    )
    status = str(alert.get("status") or "firing").lower()

    severity = _pick(labels, _LEVEL_KEYS, default="")
    if not severity:
        severity = "CRITICAL" if status == "firing" else "INFO"
    level = severity.upper()
    # Common Grafana severities
    if level in ("CRITICAL", "HIGH", "ERROR", "WARNING", "WARN", "INFO", "DEBUG"):
        if level == "HIGH":
            level = "ERROR"
        elif level == "WARNING":
            level = "WARN"
    else:
        level = "ERROR" if status == "firing" else "INFO"

    service = _pick(labels, _SERVICE_KEYS, default=_pick(labels, ("alertname",), default="grafana"))
    host = _pick(labels, _HOST_KEYS)

    message = (
        annotations.get("summary")
        or annotations.get("description")
        or annotations.get("message")
        or labels.get("alertname")
        or "Grafana alert"
    )
    if status == "resolved":
        message = f"[resolved] {message}"

    starts = alert.get("startsAt") or alert.get("starts_at")
    timestamp = str(starts) if starts else _utc_now_iso()

    merged_labels = dict(labels)
    for k, v in annotations.items():
        merged_labels.setdefault(f"annotation_{k}", v)
    merged_labels["source"] = SOURCE_ALERTING
    merged_labels["grafana_status"] = status
    if receiver:
        merged_labels["grafana_receiver"] = receiver
    if group_labels:
        for k, v in _str_map(group_labels).items():
            merged_labels.setdefault(f"group_{k}", v)
    fp = alert.get("fingerprint")
    if fp:
        merged_labels["grafana_fingerprint"] = str(fp)

    error_code = labels.get("error_code") or labels.get("alertname")
    raw: JsonObject = cast(JsonObject, {k: v for k, v in alert.items() if _is_jsonable(v)})

    return {
        "timestamp": timestamp if "T" in str(timestamp) else _utc_now_iso(),
        "level": level,
        "service": service,
        "host": host,
        "message": message,
        "error_code": None if error_code is None else str(error_code),
        "trace_id": labels.get("trace_id") or labels.get("traceId"),
        "labels": merged_labels,
        "source": SOURCE_ALERTING,
        "raw": raw,
    }


def normalize_grafana_alert_webhook(payload: dict[str, Any] | JsonObject) -> list[NormalizedLog]:
    """Parse Grafana Alerting / Alertmanager-compatible webhook body.

    Supports Grafana Unified Alerting contact-point JSON and classic Alertmanager.
    """
    if not isinstance(payload, dict):
        return []

    receiver = payload.get("receiver")
    receiver_s = str(receiver) if receiver is not None else None
    group_labels = payload.get("groupLabels") or payload.get("group_labels")
    group_dict = group_labels if isinstance(group_labels, dict) else None

    alerts = payload.get("alerts")
    if isinstance(alerts, list) and alerts:
        out: list[NormalizedLog] = []
        for item in alerts:
            if isinstance(item, dict):
                out.append(
                    normalize_grafana_alert(
                        item,
                        receiver=receiver_s,
                        group_labels=group_dict,
                    )
                )
        return out

    # Single-alert shapes / test notifications without ``alerts`` array
    if "labels" in payload or "alertname" in payload or "state" in payload:
        # Grafana "Test" contact point sometimes sends a simplified body
        synthetic = dict(payload)
        if "labels" not in synthetic and "alertname" in synthetic:
            synthetic["labels"] = {"alertname": str(synthetic["alertname"])}
        if "status" not in synthetic:
            state = str(synthetic.get("state") or synthetic.get("status") or "firing")
            synthetic["status"] = "resolved" if state.lower() in ("ok", "resolved", "normal") else "firing"
        return [
            normalize_grafana_alert(
                synthetic,
                receiver=receiver_s,
                group_labels=group_dict,
            )
        ]

    logger.warning("Unrecognized Grafana webhook payload keys: %s", list(payload.keys())[:20])
    return []


def _is_jsonable(v: object) -> bool:
    return isinstance(v, (dict, list, str, int, float, bool)) or v is None
