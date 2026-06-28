"""Alert fingerprinting — groups "same" incidents together.

Which event fields participate is controlled by ``dedup_fields`` in
``config/alerts.yaml`` (defaults and per-service / error_code overrides).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence

from alert_pipeline.schemas import LogEvent

# Only strip clearly volatile tokens in messages.
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_HEX_RE = re.compile(r"\b[0-9a-f]{32,}\b", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")

# Built-in field names (also support label:<key> for a single label).
KNOWN_FIELDS = frozenset(
    {
        "service",
        "level",
        "severity",  # alias of level
        "message",
        "labels",
        "error_code",
        "host",
        "trace_id",
    }
)

DEFAULT_DEDUP_FIELDS: tuple[str, ...] = (
    "service",
    "level",
    "labels",
    "message",
)


def _normalize_message(message: str) -> str:
    text = (message or "").strip().lower()
    text = _UUID_RE.sub("<uuid>", text)
    text = _HEX_RE.sub("<hex>", text)
    text = _WS_RE.sub(" ", text)
    return text[:500]


def _normalize_labels(labels: dict[str, str] | None) -> str:
    if not labels:
        return ""
    parts = []
    for key in sorted(labels.keys(), key=lambda k: str(k).lower()):
        k = str(key).strip().lower()
        v = str(labels[key]).strip().lower()
        parts.append(f"{k}={v}")
    return "|".join(parts)


def _field_value(event: LogEvent, field: str) -> str:
    key = (field or "").strip()
    low = key.lower()

    if low in ("level", "severity"):
        return f"level={event.level.value}"
    if low == "service":
        return f"service={(event.service or 'unknown').strip().lower()}"
    if low == "message":
        return f"message={_normalize_message(event.message)}"
    if low == "labels":
        return f"labels={{{_normalize_labels(event.labels)}}}"
    if low == "error_code":
        return f"error_code={(event.error_code or '').strip().lower()}"
    if low == "host":
        return f"host={(event.host or '').strip().lower()}"
    if low == "trace_id":
        return f"trace_id={(event.trace_id or '').strip().lower()}"
    if low.startswith("label:") or low.startswith("labels."):
        # Single label key: label:env  or  labels.env
        if low.startswith("label:"):
            label_key = key.split(":", 1)[1].strip()
        else:
            label_key = key.split(".", 1)[1].strip()
        val = ""
        if event.labels:
            # case-insensitive key match
            for lk, lv in event.labels.items():
                if str(lk).lower() == label_key.lower():
                    val = str(lv).strip().lower()
                    break
        return f"label.{label_key.lower()}={val}"

    # Unknown field — include literally so misconfig is visible in the hash input
    return f"unknown.{low}="


def normalize_dedup_fields(fields: Sequence[str] | None) -> list[str]:
    if not fields:
        return list(DEFAULT_DEDUP_FIELDS)
    out: list[str] = []
    for f in fields:
        s = str(f).strip()
        if s:
            out.append(s)
    return out or list(DEFAULT_DEDUP_FIELDS)


def compute_fingerprint(
    event: LogEvent,
    dedup_fields: Sequence[str] | None = None,
) -> str:
    """
    Stable key for deduplication using the configured field list.

    Example YAML::

        dedup_fields:
          - labels
          - message

        # or include a single label key:
        # - label:env
        # - message
        # - service
    """
    fields = normalize_dedup_fields(dedup_fields)
    parts = [_field_value(event, f) for f in fields]
    material = "|".join(parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


def build_title(event: LogEvent) -> str:
    if event.error_code:
        return f"[{event.service}] {event.error_code}: {event.message[:120]}"
    return f"[{event.service}] {event.message[:140]}"
