"""Shared typing aliases used across the alert pipeline.

Prefer these over ``typing.Any`` for JSON payloads, wire formats, and
known-structure dicts so return types stay checkable.
"""

from __future__ import annotations

from typing import TypeAlias, TypedDict

# ---------------------------------------------------------------------------
# JSON (Kafka payloads, model_dump(mode="json"), Redis snapshot blobs, …)
# ---------------------------------------------------------------------------

JsonPrimitive: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

# ---------------------------------------------------------------------------
# Dedup incident state (canonical blob shared by Quix + memory adapters)
# ---------------------------------------------------------------------------


class IncidentStateDict(TypedDict):
    """JSON-friendly incident window state persisted in Quix / memory."""

    alert_id: str
    fingerprint: str
    first_seen: str
    last_seen: str
    occurrence_count: int
    last_emitted_at: float
    severity: str
    service: str
    host: str
    title: str
    sample_message: str
    error_code: str | None
    trace_id: str | None
    labels: dict[str, str]
    window_seconds: int


# ---------------------------------------------------------------------------
# Quix enrichment / sink wire rows
# ---------------------------------------------------------------------------


class EnrichmentRow(TypedDict):
    fingerprint: str
    event: JsonObject
    window_seconds: int
    refire_interval_seconds: int
    suppress_dispatch_while_acknowledged: bool
    allow_reopen_after_resolve: bool
    title: str


class DedupEmitRow(TypedDict):
    alert: JsonObject
    suppress_dispatch_while_acknowledged: bool
    allow_reopen_after_resolve: bool


# ---------------------------------------------------------------------------
# ProcessResult / config / UI cache meta
# ---------------------------------------------------------------------------


class ProcessResultDict(TypedDict):
    alert_id: str | None
    fingerprint: str | None
    is_new: bool | None
    occurrence_count: int | None
    service: str | None
    severity: str | None
    dispatch_suppressed: bool


class RefirePartial(TypedDict, total=False):
    """Subset of RefireSettings keys from YAML service/error_code overlays."""

    min_level: str
    dedup_window_seconds: int
    refire_interval_seconds: int
    suppress_dispatch_while_acknowledged: bool
    allow_reopen_after_resolve: bool
    dedup_fields: list[str]


class CacheMeta(TypedDict):
    source: str
    snapshot_key: str
    ttl_seconds: float
    lock_ttl_seconds: float
    stampede_protection: str
    redis_generation: int | None
    redis_expires_at_unix: float | None
    local_generation: int
    local_alert_count: int
    redis_alert_count: int
    db_fetch_count: int
    db_fetch_log_marker: str
