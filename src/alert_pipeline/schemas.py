"""Pydantic models for logs, alerts, and dispatch payloads."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"

    @classmethod
    def normalize(cls, value: str) -> "LogLevel":
        key = (value or "INFO").upper()
        if key == "WARNING":
            return cls.WARN
        if key == "FATAL":
            return cls.CRITICAL
        try:
            return cls(key)
        except ValueError:
            return cls.INFO


LEVEL_RANK = {
    LogLevel.DEBUG: 10,
    LogLevel.INFO: 20,
    LogLevel.WARN: 30,
    LogLevel.WARNING: 30,
    LogLevel.ERROR: 40,
    LogLevel.CRITICAL: 50,
    LogLevel.FATAL: 50,
}


class LogEvent(BaseModel):
    """Normalized log event consumed from Kafka."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    level: LogLevel = LogLevel.INFO
    service: str = "unknown"
    host: str = "unknown"
    message: str = ""
    error_code: str | None = None
    trace_id: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("level", mode="before")
    @classmethod
    def _coerce_level(cls, v: Any) -> LogLevel:
        if isinstance(v, LogLevel):
            return v
        return LogLevel.normalize(str(v))

    @field_validator("timestamp", mode="before")
    @classmethod
    def _coerce_ts(cls, v: Any) -> datetime:
        if v is None or v == "":
            return datetime.now(timezone.utc)
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if isinstance(v, (int, float)):
            # accept epoch seconds or ms
            ts = float(v)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        # ISO-8601 string
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    @classmethod
    def from_kafka_value(cls, payload: dict[str, Any]) -> "LogEvent":
        """Best-effort parse of heterogeneous log shapes."""
        level = payload.get("level") or payload.get("severity") or payload.get("log_level") or "INFO"
        message = (
            payload.get("message")
            or payload.get("msg")
            or payload.get("error")
            or payload.get("text")
            or ""
        )
        service = (
            payload.get("service")
            or payload.get("app")
            or payload.get("application")
            or (payload.get("labels") or {}).get("service")
            or "unknown"
        )
        host = payload.get("host") or payload.get("hostname") or payload.get("pod") or "unknown"
        labels = payload.get("labels") if isinstance(payload.get("labels"), dict) else {}
        return cls(
            timestamp=payload.get("timestamp") or payload.get("@timestamp") or payload.get("time"),
            level=level,
            service=str(service),
            host=str(host),
            message=str(message),
            error_code=payload.get("error_code") or payload.get("code"),
            trace_id=payload.get("trace_id") or payload.get("traceId"),
            labels={str(k): str(v) for k, v in labels.items()},
            raw=payload,
        )


class AlertStatus(str, Enum):
    OPEN = "open"
    UPDATED = "updated"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


# Statuses that still represent an active incident (dedup merges into these rows).
ACTIVE_ALERT_STATUSES = frozenset(
    {
        AlertStatus.OPEN.value,
        AlertStatus.UPDATED.value,
        AlertStatus.ACKNOWLEDGED.value,
    }
)


class AlertEvent(BaseModel):
    """Deduplicated alert / incident emitted by the pipeline."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    fingerprint: str
    title: str
    description: str
    severity: LogLevel
    service: str
    host: str
    status: AlertStatus = AlertStatus.OPEN
    occurrence_count: int = 1
    first_seen: datetime
    last_seen: datetime
    error_code: str | None = None
    trace_id: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    sample_message: str = ""
    is_new: bool = True  # False when this is a dedup update for an existing incident

    def to_dispatch_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
