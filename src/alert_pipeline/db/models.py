"""SQLAlchemy ORM models for persisted alerts and dispatch audit log."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class AlertRecord(Base):
    """One row per deduplicated incident (open incidents keyed by fingerprint in app logic)."""

    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    service: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    host: Mapped[str] = mapped_column(String(256), nullable=False, default="unknown")
    status: Mapped[str] = mapped_column(String(32), index=True, nullable=False, default="open")
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    trace_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    labels_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    sample_message: Mapped[str] = mapped_column(Text, nullable=False, default="")

    # Operator timeline + SLIs
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # TTA = acknowledged_at - first_seen (seconds); null until acked (or resolved without prior ack)
    tta_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # TTR = resolved_at - first_seen (seconds); null until resolved
    ttr_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )


class DispatchLog(Base):
    """Audit trail of outbound notifications."""

    __tablename__ = "dispatch_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_id: Mapped[str] = mapped_column(String(36), index=True, nullable=False)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    success: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )
