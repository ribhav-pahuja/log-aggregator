"""Application configuration loaded from environment / .env."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Historical env var — never selected a real store after Quix keyed-state dedup.
# Reject redis; warn and ignore everything else so old .env files still boot.
_REMOVED_DEDUP_BACKEND_ENV = "DEDUP_BACKEND"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Only Quix is supported (aliases kept for older .env values)
    pipeline_runtime: Literal["quix", "quixstreams"] = "quix"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_input_topic: str = "logs"
    kafka_dlq_topic: str = "logs-dlq"
    kafka_dlq_enabled: bool = True
    kafka_consumer_group: str = "alert-pipeline"
    kafka_auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    kafka_auto_create_topics: bool = False

    alert_config_path: str = "config/alerts.yaml"

    # Window/refire fallbacks when YAML is missing a field (not a "backend" switch).
    # Production windows come from config/alerts.yaml via Quix enrichment.
    dedup_window_seconds: int = Field(default=300, ge=1)
    dedup_update_interval_seconds: int = Field(default=60, ge=1)
    alert_min_level: str = "ERROR"

    # Dedup is not configurable via env:
    #   production → Quix keyed state (runtime/quix_runtime.py)
    #   unit tests → in-process MemoryDedupStore (DedupEngine)
    # DEDUP_BACKEND was removed (see _reject_removed_dedup_backend).

    # PostgreSQL only (SQLAlchemy + psycopg). SQLite is not supported.
    database_url: str = "postgresql+psycopg://alerts:alerts@localhost:5432/alerts"

    dispatch_enabled: bool = True
    # outbox = enqueue on emit path + separate worker (default, production)
    # inline = sync fan-out on emit (tests / single-process demos)
    dispatch_mode: Literal["outbox", "inline"] = "outbox"
    dispatch_outbox_poll_seconds: float = Field(default=1.0, ge=0.1)
    dispatch_outbox_batch_size: int = Field(default=50, ge=1, le=500)
    dispatch_outbox_max_attempts: int = Field(default=8, ge=1)
    dispatch_outbox_stale_processing_seconds: int = Field(default=120, ge=10)

    dispatch_zenduty_enabled: bool = False
    zenduty_integration_key: str = ""
    zenduty_api_url: str = "https://www.zenduty.com/api/events"

    dispatch_teams_enabled: bool = False
    teams_webhook_url: str = ""

    dispatch_webhook_enabled: bool = False
    webhook_url: str = ""
    webhook_headers_json: str = "{}"

    # Demo API rate limits (per process, sliding window)
    demo_rate_limit_per_minute: int = Field(default=30, ge=1)

    # UI read cache only (not pipeline dedup)
    redis_url: str = "redis://localhost:6379/0"
    ui_cache_ttl_seconds: float = Field(default=10.0, ge=1.0)
    ui_cache_lock_ttl_seconds: float = Field(default=5.0, ge=1.0)
    ui_cache_max_alerts: int = Field(default=2000, ge=100)
    ui_cache_key_prefix: str = "alert_ui"
    ui_cache_invalidate_on_write: bool = True

    # --- Grafana ingress (optional bridge → Kafka logs topic) ---
    # Run with: grafana-source  (see sources/grafana/)
    # mode: loki = poll LogQL; webhook = Grafana Alerting contact point; both
    grafana_source_mode: Literal["loki", "webhook", "both"] = "loki"
    grafana_loki_url: str = ""  # e.g. http://loki:3100 or Grafana Cloud Loki URL
    grafana_loki_query: str = '{job=~".+"}'
    grafana_loki_poll_seconds: float = Field(default=15.0, ge=1.0)
    grafana_loki_lookback_seconds: int = Field(default=60, ge=1)
    grafana_loki_limit: int = Field(default=1000, ge=1, le=5000)
    grafana_loki_username: str = ""
    grafana_loki_password: str = ""
    grafana_loki_bearer_token: str = ""
    grafana_loki_org_id: str = ""  # multi-tenant: X-Scope-OrgID header
    grafana_webhook_host: str = "0.0.0.0"
    grafana_webhook_port: int = Field(default=8090, ge=1, le=65535)
    grafana_webhook_path: str = "/grafana/webhook"

    log_level: str = "INFO"

    @field_validator("pipeline_runtime", mode="before")
    @classmethod
    def _normalize_runtime(cls, v: object) -> str:
        if v is None or v == "":
            return "quix"
        name = str(v).strip().lower()
        if name in ("flink", "pyflink"):
            raise ValueError("PIPELINE_RUNTIME=flink is no longer supported. Use quix (default).")
        if name in ("quixstreams",):
            return "quixstreams"
        return name

    @field_validator("database_url", mode="before")
    @classmethod
    def _require_postgres(cls, v: object) -> str:
        url = str(v or "").strip()
        if not url:
            raise ValueError(
                "DATABASE_URL is required "
                "(e.g. postgresql+psycopg://alerts:alerts@localhost:5432/alerts)"
            )
        low = url.lower()
        if "sqlite" in low:
            raise ValueError(
                "SQLite is not supported. Use PostgreSQL, e.g. "
                "postgresql+psycopg://alerts:alerts@localhost:5432/alerts "
                "(Compose: host `postgres`, host processes: `localhost`)."
            )
        if not low.startswith("postgresql"):
            raise ValueError(
                f"DATABASE_URL must be a PostgreSQL URL (postgresql+psycopg://...), got {url!r}"
            )
        return url

    @model_validator(mode="after")
    def _reject_removed_dedup_backend(self) -> Self:
        """Fail closed on DEDUP_BACKEND=redis; warn if process env still sets it."""
        # Process env (Compose/K8s/export) — the real footgun surface.
        raw = (os.environ.get(_REMOVED_DEDUP_BACKEND_ENV) or "").strip()
        # Also catch redis left only in a local .env (not exported).
        if not raw:
            try:
                from dotenv import dotenv_values

                raw = (dotenv_values(".env").get(_REMOVED_DEDUP_BACKEND_ENV) or "").strip()
                # Don't warn about harmless leftovers in .env; only fail redis.
                if raw and raw.lower() != "redis":
                    return self
            except Exception:  # noqa: BLE001
                return self
        if not raw:
            return self
        if raw.lower() == "redis":
            raise ValueError(
                "DEDUP_BACKEND=redis was removed. Production dedup is always Quix "
                "keyed state (group_by fingerprint). Unit tests use in-process memory. "
                "Redis remains for UI cache only (REDIS_URL). Remove DEDUP_BACKEND from "
                "your environment."
            )
        # Only warn when the shell/container still injects the var (not silent .env).
        if _REMOVED_DEDUP_BACKEND_ENV in os.environ:
            logger.warning(
                "%s=%s is ignored and deprecated — dedup is always Quix keyed state in "
                "production (in-process memory only for unit tests). Remove %s from your "
                "environment.",
                _REMOVED_DEDUP_BACKEND_ENV,
                raw,
                _REMOVED_DEDUP_BACKEND_ENV,
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
