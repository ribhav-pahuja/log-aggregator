"""Application configuration loaded from environment / .env."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    dedup_window_seconds: int = Field(default=300, ge=1)
    dedup_update_interval_seconds: int = Field(default=60, ge=1)
    alert_min_level: str = "ERROR"

    # quix = Quix keyed state (production); memory = unit tests / in-process engine
    # redis = deprecated for dedup (ignored)
    dedup_backend: Literal["quix", "memory", "redis"] = "quix"

    database_url: str = "sqlite+pysqlite:////tmp/alerts.db"

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

    @field_validator("dedup_backend", mode="before")
    @classmethod
    def _normalize_backend(cls, v: object) -> str:
        if v is None or v == "":
            return "quix"
        name = str(v).strip().lower()
        if name in ("external", "quix-state", "state"):
            return "quix"
        return name


@lru_cache
def get_settings() -> Settings:
    return Settings()
