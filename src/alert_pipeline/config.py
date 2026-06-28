"""Application configuration loaded from environment / .env."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    pipeline_runtime: Literal["quix", "flink", "quixstreams", "pyflink"] = "quix"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_input_topic: str = "logs"
    kafka_consumer_group: str = "alert-pipeline"
    kafka_auto_offset_reset: Literal["earliest", "latest"] = "earliest"

    alert_config_path: str = "config/alerts.yaml"

    dedup_window_seconds: int = Field(default=300, ge=1)
    dedup_update_interval_seconds: int = Field(default=60, ge=1)
    alert_min_level: str = "ERROR"

    database_url: str = "sqlite+pysqlite:////tmp/alerts.db"

    flink_parallelism: int = Field(default=1, ge=1)
    flink_print_results: bool = False

    dispatch_enabled: bool = True
    dispatch_zenduty_enabled: bool = False
    zenduty_integration_key: str = ""
    zenduty_api_url: str = "https://www.zenduty.com/api/events"

    dispatch_teams_enabled: bool = False
    teams_webhook_url: str = ""

    dispatch_webhook_enabled: bool = False
    webhook_url: str = ""
    webhook_headers_json: str = "{}"

    # Shared UI read cache (Redis) — multi-instance consistent views
    redis_url: str = "redis://localhost:6379/0"
    ui_cache_ttl_seconds: float = Field(default=10.0, ge=1.0)
    ui_cache_lock_ttl_seconds: float = Field(default=5.0, ge=1.0)
    ui_cache_max_alerts: int = Field(default=2000, ge=100)
    ui_cache_key_prefix: str = "alert_ui"

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
