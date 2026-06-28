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

    # Stream engine: quix (default) | flink
    pipeline_runtime: Literal["quix", "flink", "quixstreams", "pyflink"] = "quix"

    # Kafka
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

    # UI read cache (seconds). Reads hit memory; BG thread reloads from Postgres.
    ui_cache_ttl_seconds: float = Field(default=2.0, ge=0.2)
    ui_cache_max_alerts: int = Field(default=2000, ge=100)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
