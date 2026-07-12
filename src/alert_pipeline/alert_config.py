"""Load alert refiring / dedup configuration from YAML."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

from alert_pipeline.schemas import LogEvent, LogLevel

DEFAULT_DEDUP_FIELDS: tuple[str, ...] = ("service", "level", "labels", "message")

logger = logging.getLogger(__name__)

DEFAULT_SEARCH_PATHS = (
    Path(os.environ.get("ALERT_CONFIG_PATH", "")),
    Path("/config/alerts.yaml"),
    Path("config/alerts.yaml"),
    Path(__file__).resolve().parents[2] / "config" / "alerts.yaml",
)


class RefireSettings(BaseModel):
    """Effective settings used for one log event / incident."""

    min_level: str = "ERROR"
    dedup_window_seconds: int = Field(default=300, ge=1)
    refire_interval_seconds: int = Field(default=60, ge=1)
    suppress_dispatch_while_acknowledged: bool = True
    allow_reopen_after_resolve: bool = True
    # Fields that form the dedup fingerprint (order matters only for readability).
    dedup_fields: list[str] = Field(default_factory=lambda: list(DEFAULT_DEDUP_FIELDS))

    @field_validator("min_level", mode="before")
    @classmethod
    def _upper_level(cls, v: Any) -> str:
        return str(v or "ERROR").upper()

    @field_validator("dedup_fields", mode="before")
    @classmethod
    def _coerce_fields(cls, v: Any) -> list[str]:
        if v is None:
            return list(DEFAULT_DEDUP_FIELDS)
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        if isinstance(v, (list, tuple)):
            out = [str(x).strip() for x in v if str(x).strip()]
            return out or list(DEFAULT_DEDUP_FIELDS)
        return list(DEFAULT_DEDUP_FIELDS)

    @property
    def min_level_enum(self) -> LogLevel:
        return LogLevel.normalize(self.min_level)


class AlertYamlConfig(BaseModel):
    version: int = 1
    defaults: RefireSettings = Field(default_factory=RefireSettings)
    # Partial overlays — only keys present in YAML are applied on top of defaults
    services: dict[str, dict[str, Any]] = Field(default_factory=dict)
    error_codes: dict[str, dict[str, Any]] = Field(default_factory=dict)

    def resolve_for(self, event: LogEvent) -> RefireSettings:
        """Merge defaults <- service partial <- error_code partial."""
        data: dict[str, Any] = self.defaults.model_dump()
        if event.service in self.services:
            data.update(self.services[event.service])
        if event.error_code and event.error_code in self.error_codes:
            data.update(self.error_codes[event.error_code])
        return RefireSettings(**data)

    def resolve_for_service(self, service: str, error_code: str | None = None) -> RefireSettings:
        ev = LogEvent(service=service, error_code=error_code, message="", level=LogLevel.ERROR)
        return self.resolve_for(ev)


def _partial_layer(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Keep only known RefireSettings keys that were actually set in YAML."""
    if not raw or not isinstance(raw, dict):
        return {}
    if "update_interval_seconds" in raw and "refire_interval_seconds" not in raw:
        raw = {**raw, "refire_interval_seconds": raw["update_interval_seconds"]}
    allowed = set(RefireSettings.model_fields.keys())
    return {k: v for k, v in raw.items() if k in allowed}


def _parse_defaults(raw: dict[str, Any] | None) -> RefireSettings:
    partial = _partial_layer(raw)
    return RefireSettings(**partial) if partial else RefireSettings()


def load_alert_config(path: str | Path | None = None) -> AlertYamlConfig:
    candidates: list[Path] = []
    if path:
        candidates.append(Path(path))
    candidates.extend(p for p in DEFAULT_SEARCH_PATHS if str(p))

    for candidate in candidates:
        if candidate.is_file():
            with candidate.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            cfg = AlertYamlConfig(
                version=int(data.get("version") or 1),
                defaults=_parse_defaults(data.get("defaults")),
                services={
                    str(k): _partial_layer(v if isinstance(v, dict) else {})
                    for k, v in (data.get("services") or {}).items()
                },
                error_codes={
                    str(k): _partial_layer(v if isinstance(v, dict) else {})
                    for k, v in (data.get("error_codes") or {}).items()
                },
            )
            logger.info(
                "Loaded alert config from %s (default dedup_fields=%s)",
                candidate,
                cfg.defaults.dedup_fields,
            )
            return cfg

    logger.warning("No alerts.yaml found; using built-in defaults")
    return AlertYamlConfig()


@lru_cache
def get_alert_config() -> AlertYamlConfig:
    return load_alert_config()


def reload_alert_config() -> AlertYamlConfig:
    get_alert_config.cache_clear()
    return get_alert_config()
