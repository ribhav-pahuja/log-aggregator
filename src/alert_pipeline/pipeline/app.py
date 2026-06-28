"""Backward-compatible Quix entry; prefer ``alert_pipeline.runtime.run_pipeline``."""

from __future__ import annotations

from alert_pipeline.config import Settings, get_settings
from alert_pipeline.runtime.quix_runtime import QuixStreamRuntime


def build_application(settings: Settings | None = None):
    raise NotImplementedError(
        "Use alert_pipeline.runtime.run_pipeline() or QuixStreamRuntime().run(settings)."
    )


def run(settings: Settings | None = None) -> None:
    QuixStreamRuntime().run(settings or get_settings())
