"""Select stream runtime by name / env."""

from __future__ import annotations

import logging

from alert_pipeline.config import Settings, get_settings
from alert_pipeline.runtime.base import StreamRuntime
from alert_pipeline.runtime.quix_runtime import QuixStreamRuntime

logger = logging.getLogger(__name__)

_RUNTIMES: dict[str, type] = {
    "quix": QuixStreamRuntime,
    "quixstreams": QuixStreamRuntime,
}


def get_runtime(name: str | None = None, settings: Settings | None = None) -> StreamRuntime:
    settings = settings or get_settings()
    key = (name or settings.pipeline_runtime or "quix").strip().lower()
    cls = _RUNTIMES.get(key)
    if cls is None:
        known = ", ".join(sorted(set(_RUNTIMES)))
        raise ValueError(
            f"Unknown pipeline runtime {key!r}. Choose one of: {known}. "
            f"(Flink support has been removed; use quix.)"
        )
    runtime = cls()
    logger.info("Using stream runtime: %s", runtime.name)
    return runtime


def run_pipeline(settings: Settings | None = None, runtime_name: str | None = None) -> None:
    settings = settings or get_settings()
    get_runtime(runtime_name, settings).run(settings)
