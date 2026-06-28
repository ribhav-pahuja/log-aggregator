"""CLI entrypoint: ``alert-pipeline`` — runtime selected via PIPELINE_RUNTIME."""

from __future__ import annotations

import logging
import sys

from alert_pipeline.config import get_settings
from alert_pipeline.runtime import run_pipeline


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.info("pipeline_runtime=%s", settings.pipeline_runtime)
    run_pipeline(settings)


if __name__ == "__main__":
    main()
