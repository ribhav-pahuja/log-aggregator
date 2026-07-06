"""Stream runtime protocol."""

from __future__ import annotations

from typing import Protocol

from alert_pipeline.config import Settings


class StreamRuntime(Protocol):
    """Kafka consumer adapter. Business logic stays in AlertProcessor / Quix state."""

    name: str

    def run(self, settings: Settings) -> None:
        """Block and process the input topic until shutdown."""
        ...
