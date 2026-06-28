"""Runtime port — implement this to add a new stream engine."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from alert_pipeline.config import Settings


@runtime_checkable
class StreamRuntime(Protocol):
    """
    A stream runtime wires Kafka (or another source) to ``AlertProcessor``.

    Implementations must not embed dedup/DB/dispatch logic — only I/O and
    parallelism. That keeps Flink and Quix interchangeable.
    """

    name: str

    def run(self, settings: Settings) -> None:
        """Block and process until shutdown."""
        ...
