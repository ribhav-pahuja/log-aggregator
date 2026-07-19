"""Source interface + shared types for multi-ingress log adapters.

Design
------
* **Sources** know only how to pull/receive and *normalize* into
  :class:`NormalizedLog`. They never talk to Kafka (or any bus) directly.
* **Sinks** are the final adapter. Today that is almost always
  :class:`~alert_pipeline.sources.kafka_sink.KafkaLogSink` -> ``logs`` topic.
* Wire them with :func:`run_sources` (or construct sources with a shared sink).

Adding a new ingress::

    class MySource(BaseLogSource):
        name = "my-system"

        def run(self) -> None:
            for event in self._poll_and_normalize():
                self.emit(event)
            self.flush()
"""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from typing import Protocol, TypedDict, runtime_checkable

from alert_pipeline.types import JsonObject

logger = logging.getLogger(__name__)


class NormalizedLog(TypedDict, total=False):
    """Wire-compatible log event (same shape as Kafka ``logs`` messages).

    Required for a useful incident: ``level``, ``service``, ``message``.
    Extra keys are preserved; ``LogEvent.from_kafka_value`` maps common aliases.
    """

    timestamp: str
    level: str
    service: str
    host: str
    message: str
    error_code: str | None
    trace_id: str | None
    labels: dict[str, str]
    raw: JsonObject
    # Provenance — not used for fingerprint by default unless listed in YAML
    source: str


@runtime_checkable
class LogSink(Protocol):
    """Final adapter: accept normalized logs (Kafka today, anything tomorrow)."""

    def publish(self, event: NormalizedLog | JsonObject, *, key: str | None = None) -> None:
        """Send one normalized event downstream."""
        ...

    def flush(self, timeout: float = 10.0) -> None:
        """Block until buffered events are delivered (best-effort)."""
        ...

    def close(self) -> None:
        """Flush and release resources."""
        ...


@runtime_checkable
class LogSource(Protocol):
    """Ingress that produces normalized logs via a sink (or its own run loop)."""

    name: str

    def run(self) -> None:
        """Block until shutdown (Ctrl+C / signal)."""
        ...

    def close(self) -> None:
        """Release source-owned resources (not the shared sink)."""
        ...


class BaseLogSource(ABC):
    """Base class for ingress sources.

    Subclasses implement :meth:`run` (and optionally :meth:`close`).
    Publishing always goes through the injected :class:`LogSink` — the final
    adapter — so sources stay free of Kafka (or other bus) details.
    """

    name: str = "source"

    def __init__(self, sink: LogSink) -> None:
        self._sink = sink

    @property
    def sink(self) -> LogSink:
        return self._sink

    def emit(self, event: NormalizedLog, *, key: str | None = None) -> None:
        """Publish one normalized event through the final adapter."""
        pub_key = key if key is not None else (event.get("service") or "unknown")
        self._sink.publish(event, key=str(pub_key))

    def emit_many(
        self,
        events: Iterable[NormalizedLog],
        *,
        flush: bool = True,
    ) -> int:
        """Publish many events; optionally flush once at the end. Returns count."""
        n = 0
        for event in events:
            self.emit(event)
            n += 1
        if flush and n:
            self.flush()
        return n

    def flush(self, timeout: float = 10.0) -> None:
        self._sink.flush(timeout)

    @abstractmethod
    def run(self) -> None:
        """Block and produce events until shutdown."""

    def close(self) -> None:
        """Override to release source resources. Does **not** close the sink."""


def run_sources(
    sources: Sequence[LogSource],
    *,
    sink: LogSink | None = None,
    background: Sequence[str] | None = None,
) -> None:
    """Run one or more sources sharing a sink.

    * Sources whose ``name`` appears in ``background`` start in daemon threads.
    * The first non-background source blocks the main thread.
    * If every source is backgrounded, the main thread waits on those threads.
    * ``sink`` is closed on exit when provided (sources must not close it).
    """
    if not sources:
        raise ValueError("run_sources requires at least one source")

    bg_names = set(background or ())
    threads: list[threading.Thread] = []
    blocking: list[LogSource] = []

    for src in sources:
        if src.name in bg_names:
            t = threading.Thread(target=src.run, name=f"source-{src.name}", daemon=True)
            t.start()
            threads.append(t)
            logger.info("Started source %s in background thread", src.name)
        else:
            blocking.append(src)

    for src in blocking[1:]:
        t = threading.Thread(target=src.run, name=f"source-{src.name}", daemon=True)
        t.start()
        threads.append(t)
        logger.info("Started extra source %s in background thread", src.name)

    try:
        if blocking:
            logger.info(
                "Running primary source=%s (+%s background)",
                blocking[0].name,
                len(threads),
            )
            blocking[0].run()
        else:
            logger.info("All sources backgrounded; waiting on %s threads", len(threads))
            try:
                while any(t.is_alive() for t in threads):
                    for t in threads:
                        t.join(timeout=0.5)
            except KeyboardInterrupt:
                logger.info("Sources interrupted")
    finally:
        for src in sources:
            try:
                src.close()
            except Exception:  # noqa: BLE001
                logger.warning(
                    "Error closing source %s",
                    getattr(src, "name", src),
                    exc_info=True,
                )
        if sink is not None:
            try:
                sink.close()
            except Exception:  # noqa: BLE001
                logger.warning("Error closing sink", exc_info=True)
