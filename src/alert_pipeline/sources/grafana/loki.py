"""Grafana Loki client and poll source (normalize only; sink is final adapter)."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import httpx

from alert_pipeline.config import Settings
from alert_pipeline.sources.base import BaseLogSource, LogSink, NormalizedLog
from alert_pipeline.sources.grafana.normalize import normalize_loki_query_response

logger = logging.getLogger(__name__)


class LokiClient:
    """Minimal Loki HTTP client (query_range)."""

    def __init__(
        self,
        base_url: str,
        *,
        username: str = "",
        password: str = "",
        bearer_token: str = "",
        org_id: str = "",
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        headers: dict[str, str] = {"Accept": "application/json"}
        if org_id:
            headers["X-Scope-OrgID"] = org_id
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"
        auth = None
        if username or password:
            auth = httpx.BasicAuth(username, password)
        self._client = httpx.Client(
            base_url=self.base_url,
            headers=headers,
            auth=auth,
            timeout=timeout,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def query_range(
        self,
        query: str,
        *,
        start: datetime,
        end: datetime,
        limit: int = 1000,
        direction: str = "forward",
    ) -> dict[str, Any]:
        """GET /loki/api/v1/query_range — times as nanosecond epoch strings."""
        params = {
            "query": query,
            "start": _to_ns(start),
            "end": _to_ns(end),
            "limit": str(limit),
            "direction": direction,
        }
        url = urljoin(self.base_url, "loki/api/v1/query_range")
        resp = self._client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected Loki response type: {type(data)}")
        return data


def _to_ns(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return str(int(dt.timestamp() * 1_000_000_000))


class LokiSource(BaseLogSource):
    """Poll Loki with LogQL; emit normalized logs through the shared sink.

    Cursor advances by wall-clock end time of each successful poll so the same
    window is not re-queried. Overlap of one second avoids edge-gaps at poll
    boundaries; Kafka/pipeline must tolerate at-least-once duplicates.
    """

    name = "grafana-loki"

    def __init__(
        self,
        settings: Settings,
        sink: LogSink,
        *,
        client: LokiClient | None = None,
    ) -> None:
        if not settings.grafana_loki_url:
            raise ValueError(
                "GRAFANA_LOKI_URL is required for the Loki source "
                "(e.g. http://loki:3100 or https://logs-prod.grafana.net)"
            )
        super().__init__(sink)
        self.settings = settings
        self.client = client or LokiClient(
            settings.grafana_loki_url,
            username=settings.grafana_loki_username,
            password=settings.grafana_loki_password,
            bearer_token=settings.grafana_loki_bearer_token,
            org_id=settings.grafana_loki_org_id,
        )
        self.query = settings.grafana_loki_query
        self.poll_seconds = float(settings.grafana_loki_poll_seconds)
        self.lookback_seconds = int(settings.grafana_loki_lookback_seconds)
        self.limit = int(settings.grafana_loki_limit)
        self._cursor: datetime | None = None
        self._seen_keys: set[str] = set()
        self._seen_max = 50_000

    def poll_once(self, *, now: datetime | None = None) -> list[NormalizedLog]:
        """Run one query_range and emit new events. Returns emitted events."""
        now = now or datetime.now(timezone.utc)
        if self._cursor is None:
            start = now - timedelta(seconds=self.lookback_seconds)
        else:
            start = self._cursor - timedelta(seconds=1)
        end = now

        try:
            payload = self.client.query_range(
                self.query,
                start=start,
                end=end,
                limit=self.limit,
                direction="forward",
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Loki query_range failed: %s", exc)
            return []

        events = normalize_loki_query_response(payload)
        published: list[NormalizedLog] = []
        for event in events:
            dedupe_key = _event_dedupe_key(event)
            if dedupe_key in self._seen_keys:
                continue
            self._remember(dedupe_key)
            try:
                self.emit(event)
                published.append(event)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to emit Loki event: %s", exc)

        if published:
            self.flush(5)
            logger.info(
                "Loki source emitted=%s query=%r window=%s..%s",
                len(published),
                self.query,
                start.isoformat(),
                end.isoformat(),
            )
        else:
            logger.debug(
                "Loki source no new events query=%r window=%s..%s raw=%s",
                self.query,
                start.isoformat(),
                end.isoformat(),
                len(events),
            )

        self._cursor = end
        return published

    def _remember(self, key: str) -> None:
        self._seen_keys.add(key)
        if len(self._seen_keys) > self._seen_max:
            for drop in list(self._seen_keys)[: self._seen_max // 2]:
                self._seen_keys.discard(drop)

    def run(self) -> None:
        logger.info(
            "Starting Grafana Loki source url=%s query=%r poll=%ss lookback=%ss",
            self.settings.grafana_loki_url,
            self.query,
            self.poll_seconds,
            self.lookback_seconds,
        )
        try:
            while True:
                self.poll_once()
                time.sleep(self.poll_seconds)
        except KeyboardInterrupt:
            logger.info("Loki source stopping…")

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:  # noqa: BLE001
            pass


# Back-compat alias
LokiPollBridge = LokiSource


def _event_dedupe_key(event: NormalizedLog) -> str:
    raw = event.get("raw") or {}
    ts_ns = ""
    line = event.get("message") or ""
    if isinstance(raw, dict):
        ts_ns = str(raw.get("timestamp_ns") or "")
        line = str(raw.get("line") or line)
    service = event.get("service") or ""
    return f"{ts_ns}|{service}|{line}"
