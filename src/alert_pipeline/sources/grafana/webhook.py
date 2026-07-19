"""HTTP webhook source for Grafana Alerting contact points."""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from alert_pipeline.config import Settings
from alert_pipeline.sources.base import BaseLogSource, LogSink
from alert_pipeline.sources.grafana.normalize import normalize_grafana_alert_webhook

logger = logging.getLogger(__name__)


class GrafanaWebhookSource(BaseLogSource):
    """Stdlib HTTP server — normalizes Alerting payloads and emits via sink.

    No FastAPI dependency (pipeline image friendly). Does not close the shared
    sink; the process owner / :func:`~alert_pipeline.sources.base.run_sources`
    does that.
    """

    name = "grafana-webhook"

    def __init__(
        self,
        settings: Settings,
        sink: LogSink,
    ) -> None:
        super().__init__(sink)
        self.settings = settings
        self.host = settings.grafana_webhook_host
        self.port = int(settings.grafana_webhook_port)
        path = settings.grafana_webhook_path or "/grafana/webhook"
        if not path.startswith("/"):
            path = "/" + path
        self.path = path.rstrip("/") or "/grafana/webhook"
        self._httpd: ThreadingHTTPServer | None = None

    def handle_payload(self, payload: dict[str, Any]) -> int:
        """Normalize webhook body and emit. Returns number of events published."""
        events = normalize_grafana_alert_webhook(payload)
        if not events:
            logger.warning("Grafana webhook: no alerts extracted from payload")
            return 0
        n = self.emit_many(events, flush=True)
        logger.info("Grafana webhook emitted=%s path=%s", n, self.path)
        return n

    def run(self) -> None:
        path = self.path
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: object) -> None:
                logger.debug("grafana-webhook " + fmt, *args)

            def _send(self, code: int, body: dict[str, Any]) -> None:
                raw = json.dumps(body).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path.rstrip("/") in (path, path + "/health", "/health", "/"):
                    self._send(200, {"ok": True, "service": "grafana-webhook", "path": path})
                    return
                self._send(404, {"ok": False, "error": "not_found"})

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                req_path = parsed.path.rstrip("/") or "/"
                if req_path != path and req_path != path.rstrip("/"):
                    self._send(404, {"ok": False, "error": "not_found"})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                if length <= 0:
                    self._send(400, {"ok": False, "error": "empty_body"})
                    return
                if length > 5_000_000:
                    self._send(413, {"ok": False, "error": "body_too_large"})
                    return
                try:
                    raw = self.rfile.read(length)
                    data = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    self._send(400, {"ok": False, "error": f"invalid_json: {exc}"})
                    return
                if not isinstance(data, dict):
                    self._send(400, {"ok": False, "error": "json_object_required"})
                    return
                try:
                    n = owner.handle_payload(data)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Grafana webhook emit failed")
                    self._send(500, {"ok": False, "error": str(exc)})
                    return
                self._send(200, {"ok": True, "published": n})

        self._httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        logger.info(
            "Grafana Alerting webhook listening on http://%s:%s%s",
            self.host,
            self.port,
            self.path,
        )
        try:
            self._httpd.serve_forever(poll_interval=0.5)
        except KeyboardInterrupt:
            logger.info("Grafana webhook stopping…")
        finally:
            self.close()

    def close(self) -> None:
        if self._httpd is not None:
            try:
                self._httpd.shutdown()
            except Exception:  # noqa: BLE001
                pass
            try:
                self._httpd.server_close()
            except Exception:  # noqa: BLE001
                pass
            self._httpd = None


# Back-compat aliases
GrafanaWebhookServer = GrafanaWebhookSource


def run_webhook_in_thread(server: GrafanaWebhookSource) -> None:
    """Deprecated: use :func:`alert_pipeline.sources.base.run_sources` instead."""
    import threading

    t = threading.Thread(target=server.run, name="grafana-webhook", daemon=True)
    t.start()
