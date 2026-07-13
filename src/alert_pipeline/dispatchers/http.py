"""Shared HTTP client for outbound alert dispatch.

Opening ``httpx.Client`` per request burns TLS handshakes under outbox drain.
Use :func:`get_dispatch_http_client` (process singleton) or inject a client
owned by the outbox worker / tests.
"""

from __future__ import annotations

import threading
from typing import Final

import httpx

DEFAULT_TIMEOUT_SECONDS: Final[float] = 15.0

_lock = threading.Lock()
_process_client: httpx.Client | None = None


def create_dispatch_http_client(
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> httpx.Client:
    """Build a connection-pooling client for dispatch POST traffic."""
    return httpx.Client(
        timeout=timeout,
        limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        follow_redirects=True,
    )


def get_dispatch_http_client() -> httpx.Client:
    """Return a process-wide client, creating it on first use."""
    global _process_client
    with _lock:
        if _process_client is None or _process_client.is_closed:
            _process_client = create_dispatch_http_client()
        return _process_client


def close_dispatch_http_client() -> None:
    """Close the process-wide client (call on worker shutdown)."""
    global _process_client
    with _lock:
        if _process_client is not None:
            _process_client.close()
            _process_client = None


def resolve_http_client(client: httpx.Client | None) -> httpx.Client:
    """Prefer an injected client; otherwise the process singleton."""
    if client is not None:
        return client
    return get_dispatch_http_client()
