"""Dead-letter helpers shared by stream runtimes."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def unwrap_unparseable_marker(payload: Any) -> tuple[bool, Any]:
    """If payload is the safe-deserializer marker, return (True, raw)."""
    if isinstance(payload, dict) and payload.get("__unparseable__") is True:
        return True, payload.get("raw")
    return False, payload
