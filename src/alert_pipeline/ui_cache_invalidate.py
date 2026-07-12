"""Invalidate the operator UI Redis snapshot after pipeline writes.

Keeps UI lag bounded when incidents change outside the UI process.
Best-effort: failures are logged and never fail the pipeline.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Must match alert_cache.REDIS_SNAPSHOT_KEY default pattern
_SNAPSHOT_SUFFIX = "snapshot"


def invalidate_ui_snapshot(
    redis_url: str,
    *,
    key_prefix: str = "alert_ui",
) -> None:
    if not redis_url:
        return
    try:
        import redis

        client = redis.Redis.from_url(redis_url, decode_responses=True)
        # alert_cache uses f-string alert_ui:snapshot via constants — keep compatible
        keys = [
            f"{key_prefix.rstrip(':')}:snapshot",
            f"{key_prefix.rstrip(':')}:snapshot:lock",
        ]
        client.delete(*keys)
        client.close()
        logger.debug("Invalidated UI cache keys %s", keys)
    except Exception as exc:  # noqa: BLE001
        logger.warning("UI cache invalidate failed: %s", exc)
