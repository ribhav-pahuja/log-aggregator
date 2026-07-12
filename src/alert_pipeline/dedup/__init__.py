"""Fingerprint + deduplication window engine."""

from alert_pipeline.dedup.engine import DedupEngine
from alert_pipeline.dedup.fingerprint import compute_fingerprint
from alert_pipeline.dedup.store import (
    DedupStore,
    MemoryDedupStore,
    RedisDedupStore,
    build_dedup_store,
)

__all__ = [
    "DedupEngine",
    "DedupStore",
    "MemoryDedupStore",
    "RedisDedupStore",
    "build_dedup_store",
    "compute_fingerprint",
]
