"""Fingerprint + deduplication window engine.

Window/refire rules live in ``transition.apply_dedup_transition`` (shared by
Quix keyed state and the in-process DedupEngine).
"""

from alert_pipeline.dedup.engine import DedupEngine
from alert_pipeline.dedup.fingerprint import compute_fingerprint
from alert_pipeline.dedup.store import DedupStore, MemoryDedupStore, build_memory_dedup_store
from alert_pipeline.dedup.transition import apply_dedup_transition

__all__ = [
    "DedupEngine",
    "DedupStore",
    "MemoryDedupStore",
    "apply_dedup_transition",
    "build_memory_dedup_store",
    "compute_fingerprint",
]
