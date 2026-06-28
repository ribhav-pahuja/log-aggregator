"""Deduplication helpers."""

from alert_pipeline.dedup.fingerprint import compute_fingerprint

__all__ = ["compute_fingerprint", "DedupEngine"]


def __getattr__(name: str):
    if name == "DedupEngine":
        from alert_pipeline.dedup.engine import DedupEngine

        return DedupEngine
    raise AttributeError(name)
