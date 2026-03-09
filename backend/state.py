"""
state.py — Unified Data Access Layer.

Single stable module that every API handler imports from. Replaces fragmented
dict stores, raw orchestrator imports, and cross-module store references with
a clean, TTL-backed public contract.

Usage:
    from state import get_df, set_df, get_profile, get_cleaning_state
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

import os
import diskcache
from collections.abc import MutableMapping

from ingestion.orchestrator import (
    get_stored_data,
    get_stored_dataframe,
    update_stored_dataframe,
)

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".prism_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

class TTLStore(MutableMapping):
    """
    Disk-backed store with TTL-based eviction.
    Drop-in replacement for dict[str, dict] to enable cross-session persistence.
    """
    def __init__(self, name: str, max_entries: int = 50, ttl_seconds: float = 7200):
        self.cache = diskcache.Cache(
            os.path.join(CACHE_DIR, name),
            size_limit=int(5 * 1024**3), # 5GB limit per cache
            eviction_policy="none"       # We use manual TTL
        )
        self.ttl_seconds = ttl_seconds

    def __setitem__(self, key: str, value: Any):
        self.cache.set(key, value, expire=self.ttl_seconds)

    def __getitem__(self, key: str):
        val = self.cache.get(key)
        if val is None and key not in self.cache:
            raise KeyError(key)
        return val

    def __delitem__(self, key: str):
        if not self.cache.delete(key):
            raise KeyError(key)

    def __iter__(self):
        return iter(self.cache)

    def __len__(self):
        return len(self.cache)

    def __contains__(self, key):
        return key in self.cache
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.cache.get(key, default=default)

# ── Centralised TTL stores ────────────────────────────────────────────
profile_store   = TTLStore("profiles", max_entries=50,  ttl_seconds=3600*24*7) # 7 days
cleaning_store  = TTLStore("cleaning", max_entries=50,  ttl_seconds=3600*24*7)
tag_store       = TTLStore("tags", max_entries=500, ttl_seconds=3600*24*30)
annotation_store = TTLStore("annotations", max_entries=500, ttl_seconds=3600*24*30)
recipe_store    = TTLStore("recipes", max_entries=500, ttl_seconds=3600*24*365)
watchlist_store = TTLStore("watchlist", max_entries=1, ttl_seconds=3600*24*30)


# ── Convenience accessors ─────────────────────────────────────────────

def get_df(file_id: str) -> Optional[pd.DataFrame]:
    """Get the stored DataFrame for a file_id. Returns None if not found."""
    return get_stored_dataframe(file_id)


def set_df(file_id: str, df: pd.DataFrame) -> None:
    """Update / replace the stored DataFrame for a file_id."""
    update_stored_dataframe(file_id, df)


def get_meta(file_id: str) -> Optional[dict]:
    """Get the full ingestion metadata envelope for a file_id."""
    return get_stored_data(file_id)


def get_profile(file_id: str) -> Optional[dict]:
    """Get the profiling result dict for a file_id. Returns None if not profiled."""
    return profile_store.get(file_id)


def set_profile(file_id: str, result: Any) -> None:
    """Store a profiling result for a file_id."""
    profile_store[file_id] = result


def get_cleaning_state(file_id: str) -> Optional[dict]:
    """Get the cleaning engine + plan for a file_id."""
    return cleaning_store.get(file_id)


def set_cleaning_state(file_id: str, state: dict) -> None:
    """Store the cleaning engine + plan for a file_id."""
    cleaning_store[file_id] = state


# ── Backward-compat exports ──────────────────────────────────────────
# These keep existing `from state import get_stored_dataframe` working
# while callers migrate to the short names above.

__all__ = [
    # New stable API
    "get_df", "set_df",
    "get_meta",
    "get_profile", "set_profile",
    "get_cleaning_state", "set_cleaning_state",
    # Stores (for direct access when needed)
    "TTLStore",
    "profile_store", "cleaning_store",
    "tag_store", "annotation_store", "recipe_store", "watchlist_store",
    # Legacy re-exports (still used widely)
    "get_stored_data", "get_stored_dataframe", "update_stored_dataframe",
]
