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

from ingestion.orchestrator import (
    TTLStore,
    get_stored_data,
    get_stored_dataframe,
    update_stored_dataframe,
)

# ── Centralised TTL stores ────────────────────────────────────────────
profile_store   = TTLStore(max_entries=50,  ttl_seconds=3600)
cleaning_store  = TTLStore(max_entries=50,  ttl_seconds=7200)
tag_store       = TTLStore(max_entries=500, ttl_seconds=86400)
annotation_store = TTLStore(max_entries=500, ttl_seconds=86400)
recipe_store    = TTLStore(max_entries=500, ttl_seconds=86400 * 30)


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
    "tag_store", "annotation_store", "recipe_store",
    # Legacy re-exports (still used widely)
    "get_stored_data", "get_stored_dataframe", "update_stored_dataframe",
]
