"""
state.py — Stable public contract for shared application state.

All API modules should import from here instead of reaching into
ingestion.orchestrator internals. This isolates callers from storage
implementation changes (e.g. switching from dict to TTLStore).
"""

from ingestion.orchestrator import (
    TTLStore,
    get_stored_data,
    get_stored_dataframe,
    update_stored_dataframe,
)

# Standardized TTL stores for all sub-systems
profile_store = TTLStore(max_entries=50, ttl_seconds=3600)
tag_store = TTLStore(max_entries=500, ttl_seconds=86400) # metadata tags
annotation_store = TTLStore(max_entries=500, ttl_seconds=86400) # collab annotations
recipe_store = TTLStore(max_entries=500, ttl_seconds=86400 * 30) # recipes last longer

__all__ = [
    "TTLStore",
    "get_stored_data",
    "get_stored_dataframe",
    "update_stored_dataframe",
    "profile_store",
    "tag_store",
    "annotation_store",
    "recipe_store",
]
