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

__all__ = [
    "TTLStore",
    "get_stored_data",
    "get_stored_dataframe",
    "update_stored_dataframe",
]
