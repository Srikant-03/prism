"""
state.py — Unified Data Access Layer.

Single stable module that every API handler imports from. Replaces fragmented
dict stores, raw orchestrator imports, and cross-module store references with
a clean, TTL-backed public contract.

Usage:
    from state import get_df, set_df, get_profile, get_cleaning_state
    from state import get_target_state, set_target_state, set_user_target
"""

from __future__ import annotations

from typing import Any, Optional, List

import pandas as pd

import os
import diskcache
from collections.abc import MutableMapping



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
profile_store    = TTLStore("profiles",    max_entries=50,  ttl_seconds=3600*24*7)  # 7 days
cleaning_store   = TTLStore("cleaning",   max_entries=50,  ttl_seconds=3600*24*7)
tag_store        = TTLStore("tags",        max_entries=500, ttl_seconds=3600*24*30)
annotation_store = TTLStore("annotations", max_entries=500, ttl_seconds=3600*24*30)
recipe_store     = TTLStore("recipes",     max_entries=500, ttl_seconds=3600*24*365)
watchlist_store  = TTLStore("watchlist",   max_entries=1,   ttl_seconds=3600*24*30)

# Authoritative target analysis state — written ONCE by the profiling engine,
# read by ml_readiness, story, feature_ranker, and any other downstream module.
# Never re-computed by consumers. User override via set_user_target().
target_store     = TTLStore("targets",    max_entries=50,  ttl_seconds=3600*24*7)  # 7 days


# ── TargetState schema (plain dict for JSON serializability) ──────────
#
#  target_column     : str | None   — canonical target column name
#  problem_type      : str | None   — 'regression' | 'binary_classification' | 'multiclass_classification'
#  confidence        : float        — heuristic confidence 0.0–1.0 (1.0 if user-defined)
#  is_user_defined   : bool         — True only when user explicitly selected
#  is_heuristic      : bool         — True when auto-detected, not yet user-confirmed
#  justification     : str          — human-readable reason for this choice
#  top_predictors    : list[dict]   — [{"feature": str, "importance_score": float}, ...]
#  class_distribution: dict | None  — class → proportion (classification only)
#  imbalance_ratio   : float | None — majority/minority ratio (classification only)

def _make_target_state(
    target_column: Optional[str],
    problem_type: Optional[str],
    confidence: float = 0.0,
    is_user_defined: bool = False,
    justification: str = "",
    top_predictors: Optional[List[dict]] = None,
    class_distribution: Optional[dict] = None,
    imbalance_ratio: Optional[float] = None,
) -> dict:
    """Build a canonical TargetState dict."""
    return {
        "target_column": target_column,
        "problem_type": problem_type,
        "confidence": round(float(confidence), 4),
        "is_user_defined": is_user_defined,
        "is_heuristic": not is_user_defined,
        "justification": justification,
        "top_predictors": top_predictors or [],
        "class_distribution": class_distribution,
        "imbalance_ratio": imbalance_ratio,
    }


# ── Convenience accessors ─────────────────────────────────────────────

def get_df(file_id: str) -> Optional[pd.DataFrame]:
    """Get the stored DataFrame for a file_id. Returns None if not found."""
    from ingestion.orchestrator import get_stored_dataframe
    return get_stored_dataframe(file_id)


def set_df(file_id: str, df: pd.DataFrame) -> None:
    """Update / replace the stored DataFrame for a file_id."""
    from ingestion.orchestrator import update_stored_dataframe
    update_stored_dataframe(file_id, df)


def get_meta(file_id: str) -> Optional[dict]:
    """Get the full ingestion metadata envelope for a file_id."""
    from ingestion.orchestrator import get_stored_data
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


# ── Target state accessors ────────────────────────────────────────────

def get_target_state(file_id: str) -> Optional[dict]:
    """
    Return the authoritative TargetState dict for a file_id.

    Returns None if target detection has not yet run for this file.

    Downstream modules (ml_readiness, story, feature_ranker) MUST call this
    instead of independently re-running TargetDetector or guessing from column order.
    """
    return target_store.get(file_id)


def set_target_state(file_id: str, target_analysis: Any) -> None:
    """
    Persist the result of TargetDetector.analyze() as the authoritative
    TargetState for this file_id.

    Called ONCE — by the profiling engine in _stage_cross_column_assembly.
    Never called by downstream consumers (ml_readiness, story, etc.).

    Args:
        file_id: Unique file identifier.
        target_analysis: TargetAnalysis Pydantic object returned by TargetDetector.analyze(),
                         or a plain dict with the same fields.
    """
    # Accept Pydantic model, legacy .dict(), or plain dict
    if hasattr(target_analysis, "model_dump"):
        ta = target_analysis.model_dump()
    elif hasattr(target_analysis, "dict"):
        ta = target_analysis.dict()
    elif isinstance(target_analysis, dict):
        ta = target_analysis
    else:
        ta = {}

    target_col = ta.get("target_column")
    if not target_col:
        # Detection ran but found nothing — write a "not detected" sentinel so
        # downstream modules know detection ran, rather than interpreting None
        # from target_store as "hasn't run yet".
        target_store[file_id] = _make_target_state(
            target_column=None,
            problem_type=None,
            confidence=0.0,
            is_user_defined=False,
            justification="No suitable target variable was detected by heuristic analysis.",
        )
        return

    # Normalize top_predictors (may be list of Pydantic objects or plain dicts)
    raw_preds = ta.get("top_predictors") or []
    predictors: List[dict] = []
    for p in raw_preds:
        if isinstance(p, dict):
            predictors.append({
                "feature": p.get("feature", ""),
                "importance_score": float(p.get("importance_score", 0)),
            })
        elif hasattr(p, "feature"):
            predictors.append({
                "feature": p.feature,
                "importance_score": float(p.importance_score),
            })

    state = _make_target_state(
        target_column=target_col,
        problem_type=ta.get("problem_type"),
        confidence=float(ta.get("confidence", 0.0)),
        is_user_defined=False,
        justification=ta.get("justification") or "Detected by heuristic analysis.",
        top_predictors=predictors,
        class_distribution=ta.get("class_distribution"),
        imbalance_ratio=ta.get("imbalance_ratio"),
    )
    target_store[file_id] = state


def set_user_target(file_id: str, target_column: str, df_columns: List[str]) -> dict:
    """
    Override the heuristic target with an explicit user selection.

    Called by POST /api/story/target (and any future UI target-picker endpoint).
    Sets is_user_defined=True and is_heuristic=False.

    Args:
        file_id: Unique file identifier.
        target_column: The column the user has explicitly selected as the target.
        df_columns: All column names in the dataset (for validation).

    Returns:
        The updated TargetState dict.

    Raises:
        ValueError: If target_column is not present in the dataset.
    """
    if target_column not in df_columns:
        raise ValueError(
            f"Column '{target_column}' does not exist in this dataset. "
            f"Valid columns: {df_columns}"
        )

    # Preserve top_predictors and problem_type from the heuristic run if available
    existing = target_store.get(file_id) or {}

    state = _make_target_state(
        target_column=target_column,
        problem_type=existing.get("problem_type"),
        confidence=1.0,                # user-defined → maximum confidence
        is_user_defined=True,
        justification="Explicitly selected by the user as the prediction target.",
        top_predictors=existing.get("top_predictors", []),
        class_distribution=existing.get("class_distribution"),
        imbalance_ratio=existing.get("imbalance_ratio"),
    )
    target_store[file_id] = state
    return state


# ── Backward-compat exports ──────────────────────────────────────────
# These keep existing `from state import get_stored_dataframe` working
# while callers migrate to the short names above.

def get_stored_dataframe(file_id: str) -> Optional[pd.DataFrame]:
    from ingestion.orchestrator import get_stored_dataframe as _gsdf
    return _gsdf(file_id)

def get_stored_data(file_id: str) -> Optional[dict]:
    from ingestion.orchestrator import get_stored_data as _gsd
    return _gsd(file_id)

def update_stored_dataframe(file_id: str, df: pd.DataFrame) -> None:
    from ingestion.orchestrator import update_stored_dataframe as _usd
    _usd(file_id, df)

__all__ = [
    # Stable API
    "get_df", "set_df",
    "get_meta",
    "get_profile", "set_profile",
    "get_cleaning_state", "set_cleaning_state",
    # Target state — single authoritative source
    "get_target_state", "set_target_state", "set_user_target",
    # Stores (for direct access when needed)
    "TTLStore",
    "profile_store", "cleaning_store", "target_store",
    "tag_store", "annotation_store", "recipe_store", "watchlist_store",
    # Legacy re-exports
    "get_stored_data", "get_stored_dataframe", "update_stored_dataframe",
]
