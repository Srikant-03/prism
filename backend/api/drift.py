"""
Drift API — Schema drift detection between dataset versions.
Compares structural changes, type changes, and distribution shifts (KL-divergence/PSI).
"""

from __future__ import annotations

import uuid
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import numpy as np
import pandas as pd

router = APIRouter(prefix="/api/drift", tags=["drift"])


class DriftRequest(BaseModel):
    file_id_a: str
    file_id_b: str


@router.post("/compare")
async def compare_datasets(request: DriftRequest):
    """Compare two dataset versions and return structural + statistical drift report."""
    from state import get_df, get_profile

    df_a = get_df(request.file_id_a)
    df_b = get_df(request.file_id_b)

    if df_a is None:
        raise HTTPException(status_code=404, detail=f"Dataset A '{request.file_id_a}' not found")
    if df_b is None:
        raise HTTPException(status_code=404, detail=f"Dataset B '{request.file_id_b}' not found")

    # ── 1. Schema diff ────────────────────────────────────────────
    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)

    added = sorted(cols_b - cols_a)
    removed = sorted(cols_a - cols_b)
    common = sorted(cols_a & cols_b)

    type_changed = []
    for col in common:
        dtype_a = str(df_a[col].dtype)
        dtype_b = str(df_b[col].dtype)
        if dtype_a != dtype_b:
            type_changed.append({"column": col, "from": dtype_a, "to": dtype_b})

    schema_diff = {
        "added_columns": added,
        "removed_columns": removed,
        "type_changed": type_changed,
        "common_columns": common,
    }

    # ── 2. Shape comparison ───────────────────────────────────────
    shape_diff = {
        "rows_a": len(df_a),
        "rows_b": len(df_b),
        "cols_a": len(df_a.columns),
        "cols_b": len(df_b.columns),
        "row_delta": len(df_b) - len(df_a),
        "col_delta": len(df_b.columns) - len(df_a.columns),
    }

    # ── 3. Distribution drift (PSI + KS for numeric columns) ─────
    drift_results = []
    numeric_common = [c for c in common if pd.api.types.is_numeric_dtype(df_a[c]) and pd.api.types.is_numeric_dtype(df_b[c])]

    for col in numeric_common:
        a_vals = df_a[col].dropna().values.astype(float)
        b_vals = df_b[col].dropna().values.astype(float)

        if len(a_vals) < 5 or len(b_vals) < 5:
            continue

        # PSI (Population Stability Index)
        psi = _calculate_psi(a_vals, b_vals)

        # KS test
        from scipy.stats import ks_2samp
        ks_stat, ks_pval = ks_2samp(a_vals, b_vals)

        # Means
        mean_a = float(np.nanmean(a_vals))
        mean_b = float(np.nanmean(b_vals))

        drift_results.append({
            "column": col,
            "psi": round(float(psi), 4),
            "ks_statistic": round(float(ks_stat), 4),
            "ks_pvalue": round(float(ks_pval), 6),
            "drift_detected": psi > 0.2 or ks_pval < 0.05,
            "mean_a": round(mean_a, 4),
            "mean_b": round(mean_b, 4),
            "mean_shift": round(mean_b - mean_a, 4),
        })

    # Sort by drift severity
    drift_results.sort(key=lambda d: d["psi"], reverse=True)

    # ── 4. Quality score delta (if profiles exist) ────────────────
    quality_diff = None
    profile_a = get_profile(request.file_id_a)
    profile_b = get_profile(request.file_id_b)

    if profile_a and profile_b:
        q_a = _extract_quality(profile_a)
        q_b = _extract_quality(profile_b)
        if q_a and q_b:
            quality_diff = {
                "score_a": q_a.get("overall_score"),
                "score_b": q_b.get("overall_score"),
                "grade_a": q_a.get("grade"),
                "grade_b": q_b.get("grade"),
                "delta": (q_b.get("overall_score") or 0) - (q_a.get("overall_score") or 0),
            }

    # ── 5. Null rate changes ──────────────────────────────────────
    null_changes = []
    for col in common:
        null_a = df_a[col].isnull().mean()
        null_b = df_b[col].isnull().mean()
        if abs(null_b - null_a) > 0.05:
            null_changes.append({
                "column": col,
                "null_pct_a": round(null_a * 100, 2),
                "null_pct_b": round(null_b * 100, 2),
                "delta": round((null_b - null_a) * 100, 2),
            })
    null_changes.sort(key=lambda x: abs(x["delta"]), reverse=True)

    return {
        "schema_diff": schema_diff,
        "shape_diff": shape_diff,
        "distribution_drift": drift_results,
        "quality_diff": quality_diff,
        "null_changes": null_changes,
        "drift_summary": {
            "columns_added": len(added),
            "columns_removed": len(removed),
            "type_changes": len(type_changed),
            "drifted_columns": sum(1 for d in drift_results if d["drift_detected"]),
            "total_compared": len(drift_results),
        },
    }


def _calculate_psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """Calculate Population Stability Index between two distributions."""
    breakpoints = np.histogram_bin_edges(np.concatenate([expected, actual]), bins=bins)

    expected_pcts = np.histogram(expected, bins=breakpoints)[0] / len(expected)
    actual_pcts = np.histogram(actual, bins=breakpoints)[0] / len(actual)

    # Avoid division by zero
    expected_pcts = np.clip(expected_pcts, 1e-6, None)
    actual_pcts = np.clip(actual_pcts, 1e-6, None)

    psi = np.sum((actual_pcts - expected_pcts) * np.log(actual_pcts / expected_pcts))
    return psi


def _extract_quality(stored) -> dict | None:
    """Extract quality scores from stored profile."""
    if stored is None:
        return None
    insights = getattr(stored, "insights", None) or (stored.get("insights") if isinstance(stored, dict) else None)
    if not insights:
        return None
    q = getattr(insights, "quality", None) or (insights.get("quality") if isinstance(insights, dict) else None)
    if not q:
        return None
    if hasattr(q, "model_dump"):
        return q.model_dump()
    if isinstance(q, dict):
        return q
    return None
