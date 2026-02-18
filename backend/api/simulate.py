"""
Simulate API — What-If sandbox for testing preprocessing steps.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/simulate", tags=["simulate"])


class SimStep(BaseModel):
    action: str
    column: Optional[str] = None
    params: dict = {}


class SimRequest(BaseModel):
    file_id: str
    steps: list[SimStep]
    sample_pct: int = 10


@router.post("/chain")
async def simulate_chain(request: SimRequest):
    """Simulate a chain of preprocessing steps on a sample."""
    try:
        import pandas as pd
        import numpy as np
        from api.upload import _storage

        # Get the dataframe
        df = _storage.get(request.file_id, {}).get("df")
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Sample
        n_sample = max(1, int(len(df) * request.sample_pct / 100))
        n_sample = min(n_sample, 10000, len(df))
        sample = df.sample(n=n_sample, random_state=42).copy()

        # Compute before stats
        before = _compute_stats(sample)
        readiness_before = _readiness_score(sample)

        # Apply steps
        for step in request.steps:
            sample = _apply_step(sample, step)

        # Compute after stats
        after = _compute_stats(sample)
        readiness_after = _readiness_score(sample)

        # Compute deltas
        deltas = {}
        for key in before:
            if isinstance(before[key], (int, float)) and isinstance(after.get(key), (int, float)):
                deltas[key] = round(after[key] - before[key], 4)

        return {
            "before": before,
            "after": after,
            "deltas": deltas,
            "readiness_before": readiness_before,
            "readiness_after": readiness_after,
            "rows_affected": abs(len(df) - len(sample)),
            "sample_size": n_sample,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/commit")
async def commit_steps(request: SimRequest):
    """Apply steps to the full dataset (commit simulation)."""
    try:
        from api.upload import _storage

        info = _storage.get(request.file_id)
        if info is None or "df" not in info:
            raise HTTPException(status_code=404, detail="Dataset not found")

        df = info["df"].copy()
        for step in request.steps:
            df = _apply_step(df, step)
        info["df"] = df
        return {"committed": True, "new_row_count": len(df)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _apply_step(df, step: SimStep):
    """Apply a single preprocessing step."""
    import pandas as pd
    import numpy as np

    col = step.column
    action = step.action
    params = step.params

    if action == "fill_nulls" and col and col in df.columns:
        strategy = params.get("strategy", "mean")
        if strategy == "mean" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "median" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        elif strategy == "mode":
            mode_val = df[col].mode()
            if len(mode_val) > 0:
                df[col] = df[col].fillna(mode_val.iloc[0])
        elif strategy == "zero":
            df[col] = df[col].fillna(0)
        elif strategy == "ffill":
            df[col] = df[col].ffill()

    elif action == "remove_outliers" and col and col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            threshold = params.get("threshold", 1.5)
            mask = (df[col] >= q1 - threshold * iqr) & (df[col] <= q3 + threshold * iqr)
            df = df[mask].copy()

    elif action == "drop_column" and col and col in df.columns:
        df = df.drop(columns=[col])

    elif action == "drop_duplicates":
        df = df.drop_duplicates()

    elif action == "normalize" and col and col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            min_val = df[col].min()
            max_val = df[col].max()
            if max_val != min_val:
                df[col] = (df[col] - min_val) / (max_val - min_val)

    elif action == "standardize" and col and col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            mean = df[col].mean()
            std = df[col].std()
            if std > 0:
                df[col] = (df[col] - mean) / std

    elif action == "log_transform" and col and col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = np.log1p(df[col].clip(lower=0))

    elif action == "encode_categorical" and col and col in df.columns:
        method = params.get("method", "label")
        if method == "label":
            df[col] = df[col].astype("category").cat.codes

    return df


def _compute_stats(df) -> dict:
    """Compute summary statistics for comparison."""
    import numpy as np
    stats = {
        "rows": len(df),
        "columns": len(df.columns),
        "null_total": int(df.isnull().sum().sum()),
        "null_pct": round(df.isnull().sum().sum() / max(df.size, 1) * 100, 2),
        "duplicates": int(df.duplicated().sum()),
        "numeric_cols": int(df.select_dtypes(include=[np.number]).shape[1]),
    }
    return stats


def _readiness_score(df) -> int:
    """Compute a model readiness score (0-100)."""
    import numpy as np
    score = 100

    # Penalize nulls
    null_pct = df.isnull().sum().sum() / max(df.size, 1) * 100
    score -= min(null_pct * 2, 30)

    # Penalize low numeric ratio
    numeric_ratio = df.select_dtypes(include=[np.number]).shape[1] / max(len(df.columns), 1)
    if numeric_ratio < 0.3:
        score -= 15

    # Penalize duplicates
    dup_pct = df.duplicated().sum() / max(len(df), 1) * 100
    score -= min(dup_pct, 15)

    # Penalize low variance columns
    for col in df.select_dtypes(include=[np.number]).columns:
        if df[col].std() == 0:
            score -= 3

    return max(0, min(100, int(score)))
