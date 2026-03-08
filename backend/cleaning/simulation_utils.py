"""
simulation_utils — Pure data transformation functions for the What-If simulator.

Extracted from api/simulate.py so that API handlers only do request/response
marshalling. These functions have no FastAPI / HTTP dependencies.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def apply_step(df: pd.DataFrame, action: str, column: str | None = None, params: dict | None = None) -> pd.DataFrame:
    """Apply a single preprocessing step to a DataFrame.

    Args:
        df: Input DataFrame (not mutated; a copy should be passed if needed).
        action: The preprocessing action name.
        column: Target column (optional for some actions).
        params: Extra parameters for the action.

    Returns:
        Transformed DataFrame.
    """
    if params is None:
        params = {}

    if action == "fill_nulls" and column and column in df.columns:
        strategy = params.get("strategy", "mean")
        if strategy == "mean" and pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].fillna(df[column].mean())
        elif strategy == "median" and pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].fillna(df[column].median())
        elif strategy == "mode":
            mode_val = df[column].mode()
            if len(mode_val) > 0:
                df[column] = df[column].fillna(mode_val.iloc[0])
        elif strategy == "zero":
            df[column] = df[column].fillna(0)
        elif strategy == "ffill":
            df[column] = df[column].ffill()

    elif action == "remove_outliers" and column and column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            threshold = params.get("threshold", 1.5)
            mask = (df[column] >= q1 - threshold * iqr) & (df[column] <= q3 + threshold * iqr)
            df = df[mask].copy()

    elif action == "drop_column" and column and column in df.columns:
        df = df.drop(columns=[column])

    elif action == "drop_duplicates":
        df = df.drop_duplicates()

    elif action == "normalize" and column and column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            min_val = df[column].min()
            max_val = df[column].max()
            if max_val != min_val:
                df[column] = (df[column] - min_val) / (max_val - min_val)

    elif action == "standardize" and column and column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            mean = df[column].mean()
            std = df[column].std()
            if std > 0:
                df[column] = (df[column] - mean) / std

    elif action == "log_transform" and column and column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = np.log1p(df[column].clip(lower=0))

    elif action == "encode_categorical" and column and column in df.columns:
        method = params.get("method", "label")
        if method == "label":
            df[column] = df[column].astype("category").cat.codes

    return df


def compute_stats(df: pd.DataFrame) -> dict:
    """Compute summary statistics for before/after comparison.

    Args:
        df: DataFrame to summarize.

    Returns:
        Dictionary of summary stats.
    """
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "null_total": int(df.isnull().sum().sum()),
        "null_pct": round(df.isnull().sum().sum() / max(df.size, 1) * 100, 2),
        "duplicates": int(df.duplicated().sum()),
        "numeric_cols": int(df.select_dtypes(include=[np.number]).shape[1]),
    }


def readiness_score(df: pd.DataFrame) -> int:
    """Compute a model readiness score (0-100).

    Uses a lightweight heuristic suitable for simulation previews.
    The full QualityScorer requires a pydantic DatasetProfile which
    is not available during raw DataFrame simulation.

    Args:
        df: DataFrame to score.

    Returns:
        Integer score between 0 and 100.
    """
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
