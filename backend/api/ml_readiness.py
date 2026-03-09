"""
ML Readiness API — Generates a structured ML scorecard.
Evaluates dataset suitability for machine learning based on target class imbalance,
missing values, predictive power, and feature correlations.
"""

from __future__ import annotations

import math
from fastapi import APIRouter, HTTPException

import pandas as pd
import numpy as np

router = APIRouter(prefix="/api/ml-readiness", tags=["ml-readiness"])


@router.get("/{file_id}")
async def get_ml_readiness(file_id: str):
    """Generate a comprehensive ML Readiness Report Card for a dataset."""
    from state import get_df, get_profile
    from profiling.target_detector import TargetDetector
    from cleaning.imbalance_handler import ImbalanceHandler

    df = get_df(file_id)
    profile = get_profile(file_id)

    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if profile is None:
        raise HTTPException(status_code=400, detail="Dataset must be profiled first")

    cross_analysis = getattr(profile, "cross_column_analysis", None) or profile.get("cross_column_analysis")
    if not cross_analysis:
        raise HTTPException(status_code=400, detail="Cross-column profiling data is missing")

    # 1. Target Detection
    detector = TargetDetector()
    target_analysis = detector.analyze(df, profile, cross_analysis)

    if not target_analysis.is_target_detected:
        return {
            "status": "not_ready",
            "score": 0,
            "message": "No suitable target variable detected. Cannot assess ML readiness without a target.",
            "target": None,
        }

    target = target_analysis.target_column
    problem_type = target_analysis.problem_type
    
    # 2. Imbalance & Class Distribution
    imbalance_data = {}
    if problem_type in ("binary_classification", "multiclass_classification"):
        handler = ImbalanceHandler(df, file_id, target_column=target)
        try:
            handler_result = handler.analyze()
            imbalance_data = handler_result.get("report", {})
        except Exception:
            pass

    # 3. Missing Value Impact
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isnull().sum().sum()
    missing_pct = (missing_cells / total_cells) * 100 if total_cells > 0 else 0
    target_missing = df[target].isnull().sum()
    target_missing_pct = (target_missing / df.shape[0]) * 100 if df.shape[0] > 0 else 0

    # 4. Feature Predictive Power (from profile)
    predictors = []
    top_predictors = getattr(target_analysis, "top_predictors", []) or target_analysis.get("top_predictors", [])
    for p in top_predictors:
        name = getattr(p, "feature", p.get("feature", ""))
        score = getattr(p, "importance_score", p.get("importance_score", 0))
        if name and name != target:
            predictors.append({"feature": name, "importance": round(score, 4)})

    predictors.sort(key=lambda x: x["importance"], reverse=True)
    avg_predictive_power = sum(p["importance"] for p in predictors[:5]) / min(len(predictors) or 1, 5)

    # 5. Row to Feature Ratio
    n_rows, n_cols = df.shape
    row_feature_ratio = n_rows / max(n_cols - 1, 1)

    # ── Score Calculation ──
    score = 100

    # Penalize missing target values heavily
    score -= min(target_missing_pct * 2, 40)

    # Penalize general missing values
    score -= min(missing_pct, 20)

    # Penalize class imbalance
    imb_ratio = imbalance_data.get("imbalance_ratio", 1.0)
    if imb_ratio > 10:
        score -= min((imb_ratio - 10) * 0.5, 20)

    # Reward strong predictors (up to 10 points)
    if avg_predictive_power > 0.5:
        score += 10
    elif avg_predictive_power < 0.1:
        score -= 15 # Penalize weak signal

    # Penalize low row/feature ratio (Curse of Dimensionality)
    if row_feature_ratio < 10:
        score -= 20
    elif row_feature_ratio < 50:
        score -= 10

    score = max(0, min(100, round(score)))

    # Grade
    grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 45 else "F"

    # Ceiling estimate
    if problem_type == "regression":
        ceiling = f"~{min(0.95, avg_predictive_power + 0.3):.2f} R²"
    else:
        ceiling = f"~{min(0.99, avg_predictive_power + 0.5):.0%} Accuracy"

    return {
        "status": "ready" if score >= 60 else "needs_cleaning",
        "score": score,
        "grade": grade,
        "problem_type": problem_type,
        "target_column": target,
        "estimated_performance_ceiling": ceiling,
        "metrics": {
            "imbalance": imbalance_data,
            "missing_values": {
                "dataset_pct": round(missing_pct, 2),
                "target_missing_rows": int(target_missing),
                "target_missing_pct": round(target_missing_pct, 2),
            },
            "shape": {
                "rows": n_rows,
                "features": n_cols - 1,
                "row_to_feature_ratio": round(row_feature_ratio, 1),
            },
            "predictive_power": {
                "top_predictors": predictors[:10],
                "average_top_5_signal": round(avg_predictive_power, 4),
            }
        },
        "recommendations": _generate_recommendations(score, missing_pct, target_missing_pct, imb_ratio, row_feature_ratio, avg_predictive_power),
    }


def _generate_recommendations(score, missing, target_missing, imbalance, ratio, signal) -> list[str]:
    recs = []
    if target_missing > 0:
        recs.append("CRITICAL: Drop rows where the target column is missing.")
    if missing > 5:
        recs.append("Impute missing values in features to prevent data loss during model training.")
    if imbalance > 5:
        recs.append("Apply SMOTE or class weighting to handle the severe class imbalance.")
    if ratio < 50:
        recs.append("Consider applying PCA or dropping low-variance features to improve the row-to-feature ratio.")
    if signal < 0.2:
        recs.append("Current features show weak predictive power. Consider feature engineering or gathering different data.")
    
    if not recs and score >= 80:
        recs.append("Dataset looks highly ready for modeling. Proceed with baseline training.")
        
    return recs
