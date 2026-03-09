"""
Hypothesis Engine — Auto-generates data-driven hypotheses from profiling results.
"""

from __future__ import annotations

import uuid
from typing import Optional


def generate_hypotheses(profile: dict, quality: dict = None) -> list[dict]:
    """Generate hypotheses from profiling data."""
    hypotheses = []

    columns_profile = profile.get("columns", {})
    row_count = profile.get("row_count") or profile.get("total_rows", 0)
    
    if isinstance(columns_profile, dict):
        col_items = columns_profile.items()
    elif isinstance(columns_profile, list):
        col_items = [(col.get("name", "unknown"), col) for col in columns_profile if isinstance(col, dict)]
    else:
        col_items = []

    for col_name, col_info in col_items:
        dtype = col_info.get("inferred_dtype", col_info.get("dtype", ""))
        null_pct = col_info.get("null_percentage", col_info.get("null_pct", 0))
        unique_count = col_info.get("distinct_count", col_info.get("unique_count", 0))
        unique_ratio = unique_count / max(row_count, 1)

        # High null hypothesis
        if null_pct > 30:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Severe data sparsity in feature '{col_name}'",
                "evidence": f"Feature is missing {null_pct:.1f}% of its contiguous values ({int(null_pct * row_count / 100)} missing records)",
                "question": f"Is '{col_name}' critical for the target objective? Consider eliminating the feature to reduce background noise.",
                "confidence": min(0.9, null_pct / 100 + 0.3),
                "impact": "high" if null_pct > 50 else "medium",
                "action": {
                    "label": f"Analyze '{col_name}' importance",
                    "type": "navigate",
                    "payload": f"profile/{col_name}",
                },
                "status": "unreviewed",
            })

        # Potential ID column
        if unique_ratio > 0.95 and dtype in ("int64", "object", "string"):
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Feature '{col_name}' exhibits near-perfect cardinality",
                "evidence": f"{unique_ratio:.1%} of values are strictly unique — strongly indicating an index or surrogate key",
                "question": "Should this feature be excluded from predictive modeling to prevent severe overfitting?",
                "confidence": unique_ratio,
                "impact": "medium",
                "action": {
                    "label": "Mark as ID column",
                    "type": "fix",
                    "payload": f"tag/{col_name}/id",
                },
                "status": "unreviewed",
            })

        # Low variance / constant column
        if unique_count <= 1:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Feature '{col_name}' has zero to near-zero variance",
                "evidence": f"Contains only {unique_count} distinct scalar value(s) across the entire dataset",
                "question": "A constant feature inherently provides zero information gain. Should it be dropped?",
                "confidence": 0.95,
                "impact": "low",
                "action": {
                    "label": f"Drop '{col_name}'",
                    "type": "fix",
                    "payload": f"drop/{col_name}",
                },
                "status": "unreviewed",
            })

        # High cardinality categorical
        if dtype in ("object", "string", "category") and unique_count > 100:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"High cardinality detected in categorical feature '{col_name}'",
                "evidence": f"Contains {unique_count} unique categorical levels, risking extreme dimensionality if strictly one-hot encoded",
                "question": "Are these highly granular categories truly necessary, or can rare levels be binned/grouped?",
                "confidence": 0.75,
                "impact": "medium",
                "action": {
                    "label": "View distribution",
                    "type": "navigate",
                    "payload": f"profile/{col_name}",
                },
                "status": "unreviewed",
            })

        # Skewed numeric
        numeric_profile = col_info.get("numeric") or {}
        skewness = numeric_profile.get("skewness") if isinstance(numeric_profile, dict) else None
        if skewness is None:
             skewness = col_info.get("skewness")
        if skewness is not None and abs(skewness) > 2:
            direction = "right" if skewness > 0 else "left"
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Distribution of '{col_name}' is severely {direction}-skewed (skew = {skewness:.2f})",
                "evidence": f"A skewness of {skewness:.2f} significantly deviates from a normal distribution curve",
                "question": "Would applying a log or Box-Cox transformation stabilize variance for linear models?",
                "confidence": 0.7,
                "impact": "medium",
                "action": {
                    "label": "Simulate log transform",
                    "type": "fix",
                    "payload": f"simulate/log_transform/{col_name}",
                },
                "status": "unreviewed",
            })

    # Correlations
    correlations = profile.get("correlations", {})
    for pair, corr_val in correlations.items():
        if isinstance(corr_val, (int, float)) and abs(corr_val) > 0.9:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"High collinearity between features {pair}",
                "evidence": f"Pearson correlation coefficient of {corr_val:.3f} indicates significant redundant variance",
                "question": "To avoid multicollinearity and coefficient instability, should one of these features be removed?",
                "confidence": abs(corr_val),
                "impact": "high",
                "action": {
                    "label": "Compare columns",
                    "type": "navigate",
                    "payload": f"compare/{pair}",
                },
                "status": "unreviewed",
            })

    # Class imbalance detection
    for col_name, col_info in col_items:
        categorical_profile = col_info.get("categorical") or {}
        if isinstance(categorical_profile, dict):
            top_values = categorical_profile.get("top_values", [])
            if isinstance(top_values, list) and len(top_values) >= 2:
                values = [v.get("count", 0) if isinstance(v, dict) else 0 for v in top_values]
                if values and len(values) >= 2:
                    ratio = max(values) / max(min(values), 1)
                    if ratio > 10 and col_info.get("distinct_count", col_info.get("unique_count", 0)) < 10:
                        hypotheses.append({
                            "id": str(uuid.uuid4())[:8],
                            "observation": f"Extreme class imbalance observed in '{col_name}'",
                            "evidence": f"The majority class frequency is {ratio:.1f}x higher than the minority class",
                            "question": "If this is a target variable, should oversampling (e.g. SMOTE) or class weights be applied to prevent minority-class vanishing?",
                            "confidence": 0.8,
                            "impact": "high",
                            "action": {
                                "label": "View distribution",
                                "type": "navigate",
                                "payload": f"profile/{col_name}",
                            },
                            "status": "unreviewed",
                        })

    # ── Outlier detection hypothesis ──
    for col_name, col_info in col_items:
        numeric_profile = col_info.get("numeric") or {}
        if not isinstance(numeric_profile, dict):
            continue
        kurt = numeric_profile.get("kurtosis")
        if kurt is not None and kurt > 5:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Feature '{col_name}' has extremely heavy tails (kurtosis = {kurt:.2f})",
                "evidence": f"A kurtosis of {kurt:.2f} indicates significant concentration of extreme values relative to a normal distribution",
                "question": "Consider using robust scalers (e.g. RobustScaler) or Winsorizing outliers before training sensitive models like linear regression or SVMs.",
                "confidence": min(0.9, 0.5 + kurt / 20),
                "impact": "medium",
                "action": {"label": "View outliers", "type": "navigate", "payload": f"profile/{col_name}"},
                "status": "unreviewed",
            })

    # ── Low coefficient of variation (quasi-constant numeric) ──
    for col_name, col_info in col_items:
        numeric_profile = col_info.get("numeric") or {}
        if not isinstance(numeric_profile, dict):
            continue
        mean = numeric_profile.get("mean")
        std = numeric_profile.get("std")
        if mean is not None and std is not None and abs(mean) > 0.001:
            cov = abs(std / mean)
            if cov < 0.01 and col_info.get("distinct_count", col_info.get("unique_count", 0)) > 1:
                hypotheses.append({
                    "id": str(uuid.uuid4())[:8],
                    "observation": f"Feature '{col_name}' is quasi-constant (coefficient of variation = {cov:.4f})",
                    "evidence": f"With a mean of {mean:.2f} and std of {std:.4f}, this feature carries almost no discriminative signal",
                    "question": "Should this feature be dropped to reduce noise and dimensionality? It is unlikely to provide predictive value.",
                    "confidence": 0.85,
                    "impact": "low",
                    "action": {"label": f"Drop '{col_name}'", "type": "fix", "payload": f"drop/{col_name}"},
                    "status": "unreviewed",
                })

    # ── Datetime feature extraction opportunity ──
    for col_name, col_info in col_items:
        dtype = col_info.get("inferred_dtype", col_info.get("dtype", ""))
        semantic = col_info.get("semantic_type", "")
        if "datetime" in str(dtype).lower() or "temporal" in str(semantic).lower() or "date" in str(semantic).lower():
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Temporal feature '{col_name}' detected - rich feature engineering opportunity",
                "evidence": "Datetime columns can be decomposed into day-of-week, month, quarter, hour, is_weekend, days_since_epoch, and cyclical encodings",
                "question": "Extract temporal sub-features to capture seasonality, trends, and periodic patterns for the model?",
                "confidence": 0.85,
                "impact": "medium",
                "action": {"label": "Extract temporal features", "type": "fix", "payload": f"extract_datetime/{col_name}"},
                "status": "unreviewed",
            })

    # ── Text/NLP feature extraction opportunity ──
    for col_name, col_info in col_items:
        dtype = col_info.get("inferred_dtype", col_info.get("dtype", ""))
        unique_count = col_info.get("distinct_count", col_info.get("unique_count", 0))
        semantic = col_info.get("semantic_type", "")
        if dtype in ("object", "string") and unique_count > 50 and semantic not in ("id_key", "email", "url", "phone"):
            avg_len = col_info.get("avg_length", 0)
            if avg_len > 20 or unique_count > 500:
                hypotheses.append({
                    "id": str(uuid.uuid4())[:8],
                    "observation": f"High-cardinality text feature '{col_name}' may contain extractable NLP signals",
                    "evidence": f"Contains {unique_count} unique values with avg length suggesting prose or semi-structured text",
                    "question": "Consider TF-IDF vectorization, sentence embeddings, or regex-based feature extraction to derive predictive signals from this text.",
                    "confidence": 0.65,
                    "impact": "medium",
                    "action": {"label": "Analyze text", "type": "navigate", "payload": f"profile/{col_name}"},
                    "status": "unreviewed",
                })

    # ── Data leakage warning (feature too correlated with target) ──
    cross_analysis = profile.get("cross_analysis") or {}
    if isinstance(cross_analysis, dict):
        target_info = cross_analysis.get("target_analysis") or {}
        if isinstance(target_info, dict):
            target_col = target_info.get("target_column", "")
            top_predictors = target_info.get("top_predictors") or []
            for p in top_predictors:
                feat = p.get("feature", "") if isinstance(p, dict) else getattr(p, "feature", "")
                score = p.get("importance_score", 0) if isinstance(p, dict) else getattr(p, "importance_score", 0)
                if score > 0.95 and feat != target_col:
                    hypotheses.append({
                        "id": str(uuid.uuid4())[:8],
                        "observation": f"Potential data leakage: '{feat}' is almost perfectly correlated with target '{target_col}'",
                        "evidence": f"Association score of {score:.3f} is suspiciously high - this feature may contain future information or be a derivative of the target",
                        "question": "Verify that this feature is available at prediction time. If it encodes post-hoc information, it must be excluded to prevent overly optimistic model evaluation.",
                        "confidence": 0.9,
                        "impact": "high",
                        "action": {"label": f"Investigate '{feat}'", "type": "navigate", "payload": f"profile/{feat}"},
                        "status": "unreviewed",
                    })

    # Sort by confidence x impact
    impact_weights = {"high": 3, "medium": 2, "low": 1}
    hypotheses.sort(key=lambda h: h["confidence"] * impact_weights.get(h["impact"], 1), reverse=True)

    return hypotheses[:25]
