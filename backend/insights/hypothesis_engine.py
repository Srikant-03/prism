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
    row_count = profile.get("row_count", 0)

    for col_name, col_info in columns_profile.items():
        dtype = col_info.get("inferred_dtype", col_info.get("dtype", ""))
        null_pct = col_info.get("null_percentage", col_info.get("null_pct", 0))
        unique_count = col_info.get("distinct_count", col_info.get("unique_count", 0))
        unique_ratio = unique_count / max(row_count, 1)

        # High null hypothesis
        if null_pct > 30:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Column '{col_name}' has {null_pct:.0f}% missing values",
                "evidence": f"{null_pct:.1f}% of values are NULL ({int(null_pct * row_count / 100)} rows)",
                "question": f"Is '{col_name}' important enough to keep, or can it be dropped?",
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
                "observation": f"Column '{col_name}' appears to be a unique identifier",
                "evidence": f"{unique_ratio:.0%} unique values — likely a primary key or ID",
                "question": "Should this column be excluded from modeling?",
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
                "observation": f"Column '{col_name}' has only {unique_count} unique value(s) — it's constant",
                "evidence": "A constant column provides no information for analysis or modeling",
                "question": "Drop this column?",
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
                "observation": f"Column '{col_name}' has {unique_count} unique categories — very high cardinality",
                "evidence": "High-cardinality categoricals are difficult to encode and may not generalize well",
                "question": "Should we group rare categories or use a different encoding?",
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
                "observation": f"Column '{col_name}' is heavily {direction}-skewed (skewness={skewness:.2f})",
                "evidence": f"Skewness of {skewness:.2f} is far from 0 — distribution is asymmetric",
                "question": "Apply log transform to normalize the distribution?",
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
                "observation": f"Columns {pair} are highly correlated (r={corr_val:.3f})",
                "evidence": f"Correlation of {corr_val:.3f} suggests one column may be redundant",
                "question": "Should one of these columns be dropped to reduce multicollinearity?",
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
    for col_name, col_info in columns_profile.items():
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
                            "observation": f"Column '{col_name}' appears heavily imbalanced (ratio {ratio:.0f}:1)",
                            "evidence": f"The most common value appears {ratio:.0f}× more than the least common",
                            "question": "If this is a target variable, standard models will struggle. Use balancing techniques?",
                            "confidence": 0.8,
                            "impact": "high",
                            "action": {
                                "label": "View distribution",
                                "type": "navigate",
                                "payload": f"profile/{col_name}",
                            },
                            "status": "unreviewed",
                        })

    # Sort by confidence × impact
    impact_weights = {"high": 3, "medium": 2, "low": 1}
    hypotheses.sort(key=lambda h: h["confidence"] * impact_weights.get(h["impact"], 1), reverse=True)

    return hypotheses[:20]
