"""
Hypothesis Engine — Generates advanced analytical hypotheses from profiling results.

Unlike the AnomalyDetector (which flags data quality issues like nulls, constants,
and ID columns), this engine focuses on **strategic modeling hypotheses** —
the kinds of insights a senior data scientist would generate when planning
a modeling strategy.

Each hypothesis includes:
  - observation: Technical description of the pattern
  - layman: Plain-English explanation anyone can understand
  - evidence: Statistical evidence supporting the hypothesis
  - question: Strategic question the analyst should investigate
  - confidence / impact: Severity scoring
"""

from __future__ import annotations

import math
import uuid
from typing import Optional


def generate_hypotheses(profile: dict, quality: dict = None) -> list[dict]:
    """Generate advanced analytical hypotheses from profiling data."""
    hypotheses = []

    columns_profile = profile.get("columns", {})
    row_count = profile.get("row_count") or profile.get("total_rows", 0)
    total_cols = profile.get("total_columns") or profile.get("column_count", 0)

    if isinstance(columns_profile, dict):
        col_items = list(columns_profile.items())
    elif isinstance(columns_profile, list):
        col_items = [(col.get("name", "unknown"), col) for col in columns_profile if isinstance(col, dict)]
    else:
        col_items = []

    # Extract cross-analysis data
    cross = profile.get("cross_analysis") or {}
    if not isinstance(cross, dict):
        try:
            cross = cross.__dict__ if hasattr(cross, "__dict__") else {}
        except Exception:
            cross = {}

    # Target info — CrossColumnProfile serializes as 'target', but some contexts use 'target_analysis'
    target_info = cross.get("target") or cross.get("target_analysis") or {}
    if not isinstance(target_info, dict):
        try:
            target_info = target_info.__dict__
        except Exception:
            target_info = {}
    target_col = target_info.get("target_column", "")
    target_detected = target_info.get("is_target_detected", False)
    problem_type = target_info.get("problem_type", "")
    top_predictors = target_info.get("top_predictors") or []

    # Correlation data
    corr_data = cross.get("correlations") or {}
    if not isinstance(corr_data, dict):
        try:
            corr_data = corr_data.__dict__
        except Exception:
            corr_data = {}
    corr_matrix = corr_data.get("correlation_matrix") or {}
    mutual_info = corr_data.get("mutual_information") or {}
    strongest_pairs = corr_data.get("strongest_pairs") or []
    vif_scores = {}
    multi_report = corr_data.get("multicollinearity") or {}
    if isinstance(multi_report, dict):
        vif_scores = multi_report.get("vif_scores") or {}

    # Temporal data
    temporal = cross.get("temporal") or {}
    if not isinstance(temporal, dict):
        try:
            temporal = temporal.__dict__
        except Exception:
            temporal = {}

    # Geo data
    geo = cross.get("geo") or {}
    if not isinstance(geo, dict):
        try:
            geo = geo.__dict__
        except Exception:
            geo = {}

    # Also extract flat correlations dict (used in report generation)
    flat_correlations = profile.get("correlations") or {}

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 1: Non-Linear Relationship Detection
    # ═══════════════════════════════════════════════════════════════
    if target_detected and target_col and mutual_info and corr_matrix:
        target_mi = mutual_info.get(target_col, {})
        target_corr_row = corr_matrix.get(target_col, {})

        for feat, mi_score in target_mi.items():
            if feat == target_col:
                continue
            if not isinstance(mi_score, (int, float)):
                continue

            pearson_r = target_corr_row.get(feat, 0)
            if not isinstance(pearson_r, (int, float)):
                continue

            # High MI but low linear correlation = non-linear relationship
            if mi_score > 0.1 and abs(pearson_r) < 0.15:
                hypotheses.append({
                    "id": str(uuid.uuid4())[:8],
                    "observation": (
                        f"Non-linear relationship detected: '{feat}' has high mutual information "
                        f"(MI={mi_score:.3f}) with '{target_col}' but near-zero linear correlation "
                        f"(r={pearson_r:.3f})"
                    ),
                    "layman": (
                        f"The feature '{feat}' strongly influences '{target_col}', but not in a "
                        f"simple straight-line way. Think of it like this: studying more hours helps "
                        f"your score up to a point, but after that you're too tired and the score drops. "
                        f"A standard model won't catch this pattern — you need a smarter one."
                    ),
                    "evidence": (
                        f"Mutual Information ({mi_score:.3f}) captures all forms of statistical "
                        f"dependence, while Pearson r ({pearson_r:.3f}) only measures linear trends. "
                        f"The gap suggests a U-shaped, threshold, or interaction-driven relationship."
                    ),
                    "question": (
                        "Use tree-based models (XGBoost, Random Forest) which naturally handle "
                        "non-linearity, or engineer polynomial/binned features for linear models."
                    ),
                    "confidence": min(0.95, 0.5 + mi_score),
                    "impact": "high",
                    "action": {"label": f"Explore '{feat}' vs '{target_col}'", "type": "navigate",
                               "payload": f"compare/{feat}/{target_col}"},
                    "status": "unreviewed",
                })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 2: Multicollinear Feature Cluster (Latent Factor)
    # ═══════════════════════════════════════════════════════════════
    if corr_matrix and len(corr_matrix) >= 3:
        numeric_cols = [name for name, info in col_items
                        if isinstance(info, dict) and
                        info.get("semantic_type", info.get("inferred_dtype", "")) in
                        ("numeric_continuous", "numeric_discrete", "float64", "int64")]

        # Find clusters of 3+ columns all pairwise |r| > 0.6
        visited = set()
        for i, col_a in enumerate(numeric_cols):
            if col_a in visited or col_a == target_col:
                continue
            cluster = [col_a]
            for col_b in numeric_cols[i+1:]:
                if col_b in visited or col_b == target_col:
                    continue
                r_ab = corr_matrix.get(col_a, {}).get(col_b, 0)
                if not isinstance(r_ab, (int, float)):
                    continue
                if abs(r_ab) > 0.6:
                    # Check this col_b is also correlated with all existing cluster members
                    fits = all(
                        abs(corr_matrix.get(c, {}).get(col_b, 0)) > 0.5
                        for c in cluster
                        if isinstance(corr_matrix.get(c, {}).get(col_b, 0), (int, float))
                    )
                    if fits:
                        cluster.append(col_b)

            if len(cluster) >= 3:
                visited.update(cluster)
                cluster_names = ", ".join(f"'{c}'" for c in cluster[:5])
                avg_vif = 0
                vif_parts = []
                for c in cluster:
                    v = vif_scores.get(c, 0)
                    if isinstance(v, (int, float)) and v > 0:
                        vif_parts.append(f"{c}={v:.1f}")
                        avg_vif += v
                avg_vif = avg_vif / max(len(vif_parts), 1)

                hypotheses.append({
                    "id": str(uuid.uuid4())[:8],
                    "observation": (
                        f"Correlated feature cluster detected: {cluster_names} "
                        f"({len(cluster)} features all pairwise |r| > 0.6)"
                    ),
                    "layman": (
                        f"The features {cluster_names} are all measuring essentially the same "
                        f"underlying thing. It's like asking someone's height in inches, centimeters, "
                        f"AND feet — they give you the same information three times. Keeping all of "
                        f"them confuses the model. Pick the best one or combine them."
                    ),
                    "evidence": (
                        f"These {len(cluster)} features form a tightly correlated cluster, suggesting "
                        f"a single latent concept. "
                        + (f"VIF scores ({', '.join(vif_parts[:4])}) confirm redundancy." if vif_parts else
                           "Including all of them inflates coefficient variance in linear models.")
                    ),
                    "question": (
                        "Apply PCA to extract a single composite feature from this cluster, or "
                        "keep only the member with the highest target association and drop the rest."
                    ),
                    "confidence": 0.85,
                    "impact": "high",
                    "action": {"label": "View cluster correlations", "type": "navigate",
                               "payload": "correlations"},
                    "status": "unreviewed",
                })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 3: Diminishing Returns / Power-Law Distribution
    # ═══════════════════════════════════════════════════════════════
    for col_name, col_info in col_items:
        if not isinstance(col_info, dict):
            continue
        numeric_profile = col_info.get("numeric") or {}
        if not isinstance(numeric_profile, dict):
            continue

        skewness = numeric_profile.get("skewness")
        if skewness is None:
            skewness = col_info.get("skewness")
        if skewness is None or abs(skewness) <= 2:
            continue

        # Check if this feature has meaningful target association
        has_target_link = False
        if target_col and mutual_info:
            mi = mutual_info.get(target_col, {}).get(col_name, 0)
            if isinstance(mi, (int, float)) and mi > 0.03:
                has_target_link = True
        if target_col and corr_matrix:
            r = corr_matrix.get(target_col, {}).get(col_name, 0)
            if isinstance(r, (int, float)) and abs(r) > 0.1:
                has_target_link = True

        # Only generate this hypothesis if feature matters for target, or if no target detected
        if has_target_link or not target_detected:
            direction = "right" if skewness > 0 else "left"
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": (
                    f"Power-law distribution in '{col_name}' (skewness = {skewness:.2f}) "
                    f"suggests diminishing returns behavior"
                ),
                "layman": (
                    f"Most values in '{col_name}' are bunched up on one side, with a long tail "
                    f"of extreme values. Picture a salary chart: most people earn $30-80K, but a "
                    f"few earn millions. This long tail confuses models. Taking the logarithm "
                    f"of this feature spreads the values more evenly and helps the model learn better."
                ),
                "evidence": (
                    f"Skewness of {skewness:.2f} ({direction}-skewed) indicates a heavily "
                    f"asymmetric distribution consistent with exponential or power-law dynamics. "
                    f"Linear models are sensitive to such distributions — gradient updates are "
                    f"dominated by extreme values."
                ),
                "question": (
                    "Apply a log₁₀ or Box-Cox transformation to normalize the distribution. "
                    "This typically improves linear model R² by 5-20% and stabilizes gradient descent."
                ),
                "confidence": min(0.85, 0.5 + abs(skewness) / 10),
                "impact": "medium",
                "action": {"label": f"Transform '{col_name}'", "type": "fix",
                           "payload": f"simulate/log_transform/{col_name}"},
                "status": "unreviewed",
            })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 4: Target Proxy / Data Leakage Risk
    # ═══════════════════════════════════════════════════════════════
    if target_detected and target_col:
        for p in top_predictors:
            if isinstance(p, dict):
                feat = p.get("feature", "")
                score = p.get("importance_score", 0)
            else:
                feat = getattr(p, "feature", "")
                score = getattr(p, "importance_score", 0)

            if not feat or feat == target_col:
                continue

            if score > 0.90:
                hypotheses.append({
                    "id": str(uuid.uuid4())[:8],
                    "observation": (
                        f"Potential data leakage: '{feat}' has suspiciously high association "
                        f"({score:.3f}) with target '{target_col}'"
                    ),
                    "layman": (
                        f"The feature '{feat}' seems to almost perfectly predict '{target_col}'. "
                        f"This sounds great, but it's actually suspicious — like knowing tomorrow's "
                        f"winning lottery numbers today. If this feature was calculated AFTER the "
                        f"outcome, it's cheating. The model will look amazing during testing but "
                        f"fail completely in the real world."
                    ),
                    "evidence": (
                        f"An association score of {score:.3f} between a feature and the target "
                        f"is atypically high. This either indicates: (1) the feature is a direct "
                        f"mathematical derivative of the target, (2) the feature encodes post-hoc "
                        f"information, or (3) a genuinely powerful predictor."
                    ),
                    "question": (
                        f"Critical: Verify that '{feat}' is available at prediction time. "
                        f"Was it recorded BEFORE or AFTER '{target_col}' was determined? "
                        f"If after, it must be excluded immediately."
                    ),
                    "confidence": min(0.95, score),
                    "impact": "high",
                    "action": {"label": f"Investigate '{feat}'", "type": "navigate",
                               "payload": f"profile/{feat}"},
                    "status": "unreviewed",
                })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 5: Feature Interaction Opportunity
    # ═══════════════════════════════════════════════════════════════
    if target_detected and target_col and mutual_info and corr_matrix:
        target_mi = mutual_info.get(target_col, {})
        target_corr_row = corr_matrix.get(target_col, {})

        # Find pairs of features that are individually weak but have MI with target
        weak_but_informative = []
        for feat, mi_val in target_mi.items():
            if feat == target_col:
                continue
            if not isinstance(mi_val, (int, float)):
                continue
            pearson = target_corr_row.get(feat, 0)
            if not isinstance(pearson, (int, float)):
                continue

            if mi_val > 0.03 and abs(pearson) < 0.15:
                weak_but_informative.append((feat, mi_val, pearson))

        # Check pairs that are uncorrelated with each other
        for i in range(len(weak_but_informative)):
            for j in range(i + 1, min(len(weak_but_informative), i + 5)):
                fa, mi_a, _ = weak_but_informative[i]
                fb, mi_b, _ = weak_but_informative[j]
                r_ab = corr_matrix.get(fa, {}).get(fb, 0)
                if not isinstance(r_ab, (int, float)):
                    continue

                if abs(r_ab) < 0.15:
                    hypotheses.append({
                        "id": str(uuid.uuid4())[:8],
                        "observation": (
                            f"Hidden interaction opportunity between '{fa}' and '{fb}' — "
                            f"individually weak predictors of '{target_col}' that may combine powerfully"
                        ),
                        "layman": (
                            f"Neither '{fa}' nor '{fb}' on their own predict '{target_col}' well. "
                            f"But together they might. Think of cooking: flour alone is bland, "
                            f"sugar alone is too sweet, but combined they make cake. "
                            f"Try creating a new feature by multiplying or dividing these two."
                        ),
                        "evidence": (
                            f"Both features show non-linear information (MI: {fa}={mi_a:.3f}, "
                            f"{fb}={mi_b:.3f}) but weak linear signal. They are independent of each "
                            f"other (r={r_ab:.3f}), suggesting their predictive power may emerge "
                            f"only through interaction."
                        ),
                        "question": (
                            f"Engineer interaction features: {fa} × {fb}, {fa}/{fb}, or use "
                            f"tree-based models which automatically discover interaction splits."
                        ),
                        "confidence": 0.6,
                        "impact": "medium",
                        "action": {"label": f"Create interaction", "type": "fix",
                                   "payload": f"interact/{fa}/{fb}"},
                        "status": "unreviewed",
                    })
                    break  # Only one interaction hypothesis per feature

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 6: Temporal Drift / Seasonality
    # ═══════════════════════════════════════════════════════════════
    has_temporal = temporal.get("has_temporal_patterns", False)
    periodicities = temporal.get("detected_periodicities") or []
    time_col = temporal.get("primary_time_col", "")

    if has_temporal and target_detected:
        period_str = ", ".join(periodicities[:3]) if periodicities else "periodic patterns"
        hypotheses.append({
            "id": str(uuid.uuid4())[:8],
            "observation": (
                f"Temporal patterns detected ({period_str}) — time-aware modeling required "
                f"for '{target_col}'"
            ),
            "layman": (
                f"Your data changes over time in a regular pattern (like how ice cream sales "
                f"peak in summer). If you randomly shuffle your data for training and testing, "
                f"the model 'peeks' at the future — like studying the answers before an exam. "
                f"Always split by time: train on older data, test on newer data."
            ),
            "evidence": (
                f"Temporal column '{time_col}' exhibits {period_str}. Standard random "
                f"train/test splits will leak future information into training, producing "
                f"overestimated performance metrics."
            ),
            "question": (
                "Use time-based train/test splits (never random). Consider lag features, "
                "rolling averages, and cyclical time encodings (sin/cos of day-of-week, month) "
                "to capture temporal dynamics."
            ),
            "confidence": 0.9,
            "impact": "high",
            "action": {"label": "View temporal patterns", "type": "navigate",
                       "payload": "temporal"},
            "status": "unreviewed",
        })
    elif has_temporal and not target_detected:
        hypotheses.append({
            "id": str(uuid.uuid4())[:8],
            "observation": (
                f"Temporal structure detected — feature engineering opportunity"
            ),
            "layman": (
                f"Your data has dates/times in the column '{time_col}'. You can extract "
                f"useful information from it: Was it a weekend? Which month? What time of day? "
                f"These new features often reveal hidden patterns."
            ),
            "evidence": (
                f"Datetime column '{time_col}' can be decomposed into day-of-week, month, "
                f"quarter, hour, is_weekend, and cyclical encodings."
            ),
            "question": (
                "Extract temporal sub-features to capture seasonality and behavioral shifts."
            ),
            "confidence": 0.8,
            "impact": "medium",
            "action": {"label": "Extract time features", "type": "fix",
                       "payload": f"extract_datetime/{time_col}"},
            "status": "unreviewed",
        })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 7: Curse of Dimensionality
    # ═══════════════════════════════════════════════════════════════
    if row_count > 0 and total_cols > 0:
        ratio = total_cols / max(math.sqrt(row_count), 1)
        if ratio > 1.0 and total_cols > 15:
            # Count features with negligible target association
            weak_count = 0
            if target_col and mutual_info:
                target_mi = mutual_info.get(target_col, {})
                for feat, mi_val in target_mi.items():
                    if isinstance(mi_val, (int, float)) and mi_val < 0.01:
                        weak_count += 1
            weak_pct = (weak_count / max(total_cols - 1, 1)) * 100

            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": (
                    f"High-dimensional dataset: {total_cols} features vs {row_count:,} rows "
                    f"(feature-to-√sample ratio: {ratio:.1f}x)"
                ),
                "layman": (
                    f"Your dataset has a lot of columns ({total_cols}) compared to the number "
                    f"of rows ({row_count:,}). Imagine trying to draw a trend line through just "
                    f"5 data points in 50-dimensional space — there are infinitely many lines "
                    f"that fit perfectly but predict terribly. Reduce the number of features first."
                ),
                "evidence": (
                    f"With {total_cols} features and {row_count:,} samples, the feature-to-√N "
                    f"ratio is {ratio:.1f}. "
                    + (f"Approximately {weak_pct:.0f}% of features show negligible association with the target. " if weak_count > 0 else "")
                    + "This creates severe overfitting risk without dimensionality reduction."
                ),
                "question": (
                    "Apply L1 regularization (Lasso), recursive feature elimination (RFE), "
                    "or tree-based feature selection to identify the most predictive subset. "
                    "PCA can also compress correlated numeric features."
                ),
                "confidence": min(0.9, 0.5 + ratio / 5),
                "impact": "high",
                "action": {"label": "View feature importance", "type": "navigate",
                           "payload": "feature_importance"},
                "status": "unreviewed",
            })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 8: Class Boundary Overlap (for classification)
    # ═══════════════════════════════════════════════════════════════
    if target_detected and "classification" in problem_type and top_predictors:
        best_predictor = None
        best_score = 0
        for p in top_predictors[:1]:
            if isinstance(p, dict):
                best_predictor = p.get("feature", "")
                best_score = p.get("importance_score", 0)
            else:
                best_predictor = getattr(p, "feature", "")
                best_score = getattr(p, "importance_score", 0)

        if best_predictor and best_score < 0.5:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": (
                    f"Weak class separability: even the best predictor '{best_predictor}' "
                    f"has only {best_score:.3f} association with '{target_col}'"
                ),
                "layman": (
                    f"No single feature does a great job of separating the different categories "
                    f"in '{target_col}'. It's like trying to tell cat breeds apart by weight alone "
                    f"— there's too much overlap. You'll need to combine multiple features "
                    f"together, and even then, don't expect 99% accuracy."
                ),
                "evidence": (
                    f"The strongest predictor '{best_predictor}' achieves only {best_score:.3f} "
                    f"association, suggesting significant distributional overlap between classes. "
                    f"Simple threshold-based or single-feature classifiers will underperform."
                ),
                "question": (
                    "Use ensemble methods (Random Forest, Gradient Boosting) that combine many "
                    "weak features into a strong decision boundary. Set realistic accuracy "
                    "expectations and prioritize precision/recall tradeoffs over raw accuracy."
                ),
                "confidence": 0.75,
                "impact": "medium",
                "action": {"label": f"Explore class distributions", "type": "navigate",
                           "payload": f"profile/{target_col}"},
                "status": "unreviewed",
            })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 9: Geographic / Spatial Confounding
    # ═══════════════════════════════════════════════════════════════
    has_geo = geo.get("has_geo_patterns", False)
    geo_cols = geo.get("geo_columns") or []
    if has_geo and geo_cols and target_detected:
        hypotheses.append({
            "id": str(uuid.uuid4())[:8],
            "observation": (
                f"Geographic features detected ({', '.join(geo_cols[:3])}) — "
                f"spatial confounding risk for '{target_col}'"
            ),
            "layman": (
                f"Your data includes location information. If outcomes vary by geography "
                f"(e.g., house prices differ by city), a random data split could mix locations "
                f"between training and testing, giving falsely good results. Also, nearby "
                f"locations tend to have similar outcomes — your model might just be memorizing "
                f"neighborhoods, not learning real patterns."
            ),
            "evidence": (
                f"Geographic columns: {', '.join(geo_cols[:3])}. Spatial autocorrelation "
                f"can inflate cross-validation scores by 10-30% if locations are not properly "
                f"blocked during splitting."
            ),
            "question": (
                "Use spatial cross-validation (block by region/cluster). Consider deriving "
                "location-based aggregate features (regional averages, distance to landmarks) "
                "as powerful additional predictors."
            ),
            "confidence": 0.75,
            "impact": "medium",
            "action": {"label": "View geographic patterns", "type": "navigate",
                       "payload": "geo"},
            "status": "unreviewed",
        })

    # ═══════════════════════════════════════════════════════════════
    # CATEGORY 10: Heavy-Tailed Outlier Sensitivity
    # ═══════════════════════════════════════════════════════════════
    for col_name, col_info in col_items:
        if not isinstance(col_info, dict):
            continue
        numeric_profile = col_info.get("numeric") or {}
        if not isinstance(numeric_profile, dict):
            continue
        kurt = numeric_profile.get("kurtosis")
        if kurt is None:
            continue

        if kurt > 7:
            mean_val = numeric_profile.get("mean", 0)
            std_val = numeric_profile.get("std", 0)
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": (
                    f"Extreme tail concentration in '{col_name}' "
                    f"(kurtosis = {kurt:.1f}, expected ≈ 3 for normal)"
                ),
                "layman": (
                    f"The feature '{col_name}' has some wildly extreme values compared to "
                    f"the rest. Imagine measuring people's wealth: most are in the thousands, "
                    f"but a few billionaires skew everything. These extreme values can hijack "
                    f"your model, making it focus on the outliers instead of the majority."
                ),
                "evidence": (
                    f"Kurtosis of {kurt:.1f} (vs 3.0 for a normal distribution) indicates "
                    f"extreme value concentration in the tails. "
                    f"Mean={mean_val:.2f}, Std={std_val:.2f}. "
                    f"Distance-based models (KNN, SVM) and linear models are particularly sensitive."
                ),
                "question": (
                    "Apply RobustScaler (uses median/IQR instead of mean/std), "
                    "Winsorize extreme values to the 1st/99th percentile, or use "
                    "tree-based models that are inherently outlier-resistant."
                ),
                "confidence": min(0.9, 0.5 + kurt / 25),
                "impact": "medium",
                "action": {"label": f"View '{col_name}' distribution", "type": "navigate",
                           "payload": f"profile/{col_name}"},
                "status": "unreviewed",
            })

    # ═══════════════════════════════════════════════════════════════
    # Also: High collinearity from flat correlations (backwards compat)
    # ═══════════════════════════════════════════════════════════════
    for pair, corr_val in flat_correlations.items():
        if isinstance(corr_val, (int, float)) and abs(corr_val) > 0.9:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": f"Near-perfect collinearity between features {pair}",
                "layman": (
                    f"Two features ({pair}) are almost identical copies of each other. "
                    f"Keeping both is like having two speedometers in your car — the second one "
                    f"adds no useful information but makes things more confusing for the model."
                ),
                "evidence": (
                    f"Pearson correlation of {corr_val:.3f} indicates these features share "
                    f">81% of their variance. One is likely redundant."
                ),
                "question": (
                    "Drop the feature with lower target association, or combine both using PCA."
                ),
                "confidence": abs(corr_val),
                "impact": "high",
                "action": {"label": "Compare columns", "type": "navigate",
                           "payload": f"compare/{pair}"},
                "status": "unreviewed",
            })

    # ═══════════════════════════════════════════════════════════════
    # FALLBACK: Per-column hypotheses when cross_analysis is sparse
    # These fire from per-column stats alone, ensuring all datasets
    # get some hypotheses even without correlation/MI/target data.
    # ═══════════════════════════════════════════════════════════════
    if len(hypotheses) < 3:
        for col_name, col_info in col_items:
            if not isinstance(col_info, dict):
                continue
            sem_type = col_info.get("semantic_type", col_info.get("inferred_dtype", ""))

            # Skewed numeric features (lower threshold when no cross-analysis)
            numeric_profile = col_info.get("numeric") or {}
            if isinstance(numeric_profile, dict):
                skewness = numeric_profile.get("skewness")
                if skewness is not None and abs(skewness) > 1.5:
                    direction = "right" if skewness > 0 else "left"
                    # Avoid duplicating existing skewness hypotheses
                    already_present = any(
                        col_name in h.get("observation", "")
                        and "skew" in h.get("observation", "").lower()
                        for h in hypotheses
                    )
                    if not already_present:
                        hypotheses.append({
                            "id": str(uuid.uuid4())[:8],
                            "observation": (
                                f"Skewed distribution in '{col_name}' "
                                f"(skewness = {skewness:.2f})"
                            ),
                            "layman": (
                                f"'{col_name}' has most values clustered on one side with a long "
                                f"tail of extreme values. A log or square-root transformation "
                                f"can make the distribution more balanced for modeling."
                            ),
                            "evidence": (
                                f"Skewness of {skewness:.2f} ({direction}-skewed). "
                                f"Values beyond ~1.0 are considered meaningfully skewed."
                            ),
                            "question": (
                                "Consider log or Box-Cox transforms for this feature "
                                "before feeding it to distance-based or linear models."
                            ),
                            "confidence": min(0.8, 0.4 + abs(skewness) / 5),
                            "impact": "medium",
                            "action": {"label": f"View '{col_name}'", "type": "navigate",
                                       "payload": f"profile/{col_name}"},
                            "status": "unreviewed",
                        })

            # High-cardinality categoricals
            if sem_type in ("categorical_nominal", "categorical_ordinal", "object"):
                cardinality = col_info.get("distinct_count") or col_info.get("unique_count", 0)
                null_pct = col_info.get("null_percentage", 0)
                if cardinality > 50 and row_count > 0:
                    ratio = cardinality / max(row_count, 1)
                    if ratio > 0.5:
                        hypotheses.append({
                            "id": str(uuid.uuid4())[:8],
                            "observation": (
                                f"Very high cardinality in categorical feature '{col_name}' "
                                f"({cardinality:,} unique values out of {row_count:,} rows)"
                            ),
                            "layman": (
                                f"'{col_name}' has too many unique categories — almost as many as "
                                f"there are rows. One-hot encoding it would explode the feature space. "
                                f"Consider grouping rare categories into an 'Other' bucket or using "
                                f"target encoding."
                            ),
                            "evidence": (
                                f"{cardinality:,} unique values / {row_count:,} rows = {ratio:.1%} ratio. "
                                f"Standard one-hot encoding would create {cardinality} new columns."
                            ),
                            "question": (
                                "Use frequency encoding, target encoding, or group rare categories "
                                "into 'Other'. Tree models handle high cardinality better than linear ones."
                            ),
                            "confidence": min(0.85, 0.5 + ratio / 2),
                            "impact": "medium",
                            "action": {"label": f"View '{col_name}'", "type": "navigate",
                                       "payload": f"profile/{col_name}"},
                            "status": "unreviewed",
                        })

        # If still zero hypotheses, add a generic dataset-level one
        if len(hypotheses) == 0 and row_count > 0:
            hypotheses.append({
                "id": str(uuid.uuid4())[:8],
                "observation": (
                    f"Dataset overview: {row_count:,} rows × {total_cols} features — "
                    f"cross-column analysis required for deeper hypotheses"
                ),
                "layman": (
                    f"This dataset has {row_count:,} rows and {total_cols} features. "
                    f"To generate richer insights about feature relationships, interactions, "
                    f"and modeling strategies, run the full cross-column profiling step."
                ),
                "evidence": (
                    "Initial per-column analysis found no extreme distributional anomalies. "
                    "Advanced hypotheses (non-linear relationships, latent factors, leakage) "
                    "require correlation matrix and mutual information data."
                ),
                "question": (
                    "Run cross-column analysis (correlations, MI, target detection) to unlock "
                    "hypotheses about feature interactions and modeling strategy."
                ),
                "confidence": 0.5,
                "impact": "low",
                "action": {"label": "Run full profiling", "type": "navigate",
                           "payload": "profiling"},
                "status": "unreviewed",
            })

    # ═══════════════════════════════════════════════════════════════
    # Sort by confidence × impact weight
    # ═══════════════════════════════════════════════════════════════
    impact_weights = {"high": 3, "medium": 2, "low": 1}
    hypotheses.sort(
        key=lambda h: h["confidence"] * impact_weights.get(h["impact"], 1),
        reverse=True
    )

    return hypotheses[:20]
