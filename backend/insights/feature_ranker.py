import math
from typing import List, Dict, Optional
from profiling.profiling_models import DatasetProfile
from insights.insight_models import FeatureImportance

class FeatureRanker:
    """
    Ranks columns by 'usefulness' or 'information gain' using variance, entropy,
    target correlation, and mutual information.
    """

    @staticmethod
    def rank_features(profile: DatasetProfile) -> List[FeatureImportance]:
        rankings: List[FeatureImportance] = []

        target_col = None
        target_correlations = {}
        if profile.cross_analysis and profile.cross_analysis.get("target"):
            target = profile.cross_analysis["target"]
            target_col = target.get("target_column")
            for predictor in target.get("top_predictors", []):
                # predictor is a dict since it was model_dumped
                target_correlations[predictor.get("feature")] = predictor.get("importance_score")

        # Collect Mutual Information scores (avg across all pairs for a given feature)
        mi_scores: Dict[str, float] = {}
        if profile.cross_analysis and profile.cross_analysis.get("correlations"):
            c_matrix = profile.cross_analysis["correlations"].get("correlation_matrix", {})
            for col_name, row in c_matrix.items():
                if isinstance(row, dict):
                    # Average absolute correlation/MI with other valid features
                    valid_scores = [abs(v) for k, v in row.items() if k != col_name and isinstance(v, (int, float))]
                    if valid_scores:
                        mi_scores[col_name] = sum(valid_scores) / len(valid_scores)

        for col in profile.columns:
            if col.name == target_col:
                continue

            # 1. Variance/Entropy Score (0 to 1)
            # High cardinality categorical or high variance numeric = more info (up to a limit)
            variance_score = 0.0
            reasoning = "Standard feature."

            if col.distinct_count == 1:
                variance_score = 0.0
                reasoning = "Zero variance (constant value)."
            elif col.distinct_count == profile.total_rows and profile.total_rows > 100:
                variance_score = 0.1
                reasoning = "Likely an ID column (perfectly unique) offering minimal predictive value."
            elif col.numeric and col.numeric.std_dev is not None:
                # Normalize std_dev by mean (CV) capped at 1.0
                if col.numeric.mean and col.numeric.mean != 0:
                    cv = abs(col.numeric.std_dev / col.numeric.mean)
                    variance_score = min(1.0, cv)
                else:
                    variance_score = 0.5
            elif col.categorical:
                # Calculate Shannon Entropy approx based on top values percentage
                entropy_proxy = 1.0
                if col.categorical.top_values:
                    p1 = col.categorical.top_values[0].percentage / 100.0
                    if p1 > 0.95:
                        entropy_proxy = 0.1
                        reasoning = "Highly imbalanced categorical distribution."
                    elif p1 < 0.20:
                        entropy_proxy = 0.9
                variance_score = entropy_proxy

            # 2. Target Correlation Score (0 to 1)
            target_corr = target_correlations.get(col.name, 0.0)
            target_corr_abs = abs(target_corr)

            # 3. Overall MI / Collinearity Penalty
            avg_mi = mi_scores.get(col.name, 0.0)
            # If average MI is suspiciously high (>0.8), it's redundant.
            collinearity_penalty = 1.0
            if avg_mi > 0.8:
                collinearity_penalty = 0.5
                reasoning = "Highly correlated with multiple other features (redundant)."

            # Combine scores
            importance_raw = (variance_score * 0.3) + (target_corr_abs * 0.7 if target_corr_abs else avg_mi * 0.5)
            importance_final = min(100.0, importance_raw * 100 * collinearity_penalty)

            # Adjust reasoning if target correlation drove score
            if target_corr_abs > 0.5:
                reasoning = f"Strong direct correlation with the target variable ({target_col})."

            if col.null_percentage > 50:
                importance_final *= 0.2
                reasoning = "Score heavily penalized due to >50% missing values."

            rankings.append(FeatureImportance(
                feature=col.name,
                importance_score=round(importance_final, 2),
                variance_score=round(variance_score, 2),
                correlation_to_target=round(target_corr_abs, 3) if target_col else None,
                mutual_information=round(avg_mi, 3) if avg_mi else None,
                reasoning=reasoning
            ))

        # Sort descending by importance
        rankings.sort(key=lambda x: x.importance_score, reverse=True)
        return rankings
