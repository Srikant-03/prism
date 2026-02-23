import numpy as np
import pandas as pd
from typing import List, Optional, Dict, Any

from profiling.cross_column_models import TargetAnalysis, FeatureImportance, CorrelationAnalysis
from profiling.profiling_models import DatasetProfile

class TargetDetector:
    """
    Heuristic-based detector to find the most likely target variable
    for machine learning or prediction in the dataset.
    """

    TARGET_HINTS = [
        'target', 'label', 'outcome', 'class', 'churn', 'fraud', 'status',
        'is_', 'has_', 'revenue', 'price', 'diagnosis', 'survived', 'default'
    ]

    def analyze(self, df: pd.DataFrame, dataset_profile: DatasetProfile, correlations: CorrelationAnalysis) -> TargetAnalysis:
        if df.empty or len(dataset_profile.columns) < 2:
            return TargetAnalysis(is_target_detected=False)

        # 1. Score columns based on heuristics
        scores = {}
        for col in dataset_profile.columns:
            if col.name not in df.columns:
                continue

            name_lower = col.name.lower()
            score = 0.0

            # Name match
            if any(hint in name_lower for hint in TargetDetector.TARGET_HINTS):
                score += 3.0

            # Binary or Categorical with low cardinality are good class targets
            if col.semantic_type == 'boolean':
                score += 2.0
            elif col.semantic_type in ('categorical_nominal', 'categorical_ordinal'):
                # Ideal classification targets have 2-10 classes
                # Need to look up cardinality from profile if we parsed it properly,
                # but we can also just check unique values directly
                try:
                    n_unique = df[col.name].nunique()
                    if 2 <= n_unique <= 10:
                        score += 1.5
                except Exception:
                    pass

            # ID columns or free text are terrible targets
            if col.semantic_type in ('id_key', 'free_text', 'url', 'hashed', 'email', 'phone'):
                score -= 5.0

            # Structural location (often last column or first column)
            if col.name == df.columns[-1]:
                score += 1.0

            if score > 0:
                scores[col.name] = score

        if not scores:
            return TargetAnalysis(is_target_detected=False)

        # Select highest scoring column
        best_target = max(scores.items(), key=lambda x: x[1])
        target_name = best_target[0]
        confidence = min(best_target[1] / 6.0, 1.0) # Normalize somewhat

        if confidence < 0.3:
            return TargetAnalysis(is_target_detected=False)

        # 2. Determine Problem Type
        col_type = next((c.semantic_type for c in dataset_profile.columns if c.name == target_name), 'unknown')
        
        problem_type = "unknown"
        imbalance_ratio = None
        class_dist = None

        s = df[target_name].dropna()
        n_unique_vals = s.nunique()
        total_vals = len(s)
        is_numeric = pd.api.types.is_numeric_dtype(s)
        is_float = pd.api.types.is_float_dtype(s)

        if col_type == 'boolean' or n_unique_vals == 2:
            problem_type = "binary_classification"
            counts = s.value_counts(normalize=True)
            class_dist = {str(k): float(v) for k, v in counts.items()}
            if len(counts) == 2:
                imbalance_ratio = float(counts.max() / counts.min())
        
        elif not is_numeric:
            if n_unique_vals <= 100:
                problem_type = "multiclass_classification"
                counts = s.value_counts(normalize=True)
                class_dist = {str(k): float(v) for k, v in counts.head(10).items()}
                if len(counts) > 1:
                    imbalance_ratio = float(counts.max() / counts.min())
            else:
                problem_type = "classification_high_cardinality"
                
        else:
            # Numeric column
            if is_float and n_unique_vals > 15:
                # Continuous floats are almost always regression
                problem_type = "regression"
            elif n_unique_vals <= 20:
                # Small number of distinct numeric values
                problem_type = "multiclass_classification"
            elif not is_float and n_unique_vals < (total_vals * 0.1):
                # Integers where categories make up less than 10% of dataset size
                problem_type = "multiclass_classification"
            else:
                problem_type = "regression"
                
            if problem_type == "multiclass_classification":
                counts = s.value_counts(normalize=True)
                class_dist = {str(k): float(v) for k, v in counts.head(20).items()}
                if len(counts) > 1:
                    imbalance_ratio = float(counts.max() / counts.min())

        # 3. Pull Top Predictors from Correlation Matrix
        top_predictors: List[FeatureImportance] = []
        target_correlations = []

        # Check existing strong pairs
        for pair in correlations.strongest_pairs:
            if pair.col1 == target_name:
                target_correlations.append((pair.col2, pair.score))
            elif pair.col2 == target_name:
                target_correlations.append((pair.col1, pair.score))

        # Check mutual info
        if target_name in correlations.mutual_information:
            mi_dict = correlations.mutual_information[target_name]
            for f, score in mi_dict.items():
                # Avoid duplicates
                if not any(t[0] == f for t in target_correlations):
                    target_correlations.append((f, score))

        # Sort and take top 10
        target_correlations.sort(key=lambda x: abs(x[1]), reverse=True)
        for f, score in target_correlations[:10]:
            top_predictors.append(FeatureImportance(feature=f, importance_score=abs(float(score))))

        # Build justification
        reasons = []
        if any(h in target_name.lower() for h in TargetDetector.TARGET_HINTS):
            reasons.append(f"Header name contains strong hints.")
        if target_name == df.columns[-1]:
            reasons.append(f"Positioned as the last column.")
        
        justification = "Detected as target candidate because: " + " ".join(reasons)

        return TargetAnalysis(
            is_target_detected=True,
            target_column=target_name,
            confidence=confidence,
            justification=justification,
            problem_type=problem_type,
            class_distribution=class_dist,
            imbalance_ratio=imbalance_ratio,
            top_predictors=top_predictors
        )
