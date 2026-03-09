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

    # ── Strong target keywords (exact token match via word boundaries) ──
    # These cover the most common target variable naming conventions across
    # healthcare, finance, retail, IoT, HR, education, and general ML.
    TARGET_HINTS_STRONG = [
        # General ML
        'target', 'label', 'outcome', 'result', 'prediction', 'predicted',
        'class', 'y', 'dependent', 'response', 'output',
        # Scores / Ratings
        'score', 'grade', 'rating', 'rank',
        # Healthcare
        'diagnosis', 'disease', 'mortality', 'readmission', 'severity',
        'positive', 'negative', 'malignant', 'benign', 'survival',
        # Finance
        'default', 'bankrupt', 'risk', 'fraud', 'anomaly',
        'return', 'profit', 'loss', 'credit',
        # Retail / Marketing
        'churn', 'purchase', 'conversion', 'revenue', 'demand',
        'satisfaction', 'sentiment', 'recommend',
        # HR
        'attrition', 'turnover', 'performance', 'promotion',
        'hired', 'fired', 'left', 'resigned',
        # IoT / Manufacturing
        'failure', 'defect', 'quality', 'downtime', 'rul',
        # Education
        'exam_score', 'final_score', 'test_score', 'passed', 'failed',
        # Survival analysis
        'survived', 'event', 'censored',
    ]

    # ── Weaker prefix/suffix hints ──
    TARGET_HINTS_WEAK = [
        'is_', 'has_', 'was_', 'will_',
        'price', 'salary', 'amount', 'total', 'count',
        'status', 'approved', 'accepted', 'rejected', 'flag',
        'forecast', 'predicted', 'expected', 'estimated',
        'click', 'engagement', 'nps', 'ltv', 'value',
    ]

    # ── Anti-hints: columns that should NEVER be targets ──
    ANTI_HINTS = [
        'id', 'index', 'idx', 'key', 'pk',
        'name', 'first_name', 'last_name', 'username',
        'date', 'time', 'timestamp', 'created', 'updated', 'modified',
        'url', 'uri', 'link', 'path',
        'email', 'phone', 'address', 'city', 'state', 'zip', 'country',
        'description', 'comment', 'note', 'text', 'body',
        'file', 'image', 'photo', 'avatar',
    ]

    @staticmethod
    def _token_match(col_name: str, hints: list) -> bool:
        """Check if any hint appears as a full token in the column name.
        Splits on underscores/hyphens and checks for exact token equality."""
        tokens = col_name.lower().replace('-', '_').split('_')
        for hint in hints:
            hint_tokens = hint.lower().replace('-', '_').rstrip('_').split('_')
            for i in range(len(tokens) - len(hint_tokens) + 1):
                if tokens[i:i+len(hint_tokens)] == hint_tokens:
                    return True
        return False

    @staticmethod
    def _is_sequential_counter(s: pd.Series) -> bool:
        """Detect if a numeric series is a monotonically increasing counter (e.g. row index)."""
        if not pd.api.types.is_numeric_dtype(s):
            return False
        try:
            s_clean = s.dropna()
            if len(s_clean) < 10:
                return False
            diffs = s_clean.diff().dropna()
            if len(diffs) == 0:
                return False
            # If >95% of diffs are exactly 1, it's a sequential counter
            return (diffs == 1).mean() > 0.95
        except Exception:
            return False

    def analyze(self, df: pd.DataFrame, dataset_profile: DatasetProfile, correlations: CorrelationAnalysis) -> TargetAnalysis:
        if df.empty or len(dataset_profile.columns) < 2:
            return TargetAnalysis(is_target_detected=False)

        # 1. Score columns based on heuristics
        scores = {}
        reasons_map = {}
        for col in dataset_profile.columns:
            if col.name not in df.columns:
                continue

            name_lower = col.name.lower()
            score = 0.0
            col_reasons = []

            # Strong name match (token-based, not substring)
            if TargetDetector._token_match(col.name, TargetDetector.TARGET_HINTS_STRONG):
                score += 4.0
                col_reasons.append("Column name strongly suggests a target variable")

            # Weak prefix/suffix match
            if any(name_lower.startswith(h) or name_lower.endswith(h.rstrip('_')) for h in TargetDetector.TARGET_HINTS_WEAK):
                score += 1.5
                col_reasons.append("Column name contains a weak target indicator")

            # Binary or Categorical with low cardinality are good class targets
            if col.semantic_type == 'boolean':
                score += 2.0
                col_reasons.append("Boolean feature is a natural classification target")
            elif col.semantic_type in ('categorical_nominal', 'categorical_ordinal'):
                try:
                    n_unique = df[col.name].nunique()
                    if 2 <= n_unique <= 10:
                        score += 1.5
                        col_reasons.append(f"Low cardinality categorical ({n_unique} classes)")
                except Exception:
                    pass

            # ID columns or free text are terrible targets
            if col.semantic_type in ('id_key', 'free_text', 'url', 'hashed', 'email', 'phone'):
                score -= 10.0

            # Anti-hints: columns whose names indicate metadata, not targets
            if TargetDetector._token_match(col.name, TargetDetector.ANTI_HINTS):
                score -= 3.0

            # Sequential counter penalty (row index, auto-increment IDs)
            try:
                if pd.api.types.is_numeric_dtype(df[col.name]) and TargetDetector._is_sequential_counter(df[col.name]):
                    score -= 5.0
            except Exception:
                pass

            # Structural location (last column is traditionally the target)
            if col.name == df.columns[-1]:
                score += 1.5
                col_reasons.append("Positioned as the last column in the dataset")

            # Numeric continuous targets with many unique values are good regression targets
            if col.semantic_type in ('numeric_continuous',) and score > 0:
                score += 0.5

            if score > 0:
                scores[col.name] = score
                reasons_map[col.name] = col_reasons

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
        reasons = reasons_map.get(target_name, [])
        if not reasons:
            reasons = ["Statistical heuristic scoring"]
        
        justification = "Detected as target candidate because: " + "; ".join(reasons) + "."

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
