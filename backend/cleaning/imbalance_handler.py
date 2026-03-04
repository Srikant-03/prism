"""
Imbalance Handler — Detects class imbalance in categorical target columns
and recommends resampling/weighting strategies.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
)


class ImbalanceHandler:
    """Class imbalance detection and treatment recommendation."""

    def __init__(
        self, df: pd.DataFrame, file_id: str,
        target_column: Optional[str] = None,
        profile: Any = None,
    ):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.target_column = target_column
        self.profile = profile

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], dict]:
        actions: list[CleaningAction] = []
        report: dict[str, Any] = {
            "target_column": None,
            "is_imbalanced": False,
            "class_counts": {},
            "class_proportions": {},
            "imbalance_ratio": 0,
            "severity": "balanced",
            "recommended_strategy": None,
        }

        target = self._identify_target()
        if target is None:
            return actions, report

        report["target_column"] = target
        series = self.df[target].dropna()
        if len(series) < 10:
            return actions, report

        vc = series.value_counts()
        props = series.value_counts(normalize=True)
        n_classes = len(vc)

        report["class_counts"] = vc.to_dict()
        report["class_proportions"] = {str(k): round(float(v), 4) for k, v in props.items()}

        majority = int(vc.iloc[0])
        minority = int(vc.iloc[-1])
        ratio = round(majority / minority, 2) if minority > 0 else float("inf")
        report["imbalance_ratio"] = ratio

        # Classify severity
        severity = self._classify_severity(ratio)
        report["severity"] = severity
        report["is_imbalanced"] = severity != "balanced"

        if severity == "balanced":
            return actions, report

        # Per-class stats for multi-class
        per_class: list[dict] = []
        for cls_val in vc.index:
            count = int(vc[cls_val])
            prop = float(props[cls_val])
            per_class.append({
                "class": str(cls_val),
                "count": count,
                "proportion": round(prop, 4),
                "is_minority": prop < (1 / n_classes) * 0.5,
            })
        report["per_class_stats"] = per_class

        # Determine feature types for strategy selection
        has_numeric = len(self.df.select_dtypes(include=[np.number]).columns) > 0
        has_categorical = len(self.df.select_dtypes(include=["object", "category"]).columns) > 1
        is_binary = n_classes == 2

        # Recommend strategies
        strategy, reasoning = self._select_strategy(
            severity, ratio, n_classes, has_numeric, has_categorical, is_binary
        )
        report["recommended_strategy"] = strategy.value

        # Build action
        action = self._build_action(target, strategy, reasoning, report, has_numeric, has_categorical)
        actions.append(action)

        # Always offer class weights as an alternative
        if strategy != ActionType.CLASS_WEIGHTS:
            weights_action = self._build_weights_action(target, vc, report)
            actions.append(weights_action)

        return actions, report

    # ── Target detection ──────────────────────────────────────────────
    def _identify_target(self) -> Optional[str]:
        if self.target_column and self.target_column in self.df.columns:
            s = self.df[self.target_column]
            if not pd.api.types.is_numeric_dtype(s) or s.nunique() <= 20:
                return self.target_column

        # Auto-detect: look for columns named "target", "label", "class", "y"
        target_names = ["target", "label", "class", "y", "outcome", "survived",
                        "churn", "default", "fraud", "spam", "category"]
        for col in self.df.columns:
            if col.lower().strip() in target_names:
                if self.df[col].nunique() <= 20:
                    return col

        # Check last column if categorical with low cardinality
        last_col = self.df.columns[-1]
        if self.df[last_col].nunique() <= 10 and (
            self.df[last_col].dtype == object or pd.api.types.is_categorical_dtype(self.df[last_col])
        ):
            return last_col

        return None

    # ── Severity classification ───────────────────────────────────────
    @staticmethod
    def _classify_severity(ratio: float) -> str:
        if ratio < 1.5:
            return "balanced"
        elif ratio < 5:
            return "mild"
        elif ratio < 20:
            return "moderate"
        elif ratio < 100:
            return "severe"
        else:
            return "extreme"

    # ── Strategy selection ────────────────────────────────────────────
    def _select_strategy(
        self, severity: str, ratio: float, n_classes: int,
        has_numeric: bool, has_categorical: bool, is_binary: bool,
    ) -> tuple[ActionType, str]:

        # Extreme imbalance → anomaly detection framing
        if severity == "extreme":
            return ActionType.ANOMALY_FRAMING, (
                f"Extreme class imbalance (ratio {ratio}:1). At this level, "
                "standard classification fails. Recommend reframing as anomaly "
                "detection — treat the minority class as anomalous events."
            )

        # Severe → combination methods
        if severity == "severe":
            return ActionType.SMOTEENN, (
                f"Severe imbalance (ratio {ratio}:1). SMOTE+ENN combines "
                "synthetic oversampling with edited nearest neighbors cleanup "
                "to balance classes while removing noisy boundary samples."
            )

        # Moderate with mixed features → SMOTE-NC
        if severity == "moderate" and has_numeric and has_categorical:
            return ActionType.SMOTE_NC, (
                f"Moderate imbalance ({ratio}:1) with mixed numeric/categorical features. "
                "SMOTE-NC handles both feature types when generating synthetic samples."
            )

        # Moderate with numeric only → SMOTE
        if severity == "moderate" and has_numeric:
            return ActionType.SMOTE, (
                f"Moderate imbalance ({ratio}:1) with numeric features. "
                "SMOTE generates synthetic minority samples by interpolating "
                "between existing minority neighbors."
            )

        # Moderate with categorical only → random oversample
        if severity == "moderate":
            return ActionType.RANDOM_OVERSAMPLE, (
                f"Moderate imbalance ({ratio}:1) with categorical features. "
                "Random oversampling duplicates minority class samples."
            )

        # Mild → class weights (simplest, least invasive)
        return ActionType.CLASS_WEIGHTS, (
            f"Mild imbalance ({ratio}:1). Class weights are the least invasive "
            "approach — they instruct the model to penalize misclassification "
            "of minority classes more heavily without modifying the data."
        )

    # ── Build actions ─────────────────────────────────────────────────
    def _build_action(
        self, target: str, strategy: ActionType, reasoning: str,
        report: dict, has_numeric: bool, has_categorical: bool,
    ) -> CleaningAction:
        severity = report["severity"]
        ratio = report["imbalance_ratio"]
        n_classes = len(report["class_counts"])

        # Build comprehensive options
        options = self._get_all_options(has_numeric, has_categorical, strategy)

        minority_count = min(report["class_counts"].values())
        majority_count = max(report["class_counts"].values())

        return CleaningAction(
            category=ActionCategory.CLASS_IMBALANCE,
            action_type=strategy,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Target '{target}' has {n_classes} classes with {severity} imbalance "
                f"(ratio {ratio}:1). Majority class: {majority_count} samples, "
                f"minority class: {minority_count} samples."
            ),
            recommendation=f"Apply {strategy.value.replace('_', ' ')} to balance classes.",
            reasoning=reasoning,
            target_columns=[target],
            impact=ImpactEstimate(
                rows_before=self.n_rows,
                rows_after=self.n_rows,  # Will be updated after actual resampling
                description=f"Addresses {severity} class imbalance ({ratio}:1).",
            ),
            options=options,
            metadata={
                "severity": severity,
                "imbalance_ratio": ratio,
                "n_classes": n_classes,
                "class_counts": report["class_counts"],
                "has_numeric": has_numeric,
                "has_categorical": has_categorical,
            },
        )

    def _build_weights_action(self, target: str, vc: pd.Series, report: dict) -> CleaningAction:
        # Compute balanced class weights
        total = int(vc.sum())
        n_classes = len(vc)
        weights = {}
        for cls_val, count in vc.items():
            w = total / (n_classes * int(count))
            weights[str(cls_val)] = round(float(w), 4)

        return CleaningAction(
            category=ActionCategory.CLASS_IMBALANCE,
            action_type=ActionType.CLASS_WEIGHTS,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Computed balanced class weights for '{target}': {weights}. "
                "These weights can be passed to any sklearn-compatible classifier."
            ),
            recommendation="Use class weights instead of resampling.",
            reasoning=(
                "Class weighting is the least invasive approach — it doesn't modify "
                "the data but instructs the model to penalize minority class errors "
                "proportionally. Works with most classifiers."
            ),
            target_columns=[target],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description="No data modification; weights applied at model training time.",
            ),
            options=[
                UserOption(key="balanced", label="Balanced Weights (auto)", is_default=True),
                UserOption(key="skip", label="Skip"),
            ],
            metadata={"class_weights": weights, "severity": report["severity"]},
        )

    def _get_all_options(
        self, has_numeric: bool, has_categorical: bool, default: ActionType,
    ) -> list[UserOption]:
        options = []

        # Oversampling methods
        if has_numeric:
            options.append(UserOption(
                key="smote", label="SMOTE (synthetic oversampling)",
                is_default=(default == ActionType.SMOTE),
            ))
            options.append(UserOption(key="adasyn", label="ADASYN (adaptive synthetic)"))

        if has_numeric and has_categorical:
            options.append(UserOption(
                key="smote_nc", label="SMOTE-NC (handles mixed types)",
                is_default=(default == ActionType.SMOTE_NC),
            ))

        options.append(UserOption(
            key="random_over", label="Random Oversampling",
            is_default=(default == ActionType.RANDOM_OVERSAMPLE),
        ))

        # Undersampling methods
        options.append(UserOption(key="random_under", label="Random Undersampling"))
        options.append(UserOption(key="tomek", label="Tomek Links Removal"))
        options.append(UserOption(key="enn", label="Edited Nearest Neighbors"))
        options.append(UserOption(key="nearmiss", label="NearMiss"))

        # Combination
        if has_numeric:
            options.append(UserOption(
                key="smoteenn", label="SMOTE + ENN (combo)",
                is_default=(default == ActionType.SMOTEENN),
            ))
            options.append(UserOption(key="smotetomek", label="SMOTE + Tomek (combo)"))

        # Weights
        options.append(UserOption(
            key="weights", label="Class Weights Only",
            is_default=(default == ActionType.CLASS_WEIGHTS),
        ))

        # Anomaly framing
        options.append(UserOption(
            key="anomaly", label="Reframe as Anomaly Detection",
            is_default=(default == ActionType.ANOMALY_FRAMING),
        ))

        options.append(UserOption(key="skip", label="Skip"))
        return options

    # ── Static execution methods ──────────────────────────────────────

    @staticmethod
    def apply_random_oversample(df: pd.DataFrame, target: str) -> pd.DataFrame:
        """Random oversampling by duplicating minority class rows."""
        vc = df[target].value_counts()
        max_count = int(vc.max())
        frames = [df]
        for cls_val in vc.index:
            cls_df = df[df[target] == cls_val]
            if len(cls_df) < max_count:
                n_needed = max_count - len(cls_df)
                oversampled = cls_df.sample(n=n_needed, replace=True, random_state=42)
                frames.append(oversampled)
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def apply_random_undersample(df: pd.DataFrame, target: str) -> pd.DataFrame:
        """Random undersampling by removing majority class rows."""
        vc = df[target].value_counts()
        min_count = int(vc.min())
        frames = []
        for cls_val in vc.index:
            cls_df = df[df[target] == cls_val]
            if len(cls_df) > min_count:
                frames.append(cls_df.sample(n=min_count, random_state=42))
            else:
                frames.append(cls_df)
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def apply_smote(df: pd.DataFrame, target: str) -> pd.DataFrame:
        """SMOTE oversampling using imblearn."""
        try:
            from imblearn.over_sampling import SMOTE
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if target in numeric_cols:
                numeric_cols.remove(target)
            if not numeric_cols:
                return ImbalanceHandler.apply_random_oversample(df, target)

            X = df[numeric_cols].fillna(0)
            y = df[target]
            sm = SMOTE(random_state=42)
            X_res, y_res = sm.fit_resample(X, y)
            result = pd.DataFrame(X_res, columns=numeric_cols)
            result[target] = y_res
            # Re-add non-numeric columns (NaN for synthetic rows)
            for col in df.columns:
                if col not in result.columns:
                    result[col] = np.nan
            return result
        except ImportError:
            return ImbalanceHandler.apply_random_oversample(df, target)

    @staticmethod
    def compute_class_weights(series: pd.Series) -> dict:
        """Compute balanced class weights."""
        vc = series.value_counts()
        total = int(vc.sum())
        n_classes = len(vc)
        weights = {}
        for cls_val, count in vc.items():
            weights[str(cls_val)] = round(total / (n_classes * int(count)), 4)
        return weights
