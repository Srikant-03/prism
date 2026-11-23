"""
Scaling Handler -- Per-column feature scaling and normalization.
Selects scaler based on variable semantics, task type, target/predictor role,
distribution characteristics, model requirements, and domain thresholds.
Does not recommend destructive transformations (like binarize) automatically.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
)


class ScalingHandler:
    """Per-column feature scaling with semantic and distribution-aware method selection."""

    def __init__(
        self,
        df: pd.DataFrame,
        file_id: str,
        profile: Any = None,
        target_column: Optional[str] = None,
        task_type: Optional[str] = None,
        model_type: Optional[str] = None,
        metadata: Optional[dict] = None,
        business_rules: Optional[list] = None,
    ):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile
        self.target_column = target_column
        self.task_type = task_type
        self.model_type = model_type
        self.metadata = metadata or {}
        self.business_rules = business_rules or []

        # Read target state from shared state if available and not explicitly provided
        if not self.target_column or not self.task_type:
            try:
                from state import get_target_state
                ts = get_target_state(file_id)
                if ts:
                    if not self.target_column:
                        self.target_column = ts.get("target_column")
                    if not self.task_type:
                        self.task_type = ts.get("problem_type") or ts.get("task_type")
            except Exception:
                pass

        if not self.target_column and self.profile:
            self.target_column = getattr(self.profile, "target_column", None)
        if not self.task_type and self.profile:
            self.task_type = getattr(self.profile, "task_type", None)

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], dict]:
        actions: list[CleaningAction] = []
        report: dict[str, Any] = {
            "columns_analyzed": 0,
            "scaling_recommendations": [],
        }

        numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
        report["columns_analyzed"] = len(numeric_cols)

        for col in numeric_cols:
            series = self.df[col].dropna()
            if len(series) < 5:
                continue

            stats = self._compute_stats(series)
            strategy, reasoning, threshold = self._select_scaler(col, stats)

            action = self._build_action(col, strategy, reasoning, stats, threshold=threshold)
            actions.append(action)
            report["scaling_recommendations"].append({
                "column": col,
                "strategy": strategy.value,
                "skewness": stats["skewness"],
                "kurtosis": stats["kurtosis"],
            })

        return actions, report

    # ── Distribution stats ────────────────────────────────────────────
    def _compute_stats(self, series: pd.Series) -> dict:
        return {
            "mean": float(series.mean()),
            "std": float(series.std()),
            "min": float(series.min()),
            "max": float(series.max()),
            "median": float(series.median()),
            "q1": float(series.quantile(0.25)),
            "q3": float(series.quantile(0.75)),
            "skewness": float(series.skew()),
            "kurtosis": float(series.kurtosis()),
            "count": len(series),
            "is_positive": bool(series.min() >= 0),
            "is_sparse": bool((series == 0).mean() > 0.3),
            "is_bounded": bool(series.min() >= 0 and series.max() <= 1),
            "n_unique": int(series.nunique()),
        }

    # ── Semantic type detection ───────────────────────────────────────
    def _detect_semantic_type(self, col: str, stats: dict) -> str:
        """Detect column semantic type to prevent destructive transformations."""
        col_lower = col.lower().strip()
        mn, mx = stats["min"], stats["max"]

        # 1. Ordinal Rating / Likert Scale (e.g. 1-5, 1-10)
        if any(w in col_lower for w in ["satisfaction", "rating", "score", "csat", "nps", "stars"]):
            if mn >= 0 and mx <= 10:
                return "ORDINAL_RATING"

        # 2. Percentage / Bounded Rate (e.g. 0-100 or 0-1)
        if any(w in col_lower for w in ["pct", "percent", "percentage", "rate", "ratio", "margin", "discount"]):
            if mn >= 0 and mx <= 100:
                return "PERCENTAGE"

        # 3. Monetary / Financial
        if any(w in col_lower for w in ["revenue", "cost", "profit", "price", "sales", "spend", "expense", "budget", "aov", "avg_order_value"]):
            return "MONETARY"

        # 4. Count / Volume
        if any(w in col_lower for w in ["count", "quantity", "qty", "units_sold", "units", "num_", "orders", "clicks", "prospect_count"]):
            if mn >= 0:
                return "COUNT"

        # 5. Duration / Latency
        if any(w in col_lower for w in ["days", "hours", "minutes", "seconds", "duration", "latency", "delay", "turnaround", "response_time"]):
            if mn >= 0:
                return "DURATION"

        # 6. Datetime Component
        if any(w in col_lower for w in ["year", "quarter", "month", "day", "hour"]):
            if mn >= 0 and mx <= 3000:
                return "DATETIME_COMPONENT"

        return "GENERIC_CONTINUOUS"

    # ── Explicit binarize checker ─────────────────────────────────────
    def _is_explicit_binarize_candidate(self, col: str, stats: dict) -> tuple[bool, Optional[float], str]:
        """
        Check whether binarization is explicitly justified by business problem requirements,
        domain thresholds, explicit metadata, or binary classification target cutoff.
        Never infers binarization from distribution shape or bimodality alone.
        """
        # Check explicit domain thresholds
        domain_thresholds = self.metadata.get("domain_thresholds", {})
        if col in domain_thresholds:
            thresh = float(domain_thresholds[col])
            return True, thresh, f"Explicit domain threshold ({thresh}) configured for '{col}'."

        # Check explicit metadata binarize flags
        binarize_cols = self.metadata.get("binarize_columns", [])
        if col in binarize_cols:
            thresh = float(self.metadata.get("thresholds", {}).get(col, stats["median"]))
            return True, thresh, f"Explicit business requirement configured to binarize '{col}' at threshold {thresh}."

        # Check documented business rules
        col_lower = col.lower()
        for rule in self.business_rules:
            rule_str = str(rule).lower()
            if col_lower in rule_str and any(k in rule_str for k in ["binar", "0/1", "binary", "cutoff", "threshold"]):
                thresh = float(stats["median"])
                return True, thresh, f"Documented business rule specifies binary condition for '{col}': '{rule}'."

        # Check binary classification target with explicit threshold
        if (
            self.task_type in ("binary_classification", "classification")
            and self.target_column == col
            and self.metadata.get("binary_target_threshold") is not None
        ):
            thresh = float(self.metadata["binary_target_threshold"])
            return True, thresh, f"Modeling task is binary classification with target cutoff {thresh} for '{col}'."

        return False, None, ""

    # ── Scaler selection ──────────────────────────────────────────────
    def _select_scaler(self, col: str, stats: dict) -> tuple[ActionType, str, Optional[float]]:
        # 1. Explicit binarization rule check
        is_explicit, threshold, reason = self._is_explicit_binarize_candidate(col, stats)
        if is_explicit:
            return ActionType.BINARIZE, (
                f"Binarize '{col}' into 0/1 using domain threshold {threshold}. {reason}"
            ), threshold

        # Do NOT binarize based on distribution shape or bimodality alone.
        # Continuous variables must preserve granularity and avoid destructive transformations.
        semantic = self._detect_semantic_type(col, stats)
        skew_abs = abs(stats["skewness"])
        skew_raw = stats["skewness"]
        is_positive = stats["is_positive"]
        is_sparse = stats["is_sparse"]
        is_bounded = stats["is_bounded"]
        kurtosis = stats["kurtosis"]
        outlier_pct = self._estimate_outlier_pct(col, stats)

        # 2. Already [0, 1] bounded
        if is_bounded:
            return ActionType.MINMAX_SCALE, (
                f"Column '{col}' is already bounded [0, 1]. MinMaxScaler maintains "
                "this natural range."
            ), None

        # 3. Sparse data -> MaxAbsScaler
        if is_sparse:
            return ActionType.MAXABS_SCALE, (
                f"Column '{col}' is sparse (>30% zeros). MaxAbsScaler preserves "
                "sparsity and zero structure."
            ), None

        # 4. Ordinal ratings / Likert scales (e.g. customer_satisfaction [1.0, 5.0])
        if semantic == "ORDINAL_RATING":
            return ActionType.MINMAX_SCALE, (
                f"Column '{col}' has an ordinal rating scale [{stats['min']:.1f}, {stats['max']:.1f}]. "
                "MinMaxScaler maps values to [0, 1] while preserving relative spacing without "
                "destroying ordinal information."
            ), None

        # 5. Percentages / Bounded rates (e.g. discount_pct, return_rate [0, 100])
        if semantic == "PERCENTAGE":
            if stats["max"] <= 1.0:
                return ActionType.MINMAX_SCALE, (
                    f"Column '{col}' is a percentage bounded [0, 1]. MinMaxScaler preserves "
                    "natural proportion bounds."
                ), None
            elif stats["max"] <= 100.0:
                if skew_abs < 1.0:
                    return ActionType.STANDARD_SCALE, (
                        f"Column '{col}' is a percentage with symmetric distribution (skew={stats['skewness']:.2f}). "
                        "StandardScaler standardizes the range while preserving continuous variation."
                    ), None
                else:
                    return ActionType.MINMAX_SCALE, (
                        f"Column '{col}' is a bounded percentage range [0, 100]. MinMaxScaler "
                        "normalizes the scale to [0, 1]."
                    ), None

        # 6. Financial / Monetary (revenue, cost, profit, etc.)
        if semantic == "MONETARY":
            if skew_raw > 2.0 and is_positive:
                return ActionType.LOG1P_TRANSFORM, (
                    f"Column '{col}' is a financial metric with heavy right skew (skew={stats['skewness']:.2f}). "
                    "Log1p compresses extreme positive values while preserving continuous financial magnitude."
                ), None
            elif skew_raw > 1.0 and is_positive and stats["min"] > 0:
                return ActionType.BOXCOX_TRANSFORM, (
                    f"Column '{col}' is a positive financial metric (skew={stats['skewness']:.2f}). "
                    "Box-Cox stabilizes variance and normalizes the distribution without loss of granularity."
                ), None
            elif skew_raw > 1.0 and not is_positive:
                return ActionType.YEOJOHNSON_TRANSFORM, (
                    f"Column '{col}' is a financial metric with skewness (skew={stats['skewness']:.2f}) "
                    "containing non-positive values. Yeo-Johnson normalizes without discarding negative values."
                ), None
            elif outlier_pct > 5.0 or kurtosis > 5.0:
                return ActionType.ROBUST_SCALE, (
                    f"Column '{col}' is a financial metric with significant outliers ({outlier_pct:.1f}% beyond IQR). "
                    "RobustScaler scales via median and IQR to resist extreme values."
                ), None
            elif skew_abs < 1.0:
                return ActionType.STANDARD_SCALE, (
                    f"Column '{col}' is a financial metric with approximately normal distribution (skew={stats['skewness']:.2f}). "
                    "StandardScaler (Z-score) is the standard choice."
                ), None

        # 7. Count / Volume data (units_sold, prospect_count)
        if semantic == "COUNT":
            if outlier_pct > 10.0 or kurtosis > 10.0 or skew_raw > 3.0:
                return ActionType.ROBUST_SCALE, (
                    f"Column '{col}' is a count metric with heavy-tailed outliers ({outlier_pct:.1f}% beyond IQR, kurtosis={kurtosis:.1f}). "
                    "RobustScaler normalizes using median and IQR without distorting count structure."
                ), None
            elif skew_raw > 1.0 and is_positive:
                return ActionType.LOG1P_TRANSFORM, (
                    f"Column '{col}' is a non-negative count variable with positive skew (skew={stats['skewness']:.2f}). "
                    "Log1p compresses the count range while preserving non-negative ordering."
                ), None
            elif skew_abs < 1.0:
                return ActionType.STANDARD_SCALE, (
                    f"Column '{col}' is a count variable with near-symmetric distribution (skew={stats['skewness']:.2f}). "
                    "StandardScaler standardizes the count scale."
                ), None

        # 8. Duration / Latency (response_time_days)
        if semantic == "DURATION":
            if skew_abs < 1.0 and kurtosis < 5.0:
                return ActionType.STANDARD_SCALE, (
                    f"Column '{col}' represents continuous duration with approximately normal distribution (skew={stats['skewness']:.2f}). "
                    "StandardScaler standardizes the time range without collapsing continuous durations."
                ), None
            elif skew_raw > 1.0 and is_positive:
                return ActionType.LOG1P_TRANSFORM, (
                    f"Column '{col}' represents continuous duration with positive skew (skew={stats['skewness']:.2f}). "
                    "Log1p compresses long duration tails while maintaining continuous time resolution."
                ), None
            elif outlier_pct > 5.0:
                return ActionType.ROBUST_SCALE, (
                    f"Column '{col}' represents continuous duration with outliers ({outlier_pct:.1f}% beyond IQR). "
                    "RobustScaler scales via median and IQR."
                ), None

        # 9. Datetime components (year, month, etc.)
        if semantic == "DATETIME_COMPONENT":
            return ActionType.STANDARD_SCALE, (
                f"Column '{col}' is a discrete datetime component. StandardScaler centers "
                "and standardizes the component for modeling."
            ), None

        # 10. General continuous distribution fallback
        if skew_raw > 2.0 and is_positive:
            return ActionType.LOG1P_TRANSFORM, (
                f"Column '{col}' is highly right-skewed (skew={stats['skewness']:.2f}) "
                "with all positive values. Log1p compresses the range."
            ), None

        if skew_raw > 1.0 and is_positive and stats["min"] > 0:
            return ActionType.BOXCOX_TRANSFORM, (
                f"Column '{col}' is moderately skewed (skew={stats['skewness']:.2f}) "
                "with all positive values. Box-Cox finds the optimal power transformation via MLE."
            ), None

        if skew_abs > 1.0:
            return ActionType.YEOJOHNSON_TRANSFORM, (
                f"Column '{col}' is skewed (skew={stats['skewness']:.2f}) and contains "
                "zero/negative values. Yeo-Johnson handles all value ranges."
            ), None

        if outlier_pct > 5.0 or kurtosis > 10:
            return ActionType.ROBUST_SCALE, (
                f"Column '{col}' has significant outliers ({outlier_pct:.1f}% beyond IQR) "
                f"or heavy tails (kurtosis={kurtosis:.1f}). RobustScaler uses "
                "median/IQR instead of mean/std."
            ), None

        if skew_abs < 1.0:
            return ActionType.STANDARD_SCALE, (
                f"Column '{col}' is approximately normal (skew={stats['skewness']:.2f}). "
                "StandardScaler (Z-score) is the standard choice."
            ), None

        return ActionType.MINMAX_SCALE, (
            f"Column '{col}' has a bounded distribution. MinMaxScaler maps to [0, 1]."
        ), None

    def _is_bimodal(self, col: str) -> bool:
        """Quick bimodality check using KDE local maxima."""
        series = self.df[col].dropna()
        if len(series) < 20:
            return False
        try:
            from scipy.stats import gaussian_kde
            kde = gaussian_kde(series)
            x_range = np.linspace(float(series.min()), float(series.max()), 100)
            density = kde(x_range)
            # Count peaks
            peaks = 0
            for i in range(1, len(density) - 1):
                if density[i] > density[i - 1] and density[i] > density[i + 1]:
                    peaks += 1
            return peaks >= 2
        except Exception:
            return False

    def _estimate_outlier_pct(self, col: str, stats: dict) -> float:
        """Estimate percentage of outliers using IQR."""
        iqr = stats["q3"] - stats["q1"]
        if iqr == 0:
            return 0.0
        lower = stats["q1"] - 1.5 * iqr
        upper = stats["q3"] + 1.5 * iqr
        col_data = self.df[col].dropna() if col in self.df.columns else None
        if col_data is None or len(col_data) == 0:
            return 0.0
        outlier_count = ((col_data < lower) | (col_data > upper)).sum()
        return float(outlier_count / len(col_data) * 100)

    # ── Build action ──────────────────────────────────────────────────
    def _build_action(
        self,
        col: str,
        strategy: ActionType,
        reasoning: str,
        stats: dict,
        threshold: Optional[float] = None,
    ) -> CleaningAction:
        preview_vals = self.df[col].dropna().head(3).tolist()

        all_options = [
            UserOption(key="standard", label="StandardScaler (Z-score)", is_default=(strategy == ActionType.STANDARD_SCALE)),
            UserOption(key="minmax", label="MinMaxScaler [0,1]", is_default=(strategy == ActionType.MINMAX_SCALE)),
            UserOption(key="maxabs", label="MaxAbsScaler", is_default=(strategy == ActionType.MAXABS_SCALE)),
            UserOption(key="robust", label="RobustScaler (median/IQR)", is_default=(strategy == ActionType.ROBUST_SCALE)),
            UserOption(key="log1p", label="Log1p Transform", is_default=(strategy == ActionType.LOG1P_TRANSFORM)),
            UserOption(key="boxcox", label="Box-Cox Transform", is_default=(strategy == ActionType.BOXCOX_TRANSFORM)),
            UserOption(key="yeojohnson", label="Yeo-Johnson Transform", is_default=(strategy == ActionType.YEOJOHNSON_TRANSFORM)),
            UserOption(key="quantile_uniform", label="Quantile (Uniform)", is_default=(strategy == ActionType.QUANTILE_UNIFORM)),
            UserOption(key="quantile_normal", label="Quantile (Normal)", is_default=(strategy == ActionType.QUANTILE_NORMAL)),
            UserOption(key="binarize", label="Binarize (threshold)", is_default=(strategy == ActionType.BINARIZE)),
            UserOption(key="skip", label="Skip"),
        ]

        if strategy == ActionType.BINARIZE:
            thresh_str = f"{threshold:.2f}" if threshold is not None else "threshold"
            recommendation = f"Binarize '{col}' into 0/1 using cutoff threshold {thresh_str}."
            impact_desc = f"Binarizes {stats['count']} values in '{col}' into 0/1 indicator."
        elif strategy in (ActionType.LOG1P_TRANSFORM, ActionType.BOXCOX_TRANSFORM, ActionType.YEOJOHNSON_TRANSFORM):
            recommendation = f"Apply {strategy.value.replace('_', ' ')} to '{col}'."
            impact_desc = f"Transforms all {stats['count']} values in '{col}' using {strategy.value.replace('_', ' ')}."
        else:
            recommendation = f"Apply {strategy.value.replace('_', ' ')} to '{col}'."
            impact_desc = f"Scales all {stats['count']} values in '{col}'."

        return CleaningAction(
            category=ActionCategory.FEATURE_SCALING,
            action_type=strategy,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': mean={stats['mean']:.2f}, std={stats['std']:.2f}, "
                f"skew={stats['skewness']:.2f}, kurtosis={stats['kurtosis']:.2f}, "
                f"range=[{stats['min']:.2f}, {stats['max']:.2f}]."
            ),
            recommendation=recommendation,
            reasoning=reasoning,
            target_columns=[col],
            preview=PreviewSample(
                before=[{col: str(v)} for v in preview_vals],
                columns_before=[col], columns_after=[col],
            ),
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description=impact_desc,
            ),
            options=all_options,
            metadata={
                "skewness": stats["skewness"],
                "kurtosis": stats["kurtosis"],
                "is_positive": stats["is_positive"],
                "is_sparse": stats["is_sparse"],
                "threshold": threshold,
            },
        )

    # ── Static execution methods ──────────────────────────────────────

    @staticmethod
    def apply_scaling(
        df: pd.DataFrame, col: str, method: str, threshold: Optional[float] = None,
    ) -> pd.DataFrame:
        """Apply a scaling method to a column."""
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            return df

        series = df[col].copy()
        non_null = series.dropna()

        if method == "standard":
            mean, std = non_null.mean(), non_null.std()
            if std > 0:
                df[col] = (series - mean) / std

        elif method == "minmax":
            mn, mx = non_null.min(), non_null.max()
            rng = mx - mn
            if rng > 0:
                df[col] = (series - mn) / rng

        elif method == "maxabs":
            ma = non_null.abs().max()
            if ma > 0:
                df[col] = series / ma

        elif method == "robust":
            med = non_null.median()
            q1, q3 = non_null.quantile(0.25), non_null.quantile(0.75)
            iqr = q3 - q1
            if iqr > 0:
                df[col] = (series - med) / iqr

        elif method == "log1p":
            mn = non_null.min()
            if mn < 0:
                df[col] = np.log1p(series - mn + 1)
            else:
                df[col] = np.log1p(series)

        elif method in ("boxcox", "yeojohnson"):
            try:
                from scipy.stats import boxcox, yeojohnson
                if method == "boxcox" and non_null.min() > 0:
                    transformed, _ = boxcox(non_null.values)
                    df.loc[non_null.index, col] = transformed
                elif method == "yeojohnson":
                    transformed, _ = yeojohnson(non_null.values)
                    df.loc[non_null.index, col] = transformed
            except (ImportError, ValueError):
                # Fallback to log1p
                df[col] = np.log1p(series.clip(lower=0))

        elif method == "quantile_uniform":
            try:
                from sklearn.preprocessing import QuantileTransformer
                qt = QuantileTransformer(output_distribution="uniform", random_state=42)
                df.loc[non_null.index, col] = qt.fit_transform(non_null.values.reshape(-1, 1)).flatten()
            except ImportError:
                pass

        elif method == "quantile_normal":
            try:
                from sklearn.preprocessing import QuantileTransformer
                qt = QuantileTransformer(output_distribution="normal", random_state=42)
                df.loc[non_null.index, col] = qt.fit_transform(non_null.values.reshape(-1, 1)).flatten()
            except ImportError:
                pass

        elif method == "binarize":
            cutoff = threshold if threshold is not None else float(non_null.median())
            df[col] = (series >= cutoff).astype(int)

        return df
