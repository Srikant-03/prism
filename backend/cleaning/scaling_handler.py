"""
Scaling Handler — Per-column feature scaling and normalization.
Selects scaler based on distribution characteristics.
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
    """Per-column feature scaling with distribution-aware method selection."""

    def __init__(self, df: pd.DataFrame, file_id: str, profile: Any = None):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile

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
            strategy, reasoning = self._select_scaler(col, stats)

            action = self._build_action(col, strategy, reasoning, stats)
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

    # ── Scaler selection ──────────────────────────────────────────────
    def _select_scaler(self, col: str, stats: dict) -> tuple[ActionType, str]:
        skew = abs(stats["skewness"])
        is_positive = stats["is_positive"]
        is_sparse = stats["is_sparse"]
        is_bounded = stats["is_bounded"]
        kurtosis = stats["kurtosis"]
        q1, q3, iqr = stats["q1"], stats["q3"], stats["q3"] - stats["q1"]

        # Check for bimodal distribution (for binarization)
        if self._is_bimodal(col) and stats["n_unique"] > 5:
            return ActionType.BINARIZE, (
                f"Column '{col}' appears bimodal — a natural threshold exists for "
                "splitting into 0/1 categories."
            )

        # Already [0,1] bounded
        if is_bounded:
            return ActionType.MINMAX_SCALE, (
                f"Column '{col}' is already bounded [0,1]. MinMaxScaler maintains "
                "this natural range."
            )

        # Sparse data → MaxAbsScaler
        if is_sparse:
            return ActionType.MAXABS_SCALE, (
                f"Column '{col}' is sparse (>30% zeros). MaxAbsScaler preserves "
                "sparsity and zero structure."
            )

        # Highly skewed, positive → Log1p
        if skew > 2.0 and is_positive:
            return ActionType.LOG1P_TRANSFORM, (
                f"Column '{col}' is highly right-skewed (skew={stats['skewness']:.2f}) "
                "with all positive values. Log1p compresses the range."
            )

        # Skewed, positive → Box-Cox
        if skew > 1.0 and is_positive and stats["min"] > 0:
            return ActionType.BOXCOX_TRANSFORM, (
                f"Column '{col}' is moderately skewed (skew={stats['skewness']:.2f}) "
                "with all positive values. Box-Cox finds the optimal power "
                "transformation via MLE."
            )

        # Skewed with negatives → Yeo-Johnson
        if skew > 1.0:
            return ActionType.YEOJOHNSON_TRANSFORM, (
                f"Column '{col}' is skewed (skew={stats['skewness']:.2f}) and contains "
                "zero/negative values. Yeo-Johnson handles all value ranges."
            )

        # Heavy outliers → RobustScaler
        outlier_pct = self._estimate_outlier_pct(stats)
        if outlier_pct > 5.0 or kurtosis > 10:
            return ActionType.ROBUST_SCALE, (
                f"Column '{col}' has significant outliers ({outlier_pct:.1f}% beyond IQR) "
                f"or heavy tails (kurtosis={kurtosis:.1f}). RobustScaler uses "
                "median/IQR instead of mean/std."
            )

        # Approximately normal → StandardScaler
        if skew < 1.0:
            return ActionType.STANDARD_SCALE, (
                f"Column '{col}' is approximately normal (skew={stats['skewness']:.2f}). "
                "StandardScaler (Z-score) is the standard choice."
            )

        # Default → MinMaxScaler
        return ActionType.MINMAX_SCALE, (
            f"Column '{col}' has a bounded distribution. MinMaxScaler maps to [0,1]."
        )

    def _is_bimodal(self, col: str) -> bool:
        """Quick bimodality check using Hartigan's dip test approximation."""
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

    def _estimate_outlier_pct(self, stats: dict) -> float:
        """Estimate percentage of outliers using IQR."""
        iqr = stats["q3"] - stats["q1"]
        if iqr == 0:
            return 0.0
        lower = stats["q1"] - 1.5 * iqr
        upper = stats["q3"] + 1.5 * iqr
        col_data = next(
            (self.df[c].dropna() for c in self.df.select_dtypes(include=[np.number]).columns
             if float(self.df[c].dropna().mean()) == stats["mean"]),
            None
        )
        if col_data is None:
            return 0.0
        outlier_count = ((col_data < lower) | (col_data > upper)).sum()
        return float(outlier_count / len(col_data) * 100)

    # ── Build action ──────────────────────────────────────────────────
    def _build_action(
        self, col: str, strategy: ActionType, reasoning: str, stats: dict,
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

        return CleaningAction(
            category=ActionCategory.FEATURE_SCALING,
            action_type=strategy,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': mean={stats['mean']:.2f}, std={stats['std']:.2f}, "
                f"skew={stats['skewness']:.2f}, kurtosis={stats['kurtosis']:.2f}, "
                f"range=[{stats['min']:.2f}, {stats['max']:.2f}]."
            ),
            recommendation=f"Apply {strategy.value.replace('_', ' ')} to '{col}'.",
            reasoning=reasoning,
            target_columns=[col],
            preview=PreviewSample(
                before=[{col: str(v)} for v in preview_vals],
                columns_before=[col], columns_after=[col],
            ),
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                description=f"Scales all {stats['count']} values in '{col}'.",
            ),
            options=all_options,
            metadata={
                "skewness": stats["skewness"],
                "kurtosis": stats["kurtosis"],
                "is_positive": stats["is_positive"],
                "is_sparse": stats["is_sparse"],
            },
        )

    # ── Static execution methods ──────────────────────────────────────

    @staticmethod
    def apply_scaling(df: pd.DataFrame, col: str, method: str) -> pd.DataFrame:
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
            threshold = float(non_null.median())
            df[col] = (series >= threshold).astype(int)

        return df
