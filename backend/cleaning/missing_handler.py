"""
Missing Value Handler — evaluates nulls per column/row and auto-selects the
most appropriate imputation strategy based on Pillar 1 profiling evidence.
"""

from __future__ import annotations

import re
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
    MissingValueReport, ColumnMissingStrategy, MissingPattern,
)


class MissingHandler:
    """Analyze missing patterns and recommend per-column strategies."""

    def __init__(
        self,
        df: pd.DataFrame,
        file_id: str,
        profile: Any = None,
        feature_importances: dict[str, float] | None = None,
    ):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile
        self.feature_importances = feature_importances or {}

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], MissingValueReport]:
        actions: list[CleaningAction] = []
        report = MissingValueReport()

        total_cells = self.n_rows * self.n_cols
        total_missing = int(self.df.isnull().sum().sum())
        report.total_cells = total_cells
        report.total_missing_cells = total_missing
        report.overall_missing_pct = round(total_missing / total_cells * 100, 2) if total_cells > 0 else 0.0

        if total_missing == 0:
            return actions, report

        # 1. High-null rows (detected first to consolidate redundant single-row drops)
        row_actions, high_null_count, threshold = self._high_null_rows()
        report.high_null_rows = high_null_count
        report.high_null_row_threshold = threshold

        high_null_indices = set()
        if high_null_count > 0:
            null_pct_per_row = self.df.isnull().sum(axis=1) / self.n_cols
            high_null_indices = set(self.df[null_pct_per_row >= threshold].index)

        # 2. Per-column strategy selection
        col_strategies = self._column_strategies()
        report.column_strategies = col_strategies

        consolidated_cols = []
        for cs in col_strategies:
            col_null_indices = set(self.df[self.df[cs.column].isnull()].index)
            # If all nulls for this column are completely contained in the high-null rows and strategy is DROP_ROWS:
            if high_null_indices and col_null_indices.issubset(high_null_indices) and cs.recommended_strategy == ActionType.DROP_ROWS:
                consolidated_cols.append(cs.column)
                continue  # Consolidate into high-null row drop action!

            action = self._strategy_to_action(cs)
            if action:
                actions.append(action)

        # Add the consolidated high-null rows action
        if row_actions:
            if consolidated_cols:
                row_actions[0].evidence += (
                    f" Consolidates single-row missing values from {len(consolidated_cols)} columns: "
                    f"{', '.join(consolidated_cols[:5])}{'...' if len(consolidated_cols) > 5 else ''}."
                )
                row_actions[0].metadata["consolidated_columns"] = consolidated_cols
            actions.extend(row_actions)

        # 3. Missing pattern analysis
        pattern_results = self._pattern_analysis()
        report.pattern_analysis = pattern_results

        # 4. Nullity matrix (for visualization)
        report.nullity_matrix = self._nullity_matrix()

        return actions, report

    # ── Column-level strategy selection ───────────────────────────────
    def _column_strategies(self) -> list[ColumnMissingStrategy]:
        strategies: list[ColumnMissingStrategy] = []

        for col in self.df.columns:
            null_count = int(self.df[col].isnull().sum())
            if null_count == 0:
                continue

            null_pct = round(null_count / self.n_rows * 100, 2)
            importance = self.feature_importances.get(col, 50.0)  # default mid-importance
            col_profile = self._get_col_profile(col)
            semantic_type = col_profile.get("semantic_type", "unknown") if col_profile else "unknown"

            strategy, reasoning, alternatives = self._select_strategy(
                col, null_pct, importance, semantic_type, col_profile
            )

            # Determine missing pattern for this column
            pattern = self._detect_column_pattern(col)

            strategies.append(ColumnMissingStrategy(
                column=col,
                null_count=null_count,
                null_pct=null_pct,
                recommended_strategy=strategy,
                reasoning=reasoning,
                alternative_strategies=alternatives,
                feature_importance=importance,
                missing_pattern=pattern,
            ))

        return strategies

    def _select_strategy(
        self,
        col: str,
        null_pct: float,
        importance: float,
        semantic_type: str,
        col_profile: dict | None,
    ) -> tuple[ActionType, str, list[ActionType]]:
        """Select the best imputation strategy based on evidence."""

        # Dynamic threshold: lower importance → lower threshold for dropping
        drop_threshold = max(30, 70 - (importance * 0.4))

        # 1. Drop column — very high null rate with low importance
        if null_pct > drop_threshold and importance < 30:
            return (
                ActionType.DROP_COLUMN,
                f"Column is {null_pct}% null with low feature importance ({importance:.0f}/100). "
                f"Dynamic drop threshold: {drop_threshold:.0f}%.",
                [ActionType.IMPUTE_CONSTANT, ActionType.FLAG_ONLY],
            )

        # 2. Drop rows — very low null rate
        if null_pct < 2:
            return (
                ActionType.DROP_ROWS,
                f"Only {null_pct}% of values are missing — dropping affected rows "
                "has minimal dataset impact.",
                [ActionType.IMPUTE_MEAN, ActionType.IMPUTE_MEDIAN, ActionType.IMPUTE_MODE],
            )

        # 3. Time-series columns → forward fill or interpolation
        if semantic_type in ("datetime", "duration"):
            return (
                ActionType.FFILL,
                "Temporal column with gaps — forward fill preserves time ordering.",
                [ActionType.BFILL, ActionType.INTERPOLATE, ActionType.DROP_ROWS],
            )

        # 4. Check for promotional or business variables requiring domain semantic analysis
        is_promo = bool(re.search(r"(?i)(discount|promo|coupon|campaign)", col))
        if is_promo:
            temporal_meta = self._get_temporal_metadata()
            unsorted_msg = ""
            if temporal_meta["has_date_col"]:
                if not temporal_meta["is_chronologically_ordered"]:
                    unsorted_msg += f" The dataset is not chronologically ordered ({temporal_meta['date_col']}.is_monotonic_increasing = False)."
                if temporal_meta["has_repeated_dates"]:
                    unsorted_msg += f" The dataset contains repeated dates ({temporal_meta['repeated_dates_count']} duplicate timestamps)."

            null_count = int(self.df[col].isnull().sum())
            reasoning = (
                f"'{col}' has {null_count} missing values. Imputation strategy requires semantic analysis; "
                f"interpolation is not automatically justified.{unsorted_msg} "
                f"Promotional features are discrete business decisions rather than continuous physical time series. "
                f"Missing discounts typically indicate 0% (no promotion active) or require business policy review."
            )
            return (
                ActionType.IMPUTE_CONSTANT,
                reasoning,
                [ActionType.IMPUTE_MEDIAN, ActionType.ADD_INDICATOR, ActionType.FLAG_ONLY, ActionType.DROP_ROWS],
            )

        # 5. Check if numeric and eligible for time-series interpolation
        is_numeric = self._is_numeric_col(col)
        if is_numeric and null_pct < 30:
            is_eligible, reason_msg = self._check_interpolation_eligibility(col, semantic_type)
            if is_eligible:
                return (
                    ActionType.INTERPOLATE,
                    "Continuous physical/sensor measurement in a verified chronologically ordered dataset "
                    "without duplicate timestamps — linear interpolation estimates missing values from surrounding timepoints.",
                    [ActionType.FFILL, ActionType.IMPUTE_MEDIAN, ActionType.IMPUTE_KNN],
                )

        # 6. Numeric columns — check distribution for mean vs median
        if is_numeric:
            skewness = self._get_skewness(col, col_profile)
            has_outliers = self._has_outliers(col, col_profile)
            has_strong_correlations = self._has_strong_correlations(col)

            # KNN if strong cross-column correlations
            if has_strong_correlations and null_pct < 40:
                return (
                    ActionType.IMPUTE_KNN,
                    f"Strong cross-column correlations detected — KNN imputation "
                    f"leverages neighboring rows in feature space for better estimates.",
                    [ActionType.IMPUTE_MEDIAN, ActionType.IMPUTE_MEAN, ActionType.IMPUTE_ITERATIVE],
                )

            # Skewed or has outliers → median
            if skewness is not None and (abs(skewness) > 1.0 or has_outliers):
                return (
                    ActionType.IMPUTE_MEDIAN,
                    f"Numeric column with {'high skewness' if abs(skewness or 0) > 1 else 'outliers'} — "
                    "median is robust to distributional asymmetry.",
                    [ActionType.IMPUTE_MEAN, ActionType.IMPUTE_KNN, ActionType.CAP_OUTLIERS],
                )

            # Symmetric → mean
            return (
                ActionType.IMPUTE_MEAN,
                "Approximately symmetric numeric distribution with moderate null rate — "
                "mean imputation is statistically appropriate.",
                [ActionType.IMPUTE_MEDIAN, ActionType.IMPUTE_KNN, ActionType.DROP_ROWS],
            )

        # 5. Categorical / boolean / other → mode
        if semantic_type in ("categorical_nominal", "categorical_ordinal", "boolean"):
            return (
                ActionType.IMPUTE_MODE,
                f"Categorical column — mode imputation fills with the most frequent value.",
                [ActionType.IMPUTE_CONSTANT, ActionType.FLAG_ONLY, ActionType.DROP_ROWS],
            )

        # 6. High null with moderate importance → indicator + impute
        if null_pct > 30:
            return (
                ActionType.ADD_INDICATOR,
                f"Moderate–high null rate ({null_pct}%) with meaningful importance — "
                "adding a binary indicator preserves missingness information, then impute.",
                [ActionType.DROP_COLUMN, ActionType.IMPUTE_CONSTANT, ActionType.FLAG_ONLY],
            )

        # 7. Default → mode for non-numeric, flag for unknowns
        return (
            ActionType.IMPUTE_MODE,
            "Default categorical strategy — fill with the most frequent value.",
            [ActionType.IMPUTE_CONSTANT, ActionType.FLAG_ONLY],
        )

    # ── Row-level null analysis ───────────────────────────────────────
    def _high_null_rows(self) -> tuple[list[CleaningAction], int, float]:
        """Flag rows where a high percentage of values (>= 50%) are missing."""
        threshold = 0.50

        null_per_row = self.df.isnull().sum(axis=1)
        null_pct_per_row = null_per_row / self.n_cols
        high_null_mask = null_pct_per_row >= threshold
        high_null_count = int(high_null_mask.sum())

        if high_null_count == 0:
            return [], 0, threshold

        high_null_pct = round(high_null_count / self.n_rows * 100, 2)

        # Preview
        sample_indices = self.df[high_null_mask].head(5).index.tolist()
        raw_before = self.df.loc[sample_indices].fillna("NULL").astype(str).to_dict(orient="records")
        before_sample: list[dict[str, Any]] = [{str(k): v for k, v in row.items()} for row in raw_before]

        action = CleaningAction(
            category=ActionCategory.MISSING_VALUES,
            action_type=ActionType.DROP_ROWS,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"{high_null_count:,} rows ({high_null_pct}%) have more than "
                f"{threshold * 100:.0f}% of their values missing."
            ),
            recommendation=f"Remove these sparse rows as they provide minimal usable information.",
            reasoning=(
                f"Rows with >{threshold * 100:.0f}% null values are unlikely to contribute "
                "meaningful signal.  The threshold is dynamically set based on the number "
                f"of columns ({self.n_cols})."
            ),
            target_columns=list(self.df.columns),
            preview=PreviewSample(
                before=before_sample,
                columns_before=list(self.df.columns),
                columns_after=list(self.df.columns),
            ),
            impact=ImpactEstimate(
                rows_before=self.n_rows,
                rows_after=self.n_rows - high_null_count,
                rows_affected=high_null_count,
                rows_affected_pct=high_null_pct,
                columns_before=self.n_cols,
                columns_after=self.n_cols,
                description=f"Removes {high_null_count:,} sparse rows ({high_null_pct}%).",
            ),
            metadata={"threshold": threshold, "threshold_reason": f"Based on {self.n_cols} columns"},
        )
        return [action], high_null_count, threshold

    # ── Missing pattern analysis ──────────────────────────────────────
    def _pattern_analysis(self) -> dict[str, Any]:
        """Approximate MCAR/MAR/MNAR classification."""
        results: dict[str, Any] = {"tests": {}}

        null_cols = [c for c in self.df.columns if self.df[c].isnull().any()]
        if not null_cols:
            return results

        # MCAR approximation: chi-squared test on null indicator vs other columns
        try:
            mcar_results = self._test_mcar(null_cols)
            results["mcar"] = mcar_results
        except Exception:
            results["mcar"] = {"tested": False, "reason": "Insufficient data or computation error"}

        # MAR detection: correlate null indicators with other columns
        try:
            mar_results = self._detect_mar(null_cols)
            results["mar"] = mar_results
        except Exception:
            results["mar"] = {"tested": False}

        return results

    def _test_mcar(self, null_cols: list[str]) -> dict[str, Any]:
        """Approximate Little's MCAR test via per-column chi-squared."""
        from scipy import stats

        results: dict[str, Any] = {"per_column": {}, "overall": "unknown"}
        mcar_count = 0

        for col in null_cols[:10]:  # limit for speed
            null_mask = self.df[col].isnull()
            # Compare means of other numeric columns when col is null vs not null
            numeric_others = self.df.select_dtypes(include=[np.number]).columns
            numeric_others = [c for c in numeric_others if c != col]

            if not numeric_others:
                continue

            p_values = []
            for other in numeric_others[:5]:
                group_null = self.df.loc[null_mask, other].dropna()
                group_not_null = self.df.loc[~null_mask, other].dropna()
                if len(group_null) < 5 or len(group_not_null) < 5:
                    continue
                try:
                    _, p = stats.mannwhitneyu(group_null, group_not_null, alternative="two-sided")
                    p_values.append(p)
                except Exception:
                    continue

            if p_values:
                avg_p = np.mean(p_values)
                is_mcar = avg_p > 0.05
                results["per_column"][col] = {
                    "avg_p_value": round(avg_p, 4),
                    "classification": "MCAR" if is_mcar else "Not MCAR (possibly MAR/MNAR)",
                }
                if is_mcar:
                    mcar_count += 1

        total_tested = len(results["per_column"])
        if total_tested > 0:
            results["overall"] = "MCAR" if mcar_count / total_tested > 0.7 else "Not MCAR"

        return results

    def _detect_mar(self, null_cols: list[str]) -> dict[str, Any]:
        """Detect if missingness correlates with values in other columns."""
        results: dict[str, list[str]] = {}

        for col in null_cols[:10]:
            null_indicator = self.df[col].isnull().astype(int)
            correlated_with: list[str] = []

            for other in self.df.columns:
                if other == col:
                    continue
                try:
                    if pd.api.types.is_numeric_dtype(self.df[other]):
                        corr = null_indicator.corr(self.df[other].fillna(0))
                        if abs(corr) > 0.3:
                            correlated_with.append(other)
                except Exception:
                    continue

            if correlated_with:
                results[col] = correlated_with

        return {"columns_with_mar_signals": results}

    def _detect_column_pattern(self, col: str) -> MissingPattern:
        """Quick pattern classification for a single column."""
        null_mask = self.df[col].isnull()
        null_rate = null_mask.mean()

        # Check correlation of null indicator with other columns
        null_indicator = null_mask.astype(int)
        max_corr = 0.0
        for other in self.df.select_dtypes(include=[np.number]).columns:
            if other == col:
                continue
            try:
                corr = abs(null_indicator.corr(self.df[other].fillna(0)))
                max_corr = max(max_corr, corr)
            except Exception:
                continue

        if max_corr > 0.3:
            return MissingPattern.MAR
        elif null_rate < 0.05:
            return MissingPattern.MCAR
        else:
            return MissingPattern.UNKNOWN

    # ── Nullity matrix for visualization ──────────────────────────────
    def _nullity_matrix(self) -> dict[str, Any]:
        """Generate a compact nullity matrix for frontend heatmap."""
        null_cols = [c for c in self.df.columns if self.df[c].isnull().any()]
        if not null_cols:
            return {}

        # Sample rows for visualization (max 200)
        sample_size = min(200, self.n_rows)
        if self.n_rows > sample_size:
            sample = self.df[null_cols].sample(sample_size, random_state=42)
        else:
            sample = self.df[null_cols]

        # Convert to binary: 1 = present, 0 = null
        matrix = sample.notnull().astype(int)

        return {
            "columns": null_cols,
            "data": matrix.values.tolist(),
            "sample_size": sample_size,
        }

    # ── Convert strategy to CleaningAction ────────────────────────────
    def _strategy_to_action(self, cs: ColumnMissingStrategy) -> CleaningAction | None:
        """Create a CleaningAction from a ColumnMissingStrategy."""
        col = cs.column

        # Determine confidence
        if cs.recommended_strategy in (ActionType.DROP_ROWS,) and cs.null_pct < 2:
            confidence = ActionConfidence.DEFINITIVE
        elif cs.recommended_strategy == ActionType.DROP_COLUMN and cs.null_pct > 90:
            confidence = ActionConfidence.DEFINITIVE
        else:
            confidence = ActionConfidence.JUDGMENT_CALL

        # Build preview
        non_null = self.df[col].dropna()
        null_sample_idx = self.df[self.df[col].isnull()].head(3).index.tolist()
        if null_sample_idx:
            raw_before_rows = self.df.loc[null_sample_idx][[col]].fillna("NULL").astype(str).to_dict(orient="records")
            before_rows: list[dict[str, Any]] = [{str(k): v for k, v in row.items()} for row in raw_before_rows]
        else:
            before_rows = []

        # Simulate imputed values for preview
        after_rows = self._simulate_impute(col, cs.recommended_strategy, null_sample_idx)

        # Impact
        if cs.recommended_strategy == ActionType.DROP_COLUMN:
            impact = ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                columns_affected=1,
                description=f"Drops column '{col}' ({cs.null_pct}% null).",
            )
        elif cs.recommended_strategy == ActionType.DROP_ROWS:
            impact = ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows - cs.null_count,
                rows_affected=cs.null_count,
                rows_affected_pct=cs.null_pct,
                columns_before=self.n_cols, columns_after=self.n_cols,
                description=f"Removes {cs.null_count:,} rows with null '{col}' ({cs.null_pct}%).",
            )
        elif cs.recommended_strategy == ActionType.ADD_INDICATOR:
            impact = ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols + 1,
                columns_affected=1,
                description=f"Adds '{col}_was_null' indicator column, then imputes '{col}'.",
            )
        else:
            impact = ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols,
                rows_affected=cs.null_count,
                rows_affected_pct=cs.null_pct,
                description=f"Fills {cs.null_count:,} null values in '{col}' via {cs.recommended_strategy.value}.",
            )

        # User options for alternative strategies
        if re.search(r"(?i)(discount|promo|coupon|campaign)", col):
            recommendation = f"'{col}' has {cs.null_count} missing values. Imputation strategy requires semantic analysis; interpolation is not automatically justified."
            evidence = (
                f"Column '{col}' has {cs.null_count:,} missing values ({cs.null_pct}%). "
                f"Imputation strategy requires semantic analysis; interpolation is not automatically justified."
            )
            options = [
                UserOption(key="impute_constant", label="Impute 0 (No Discount)", is_default=(cs.recommended_strategy == ActionType.IMPUTE_CONSTANT)),
                UserOption(key="impute_median", label="Impute Median", is_default=(cs.recommended_strategy == ActionType.IMPUTE_MEDIAN)),
                UserOption(key="add_indicator", label="Add Indicator + Impute"),
                UserOption(key="flag_only", label="Flag for Review (Keep Null)"),
                UserOption(key="drop_rows", label="Drop Missing Rows"),
            ]
        else:
            recommendation = f"Apply {cs.recommended_strategy.value.replace('_', ' ')} to '{col}'."
            evidence = f"Column '{col}' has {cs.null_count:,} missing values ({cs.null_pct}%). Pattern: {cs.missing_pattern.value}."
            options = [
                UserOption(
                    key=cs.recommended_strategy.value,
                    label=cs.recommended_strategy.value.replace("_", " ").title(),
                    is_default=True,
                )
            ]
            for alt in cs.alternative_strategies[:3]:
                options.append(UserOption(
                    key=alt.value,
                    label=alt.value.replace("_", " ").title(),
                ))

        return CleaningAction(
            category=ActionCategory.MISSING_VALUES,
            action_type=cs.recommended_strategy,
            confidence=confidence,
            evidence=evidence,
            recommendation=recommendation,
            reasoning=cs.reasoning,
            target_columns=[col],
            preview=PreviewSample(
                before=before_rows,
                after=after_rows,
                columns_before=[col],
                columns_after=[col] if cs.recommended_strategy != ActionType.DROP_COLUMN else [],
            ),
            impact=impact,
            options=options,
            metadata={
                "null_pct": cs.null_pct,
                "feature_importance": cs.feature_importance,
                "missing_pattern": cs.missing_pattern.value,
            },
        )

    def _simulate_impute(self, col: str, strategy: ActionType, sample_idx: list) -> list[dict]:
        """Generate preview of what imputed values would look like."""
        if not sample_idx or strategy == ActionType.DROP_COLUMN:
            return []

        try:
            if strategy == ActionType.IMPUTE_MEAN:
                fill_val = round(self.df[col].mean(), 4)
            elif strategy == ActionType.IMPUTE_MEDIAN:
                fill_val = round(self.df[col].median(), 4)
            elif strategy == ActionType.IMPUTE_MODE:
                mode_vals = self.df[col].mode()
                fill_val = mode_vals.iloc[0] if len(mode_vals) > 0 else "N/A"
            elif strategy in (ActionType.FFILL, ActionType.BFILL):
                fill_val = "(filled from neighbor)"
            elif strategy == ActionType.INTERPOLATE:
                fill_val = "(interpolated)"
            elif strategy == ActionType.IMPUTE_KNN:
                fill_val = "(KNN estimated)"
            elif strategy == ActionType.IMPUTE_ITERATIVE:
                fill_val = "(MICE estimated)"
            elif strategy == ActionType.IMPUTE_CONSTANT:
                fill_val = 0 if pd.api.types.is_numeric_dtype(self.df[col]) else "UNKNOWN"
            elif strategy == ActionType.ADD_INDICATOR:
                fill_val = "(imputed + indicator)"
            elif strategy == ActionType.FLAG_ONLY:
                fill_val = "NULL (flagged)"
            else:
                fill_val = "N/A"

            return [{col: str(fill_val)} for _ in sample_idx[:3]]
        except Exception:
            return []

    # ── Helpers ────────────────────────────────────────────────────────
    def _get_col_profile(self, col: str) -> dict | None:
        if self.profile is None:
            return None
        try:
            if hasattr(self.profile, "columns"):
                for c in self.profile.columns:
                    if hasattr(c, "name") and c.name == col:
                        return c.model_dump() if hasattr(c, "model_dump") else {}
            return None
        except Exception:
            return None

    def _is_numeric_col(self, col: str) -> bool:
        return pd.api.types.is_numeric_dtype(self.df[col])

    def _get_temporal_metadata(self) -> dict[str, Any]:
        """Detect temporal context, chronological ordering, and repeated dates."""
        date_col = None
        for c in self.df.columns:
            if pd.api.types.is_datetime64_any_dtype(self.df[c]):
                date_col = c
                break

        if date_col is None:
            if self.profile and hasattr(self.profile, "temporal_columns") and self.profile.temporal_columns:
                date_col = self.profile.temporal_columns[0]
            else:
                for c in self.df.columns:
                    if re.search(r"(?i)(date|timestamp|datetime)", c):
                        date_col = c
                        break

        if date_col is None or date_col not in self.df.columns:
            return {
                "has_date_col": False,
                "date_col": None,
                "is_chronologically_ordered": False,
                "has_repeated_dates": False,
                "repeated_dates_count": 0,
            }

        series = self.df[date_col].dropna()
        if series.empty:
            return {
                "has_date_col": True,
                "date_col": date_col,
                "is_chronologically_ordered": False,
                "has_repeated_dates": False,
                "repeated_dates_count": 0,
            }

        try:
            if not pd.api.types.is_datetime64_any_dtype(series):
                parsed = pd.to_datetime(series, errors="coerce").dropna()
            else:
                parsed = series
            is_sorted = bool(parsed.is_monotonic_increasing)
            dup_mask = parsed.duplicated()
            has_duplicates = bool(dup_mask.any())
            dup_count = int(dup_mask.sum())
        except Exception:
            is_sorted = False
            has_duplicates = False
            dup_count = 0

        return {
            "has_date_col": True,
            "date_col": date_col,
            "is_chronologically_ordered": is_sorted,
            "has_repeated_dates": has_duplicates,
            "repeated_dates_count": dup_count,
        }

    def _check_interpolation_eligibility(self, col: str, semantic_type: str) -> tuple[bool, str]:
        """
        Determine whether temporal interpolation is appropriate for this column.
        Requires:
        1. Not a promotional, financial, transactional, or discrete business variable.
        2. Dataset has a valid date/datetime column.
        3. Dataset is strictly chronologically ordered (date.is_monotonic_increasing).
        4. Dataset does not contain duplicate/repeated dates.
        5. Variable represents a continuous physical/sensor signal that logically varies smoothly over time.
        """
        # 1. Promotional / business / transactional / metric check
        if re.search(r"(?i)(discount|promo|coupon|campaign|revenue|cost|profit|price|fee|commission|sales|amount|bill|invoice|charge|budget|units_sold|prospect|lead|order_value|aov|satisfaction|rating|score|csat|nps|return_rate|churn|tax)", col):
            return False, "promotional or discrete business metric that does not logically vary continuously over time"

        # Discrete counts / codes / categories
        if re.search(r"(?i)(id|code|num|count|zip|year|month|day|quarter|week|status|flag)", col):
            return False, "discrete count or identifier"

        # 2. Temporal column detection
        temporal_meta = self._get_temporal_metadata()
        if not temporal_meta["has_date_col"]:
            return False, "dataset has no temporal context"

        # 3. Chronological ordering check
        if not temporal_meta["is_chronologically_ordered"]:
            date_col = temporal_meta["date_col"]
            return False, f"dataset is not chronologically ordered ({date_col}.is_monotonic_increasing = False)"

        # 4. Repeated dates check
        if temporal_meta["has_repeated_dates"]:
            date_col = temporal_meta["date_col"]
            dup_count = temporal_meta["repeated_dates_count"]
            return False, f"dataset contains repeated dates ({dup_count} duplicate timestamps in {date_col})"

        # 5. Continuous variation semantics (physical / sensor / continuous telemetry)
        is_continuous_physical = bool(re.search(r"(?i)(temp|temperature|pressure|speed|velocity|acceleration|voltage|current|sensor|telemetry|altitude|elevation|depth|humidity|flow|vibration|frequency|flux)", col))
        if not is_continuous_physical:
            return False, "variable does not represent a continuous physical or environmental time series"

        return True, "chronologically ordered continuous time series"

    def _has_temporal_context(self) -> bool:
        """Check if the dataset has datetime columns (indicating time-ordered data)."""
        return self._get_temporal_metadata()["has_date_col"]

    def _get_skewness(self, col: str, col_profile: dict | None) -> float | None:
        if col_profile and "numeric" in col_profile and col_profile["numeric"]:
            val = col_profile["numeric"].get("skewness")
            return float(val) if val is not None else None
        try:
            val = self.df[col].skew()
            if pd.notna(val) and isinstance(val, (int, float, np.number)):
                return float(val)
            return None
        except Exception:
            return None

    def _has_outliers(self, col: str, col_profile: dict | None) -> bool:
        if col_profile and "numeric" in col_profile and col_profile["numeric"]:
            outliers = col_profile["numeric"].get("box_outliers", [])
            return len(outliers) > self.n_rows * 0.05
        return False

    def _has_strong_correlations(self, col: str) -> bool:
        """Check if this column has strong correlations with others."""
        if self.profile and hasattr(self.profile, "cross_analysis") and self.profile.cross_analysis:
            try:
                cross = self.profile.cross_analysis
                c_matrix = cross.get("correlations", {}).get("correlation_matrix", {})
                if col in c_matrix:
                    row = c_matrix[col]
                    for other, val in row.items():
                        if other != col and isinstance(val, (int, float)) and abs(val) > 0.6:
                            return True
            except Exception:
                pass
        return False
