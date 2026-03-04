"""
Leakage Handler — Detects data leakage patterns:
temporal proxies, near-perfect predictors, ID encoding, future-looking columns.
All leakage suspects are flagged with CRITICAL severity.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, UserOption,
)

# Columns that semantically refer to the future
_FUTURE_PATTERNS = [
    r"(?i)(outcome|result|disposition|resolution|final|conclusion)",
    r"(?i)(follow[-_]?up|followup|next[-_]?step|response)",
    r"(?i)(cancel|churn|return|refund|complaint)[-_]?(date|time|at|ts)",
    r"(?i)(close|closed|resolved|completed|finished)[-_]?(date|time|at|ts)",
    r"(?i)(end[-_]?date|end[-_]?time|expiry|expiration)",
]

# ID column patterns
_ID_PATTERNS = re.compile(
    r"^(id|_id|row_?id|index|record_?id|entry_?id|seq|sequence|serial|number)$"
    r"|"
    r"([-_]id$|[-_]key$|[-_]num$)",
    re.IGNORECASE,
)


class LeakageHandler:
    """Data leakage detection engine."""

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
            "leakage_suspects": [],
            "temporal_proxies": [],
            "perfect_predictors": [],
            "id_leakage": [],
            "future_columns": [],
        }

        target = self._identify_target()
        if target is None:
            # Even without target, check for future-looking columns
            for col in self.df.columns:
                future = self._check_future_column(col)
                if future:
                    report["future_columns"].append(future)
                    actions.append(self._build_future_action(col, future))
            return actions, report

        # 1. Near-perfect predictors
        predictors = self._detect_perfect_predictors(target)
        report["perfect_predictors"] = predictors
        for pred in predictors:
            actions.append(self._build_predictor_action(pred))

        # 2. Temporal proxies
        temporal = self._detect_temporal_proxies(target)
        report["temporal_proxies"] = temporal
        for tp in temporal:
            actions.append(self._build_temporal_action(tp))

        # 3. ID leakage
        id_leaks = self._detect_id_leakage(target)
        report["id_leakage"] = id_leaks
        for leak in id_leaks:
            actions.append(self._build_id_action(leak))

        # 4. Future-looking columns
        for col in self.df.columns:
            if col == target:
                continue
            future = self._check_future_column(col)
            if future:
                report["future_columns"].append(future)
                actions.append(self._build_future_action(col, future))

        report["leakage_suspects"] = [a.target_columns[0] for a in actions]
        return actions, report

    # ── Target identification ─────────────────────────────────────────
    def _identify_target(self) -> Optional[str]:
        if self.target_column and self.target_column in self.df.columns:
            return self.target_column

        # Auto-detect
        target_names = ["target", "label", "class", "y", "outcome", "survived",
                        "churn", "default", "fraud", "spam"]
        for col in self.df.columns:
            if col.lower().strip() in target_names:
                return col
        return None

    # ── 1. Perfect predictors ─────────────────────────────────────────
    def _detect_perfect_predictors(self, target: str) -> list[dict]:
        results: list[dict] = []
        target_series = self.df[target]

        for col in self.df.columns:
            if col == target:
                continue

            try:
                s = self.df[col]

                # For numeric columns: correlation
                if pd.api.types.is_numeric_dtype(s) and pd.api.types.is_numeric_dtype(target_series):
                    corr = abs(float(s.corr(target_series)))
                    if corr > 0.95:
                        results.append({
                            "column": col,
                            "method": "correlation",
                            "score": round(corr, 4),
                            "description": f"Correlation with target: {corr:.4f}",
                        })
                        continue

                # For categorical columns: mutual information / conditional entropy
                if s.dtype == object or s.nunique() <= 50:
                    # Check if column uniquely determines target
                    contingency = pd.crosstab(s.fillna("__NULL__"), target_series.fillna("__NULL__"))
                    # If each value of col maps to exactly one target value
                    if contingency.shape[1] > 1:
                        max_per_row = contingency.max(axis=1)
                        row_totals = contingency.sum(axis=1)
                        purity = (max_per_row / row_totals).mean()

                        if purity > 0.98:
                            results.append({
                                "column": col,
                                "method": "purity",
                                "score": round(float(purity), 4),
                                "description": f"Category purity: {purity:.4f} (each value predicts target)",
                            })

            except Exception:
                pass

        return results

    # ── 2. Temporal proxies ───────────────────────────────────────────
    def _detect_temporal_proxies(self, target: str) -> list[dict]:
        results: list[dict] = []
        target_series = self.df[target]

        dt_cols = []
        for col in self.df.columns:
            if col == target:
                continue
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                dt_cols.append(col)
            elif self.df[col].dtype == object:
                sample = self.df[col].dropna().head(20)
                try:
                    parsed = pd.to_datetime(sample, errors="coerce")
                    if parsed.notna().mean() > 0.8:
                        dt_cols.append(col)
                except Exception:
                    pass

        if len(dt_cols) < 2:
            return results

        # Check if any datetime column has a temporal ordering that predicts the target
        for col in dt_cols:
            try:
                dt = pd.to_datetime(self.df[col], errors="coerce")
                valid = dt.notna()
                if valid.sum() < 10:
                    continue

                # Check if later dates correlate with specific target values
                dt_numeric = dt.astype(np.int64) // 10 ** 9  # to unix timestamp
                if pd.api.types.is_numeric_dtype(target_series):
                    corr = abs(float(dt_numeric[valid].corr(target_series[valid])))
                    if corr > 0.8:
                        results.append({
                            "column": col,
                            "correlation": round(corr, 4),
                            "description": (
                                f"Datetime '{col}' has {corr:.4f} correlation with target — "
                                "may be a temporal proxy."
                            ),
                        })
                else:
                    # For categorical target, check if datetime ordering separates classes
                    groups = self.df[valid].groupby(target_series[valid])[col].apply(
                        lambda x: pd.to_datetime(x, errors="coerce").mean()
                    )
                    if len(groups) >= 2:
                        if groups.nunique() == len(groups):
                            time_spread = (groups.max() - groups.min()).total_seconds()
                            if time_spread > 86400:  # > 1 day spread between class means
                                results.append({
                                    "column": col,
                                    "correlation": 0.9,
                                    "description": (
                                        f"Datetime '{col}' separates target classes temporally — "
                                        "likely populated after event occurred."
                                    ),
                                })
            except Exception:
                pass

        return results

    # ── 3. ID leakage ─────────────────────────────────────────────────
    def _detect_id_leakage(self, target: str) -> list[dict]:
        results: list[dict] = []
        target_series = self.df[target]

        for col in self.df.columns:
            if col == target:
                continue
            if not _ID_PATTERNS.search(col):
                continue

            s = self.df[col]
            if not pd.api.types.is_numeric_dtype(s):
                continue

            if s.nunique() < len(s) * 0.5:
                continue  # Not unique enough to be an ID

            # Check if ID values correlate with target
            try:
                if pd.api.types.is_numeric_dtype(target_series):
                    corr = abs(float(s.corr(target_series)))
                    if corr > 0.3:
                        results.append({
                            "column": col,
                            "correlation": round(corr, 4),
                            "description": (
                                f"ID column '{col}' correlates with target (r={corr:.4f}) — "
                                "sequential IDs may encode outcome information."
                            ),
                        })
                else:
                    # Check if ID ranges differ by target class
                    group_means = self.df.groupby(target_series)[col].mean()
                    if group_means.std() / group_means.mean() > 0.1 if group_means.mean() != 0 else False:
                        results.append({
                            "column": col,
                            "correlation": 0.5,
                            "description": (
                                f"ID column '{col}' has different ranges per target class — "
                                "may encode temporal ordering of outcomes."
                            ),
                        })
            except Exception:
                pass

        return results

    # ── 4. Future-looking columns ─────────────────────────────────────
    def _check_future_column(self, col: str) -> Optional[dict]:
        for pattern in _FUTURE_PATTERNS:
            if re.search(pattern, col):
                return {
                    "column": col,
                    "pattern": pattern,
                    "description": (
                        f"Column '{col}' semantically refers to a future event — "
                        "this information would not be available at prediction time."
                    ),
                }
        return None

    # ── Action builders ───────────────────────────────────────────────
    def _build_predictor_action(self, pred: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_LEAKAGE,
            action_type=ActionType.FLAG_LEAKAGE_PREDICTOR,
            confidence=ActionConfidence.DEFINITIVE,
            evidence=pred["description"],
            recommendation=f"⚠ CRITICAL: Remove '{pred['column']}' — likely data leakage.",
            reasoning=(
                "A feature that near-perfectly predicts the target is almost certainly "
                "leaking information from the future or from the target itself."
            ),
            target_columns=[pred["column"]],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                columns_affected=1,
                description=f"Removes '{pred['column']}' (score: {pred['score']}).",
            ),
            options=[
                UserOption(key="remove", label="Remove Column", is_default=True),
                UserOption(key="keep", label="Keep (I know it's legitimate)"),
            ],
            metadata=pred,
        )

    def _build_temporal_action(self, tp: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_LEAKAGE,
            action_type=ActionType.FLAG_LEAKAGE_TEMPORAL,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=tp["description"],
            recommendation=f"⚠ WARNING: Investigate '{tp['column']}' — potential temporal leakage.",
            reasoning=(
                "Datetime columns that separate target classes temporally "
                "may have been populated after the event occurred."
            ),
            target_columns=[tp["column"]],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                columns_affected=1,
                description=f"Flags '{tp['column']}' for temporal leakage investigation.",
            ),
            options=[
                UserOption(key="remove", label="Remove Column", is_default=True),
                UserOption(key="investigate", label="Flag for Investigation"),
                UserOption(key="keep", label="Keep (it's legitimate)"),
            ],
            metadata=tp,
        )

    def _build_id_action(self, leak: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_LEAKAGE,
            action_type=ActionType.FLAG_LEAKAGE_ID,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=leak["description"],
            recommendation=f"⚠ WARNING: ID column '{leak['column']}' may encode outcome information.",
            reasoning=(
                "Sequential IDs that correlate with the target may have been assigned "
                "in an order that reflects outcomes."
            ),
            target_columns=[leak["column"]],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                columns_affected=1,
                description=f"Flags ID column '{leak['column']}'.",
            ),
            options=[
                UserOption(key="remove", label="Remove Column", is_default=True),
                UserOption(key="keep", label="Keep (IDs are safe)"),
            ],
            metadata=leak,
        )

    def _build_future_action(self, col: str, info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATA_LEAKAGE,
            action_type=ActionType.FLAG_LEAKAGE_FUTURE,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=info["description"],
            recommendation=f"⚠ WARNING: '{col}' may contain future-looking information.",
            reasoning=(
                "Columns that semantically refer to future events (outcomes, results, "
                "follow-ups) provide information not available at prediction time."
            ),
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                columns_affected=1,
                description=f"Flags '{col}' as future-looking.",
            ),
            options=[
                UserOption(key="remove", label="Remove Column", is_default=True),
                UserOption(key="keep", label="Keep (it's available at prediction time)"),
            ],
            metadata=info,
        )
