"""
Leakage Handler — Detects data leakage patterns:
temporal proxies, near-perfect predictors, ID encoding, future-looking columns.
All leakage suspects are flagged with severity proportional to their leakage type.
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


def _is_date_derived_column(col_name: str, other_col_name: str) -> bool:
    """Return True if col_name and other_col_name share a date-derivation relationship."""
    try:
        from insights.relationship_classifier import _is_date_derived_name, _normalise
        n_col = _normalise(col_name)
        n_other = _normalise(other_col_name)
        col_is_date = "date" in n_col or "time" in n_col or _is_date_derived_name(col_name)
        other_is_date = "date" in n_other or "time" in n_other or _is_date_derived_name(other_col_name)
        return bool(col_is_date and other_is_date)
    except Exception:
        return False

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
        prediction_timestamp: Optional[Any] = None,
        feature_timestamps: Optional[dict[str, Any]] = None,
        target_timestamp: Optional[Any] = None,
        metadata: Optional[dict[str, Any]] = None,
        business_rules: Optional[list[str] | dict[str, Any]] = None,
    ):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.target_column = target_column
        self.profile = profile
        self.prediction_timestamp = prediction_timestamp
        self.feature_timestamps = feature_timestamps or {}
        self.target_timestamp = target_timestamp
        self.metadata = metadata or {}
        self.business_rules = business_rules or []

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

        # 5. Deterministic formula reconstruction of target (Direct Target Leakage)
        formula_leaks = self._detect_formula_target_leakage(target)
        report["formula_target_leakage"] = formula_leaks
        for fl in formula_leaks:
            actions.append(self._build_formula_leakage_action(fl, target))

        report["leakage_suspects"] = [a.target_columns[0] for a in actions if a.target_columns]
        return actions, report

    # ── Target identification ─────────────────────────────────────────
    def _identify_target(self) -> Optional[str]:
        if self.target_column and self.target_column in self.df.columns:
            return self.target_column

        # Try shared target state
        try:
            from state import get_target_state
            t_state = get_target_state(self.file_id)
            if t_state and t_state.target_column and t_state.target_column in self.df.columns:
                return t_state.target_column
        except Exception:
            pass

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

            # Skip columns that are known date-derivations of each other or target
            if _is_date_derived_column(col, target):
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
                    parsed = pd.to_datetime(sample, format="mixed", errors="coerce")
                    if parsed.notna().mean() > 0.8:
                        dt_cols.append(col)
                except Exception:
                    pass

        if len(dt_cols) < 2:
            return results

        # Check if any datetime column has a temporal ordering that predicts the target
        for col in dt_cols:
            try:
                dt = pd.to_datetime(self.df[col], format="mixed", errors="coerce")
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
                        lambda x: pd.to_datetime(x, format="mixed", errors="coerce").mean()
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
    def _verify_confirmed_future_leakage(self, col: str) -> tuple[bool, Optional[str]]:
        """
        Verify whether future leakage is factually confirmed via:
        1. Prediction timestamp vs feature timestamp
        2. Target timestamp vs feature timestamp
        3. Explicit metadata (e.g. is_post_prediction=True)
        4. Documented business rule
        """
        # 1. Explicit metadata
        if isinstance(self.metadata, dict):
            col_meta = self.metadata.get(col, {})
            if isinstance(col_meta, dict):
                if col_meta.get("is_post_prediction") is True:
                    return True, "explicit metadata declares column as post-prediction"
                if col_meta.get("available_at_inference") is False:
                    return True, "explicit metadata declares column not available at inference"
                if col_meta.get("timing") in ("future", "post_event", "post_prediction"):
                    return True, f"metadata timing marked as '{col_meta.get('timing')}'"

        # 2. Documented business rules
        rules = self.business_rules
        if isinstance(rules, list):
            for rule in rules:
                rule_str = str(rule).lower()
                if col.lower() in rule_str and any(k in rule_str for k in ("post-prediction", "future leakage", "after prediction", "after target", "not available at inference")):
                    return True, f"documented business rule: '{rule}'"
        elif isinstance(rules, dict):
            rule_val = rules.get(col)
            if rule_val and any(k in str(rule_val).lower() for k in ("post-prediction", "future", "leakage", "after")):
                return True, f"documented business rule for '{col}': {rule_val}"

        # 3. Timestamps check (prediction timestamp vs feature timestamp)
        pred_ts = self.prediction_timestamp
        feat_ts = self.feature_timestamps.get(col)

        if pred_ts is not None and feat_ts is not None:
            try:
                p_dt = pd.to_datetime(pred_ts)
                f_dt = pd.to_datetime(feat_ts)
                if f_dt > p_dt:
                    return True, f"feature timestamp ({f_dt}) occurs after prediction timestamp ({p_dt})"
            except Exception:
                pass

        # 4. Target timestamp vs feature timestamp
        target_ts = self.target_timestamp
        if target_ts is not None and feat_ts is not None:
            try:
                t_dt = pd.to_datetime(target_ts)
                f_dt = pd.to_datetime(feat_ts)
                if f_dt > t_dt:
                    return True, f"feature timestamp ({f_dt}) occurs after target timestamp ({t_dt})"
            except Exception:
                pass

        # 5. Profile prediction context
        if self.profile and hasattr(self.profile, "prediction_context"):
            ctx = getattr(self.profile, "prediction_context")
            if isinstance(ctx, dict) and col in ctx.get("post_prediction_columns", []):
                return True, "profile prediction context specifies column is post-prediction"

        return False, None

    def _check_future_column(self, col: str) -> Optional[dict]:
        matched_pattern = None
        for pattern in _FUTURE_PATTERNS:
            if re.search(pattern, col):
                matched_pattern = pattern
                break

        is_confirmed, evidence_detail = self._verify_confirmed_future_leakage(col)

        if not matched_pattern and not is_confirmed:
            return None

        if is_confirmed:
            return {
                "column": col,
                "pattern": matched_pattern,
                "is_confirmed": True,
                "evidence": f"Confirmed future leakage: {evidence_detail}",
                "description": f"Column '{col}' is confirmed to be post-prediction future leakage ({evidence_detail}).",
                "recommendation": f"CRITICAL: Remove '{col}' — confirmed post-prediction future leakage.",
                "reasoning": (
                    f"'{col}' is proven to be recorded after the prediction event or cutoff time ({evidence_detail}). "
                    "Using it during training will cause severe future information leakage."
                ),
            }

        # Semantic suspicion without factual proof -> CONDITIONAL WARNING
        warning_msg = (
            f"'{col}' may cause target/future leakage if this value is only known after the "
            "prediction timestamp. Confirm feature availability at inference time."
        )
        return {
            "column": col,
            "pattern": matched_pattern,
            "is_confirmed": False,
            "evidence": (
                f"Column '{col}' matches naming patterns that could refer to post-event outcomes. "
                "However, the dataset does not establish whether this represents an already observed historical "
                "metric or a future outcome measured after prediction."
            ),
            "description": warning_msg,
            "recommendation": warning_msg,
            "reasoning": (
                f"The column could represent an already observed historical metric or a future outcome measured after prediction. "
                f"The software does not have enough information to know which one is true without prediction-time context. "
                f"Confirm feature availability at inference time before deciding to exclude it."
            ),
        }

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
        is_confirmed = info.get("is_confirmed", False)
        if is_confirmed:
            confidence = ActionConfidence.DEFINITIVE
            recommendation = info.get("recommendation", f"CRITICAL: Remove '{col}' — confirmed post-prediction future leakage.")
            options = [
                UserOption(key="remove", label="Remove Column", is_default=True),
                UserOption(key="keep", label="Keep (I know it's legitimate)"),
            ]
        else:
            confidence = ActionConfidence.JUDGMENT_CALL
            recommendation = info.get(
                "recommendation",
                f"'{col}' may cause target/future leakage if this value is only known after the prediction timestamp. Confirm feature availability at inference time."
            )
            options = [
                UserOption(key="confirm_available", label="Confirm Available at Inference (Keep Column)", is_default=True),
                UserOption(key="remove", label="Remove Column (Post-Prediction Leakage)"),
                UserOption(key="investigate", label="Flag for Review"),
            ]

        return CleaningAction(
            category=ActionCategory.DATA_LEAKAGE,
            action_type=ActionType.FLAG_LEAKAGE_FUTURE,
            confidence=confidence,
            evidence=info.get("evidence", info.get("description")),
            recommendation=recommendation,
            reasoning=info.get("reasoning"),
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - (1 if is_confirmed else 0),
                columns_affected=1,
                description=f"Flags '{col}' for {'confirmed future leakage' if is_confirmed else 'prediction-time verification'}.",
            ),
            options=options,
            metadata=info,
        )

    def _detect_formula_target_leakage(self, target: str) -> list[dict]:
        results: list[dict] = []
        try:
            from profiling.formula_detector import detect_formula_dependencies
            formulas = detect_formula_dependencies(self.df, target_column=target)
            for f in formulas:
                if f.dependent == target:
                    op1 = f.operands[0] if len(f.operands) > 0 else "operand1"
                    op2 = f.operands[1] if len(f.operands) > 1 else "operand2"
                    results.append({
                        "dependent": target,
                        "operands": f.operands,
                        "formula_expr": f.formula_expr,
                        "score": f.match_ratio,
                        "description": f"`{target}` is deterministically derived from `{op1}` and `{op2}`.",
                        "reasoning": f"Using {op1} and {op2} to predict {target} should be classified as direct target leakage.",
                        "recommendation": f"⚠ CRITICAL: Drop '{op1}' or '{op2}' — using them to predict '{target}' is direct target leakage.",
                    })
        except Exception:
            pass
        return results

    def _build_formula_leakage_action(self, fl: dict, target: str) -> CleaningAction:
        operands = fl["operands"]
        op1 = operands[0] if len(operands) > 0 else "operand1"
        op2 = operands[1] if len(operands) > 1 else op1
        return CleaningAction(
            category=ActionCategory.DATA_LEAKAGE,
            action_type=ActionType.FLAG_LEAKAGE_PREDICTOR,
            confidence=ActionConfidence.DEFINITIVE,
            evidence=fl["description"],
            recommendation=fl["recommendation"],
            reasoning=fl["reasoning"],
            target_columns=operands,
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                columns_affected=len(operands),
                description=f"Eliminates direct target leakage into '{target}'.",
            ),
            options=[
                UserOption(key=f"drop_{op1}", label=f"Drop '{op1}'", is_default=True),
                UserOption(key=f"drop_{op2}", label=f"Drop '{op2}'"),
                UserOption(key="keep_all", label="Keep all features"),
            ],
            metadata=fl,
        )

