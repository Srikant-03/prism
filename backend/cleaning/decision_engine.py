"""
Decision Engine — central orchestrator for the Cleaning Pipeline.
Invokes all handlers (Duplicate, Missing, Outlier, Type, Text,
Categorical, Datetime, Scaling, FeatureSelection,
Imbalance, Standardization, Leakage),
collects all CleaningActions, ranks by severity/impact,
and provides apply_action mechanics for all action types.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import pandas as pd
import numpy as np

from cleaning.cleaning_models import (
    CleaningAction, CleaningPlan, ActionResult, ActionType,
    ActionConfidence, ActionCategory, ActionStatus,
)
from cleaning.duplicate_handler import DuplicateHandler
from cleaning.missing_handler import MissingHandler
from cleaning.outlier_handler import OutlierHandler
from cleaning.type_handler import TypeHandler
from cleaning.text_handler import TextHandler
from cleaning.categorical_handler import CategoricalHandler
from cleaning.datetime_handler import DatetimeHandler
from cleaning.scaling_handler import ScalingHandler
from cleaning.feature_selection_handler import FeatureSelectionHandler
from cleaning.imbalance_handler import ImbalanceHandler
from cleaning.standardization_handler import StandardizationHandler
from cleaning.leakage_handler import LeakageHandler


class DecisionEngine:
    """
    Orchestrates the full cleaning analysis pipeline.
    Takes profiling results + raw DataFrame and produces a ranked CleaningPlan.
    """

    def __init__(self, df: pd.DataFrame, file_id: str, profile: Any = None):
        self.df = df
        self.file_id = file_id
        self.profile = profile

    # ── Analyze ───────────────────────────────────────────────────────
    def analyze(self) -> CleaningPlan:
        """Run all handlers and produce a consolidated CleaningPlan."""
        all_actions: list[CleaningAction] = []

        # Extract feature importances from Pillar 1 insights
        feature_importances = self._extract_feature_importances()

        # 1. Duplicate detection
        dup_handler = DuplicateHandler(self.df, self.file_id)
        dup_actions, dup_report = dup_handler.analyze()
        all_actions.extend(dup_actions)

        # 2. Missing value analysis
        missing_handler = MissingHandler(
            self.df, self.file_id,
            profile=self.profile,
            feature_importances=feature_importances,
        )
        missing_actions, missing_report = missing_handler.analyze()
        all_actions.extend(missing_actions)

        # 3. Outlier detection
        outlier_handler = OutlierHandler(self.df, self.file_id, profile=self.profile)
        outlier_actions, outlier_report = outlier_handler.analyze()
        all_actions.extend(outlier_actions)

        # 4. Data type detection & correction
        type_handler = TypeHandler(self.df, self.file_id, profile=self.profile)
        type_actions, type_report = type_handler.analyze()
        all_actions.extend(type_actions)

        # 5. Text column preprocessing
        text_handler = TextHandler(self.df, self.file_id, profile=self.profile)
        text_actions, text_report = text_handler.analyze()
        all_actions.extend(text_actions)

        # 6. Categorical encoding
        cat_handler = CategoricalHandler(self.df, self.file_id, profile=self.profile)
        cat_actions, cat_report = cat_handler.analyze()
        all_actions.extend(cat_actions)

        # 7. Datetime feature engineering
        dt_handler = DatetimeHandler(self.df, self.file_id, profile=self.profile)
        dt_actions, dt_report = dt_handler.analyze()
        all_actions.extend(dt_actions)

        # 8. Feature scaling & normalization
        scaling_handler = ScalingHandler(self.df, self.file_id, profile=self.profile)
        scaling_actions, scaling_report = scaling_handler.analyze()
        all_actions.extend(scaling_actions)

        # 9. Feature selection & dimensionality
        fs_handler = FeatureSelectionHandler(
            self.df, self.file_id,
            feature_importances=feature_importances,
            profile=self.profile,
        )
        fs_actions, fs_report = fs_handler.analyze()
        all_actions.extend(fs_actions)

        # 10. Class imbalance
        imb_handler = ImbalanceHandler(self.df, self.file_id, profile=self.profile)
        imb_actions, imb_report = imb_handler.analyze()
        all_actions.extend(imb_actions)

        # 11. Data standardization & consistency
        std_handler = StandardizationHandler(self.df, self.file_id, profile=self.profile)
        std_actions, std_report = std_handler.analyze()
        all_actions.extend(std_actions)

        # 12. Data leakage detection
        leak_handler = LeakageHandler(self.df, self.file_id, profile=self.profile)
        leak_actions, leak_report = leak_handler.analyze()
        all_actions.extend(leak_actions)

        # — Assign indices and rank actions —
        all_actions = self._rank_actions(all_actions)

        # — Build plan —
        definitive = [a for a in all_actions if a.confidence == ActionConfidence.DEFINITIVE]
        judgment = [a for a in all_actions if a.confidence == ActionConfidence.JUDGMENT_CALL]

        total_rows_affected = sum(a.impact.rows_affected for a in all_actions)
        total_cols_affected = sum(a.impact.columns_affected for a in all_actions)

        plan = CleaningPlan(
            file_id=self.file_id,
            total_actions=len(all_actions),
            definitive_count=len(definitive),
            judgment_call_count=len(judgment),
            actions=all_actions,
            duplicate_report=dup_report,
            missing_report=missing_report,
            outlier_report=outlier_report,
            type_report=type_report,
            text_report=text_report,
            estimated_rows_affected=total_rows_affected,
            estimated_rows_affected_pct=round(
                total_rows_affected / len(self.df) * 100, 2
            ) if len(self.df) > 0 else 0,
            estimated_columns_affected=total_cols_affected,
        )

        return plan

    # ── Apply a single action ─────────────────────────────────────────
    def apply_action(
        self,
        action: CleaningAction,
        selected_option: str | None = None,
    ) -> tuple[pd.DataFrame, ActionResult]:
        """
        Apply a cleaning action to self.df and return the new DataFrame + result.
        """
        rows_before = len(self.df)
        cols_before = len(self.df.columns)
        new_df = self.df.copy()

        try:
            at = action.action_type

            # ── Duplicate actions ─────────────────────────────────
            if at == ActionType.REMOVE_EXACT_DUPLICATES:
                keep = selected_option or "first"
                if keep == "keep_all":
                    new_df["_is_duplicate"] = new_df.duplicated(keep=False)
                else:
                    keep_mode = "last" if keep == "keep_last" else "first"
                    new_df = new_df.drop_duplicates(keep=keep_mode).reset_index(drop=True)

            elif at == ActionType.DROP_DUPLICATE_COLUMN:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = new_df.drop(columns=[col])

            elif at == ActionType.DROP_DERIVED_COLUMN:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = new_df.drop(columns=[col])

            # ── Missing value actions ─────────────────────────────
            elif at == ActionType.DROP_COLUMN:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = new_df.drop(columns=[col])

            elif at == ActionType.DROP_ROWS:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = new_df.dropna(subset=[col]).reset_index(drop=True)

            elif at == ActionType.IMPUTE_MEAN:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].fillna(new_df[col].mean())

            elif at == ActionType.IMPUTE_MEDIAN:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].fillna(new_df[col].median())

            elif at == ActionType.IMPUTE_MODE:
                for col in action.target_columns:
                    if col in new_df.columns:
                        mode_val = new_df[col].mode()
                        if len(mode_val) > 0:
                            new_df[col] = new_df[col].fillna(mode_val.iloc[0])

            elif at == ActionType.IMPUTE_CONSTANT:
                fill_value = action.metadata.get("fill_value", 0)
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].fillna(fill_value)

            elif at == ActionType.FFILL:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].ffill()

            elif at == ActionType.BFILL:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].bfill()

            elif at == ActionType.INTERPOLATE:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].interpolate(method="linear")

            elif at == ActionType.ADD_INDICATOR:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[f"{col}_was_null"] = new_df[col].isnull().astype(int)
                        if pd.api.types.is_numeric_dtype(new_df[col]):
                            new_df[col] = new_df[col].fillna(new_df[col].median())
                        else:
                            mode_val = new_df[col].mode()
                            if len(mode_val) > 0:
                                new_df[col] = new_df[col].fillna(mode_val.iloc[0])

            elif at == ActionType.IMPUTE_KNN:
                new_df = self._knn_impute(new_df, action.target_columns)

            elif at == ActionType.FLAG_ONLY:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[f"{col}_is_null"] = new_df[col].isnull().astype(int)

            # ── Outlier actions ───────────────────────────────────
            elif at == ActionType.FLAG_OUTLIER:
                option = selected_option or "flag"
                for col in action.target_columns:
                    if col not in new_df.columns:
                        continue
                    if option == "flag":
                        new_df = self._flag_outliers(new_df, col)
                    elif option == "remove":
                        new_df = self._remove_outlier_rows(new_df, col)
                    # else "ignore" → no change

            elif at == ActionType.REMOVE_OUTLIER_ROWS:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = self._remove_outlier_rows(new_df, col)

            elif at == ActionType.WINSORIZE:
                lower = float(action.metadata.get("lower_pct", 0.01))
                upper = float(action.metadata.get("upper_pct", 0.99))
                for col in action.target_columns:
                    if col in new_df.columns and pd.api.types.is_numeric_dtype(new_df[col]):
                        lo = new_df[col].quantile(lower)
                        hi = new_df[col].quantile(upper)
                        new_df[col] = new_df[col].clip(lower=lo, upper=hi)

            elif at == ActionType.CAP_OUTLIERS or at == ActionType.REPLACE_BOUNDARY:
                for col in action.target_columns:
                    if col in new_df.columns and pd.api.types.is_numeric_dtype(new_df[col]):
                        q1 = new_df[col].quantile(0.25)
                        q3 = new_df[col].quantile(0.75)
                        iqr = q3 - q1
                        lo = q1 - 1.5 * iqr
                        hi = q3 + 1.5 * iqr
                        new_df[col] = new_df[col].clip(lower=lo, upper=hi)

            elif at == ActionType.LOG_TRANSFORM:
                for col in action.target_columns:
                    if col in new_df.columns and pd.api.types.is_numeric_dtype(new_df[col]):
                        min_val = new_df[col].min()
                        if min_val <= 0:
                            shift = abs(min_val) + 1
                            new_df[col] = np.log1p(new_df[col] + shift)
                        else:
                            new_df[col] = np.log1p(new_df[col])

            # ── Type correction actions ───────────────────────────
            elif at == ActionType.PARSE_DATES:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = pd.to_datetime(
                            new_df[col], errors="coerce"
                        )

            elif at == ActionType.PARSE_CURRENCY:
                for col in action.target_columns:
                    if col in new_df.columns:
                        from cleaning.type_handler import TypeHandler as TH
                        th = TH.__new__(TH)
                        new_df[col] = new_df[col].apply(
                            lambda v: th._parse_currency_value(str(v)) if pd.notna(v) else v
                        )

            elif at == ActionType.STANDARDIZE_BOOLEANS:
                from cleaning.type_handler import _BOOL_TRUE, _BOOL_FALSE
                for col in action.target_columns:
                    if col in new_df.columns:
                        def to_bool(v):
                            if pd.isna(v):
                                return v
                            s = str(v).lower().strip()
                            if s in _BOOL_TRUE:
                                return True
                            if s in _BOOL_FALSE:
                                return False
                            return v
                        new_df[col] = new_df[col].apply(to_bool)

            elif at == ActionType.REPLACE_PSEUDO_NULLS:
                from cleaning.type_handler import _PSEUDO_NULLS
                for col in action.target_columns:
                    if col in new_df.columns:
                        mask = new_df[col].astype(str).str.lower().str.strip().isin(_PSEUDO_NULLS)
                        new_df.loc[mask, col] = np.nan
                        # Try to re-infer numeric type
                        try:
                            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")
                        except Exception:
                            pass

            elif at == ActionType.FLAG_INTEGER_CATEGORIES:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].astype("category")

            elif at == ActionType.EXPAND_JSON:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = self._expand_json_column(new_df, col)

            elif at == ActionType.VALIDATE_STRUCTURED:
                pass  # Validation is informational only

            # ── Text preprocessing actions ────────────────────────
            elif at == ActionType.NORMALIZE_TEXT:
                mode = selected_option or "full"
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = new_df[col].apply(
                            lambda v: TextHandler.normalize_text(str(v), mode) if pd.notna(v) else v
                        )

            elif at == ActionType.EXTRACT_TEXT_FEATURES:
                mode = selected_option or "all"
                for col in action.target_columns:
                    if col in new_df.columns:
                        features_df = TextHandler.extract_features(new_df[col], mode)
                        # Prefix feature columns
                        features_df.columns = [f"{col}_{c}" for c in features_df.columns]
                        new_df = pd.concat([new_df, features_df], axis=1)

            elif at == ActionType.REMOVE_STOPWORDS or at == ActionType.STEM_LEMMATIZE:
                mode = selected_option or "stopwords_lemma"
                lang = action.metadata.get("language", "en")
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = TextHandler.process_nlp(new_df[col], mode, lang)

            elif at == ActionType.TFIDF_VECTORIZE:
                max_f = int(action.metadata.get("max_features", 100))
                ngram = tuple(action.metadata.get("ngram_range", [1, 2]))
                for col in action.target_columns:
                    if col in new_df.columns:
                        tfidf_df = TextHandler.tfidf_vectorize(new_df[col], max_f, ngram)
                        tfidf_df.columns = [f"{col}_{c}" for c in tfidf_df.columns]
                        new_df = pd.concat([new_df, tfidf_df], axis=1)

            elif at == ActionType.DROP_RAW_TEXT:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = new_df.drop(columns=[col])

            elif at == ActionType.CORRECT_SPELLING:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df[col] = TextHandler.correct_spelling(new_df[col])

            # ── Categorical encoding actions ──────────────────────
            elif at == ActionType.LABEL_ENCODE:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_label_encode(new_df, col)

            elif at == ActionType.ONE_HOT_ENCODE:
                drop_first = selected_option == "ohe_drop_first"
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_one_hot_encode(new_df, col, drop_first)

            elif at == ActionType.ORDINAL_ENCODE:
                order = action.metadata.get("custom_order")
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_ordinal_encode(new_df, col, order)

            elif at == ActionType.FREQUENCY_ENCODE:
                use_prop = selected_option == "frequency_proportion"
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_frequency_encode(new_df, col, use_prop)

            elif at == ActionType.TARGET_ENCODE:
                target_col = action.metadata.get("target_column", "")
                method = "loo" if selected_option != "target_kfold" else "kfold"
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_target_encode(new_df, col, target_col, method)

            elif at == ActionType.BINARY_ENCODE:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_binary_encode(new_df, col)

            elif at == ActionType.HASH_ENCODE:
                buckets = {"hash_16": 16, "hash_32": 32, "hash_64": 64}.get(selected_option or "", 32)
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_hash_encode(new_df, col, buckets)

            elif at == ActionType.CYCLICAL_ENCODE:
                period = int(action.metadata.get("cyclical_period", 12))
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = CategoricalHandler.apply_cyclical_encode(new_df, col, period)

            elif at == ActionType.SUGGEST_EMBEDDING:
                pass  # Informational only — no direct transformation

            # ── Datetime engineering actions ──────────────────────
            elif at == ActionType.EXTRACT_DATETIME:
                components = action.metadata.get("components", [])
                if selected_option == "date_only":
                    components = [c for c in components if c in ("year", "month", "month_name", "quarter", "week", "day", "dayofweek", "day_name")]
                elif selected_option == "time_only":
                    components = [c for c in components if c in ("hour", "minute", "second")]
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = DatetimeHandler.extract_components(new_df, col, components)

            elif at == ActionType.DERIVE_DATETIME_FLAGS:
                flags = action.metadata.get("flags", [])
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = DatetimeHandler.derive_flags(new_df, col, flags)

            elif at == ActionType.COMPUTE_ELAPSED_TIME:
                unit = selected_option or "days"
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = DatetimeHandler.compute_elapsed(new_df, col, unit)

            elif at == ActionType.COMPUTE_TIME_DELTAS:
                col_a = action.metadata.get("col_a", "")
                col_b = action.metadata.get("col_b", "")
                unit = selected_option or action.metadata.get("unit", "days")
                if col_a in new_df.columns and col_b in new_df.columns:
                    new_df = DatetimeHandler.compute_time_delta(new_df, col_a, col_b, unit)

            elif at == ActionType.TIME_SERIES_FEATURES:
                mode = selected_option or "full"
                dt_col = action.target_columns[0] if action.target_columns else ""
                num_cols = action.target_columns[1:] if len(action.target_columns) > 1 else []
                if dt_col in new_df.columns:
                    new_df = DatetimeHandler.generate_ts_features(new_df, dt_col, num_cols, mode)

            # ── Feature scaling actions ───────────────────────────
            elif at in (
                ActionType.STANDARD_SCALE, ActionType.MINMAX_SCALE,
                ActionType.MAXABS_SCALE, ActionType.ROBUST_SCALE,
                ActionType.LOG1P_TRANSFORM, ActionType.BOXCOX_TRANSFORM,
                ActionType.YEOJOHNSON_TRANSFORM, ActionType.QUANTILE_UNIFORM,
                ActionType.QUANTILE_NORMAL, ActionType.BINARIZE,
            ):
                method_map = {
                    ActionType.STANDARD_SCALE: "standard",
                    ActionType.MINMAX_SCALE: "minmax",
                    ActionType.MAXABS_SCALE: "maxabs",
                    ActionType.ROBUST_SCALE: "robust",
                    ActionType.LOG1P_TRANSFORM: "log1p",
                    ActionType.BOXCOX_TRANSFORM: "boxcox",
                    ActionType.YEOJOHNSON_TRANSFORM: "yeojohnson",
                    ActionType.QUANTILE_UNIFORM: "quantile_uniform",
                    ActionType.QUANTILE_NORMAL: "quantile_normal",
                    ActionType.BINARIZE: "binarize",
                }
                # If user selected a different option, use it
                method = selected_option or method_map.get(at, "standard")
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = ScalingHandler.apply_scaling(new_df, col, method)

            # ── Feature selection actions ─────────────────────────
            elif at in (
                ActionType.DROP_ZERO_VARIANCE, ActionType.DROP_NEAR_ZERO_VARIANCE,
                ActionType.DROP_HIGH_CORRELATION, ActionType.DROP_HIGH_VIF,
                ActionType.DROP_LOW_MI, ActionType.CLUSTER_FEATURES,
            ):
                cols_to_drop = action.target_columns
                if selected_option == "skip" or selected_option == "keep_both":
                    pass  # User chose to keep
                else:
                    new_df = FeatureSelectionHandler.apply_drop_columns(new_df, cols_to_drop)

            elif at == ActionType.SUGGEST_PCA:
                if selected_option and selected_option.startswith("pca_"):
                    n_map = {
                        "pca_90": action.metadata.get("components_for_90pct", 5),
                        "pca_95": action.metadata.get("components_for_95pct", 10),
                        "pca_99": action.metadata.get("components_for_99pct", 15),
                    }
                    n_comp = int(n_map.get(selected_option, 10))
                    numeric_cols = new_df.select_dtypes(include=[np.number]).columns.tolist()
                    new_df = FeatureSelectionHandler.apply_pca(new_df, numeric_cols, n_comp)

            # ── Class imbalance actions ─────────────────────────
            elif at in (
                ActionType.RANDOM_OVERSAMPLE, ActionType.SMOTE,
                ActionType.SMOTE_NC, ActionType.ADASYN,
                ActionType.RANDOM_UNDERSAMPLE, ActionType.TOMEK_LINKS,
                ActionType.EDITED_NEAREST_NEIGHBORS, ActionType.NEAR_MISS,
                ActionType.SMOTEENN, ActionType.SMOTETOMEK,
            ):
                target_col = action.target_columns[0] if action.target_columns else ""
                if target_col in new_df.columns:
                    option = selected_option or "random_over"
                    if option in ("random_over", "oversample"):
                        new_df = ImbalanceHandler.apply_random_oversample(new_df, target_col)
                    elif option in ("random_under", "undersample"):
                        new_df = ImbalanceHandler.apply_random_undersample(new_df, target_col)
                    elif option == "smote":
                        new_df = ImbalanceHandler.apply_smote(new_df, target_col)
                    else:
                        # Default to random oversampling for methods without imblearn
                        new_df = ImbalanceHandler.apply_random_oversample(new_df, target_col)

            elif at == ActionType.CLASS_WEIGHTS:
                pass  # Informational — weights stored in metadata

            elif at == ActionType.ANOMALY_FRAMING:
                pass  # Informational — recommendation only

            # ── Data standardization actions ─────────────────────
            elif at == ActionType.STANDARDIZE_CASING:
                mode = selected_option or "lower"
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = StandardizationHandler.apply_casing(new_df, col, mode)

            elif at == ActionType.STANDARDIZE_WHITESPACE:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = StandardizationHandler.apply_whitespace(new_df, col)

            elif at == ActionType.NORMALIZE_UNICODE:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = StandardizationHandler.apply_unicode(new_df, col)

            elif at == ActionType.CONSOLIDATE_SYNONYMS:
                clusters = action.metadata.get("clusters", [])
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = StandardizationHandler.apply_synonyms(new_df, col, clusters)

            elif at == ActionType.STANDARDIZE_EMAIL:
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = StandardizationHandler.apply_email_standardize(new_df, col)

            elif at == ActionType.STANDARDIZE_PRECISION:
                decimals = int(selected_option or action.metadata.get("dominant_precision", 2))
                for col in action.target_columns:
                    if col in new_df.columns:
                        new_df = StandardizationHandler.apply_precision(new_df, col, decimals)

            elif at in (
                ActionType.FIX_UNIT_INCONSISTENCY,
                ActionType.STANDARDIZE_PHONE,
                ActionType.STANDARDIZE_URL,
                ActionType.STANDARDIZE_CURRENCY_FORMAT,
            ):
                pass  # Flagging only — require manual review

            # ── Data leakage actions ─────────────────────────
            elif at in (
                ActionType.FLAG_LEAKAGE_TEMPORAL,
                ActionType.FLAG_LEAKAGE_PREDICTOR,
                ActionType.FLAG_LEAKAGE_ID,
                ActionType.FLAG_LEAKAGE_FUTURE,
            ):
                if selected_option != "keep":
                    cols_to_drop = [c for c in action.target_columns if c in new_df.columns]
                    new_df = new_df.drop(columns=cols_to_drop)

            rows_after = len(new_df)
            cols_after = len(new_df.columns)

            # Preview of first 10 rows
            preview = new_df.head(10).fillna("").astype(str).to_dict(orient="records")

            result = ActionResult(
                success=True,
                action_index=action.index,
                action_type=action.action_type,
                rows_before=rows_before,
                rows_after=rows_after,
                columns_before=cols_before,
                columns_after=cols_after,
                description=f"Applied {action.action_type.value}: {rows_before - rows_after} rows removed, "
                            f"{cols_before - cols_after} columns removed, {cols_after - cols_before} columns added.",
                preview_after=preview,
            )

            self.df = new_df
            return new_df, result

        except Exception as e:
            return self.df, ActionResult(
                success=False,
                action_index=action.index,
                action_type=action.action_type,
                rows_before=rows_before,
                rows_after=rows_before,
                columns_before=cols_before,
                columns_after=cols_before,
                description=f"Action failed: {str(e)}",
            )

    # ── Apply all definitive actions ──────────────────────────────────
    def apply_all_definitive(self, plan: CleaningPlan) -> tuple[pd.DataFrame, list[ActionResult]]:
        """Apply all definitive actions in sequence."""
        results: list[ActionResult] = []
        for action in plan.actions:
            if action.confidence == ActionConfidence.DEFINITIVE and action.status == ActionStatus.PENDING:
                new_df, result = self.apply_action(action)
                action.status = ActionStatus.APPLIED
                results.append(result)
        return self.df, results

    # ── Private helpers ───────────────────────────────────────────────
    def _rank_actions(self, actions: list[CleaningAction]) -> list[CleaningAction]:
        """Rank actions: definitive first, then by impact descending."""
        # Category priority: type corrections first (they can unlock other detections),
        # then duplicates, missing, outliers, text, encoding, datetime, scaling, selection
        category_order = {
            ActionCategory.TYPE_CORRECTION: 0,
            ActionCategory.DUPLICATES: 1,
            ActionCategory.MISSING_VALUES: 2,
            ActionCategory.OUTLIERS: 3,
            ActionCategory.TEXT_PREPROCESSING: 4,
            ActionCategory.CATEGORICAL_ENCODING: 5,
            ActionCategory.DATETIME_ENGINEERING: 6,
            ActionCategory.FEATURE_SCALING: 7,
            ActionCategory.FEATURE_SELECTION: 8,
            ActionCategory.DATA_LEAKAGE: 9,
            ActionCategory.CLASS_IMBALANCE: 10,
            ActionCategory.DATA_STANDARDIZATION: 11,
            ActionCategory.STRUCTURAL: 12,
        }

        actions.sort(key=lambda a: (
            0 if a.confidence == ActionConfidence.DEFINITIVE else 1,
            category_order.get(a.category, 9),
            -a.impact.rows_affected if a.impact.rows_affected else 0,
            -a.impact.columns_affected if a.impact.columns_affected else 0,
        ))
        for i, action in enumerate(actions):
            action.index = i
        return actions

    def _extract_feature_importances(self) -> dict[str, float]:
        """Pull feature importance scores from Pillar 1 insights."""
        importances: dict[str, float] = {}
        if self.profile is None:
            return importances

        try:
            insights = getattr(self.profile, "insights", None)
            if insights is None:
                return importances

            if hasattr(insights, "feature_ranking"):
                for fi in insights.feature_ranking:
                    importances[fi.feature] = fi.importance_score
            elif isinstance(insights, dict) and "feature_ranking" in insights:
                for fi in insights["feature_ranking"]:
                    importances[fi.get("feature", "")] = fi.get("importance_score", 50)
        except Exception:
            pass

        return importances

    def _knn_impute(self, df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        """KNN imputation on specified columns using sklearn."""
        try:
            from sklearn.impute import KNNImputer
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            target_cols = [c for c in cols if c in numeric_cols]

            if not target_cols:
                return df

            imputer = KNNImputer(n_neighbors=5)
            df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        except ImportError:
            for col in cols:
                if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].median())
        return df

    def _flag_outliers(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Add a binary outlier flag column using IQR."""
        if not pd.api.types.is_numeric_dtype(df[col]):
            return df
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return df
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df[f"{col}_is_outlier"] = ((df[col] < lower) | (df[col] > upper)).astype(int)
        return df

    def _remove_outlier_rows(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Remove rows with outlier values using IQR."""
        if not pd.api.types.is_numeric_dtype(df[col]):
            return df
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return df
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        return df[(df[col] >= lower) & (df[col] <= upper)].reset_index(drop=True)

    def _expand_json_column(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Expand a JSON column into sub-columns."""
        try:
            parsed = df[col].apply(
                lambda v: json.loads(str(v)) if pd.notna(v) and str(v).strip().startswith(("{", "[")) else {}
            )
            expanded = pd.json_normalize(parsed)
            expanded.columns = [f"{col}_{c}" for c in expanded.columns]
            expanded.index = df.index
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
        except Exception:
            pass
        return df
