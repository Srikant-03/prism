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
        Uses the _ACTION_REGISTRY for dispatch instead of an elif chain.
        """
        rows_before = len(self.df)
        cols_before = len(self.df.columns)
        new_df = self.df.copy()

        try:
            handler = self._ACTION_REGISTRY.get(action.action_type)
            if handler is not None:
                new_df = handler(self, new_df, action, selected_option)
            # else: unknown action type — no-op

            rows_after = len(new_df)
            cols_after = len(new_df.columns)

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

    # ── Command handlers ──────────────────────────────────────────────
    # Each handler takes (self, df, action, selected_option) → df

    def _cmd_remove_exact_duplicates(self, df, action, option):
        keep = option or "first"
        if keep == "keep_all":
            df["_is_duplicate"] = df.duplicated(keep=False)
        else:
            keep_mode = "last" if keep == "keep_last" else "first"
            df = df.drop_duplicates(keep=keep_mode).reset_index(drop=True)
        return df

    def _cmd_drop_columns(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = df.drop(columns=[col])
        return df

    def _cmd_drop_rows(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = df.dropna(subset=[col]).reset_index(drop=True)
        return df

    def _cmd_impute_mean(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].mean())
        return df

    def _cmd_impute_median(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        return df

    def _cmd_impute_mode(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                mode_val = df[col].mode()
                if len(mode_val) > 0:
                    df[col] = df[col].fillna(mode_val.iloc[0])
        return df

    def _cmd_impute_constant(self, df, action, option):
        fill_value = action.metadata.get("fill_value", 0)
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].fillna(fill_value)
        return df

    def _cmd_ffill(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].ffill()
        return df

    def _cmd_bfill(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].bfill()
        return df

    def _cmd_interpolate(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].interpolate(method="linear")
        return df

    def _cmd_add_indicator(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[f"{col}_was_null"] = df[col].isnull().astype(int)
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].median())
                else:
                    mode_val = df[col].mode()
                    if len(mode_val) > 0:
                        df[col] = df[col].fillna(mode_val.iloc[0])
        return df

    def _cmd_impute_knn(self, df, action, option):
        return self._knn_impute(df, action.target_columns)

    def _cmd_flag_only(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[f"{col}_is_null"] = df[col].isnull().astype(int)
        return df

    def _cmd_flag_outlier(self, df, action, option):
        opt = option or "flag"
        for col in action.target_columns:
            if col not in df.columns:
                continue
            if opt == "flag":
                df = self._flag_outliers(df, col)
            elif opt == "remove":
                df = self._remove_outlier_rows(df, col)
        return df

    def _cmd_remove_outlier_rows(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = self._remove_outlier_rows(df, col)
        return df

    def _cmd_winsorize(self, df, action, option):
        lower = float(action.metadata.get("lower_pct", 0.01))
        upper = float(action.metadata.get("upper_pct", 0.99))
        for col in action.target_columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                lo = df[col].quantile(lower)
                hi = df[col].quantile(upper)
                df[col] = df[col].clip(lower=lo, upper=hi)
        return df

    def _cmd_cap_outliers(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                df[col] = df[col].clip(lower=q1 - 1.5 * iqr, upper=q3 + 1.5 * iqr)
        return df

    def _cmd_log_transform(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                min_val = df[col].min()
                if min_val <= 0:
                    df[col] = np.log1p(df[col] + abs(min_val) + 1)
                else:
                    df[col] = np.log1p(df[col])
        return df

    def _cmd_parse_dates(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    def _cmd_parse_currency(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                from cleaning.type_handler import TypeHandler as TH
                th = TH.__new__(TH)
                df[col] = df[col].apply(
                    lambda v: th._parse_currency_value(str(v)) if pd.notna(v) else v
                )
        return df

    def _cmd_standardize_booleans(self, df, action, option):
        from cleaning.type_handler import _BOOL_TRUE, _BOOL_FALSE
        for col in action.target_columns:
            if col in df.columns:
                def to_bool(v):
                    if pd.isna(v):
                        return v
                    s = str(v).lower().strip()
                    if s in _BOOL_TRUE:
                        return True
                    if s in _BOOL_FALSE:
                        return False
                    return v
                df[col] = df[col].apply(to_bool)
        return df

    def _cmd_replace_pseudo_nulls(self, df, action, option):
        from cleaning.type_handler import _PSEUDO_NULLS
        for col in action.target_columns:
            if col in df.columns:
                mask = df[col].astype(str).str.lower().str.strip().isin(_PSEUDO_NULLS)
                df.loc[mask, col] = np.nan
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except Exception:
                    pass
        return df

    def _cmd_flag_integer_categories(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].astype("category")
        return df

    def _cmd_expand_json(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = self._expand_json_column(df, col)
        return df

    def _cmd_normalize_text(self, df, action, option):
        mode = option or "full"
        for col in action.target_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda v: TextHandler.normalize_text(str(v), mode) if pd.notna(v) else v
                )
        return df

    def _cmd_extract_text_features(self, df, action, option):
        mode = option or "all"
        for col in action.target_columns:
            if col in df.columns:
                features_df = TextHandler.extract_features(df[col], mode)
                features_df.columns = [f"{col}_{c}" for c in features_df.columns]
                df = pd.concat([df, features_df], axis=1)
        return df

    def _cmd_nlp_process(self, df, action, option):
        mode = option or "stopwords_lemma"
        lang = action.metadata.get("language", "en")
        for col in action.target_columns:
            if col in df.columns:
                df[col] = TextHandler.process_nlp(df[col], mode, lang)
        return df

    def _cmd_tfidf_vectorize(self, df, action, option):
        max_f = int(action.metadata.get("max_features", 100))
        ngram = tuple(action.metadata.get("ngram_range", [1, 2]))
        for col in action.target_columns:
            if col in df.columns:
                tfidf_df = TextHandler.tfidf_vectorize(df[col], max_f, ngram)
                tfidf_df.columns = [f"{col}_{c}" for c in tfidf_df.columns]
                df = pd.concat([df, tfidf_df], axis=1)
        return df

    def _cmd_correct_spelling(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df[col] = TextHandler.correct_spelling(df[col])
        return df

    def _cmd_label_encode(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_label_encode(df, col)
        return df

    def _cmd_one_hot_encode(self, df, action, option):
        drop_first = option == "ohe_drop_first"
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_one_hot_encode(df, col, drop_first)
        return df

    def _cmd_ordinal_encode(self, df, action, option):
        order = action.metadata.get("custom_order")
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_ordinal_encode(df, col, order)
        return df

    def _cmd_frequency_encode(self, df, action, option):
        use_prop = option == "frequency_proportion"
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_frequency_encode(df, col, use_prop)
        return df

    def _cmd_target_encode(self, df, action, option):
        target_col = action.metadata.get("target_column", "")
        method = "loo" if option != "target_kfold" else "kfold"
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_target_encode(df, col, target_col, method)
        return df

    def _cmd_binary_encode(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_binary_encode(df, col)
        return df

    def _cmd_hash_encode(self, df, action, option):
        buckets = {"hash_16": 16, "hash_32": 32, "hash_64": 64}.get(option or "", 32)
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_hash_encode(df, col, buckets)
        return df

    def _cmd_cyclical_encode(self, df, action, option):
        period = int(action.metadata.get("cyclical_period", 12))
        for col in action.target_columns:
            if col in df.columns:
                df = CategoricalHandler.apply_cyclical_encode(df, col, period)
        return df

    def _cmd_extract_datetime(self, df, action, option):
        components = action.metadata.get("components", [])
        if option == "date_only":
            components = [c for c in components if c in ("year", "month", "month_name", "quarter", "week", "day", "dayofweek", "day_name")]
        elif option == "time_only":
            components = [c for c in components if c in ("hour", "minute", "second")]
        for col in action.target_columns:
            if col in df.columns:
                df = DatetimeHandler.extract_components(df, col, components)
        return df

    def _cmd_derive_datetime_flags(self, df, action, option):
        flags = action.metadata.get("flags", [])
        for col in action.target_columns:
            if col in df.columns:
                df = DatetimeHandler.derive_flags(df, col, flags)
        return df

    def _cmd_compute_elapsed_time(self, df, action, option):
        unit = option or "days"
        for col in action.target_columns:
            if col in df.columns:
                df = DatetimeHandler.compute_elapsed(df, col, unit)
        return df

    def _cmd_compute_time_deltas(self, df, action, option):
        col_a = action.metadata.get("col_a", "")
        col_b = action.metadata.get("col_b", "")
        unit = option or action.metadata.get("unit", "days")
        if col_a in df.columns and col_b in df.columns:
            df = DatetimeHandler.compute_time_delta(df, col_a, col_b, unit)
        return df

    def _cmd_time_series_features(self, df, action, option):
        mode = option or "full"
        dt_col = action.target_columns[0] if action.target_columns else ""
        num_cols = action.target_columns[1:] if len(action.target_columns) > 1 else []
        if dt_col in df.columns:
            df = DatetimeHandler.generate_ts_features(df, dt_col, num_cols, mode)
        return df

    def _cmd_apply_scaling(self, df, action, option):
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
        method = option or method_map.get(action.action_type, "standard")
        for col in action.target_columns:
            if col in df.columns:
                df = ScalingHandler.apply_scaling(df, col, method)
        return df

    def _cmd_apply_feature_selection(self, df, action, option):
        if option == "skip" or option == "keep_both":
            return df
        return FeatureSelectionHandler.apply_drop_columns(df, action.target_columns)

    def _cmd_suggest_pca(self, df, action, option):
        if option and option.startswith("pca_"):
            n_map = {
                "pca_90": action.metadata.get("components_for_90pct", 5),
                "pca_95": action.metadata.get("components_for_95pct", 10),
                "pca_99": action.metadata.get("components_for_99pct", 15),
            }
            n_comp = int(n_map.get(option, 10))
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            df = FeatureSelectionHandler.apply_pca(df, numeric_cols, n_comp)
        return df

    def _cmd_apply_imbalance(self, df, action, option):
        target_col = action.target_columns[0] if action.target_columns else ""
        if target_col not in df.columns:
            return df
        opt = option or "random_over"
        if opt in ("random_over", "oversample"):
            return ImbalanceHandler.apply_random_oversample(df, target_col)
        elif opt in ("random_under", "undersample"):
            return ImbalanceHandler.apply_random_undersample(df, target_col)
        elif opt == "smote":
            return ImbalanceHandler.apply_smote(df, target_col)
        return ImbalanceHandler.apply_random_oversample(df, target_col)

    def _cmd_standardize_casing(self, df, action, option):
        mode = option or "lower"
        for col in action.target_columns:
            if col in df.columns:
                df = StandardizationHandler.apply_casing(df, col, mode)
        return df

    def _cmd_standardize_whitespace(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = StandardizationHandler.apply_whitespace(df, col)
        return df

    def _cmd_normalize_unicode(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = StandardizationHandler.apply_unicode(df, col)
        return df

    def _cmd_consolidate_synonyms(self, df, action, option):
        clusters = action.metadata.get("clusters", [])
        for col in action.target_columns:
            if col in df.columns:
                df = StandardizationHandler.apply_synonyms(df, col, clusters)
        return df

    def _cmd_standardize_email(self, df, action, option):
        for col in action.target_columns:
            if col in df.columns:
                df = StandardizationHandler.apply_email_standardize(df, col)
        return df

    def _cmd_standardize_precision(self, df, action, option):
        decimals = int(option or action.metadata.get("dominant_precision", 2))
        for col in action.target_columns:
            if col in df.columns:
                df = StandardizationHandler.apply_precision(df, col, decimals)
        return df

    def _cmd_drop_leakage(self, df, action, option):
        if option != "keep":
            cols_to_drop = [c for c in action.target_columns if c in df.columns]
            df = df.drop(columns=cols_to_drop)
        return df

    @staticmethod
    def _cmd_noop(self, df, action, option):
        return df  # Informational-only actions

    # ── Action registry ───────────────────────────────────────────────
    _ACTION_REGISTRY = {
        # Duplicates
        ActionType.REMOVE_EXACT_DUPLICATES: _cmd_remove_exact_duplicates,
        ActionType.DROP_DUPLICATE_COLUMN: _cmd_drop_columns,
        ActionType.DROP_DERIVED_COLUMN: _cmd_drop_columns,
        # Missing values
        ActionType.DROP_COLUMN: _cmd_drop_columns,
        ActionType.DROP_ROWS: _cmd_drop_rows,
        ActionType.IMPUTE_MEAN: _cmd_impute_mean,
        ActionType.IMPUTE_MEDIAN: _cmd_impute_median,
        ActionType.IMPUTE_MODE: _cmd_impute_mode,
        ActionType.IMPUTE_CONSTANT: _cmd_impute_constant,
        ActionType.FFILL: _cmd_ffill,
        ActionType.BFILL: _cmd_bfill,
        ActionType.INTERPOLATE: _cmd_interpolate,
        ActionType.ADD_INDICATOR: _cmd_add_indicator,
        ActionType.IMPUTE_KNN: _cmd_impute_knn,
        ActionType.FLAG_ONLY: _cmd_flag_only,
        # Outliers
        ActionType.FLAG_OUTLIER: _cmd_flag_outlier,
        ActionType.REMOVE_OUTLIER_ROWS: _cmd_remove_outlier_rows,
        ActionType.WINSORIZE: _cmd_winsorize,
        ActionType.CAP_OUTLIERS: _cmd_cap_outliers,
        ActionType.REPLACE_BOUNDARY: _cmd_cap_outliers,
        ActionType.LOG_TRANSFORM: _cmd_log_transform,
        # Type corrections
        ActionType.PARSE_DATES: _cmd_parse_dates,
        ActionType.PARSE_CURRENCY: _cmd_parse_currency,
        ActionType.STANDARDIZE_BOOLEANS: _cmd_standardize_booleans,
        ActionType.REPLACE_PSEUDO_NULLS: _cmd_replace_pseudo_nulls,
        ActionType.FLAG_INTEGER_CATEGORIES: _cmd_flag_integer_categories,
        ActionType.EXPAND_JSON: _cmd_expand_json,
        ActionType.VALIDATE_STRUCTURED: _cmd_noop,
        # Text preprocessing
        ActionType.NORMALIZE_TEXT: _cmd_normalize_text,
        ActionType.EXTRACT_TEXT_FEATURES: _cmd_extract_text_features,
        ActionType.REMOVE_STOPWORDS: _cmd_nlp_process,
        ActionType.STEM_LEMMATIZE: _cmd_nlp_process,
        ActionType.TFIDF_VECTORIZE: _cmd_tfidf_vectorize,
        ActionType.DROP_RAW_TEXT: _cmd_drop_columns,
        ActionType.CORRECT_SPELLING: _cmd_correct_spelling,
        # Categorical encoding
        ActionType.LABEL_ENCODE: _cmd_label_encode,
        ActionType.ONE_HOT_ENCODE: _cmd_one_hot_encode,
        ActionType.ORDINAL_ENCODE: _cmd_ordinal_encode,
        ActionType.FREQUENCY_ENCODE: _cmd_frequency_encode,
        ActionType.TARGET_ENCODE: _cmd_target_encode,
        ActionType.BINARY_ENCODE: _cmd_binary_encode,
        ActionType.HASH_ENCODE: _cmd_hash_encode,
        ActionType.CYCLICAL_ENCODE: _cmd_cyclical_encode,
        ActionType.SUGGEST_EMBEDDING: _cmd_noop,
        # Datetime engineering
        ActionType.EXTRACT_DATETIME: _cmd_extract_datetime,
        ActionType.DERIVE_DATETIME_FLAGS: _cmd_derive_datetime_flags,
        ActionType.COMPUTE_ELAPSED_TIME: _cmd_compute_elapsed_time,
        ActionType.COMPUTE_TIME_DELTAS: _cmd_compute_time_deltas,
        ActionType.TIME_SERIES_FEATURES: _cmd_time_series_features,
        # Feature scaling (all share the same dispatcher)
        ActionType.STANDARD_SCALE: _cmd_apply_scaling,
        ActionType.MINMAX_SCALE: _cmd_apply_scaling,
        ActionType.MAXABS_SCALE: _cmd_apply_scaling,
        ActionType.ROBUST_SCALE: _cmd_apply_scaling,
        ActionType.LOG1P_TRANSFORM: _cmd_apply_scaling,
        ActionType.BOXCOX_TRANSFORM: _cmd_apply_scaling,
        ActionType.YEOJOHNSON_TRANSFORM: _cmd_apply_scaling,
        ActionType.QUANTILE_UNIFORM: _cmd_apply_scaling,
        ActionType.QUANTILE_NORMAL: _cmd_apply_scaling,
        ActionType.BINARIZE: _cmd_apply_scaling,
        # Feature selection
        ActionType.DROP_ZERO_VARIANCE: _cmd_apply_feature_selection,
        ActionType.DROP_NEAR_ZERO_VARIANCE: _cmd_apply_feature_selection,
        ActionType.DROP_HIGH_CORRELATION: _cmd_apply_feature_selection,
        ActionType.DROP_HIGH_VIF: _cmd_apply_feature_selection,
        ActionType.DROP_LOW_MI: _cmd_apply_feature_selection,
        ActionType.CLUSTER_FEATURES: _cmd_apply_feature_selection,
        ActionType.SUGGEST_PCA: _cmd_suggest_pca,
        # Class imbalance (all share the same dispatcher)
        ActionType.RANDOM_OVERSAMPLE: _cmd_apply_imbalance,
        ActionType.SMOTE: _cmd_apply_imbalance,
        ActionType.SMOTE_NC: _cmd_apply_imbalance,
        ActionType.ADASYN: _cmd_apply_imbalance,
        ActionType.RANDOM_UNDERSAMPLE: _cmd_apply_imbalance,
        ActionType.TOMEK_LINKS: _cmd_apply_imbalance,
        ActionType.EDITED_NEAREST_NEIGHBORS: _cmd_apply_imbalance,
        ActionType.NEAR_MISS: _cmd_apply_imbalance,
        ActionType.SMOTEENN: _cmd_apply_imbalance,
        ActionType.SMOTETOMEK: _cmd_apply_imbalance,
        ActionType.CLASS_WEIGHTS: _cmd_noop,
        ActionType.ANOMALY_FRAMING: _cmd_noop,
        # Data standardization
        ActionType.STANDARDIZE_CASING: _cmd_standardize_casing,
        ActionType.STANDARDIZE_WHITESPACE: _cmd_standardize_whitespace,
        ActionType.NORMALIZE_UNICODE: _cmd_normalize_unicode,
        ActionType.CONSOLIDATE_SYNONYMS: _cmd_consolidate_synonyms,
        ActionType.STANDARDIZE_EMAIL: _cmd_standardize_email,
        ActionType.STANDARDIZE_PRECISION: _cmd_standardize_precision,
        ActionType.FIX_UNIT_INCONSISTENCY: _cmd_noop,
        ActionType.STANDARDIZE_PHONE: _cmd_noop,
        ActionType.STANDARDIZE_URL: _cmd_noop,
        ActionType.STANDARDIZE_CURRENCY_FORMAT: _cmd_noop,
        # Data leakage
        ActionType.FLAG_LEAKAGE_TEMPORAL: _cmd_drop_leakage,
        ActionType.FLAG_LEAKAGE_PREDICTOR: _cmd_drop_leakage,
        ActionType.FLAG_LEAKAGE_ID: _cmd_drop_leakage,
        ActionType.FLAG_LEAKAGE_FUTURE: _cmd_drop_leakage,
    }

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
            logger.warning("sklearn not installed — KNN imputation falling back to column median for %s", cols)
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
