"""
Categorical Handler — Selects and applies encoding strategies per column
based on cardinality, ordinality, target relationship, and downstream use.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
)

# Cyclical patterns: column name → period
_CYCLICAL_PATTERNS = [
    {"pattern": r"(?i)(month|mon)$", "period": 12, "label": "month (1-12)"},
    {"pattern": r"(?i)(day_of_week|dayofweek|dow|weekday)", "period": 7, "label": "day of week (0-6)"},
    {"pattern": r"(?i)(hour|hr)$", "period": 24, "label": "hour (0-23)"},
    {"pattern": r"(?i)(minute|min)$", "period": 60, "label": "minute (0-59)"},
    {"pattern": r"(?i)(second|sec)$", "period": 60, "label": "second (0-59)"},
    {"pattern": r"(?i)(quarter|qtr)$", "period": 4, "label": "quarter (1-4)"},
    {"pattern": r"(?i)(bearing|direction|heading|angle)", "period": 360, "label": "bearing (0-360)"},
]

# Ordinal-like patterns
_ORDINAL_PATTERNS = [
    r"(?i)(rating|grade|level|tier|rank|stage|priority|severity|quality)",
    r"(?i)(satisfaction|experience|skill|education)",
    r"(?i)(low|medium|high|very|extremely)",
    r"(?i)(small|medium|large|xl|xxl)",
]


class CategoricalHandler:
    """Categorical encoding strategy selector and executor."""

    def __init__(
        self, df: pd.DataFrame, file_id: str,
        target_column: Optional[str] = None,
        ohe_threshold: int = 20,
        profile: Any = None,
    ):
        self.df = df
        self.file_id = file_id
        self.target_column = target_column
        self.ohe_threshold = ohe_threshold
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], dict]:
        actions: list[CleaningAction] = []
        report: dict[str, Any] = {
            "total_categorical": 0,
            "encoding_recommendations": [],
        }

        cat_cols = self._identify_categorical_columns()
        report["total_categorical"] = len(cat_cols)

        for col in cat_cols:
            series = self.df[col].dropna()
            nunique = int(series.nunique())
            if nunique < 2:
                continue

            cardinality = nunique
            is_binary = nunique == 2
            is_cyclical, cyc_info = self._check_cyclical(col, series)
            is_ordinal = self._check_ordinal(col, series)
            unique_ratio = nunique / len(series) if len(series) > 0 else 0

            strategy, reasoning = self._select_strategy(
                col, series, cardinality, is_binary, is_cyclical, is_ordinal, unique_ratio
            )

            action = self._build_action(
                col, strategy, reasoning, cardinality,
                is_binary, is_cyclical, is_ordinal,
                cyc_info, series,
            )
            actions.append(action)
            report["encoding_recommendations"].append({
                "column": col,
                "cardinality": cardinality,
                "strategy": strategy.value,
                "is_binary": is_binary,
                "is_cyclical": is_cyclical,
                "is_ordinal": is_ordinal,
            })

        return actions, report

    # ── Identify categoricals ─────────────────────────────────────────
    def _identify_categorical_columns(self) -> list[str]:
        cats: list[str] = []
        for col in self.df.columns:
            if col == self.target_column:
                continue
            s = self.df[col]
            # Already categorical dtype
            if pd.api.types.is_categorical_dtype(s):
                cats.append(col)
                continue
            # Object/string columns
            if s.dtype == object or str(s.dtype) == "string":
                cats.append(col)
                continue
            # Integer columns with low cardinality (likely codes)
            if pd.api.types.is_integer_dtype(s):
                if s.nunique() <= 20:
                    cats.append(col)
        return cats

    # ── Cyclical check ────────────────────────────────────────────────
    def _check_cyclical(self, col: str, series: pd.Series) -> tuple[bool, Optional[dict]]:
        for pat in _CYCLICAL_PATTERNS:
            if re.search(pat["pattern"], col):
                vals = series.dropna()
                if pd.api.types.is_numeric_dtype(vals):
                    min_v, max_v = float(vals.min()), float(vals.max())
                    if min_v >= 0 and max_v <= pat["period"]:
                        return True, pat
        return False, None

    # ── Ordinal check ─────────────────────────────────────────────────
    def _check_ordinal(self, col: str, series: pd.Series) -> bool:
        # Check column name
        for pat in _ORDINAL_PATTERNS:
            if re.search(pat, col):
                return True
        # Check value patterns
        unique_vals = sorted(series.astype(str).str.lower().unique())
        ordinal_keywords = {"low", "medium", "high", "very low", "very high",
                            "poor", "fair", "good", "excellent", "outstanding",
                            "none", "basic", "intermediate", "advanced", "expert",
                            "small", "medium", "large", "xl", "xxl",
                            "1", "2", "3", "4", "5"}
        overlap = set(unique_vals) & ordinal_keywords
        if len(overlap) >= 2:
            return True
        return False

    # ── Strategy selection ────────────────────────────────────────────
    def _select_strategy(
        self, col: str, series: pd.Series,
        cardinality: int, is_binary: bool,
        is_cyclical: bool, is_ordinal: bool,
        unique_ratio: float,
    ) -> tuple[ActionType, str]:

        # 1. Cyclical encoding (highest priority for cyclical data)
        if is_cyclical:
            return ActionType.CYCLICAL_ENCODE, (
                "Column matches a cyclical pattern. Sine/cosine encoding preserves "
                "the circular distance relationship (e.g., month 12 is close to month 1)."
            )

        # 2. Binary columns → label encoding
        if is_binary:
            return ActionType.LABEL_ENCODE, (
                "Binary column with exactly 2 unique values. Label encoding (0/1) "
                "is the simplest and most efficient representation."
            )

        # 3. Confirmed ordinal → ordinal encoding
        if is_ordinal:
            return ActionType.ORDINAL_ENCODE, (
                "Column appears ordinal — values suggest a natural ordering. "
                "Ordinal encoding preserves rank information. "
                "You can confirm or adjust the inferred order."
            )

        # 4. Low cardinality nominal → one-hot encoding
        if cardinality <= self.ohe_threshold:
            return ActionType.ONE_HOT_ENCODE, (
                f"Nominal column with {cardinality} unique values (≤ {self.ohe_threshold} threshold). "
                "One-hot encoding is the standard approach for nominal features with "
                "manageable cardinality."
            )

        # 5. Medium-high cardinality → frequency encoding
        if cardinality <= 100:
            return ActionType.FREQUENCY_ENCODE, (
                f"Nominal column with {cardinality} unique values (above OHE threshold of "
                f"{self.ohe_threshold}). Frequency encoding avoids column explosion while "
                "capturing category prevalence."
            )

        # 6. Has target variable → target encoding
        if self.target_column and self.target_column in self.df.columns:
            return ActionType.TARGET_ENCODE, (
                f"High-cardinality column ({cardinality} unique values) with an available "
                "target variable. Target encoding captures the relationship between "
                "category and target using leave-one-out cross-validation to prevent leakage."
            )

        # 7. High cardinality → binary encoding
        if cardinality <= 500:
            return ActionType.BINARY_ENCODE, (
                f"High-cardinality column ({cardinality} unique values). "
                "Binary encoding creates log2(N) columns — much fewer than OHE — "
                "while preserving uniqueness."
            )

        # 8. Very high cardinality → hashing encoding
        if cardinality <= 5000:
            return ActionType.HASH_ENCODE, (
                f"Very high cardinality column ({cardinality} unique values). "
                "Hash encoding maps categories to a fixed number of hash buckets, "
                "trading some information for dimensionality control."
            )

        # 9. Extremely high cardinality → recommend embeddings
        return ActionType.SUGGEST_EMBEDDING, (
            f"Extremely high cardinality column ({cardinality} unique values). "
            "Standard encoding methods are impractical. Recommend using learned "
            "embeddings (e.g., entity embedding in neural networks) to create "
            "dense, lower-dimensional representations."
        )

    # ── Build action ──────────────────────────────────────────────────
    def _build_action(
        self, col: str, strategy: ActionType, reasoning: str,
        cardinality: int, is_binary: bool, is_cyclical: bool,
        is_ordinal: bool, cyc_info: Optional[dict],
        series: pd.Series,
    ) -> CleaningAction:

        unique_vals = series.value_counts().head(10).to_dict()
        sample_vals = list(unique_vals.keys())[:5]

        # Estimate output columns
        cols_added = 0
        if strategy == ActionType.ONE_HOT_ENCODE:
            cols_added = cardinality - 1  # drop_first
        elif strategy == ActionType.BINARY_ENCODE:
            import math
            cols_added = int(math.ceil(math.log2(max(cardinality, 2))))
        elif strategy == ActionType.HASH_ENCODE:
            cols_added = min(32, cardinality)
        elif strategy == ActionType.CYCLICAL_ENCODE:
            cols_added = 2  # sin + cos
        elif strategy == ActionType.FREQUENCY_ENCODE:
            cols_added = 0  # in-place
        elif strategy == ActionType.TARGET_ENCODE:
            cols_added = 0  # in-place
        elif strategy == ActionType.LABEL_ENCODE:
            cols_added = 0  # in-place
        elif strategy == ActionType.ORDINAL_ENCODE:
            cols_added = 0  # in-place

        # Confidence
        is_definitive = is_binary or (strategy == ActionType.CYCLICAL_ENCODE)

        # Options
        options = self._get_options(strategy, cardinality, is_ordinal)

        preview_before = [{col: str(v)} for v in sample_vals[:3]]

        return CleaningAction(
            category=ActionCategory.CATEGORICAL_ENCODING,
            action_type=strategy,
            confidence=ActionConfidence.DEFINITIVE if is_definitive else ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': {cardinality} unique categories "
                f"(binary={is_binary}, ordinal={is_ordinal}, cyclical={is_cyclical}). "
                f"Top values: {sample_vals[:5]}"
            ),
            recommendation=f"Apply {strategy.value.replace('_', ' ')} to '{col}'.",
            reasoning=reasoning,
            target_columns=[col],
            preview=PreviewSample(
                before=preview_before,
                columns_before=[col],
                columns_after=[col] if cols_added == 0 else [f"{col}_*"],
            ),
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols,
                columns_after=self.n_cols + cols_added - (1 if cols_added > 0 else 0),
                columns_affected=max(cols_added, 1),
                description=(
                    f"Encodes '{col}' ({cardinality} categories) "
                    f"→ {'+' + str(cols_added) + ' columns' if cols_added > 0 else 'in-place'}."
                ),
            ),
            options=options,
            metadata={
                "cardinality": cardinality,
                "is_binary": is_binary,
                "is_ordinal": is_ordinal,
                "is_cyclical": is_cyclical,
                "top_values": unique_vals,
                "cyclical_period": cyc_info["period"] if cyc_info else None,
            },
        )

    def _get_options(self, strategy: ActionType, cardinality: int, is_ordinal: bool) -> list[UserOption]:
        base = [UserOption(key="skip", label="Skip")]

        if strategy == ActionType.LABEL_ENCODE:
            return [
                UserOption(key="label", label="Label Encode (0/1)", is_default=True),
                UserOption(key="ohe", label="One-Hot Encode Instead"),
            ] + base

        if strategy == ActionType.ONE_HOT_ENCODE:
            opts = [
                UserOption(key="ohe", label=f"One-Hot Encode ({cardinality} cols)", is_default=True),
                UserOption(key="ohe_drop_first", label="One-Hot (drop first)"),
                UserOption(key="frequency", label="Frequency Encode Instead"),
            ]
            return opts + base

        if strategy == ActionType.ORDINAL_ENCODE:
            return [
                UserOption(key="ordinal", label="Ordinal Encode (auto-order)", is_default=True),
                UserOption(key="ordinal_custom", label="Ordinal (custom order)"),
                UserOption(key="label", label="Label Encode Instead"),
            ] + base

        if strategy == ActionType.FREQUENCY_ENCODE:
            return [
                UserOption(key="frequency_count", label="Frequency (count)", is_default=True),
                UserOption(key="frequency_proportion", label="Frequency (proportion)"),
                UserOption(key="ohe", label="One-Hot Encode (warning: high cardinality)"),
            ] + base

        if strategy == ActionType.TARGET_ENCODE:
            return [
                UserOption(key="target_loo", label="Target Encode (leave-one-out)", is_default=True),
                UserOption(key="target_kfold", label="Target Encode (5-fold CV)"),
                UserOption(key="frequency", label="Frequency Encode Instead"),
            ] + base

        if strategy == ActionType.BINARY_ENCODE:
            return [
                UserOption(key="binary", label="Binary Encode", is_default=True),
                UserOption(key="hash", label="Hash Encode Instead"),
            ] + base

        if strategy == ActionType.HASH_ENCODE:
            return [
                UserOption(key="hash_32", label="Hash (32 buckets)", is_default=True),
                UserOption(key="hash_64", label="Hash (64 buckets)"),
                UserOption(key="hash_16", label="Hash (16 buckets)"),
            ] + base

        if strategy == ActionType.CYCLICAL_ENCODE:
            return [
                UserOption(key="cyclical", label="Sin/Cos Encode", is_default=True),
            ] + base

        if strategy == ActionType.SUGGEST_EMBEDDING:
            return [
                UserOption(key="embedding", label="Acknowledge (embedding recommended)", is_default=True),
                UserOption(key="hash", label="Hash Encode as Fallback"),
                UserOption(key="frequency", label="Frequency Encode as Fallback"),
            ] + base

        return [UserOption(key="apply", label="Apply", is_default=True)] + base

    # ── Static execution methods ──────────────────────────────────────

    @staticmethod
    def apply_label_encode(df: pd.DataFrame, col: str) -> pd.DataFrame:
        mapping = {v: i for i, v in enumerate(sorted(df[col].dropna().unique()))}
        df[col] = df[col].map(mapping)
        return df

    @staticmethod
    def apply_one_hot_encode(df: pd.DataFrame, col: str, drop_first: bool = False) -> pd.DataFrame:
        dummies = pd.get_dummies(df[col], prefix=col, drop_first=drop_first, dtype=int)
        df = pd.concat([df.drop(columns=[col]), dummies], axis=1)
        return df

    @staticmethod
    def apply_frequency_encode(df: pd.DataFrame, col: str, use_proportion: bool = False) -> pd.DataFrame:
        freq = df[col].value_counts(normalize=use_proportion)
        df[col] = df[col].map(freq)
        return df

    @staticmethod
    def apply_ordinal_encode(df: pd.DataFrame, col: str, order: Optional[list] = None) -> pd.DataFrame:
        if order is None:
            order = sorted(df[col].dropna().unique())
        mapping = {v: i for i, v in enumerate(order)}
        df[col] = df[col].map(mapping)
        return df

    @staticmethod
    def apply_target_encode(df: pd.DataFrame, col: str, target_col: str, method: str = "loo") -> pd.DataFrame:
        """Target encoding with leave-one-out to prevent leakage."""
        if target_col not in df.columns:
            return df

        target = df[target_col]
        if not pd.api.types.is_numeric_dtype(target):
            return df

        global_mean = float(target.mean())

        if method == "loo":
            # Leave-one-out: (group_sum - value) / (group_count - 1)
            group_sum = df.groupby(col)[target_col].transform("sum")
            group_count = df.groupby(col)[target_col].transform("count")
            df[col] = (group_sum - target) / (group_count - 1)
            df[col] = df[col].fillna(global_mean)
        else:
            # Simple mean encoding as fallback
            means = df.groupby(col)[target_col].mean()
            df[col] = df[col].map(means).fillna(global_mean)

        return df

    @staticmethod
    def apply_binary_encode(df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Binary encoding: map categories to binary digits in separate columns."""
        import math
        uniques = df[col].dropna().unique()
        n_bits = int(math.ceil(math.log2(max(len(uniques), 2))))
        mapping = {v: i for i, v in enumerate(uniques)}
        codes = df[col].map(mapping).fillna(0).astype(int)

        for bit in range(n_bits):
            df[f"{col}_bit{bit}"] = (codes >> bit) & 1

        df = df.drop(columns=[col])
        return df

    @staticmethod
    def apply_hash_encode(df: pd.DataFrame, col: str, n_buckets: int = 32) -> pd.DataFrame:
        """Hash encoding: map categories to hash buckets."""
        hashed = df[col].apply(lambda v: hash(str(v)) % n_buckets if pd.notna(v) else 0)
        dummies = pd.get_dummies(hashed, prefix=f"{col}_hash", dtype=int)
        df = pd.concat([df.drop(columns=[col]), dummies], axis=1)
        return df

    @staticmethod
    def apply_cyclical_encode(df: pd.DataFrame, col: str, period: int) -> pd.DataFrame:
        """Cyclical encoding: sin/cos pairs preserve circular distance."""
        vals = pd.to_numeric(df[col], errors="coerce")
        df[f"{col}_sin"] = np.sin(2 * np.pi * vals / period)
        df[f"{col}_cos"] = np.cos(2 * np.pi * vals / period)
        df = df.drop(columns=[col])
        return df
