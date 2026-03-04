"""
Boolean column profiler.
Handles native booleans and disguised booleans (Yes/No, Y/N, 1/0, etc.)
"""

from __future__ import annotations

import pandas as pd

from profiling.profiling_models import BooleanProfile
from profiling.type_detector import BOOLEAN_PAIRS


class BooleanProfiler:
    """Deep profiling for boolean and disguised-boolean columns."""

    @staticmethod
    def profile(series: pd.Series) -> BooleanProfile:
        profile = BooleanProfile()
        non_null = series.dropna()
        if len(non_null) == 0:
            return profile

        n = len(non_null)

        # Native boolean
        if pd.api.types.is_bool_dtype(series):
            profile.true_count = int(non_null.sum())
            profile.false_count = n - profile.true_count
            profile.true_ratio = round(profile.true_count / n, 4)
            profile.false_ratio = round(profile.false_count / n, 4)
            return profile

        # Numeric 0/1
        if pd.api.types.is_numeric_dtype(series):
            unique_vals = set(non_null.unique())
            if unique_vals <= {0, 1, 0.0, 1.0}:
                profile.true_count = int((non_null == 1).sum())
                profile.false_count = int((non_null == 0).sum())
                profile.true_ratio = round(profile.true_count / n, 4)
                profile.false_ratio = round(profile.false_count / n, 4)
                profile.is_disguised = True
                profile.disguised_mapping = {"1": "True", "0": "False"}
                return profile

        # String-based disguised booleans
        str_series = non_null.astype(str).str.lower().str.strip()
        unique_lower = set(str_series.unique())

        for pair in BOOLEAN_PAIRS:
            if unique_lower <= pair:
                pair_list = sorted(pair)
                # Determine which is "true" (positive) and which is "false"
                true_val, false_val = BooleanProfiler._classify_pair(pair_list)

                profile.true_count = int(str_series.isin({true_val}).sum())
                profile.false_count = int(str_series.isin({false_val}).sum())
                profile.true_ratio = round(profile.true_count / n, 4)
                profile.false_ratio = round(profile.false_count / n, 4)
                profile.is_disguised = True

                # Build mapping from original values
                orig_vals = non_null.astype(str).str.strip().unique()
                for ov in orig_vals:
                    if ov.lower().strip() == true_val:
                        profile.disguised_mapping[ov] = "True"
                    else:
                        profile.disguised_mapping[ov] = "False"

                return profile

        # Fallback for any binary column
        if len(unique_lower) == 2:
            vals = list(unique_lower)
            counts = str_series.value_counts()
            profile.true_count = int(counts.iloc[0])
            profile.false_count = int(counts.iloc[1]) if len(counts) > 1 else 0
            profile.true_ratio = round(profile.true_count / n, 4)
            profile.false_ratio = round(profile.false_count / n, 4)
            profile.is_disguised = True
            profile.disguised_mapping = {
                str(non_null.value_counts().index[0]): "True (majority)",
                str(non_null.value_counts().index[1]): "False (minority)",
            }

        return profile

    @staticmethod
    def _classify_pair(pair: list[str]) -> tuple[str, str]:
        """Determine which value in a pair is the 'true' (positive) value."""
        positive_signals = {
            "yes", "y", "true", "t", "1", "on", "active", "enabled",
            "pass", "positive", "male",
        }
        for val in pair:
            if val in positive_signals:
                other = [v for v in pair if v != val][0]
                return val, other
        return pair[0], pair[1]
