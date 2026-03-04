"""
Categorical column profiler.
Computes cardinality, frequency distributions, case/whitespace issues,
ordinal detection, and data for pie charts, treemaps, and word clouds.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

import pandas as pd
import numpy as np

from profiling.profiling_models import (
    CategoricalProfile,
    CardinalityClass,
    ValueFrequency,
)
from profiling.type_detector import ORDINAL_PATTERNS


class CategoricalProfiler:
    """Deep profiling for categorical columns."""

    @staticmethod
    def profile(series: pd.Series) -> CategoricalProfile:
        profile = CategoricalProfile()
        non_null = series.dropna()
        if len(non_null) == 0:
            return profile

        n = len(non_null)
        str_series = non_null.astype(str)

        # ── Cardinality ──
        n_unique = non_null.nunique()
        profile.cardinality = n_unique
        profile.cardinality_class = CategoricalProfiler._classify_cardinality(
            n_unique, n
        )

        # ── Value Frequencies ──
        value_counts = non_null.value_counts()
        top_n = min(20, len(value_counts))
        top_vals = value_counts.head(top_n)
        profile.top_values = [
            ValueFrequency(
                value=str(val),
                count=int(cnt),
                percentage=round(cnt / n * 100, 2),
            )
            for val, cnt in top_vals.items()
        ]

        # Bottom values (least frequent)
        bottom_vals = value_counts.tail(min(5, len(value_counts)))
        profile.bottom_values = [
            ValueFrequency(
                value=str(val),
                count=int(cnt),
                percentage=round(cnt / n * 100, 2),
            )
            for val, cnt in bottom_vals.items()
        ]

        # ── Pie Chart Data (for low cardinality ≤10) ──
        if n_unique <= 10:
            profile.pie_data = [
                {"name": str(val), "value": int(cnt)}
                for val, cnt in value_counts.items()
            ]

        # ── Treemap Data (for medium cardinality) ──
        if 10 < n_unique <= 50:
            profile.treemap_data = [
                {"name": str(val), "value": int(cnt)}
                for val, cnt in value_counts.head(50).items()
            ]

        # ── Word Cloud Data (high cardinality or free text) ──
        if n_unique > 10:
            # Tokenize all values, count word frequencies
            word_counts: Counter = Counter()
            for val in str_series.head(5000):
                words = re.findall(r"\b\w+\b", str(val).lower())
                word_counts.update(words)

            profile.word_cloud_data = [
                {"text": word, "value": cnt}
                for word, cnt in word_counts.most_common(100)
                if len(word) > 1  # skip single chars
            ]

        # ── Case Consistency ──
        profile.case_inconsistencies = CategoricalProfiler._check_case_consistency(
            str_series
        )

        # ── Whitespace / Padding ──
        profile.whitespace_issues = CategoricalProfiler._check_whitespace(str_series)

        # ── Special Character Contamination ──
        profile.special_char_contamination = CategoricalProfiler._check_special_chars(
            str_series
        )

        # ── Ordinal Detection ──
        unique_lower = set(str_series.str.lower().str.strip().unique())
        for pattern in ORDINAL_PATTERNS:
            if unique_lower <= pattern["values"]:
                profile.suspected_ordinal = True
                # Reconstruct order from pattern
                profile.ordinal_order = [
                    v for v in sorted(pattern["values"],
                    key=lambda x: list(pattern["values"]).index(x)
                    if x in pattern["values"] else 999)
                ]
                break

        return profile

    @staticmethod
    def _classify_cardinality(n_unique: int, n_total: int) -> CardinalityClass:
        distinct_ratio = n_unique / n_total if n_total > 0 else 0

        if n_unique == 2:
            return CardinalityClass.BINARY
        if distinct_ratio == 1.0:
            return CardinalityClass.UNIQUE
        if distinct_ratio > 0.95:
            return CardinalityClass.NEAR_UNIQUE
        if n_unique < 10:
            return CardinalityClass.LOW
        if n_unique < 50:
            return CardinalityClass.MEDIUM
        if n_unique < 500:
            return CardinalityClass.HIGH
        return CardinalityClass.VERY_HIGH

    @staticmethod
    def _check_case_consistency(str_series: pd.Series) -> list[dict[str, Any]]:
        """Detect case inconsistencies like 'New York' vs 'new york'."""
        issues = []
        sample = str_series.head(10000)
        lower_map: dict[str, list[str]] = {}

        for val in sample.unique():
            key = str(val).lower().strip()
            if key not in lower_map:
                lower_map[key] = []
            lower_map[key].append(str(val))

        for key, variants in lower_map.items():
            if len(variants) > 1:
                issues.append({
                    "normalized": key,
                    "variants": variants[:5],  # Cap for display
                    "variant_count": len(variants),
                })

        return issues[:20]  # Top 20 issues

    @staticmethod
    def _check_whitespace(str_series: pd.Series) -> list[dict[str, Any]]:
        """Detect leading/trailing whitespace padding."""
        issues = []
        sample = str_series.head(10000)

        has_leading = sample.str.match(r"^\s+", na=False)
        has_trailing = sample.str.match(r".*\s+$", na=False)

        leading_count = int(has_leading.sum())
        trailing_count = int(has_trailing.sum())

        if leading_count > 0:
            examples = sample[has_leading].head(3).tolist()
            issues.append({
                "type": "leading_whitespace",
                "count": leading_count,
                "examples": [repr(e) for e in examples],
            })

        if trailing_count > 0:
            examples = sample[has_trailing].head(3).tolist()
            issues.append({
                "type": "trailing_whitespace",
                "count": trailing_count,
                "examples": [repr(e) for e in examples],
            })

        return issues

    @staticmethod
    def _check_special_chars(str_series: pd.Series) -> list[str]:
        """Detect special character contamination."""
        contaminated = []
        sample = str_series.head(5000)

        # Check for non-printable characters
        has_nonprint = sample.str.contains(r"[\x00-\x1f\x7f-\x9f]", regex=True, na=False)
        if has_nonprint.sum() > 0:
            contaminated.append(
                f"{int(has_nonprint.sum())} values contain non-printable characters"
            )

        # Check for unusual Unicode that might be encoding artifacts
        has_replacement = sample.str.contains("\ufffd", na=False)
        if has_replacement.sum() > 0:
            contaminated.append(
                f"{int(has_replacement.sum())} values contain Unicode replacement character (�)"
            )

        return contaminated
