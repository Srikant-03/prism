"""
Numeric column profiler.
Computes comprehensive statistics, histogram (Freedman-Diaconis),
KDE, box plot data, Q-Q plot data, and formatting issue detection.
"""

from __future__ import annotations

import math
import re
from typing import Optional

import numpy as np
import pandas as pd

from profiling.profiling_models import (
    NumericProfile,
    SkewnessInterpretation,
    KurtosisInterpretation,
)


class NumericProfiler:
    """Compute deep statistics and visualization data for numeric columns."""

    @staticmethod
    def profile(series: pd.Series, original_series: Optional[pd.Series] = None) -> NumericProfile:
        """
        Profile a numeric series.

        Args:
            series: The numeric column (already filtered to non-null).
            original_series: Original series (for formatting issue detection on objects).
        """
        profile = NumericProfile()

        non_null = series.dropna()
        if len(non_null) == 0:
            return profile

        vals = non_null.astype(float)
        n = len(vals)

        # ── Basic Stats ──
        profile.min = float(vals.min())
        profile.max = float(vals.max())
        profile.range = profile.max - profile.min
        profile.sum = float(vals.sum())

        # ── Central Tendency ──
        profile.mean = float(vals.mean())
        profile.median = float(vals.median())

        # Trimmed mean (5%)
        try:
            from scipy.stats import trim_mean
            profile.trimmed_mean_5 = float(trim_mean(vals.values, 0.05))
        except Exception:
            profile.trimmed_mean_5 = profile.mean

        # Geometric mean (if all positive)
        try:
            if (vals > 0).all():
                from scipy.stats import gmean
                profile.geometric_mean = float(gmean(vals.values))
        except Exception:
            pass

        # Harmonic mean (if all positive)
        try:
            if (vals > 0).all():
                from scipy.stats import hmean
                profile.harmonic_mean = float(hmean(vals.values))
        except Exception:
            pass

        # Mode(s)
        try:
            mode_result = vals.mode()
            profile.modes = [float(m) for m in mode_result.head(5)]
        except Exception:
            pass

        # ── Dispersion ──
        profile.std_dev = float(vals.std())
        profile.variance = float(vals.var())

        if profile.mean != 0:
            profile.coefficient_of_variation = abs(profile.std_dev / profile.mean)

        # ── Shape ──
        try:
            from scipy.stats import skew, kurtosis
            skew_val = float(skew(vals.values, nan_policy="omit"))
            kurt_val = float(kurtosis(vals.values, nan_policy="omit"))

            profile.skewness = skew_val
            if abs(skew_val) < 0.5:
                profile.skewness_interpretation = SkewnessInterpretation.SYMMETRIC
            elif abs(skew_val) < 1.0:
                profile.skewness_interpretation = SkewnessInterpretation.MODERATELY_SKEWED
            else:
                profile.skewness_interpretation = SkewnessInterpretation.HIGHLY_SKEWED

            profile.kurtosis = kurt_val
            if abs(kurt_val) < 0.5:
                profile.kurtosis_interpretation = KurtosisInterpretation.MESOKURTIC
            elif kurt_val > 0:
                profile.kurtosis_interpretation = KurtosisInterpretation.LEPTOKURTIC
            else:
                profile.kurtosis_interpretation = KurtosisInterpretation.PLATYKURTIC
        except Exception:
            pass

        # ── Percentiles ──
        pct_keys = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        try:
            pcts = np.percentile(vals.values, pct_keys)
            profile.percentiles = {f"p{k}": float(v) for k, v in zip(pct_keys, pcts)}
        except Exception:
            pass

        # IQR
        try:
            q1 = float(np.percentile(vals.values, 25))
            q3 = float(np.percentile(vals.values, 75))
            profile.iqr = q3 - q1
        except Exception:
            pass

        # ── Value Counts ──
        profile.zero_count = int((vals == 0).sum())
        profile.negative_count = int((vals < 0).sum())
        profile.positive_count = int((vals > 0).sum())

        # Integer-valued count in float column
        try:
            profile.integer_valued_count = int((vals == vals.astype(int)).sum())
        except (ValueError, OverflowError):
            profile.integer_valued_count = 0

        # ── Histogram (Freedman-Diaconis rule) ──
        try:
            counts, bin_edges = NumericProfiler._histogram_fd(vals.values)
            profile.histogram_bins = [float(b) for b in bin_edges]
            profile.histogram_counts = [int(c) for c in counts]
            profile.histogram_method = "freedman_diaconis"
        except Exception:
            try:
                counts, bin_edges = np.histogram(vals.values, bins="auto")
                profile.histogram_bins = [float(b) for b in bin_edges]
                profile.histogram_counts = [int(c) for c in counts]
                profile.histogram_method = "auto"
            except Exception:
                pass

        # ── KDE ──
        try:
            if n >= 10 and profile.std_dev and profile.std_dev > 0:
                kde_x, kde_y = NumericProfiler._compute_kde(vals.values)
                profile.kde_x = [float(x) for x in kde_x]
                profile.kde_y = [float(y) for y in kde_y]
        except Exception:
            pass

        # ── Box Plot ──
        try:
            q1 = float(np.percentile(vals.values, 25))
            q2 = float(np.percentile(vals.values, 50))
            q3 = float(np.percentile(vals.values, 75))
            iqr = q3 - q1

            whisker_low = float(vals[vals >= q1 - 1.5 * iqr].min())
            whisker_high = float(vals[vals <= q3 + 1.5 * iqr].max())

            outliers = vals[(vals < q1 - 1.5 * iqr) | (vals > q3 + 1.5 * iqr)]
            # Cap outliers for display
            outlier_list = sorted(outliers.tolist())
            if len(outlier_list) > 100:
                outlier_list = outlier_list[:50] + outlier_list[-50:]

            profile.box_q1 = q1
            profile.box_q2 = q2
            profile.box_q3 = q3
            profile.box_whisker_low = whisker_low
            profile.box_whisker_high = whisker_high
            profile.box_outliers = [float(o) for o in outlier_list]
        except Exception:
            pass

        # ── Q-Q Plot ──
        try:
            if n >= 20:
                from scipy import stats
                theoretical, sample = stats.probplot(vals.values, dist="norm", fit=False)
                # Subsample for performance
                step = max(1, len(theoretical) // 500)
                profile.qq_theoretical = [float(x) for x in theoretical[::step]]
                profile.qq_sample = [float(x) for x in sample[::step]]
        except Exception:
            pass

        # ── Formatting Issues ──
        if original_series is not None:
            profile.formatting_issues = NumericProfiler._detect_formatting_issues(
                original_series
            )

        return profile

    @staticmethod
    def _histogram_fd(data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """
        Compute histogram using Freedman-Diaconis rule for optimal bin width.
        Falls back to Sturges rule if IQR is zero.
        """
        q75, q25 = np.percentile(data, [75, 25])
        iqr = q75 - q25
        n = len(data)

        if iqr > 0 and n > 0:
            # Freedman-Diaconis
            bin_width = 2 * iqr * (n ** (-1.0 / 3.0))
            n_bins = max(1, int(math.ceil((data.max() - data.min()) / bin_width)))
            n_bins = min(n_bins, 200)  # Cap for sanity
        else:
            # Sturges rule fallback
            n_bins = max(1, int(math.ceil(math.log2(n) + 1)))

        return np.histogram(data, bins=n_bins)

    @staticmethod
    def _compute_kde(data: np.ndarray, n_points: int = 200) -> tuple[np.ndarray, np.ndarray]:
        """Compute Kernel Density Estimate for smooth density curve."""
        from scipy.stats import gaussian_kde

        # Remove infinities
        clean = data[np.isfinite(data)]
        if len(clean) < 5:
            return np.array([]), np.array([])

        kde = gaussian_kde(clean)
        x_min = clean.min() - 0.1 * (clean.max() - clean.min())
        x_max = clean.max() + 0.1 * (clean.max() - clean.min())
        x = np.linspace(x_min, x_max, n_points)
        y = kde(x)
        return x, y

    @staticmethod
    def _detect_formatting_issues(series: pd.Series) -> list[str]:
        """Detect number formatting issues in original string data."""
        issues = []
        if not pd.api.types.is_object_dtype(series):
            return issues

        sample = series.dropna().head(500).astype(str)

        # Currency symbols
        currency_pattern = re.compile(r"[\$€£¥₹₽]")
        currency_count = sample.str.contains(currency_pattern, na=False).sum()
        if currency_count > 0:
            issues.append(
                f"{currency_count} values contain currency symbols (e.g., $, €, £)"
            )

        # Thousand separators
        thousand_pattern = re.compile(r"\d{1,3}(,\d{3})+")
        thousand_count = sample.str.contains(thousand_pattern, na=False).sum()
        if thousand_count > 0:
            issues.append(
                f"{thousand_count} values contain thousand separators (e.g., 1,000)"
            )

        # Percentage signs
        pct_count = sample.str.contains(r"%", na=False).sum()
        if pct_count > 0:
            issues.append(
                f"{pct_count} values contain percentage signs stored as strings"
            )

        return issues
