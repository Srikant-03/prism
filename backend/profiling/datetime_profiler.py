"""
Datetime column profiler.
Analyses time ranges, frequency detection, gap analysis,
timezone detection, future/implausible dates, and seasonality indicators.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Optional

import pandas as pd
import numpy as np

from profiling.profiling_models import DatetimeProfile, FrequencyPattern


class DatetimeProfiler:
    """Deep profiling for datetime columns."""

    @staticmethod
    def profile(series: pd.Series) -> DatetimeProfile:
        profile = DatetimeProfile()

        # Ensure datetime dtype
        if not pd.api.types.is_datetime64_any_dtype(series):
            try:
                dt_series = pd.to_datetime(series, errors="coerce", infer_datetime_format=True)
            except Exception:
                return profile
        else:
            dt_series = series

        non_null = dt_series.dropna().sort_values()
        if len(non_null) == 0:
            return profile

        n = len(non_null)

        # ── Format Detection ──
        if pd.api.types.is_object_dtype(series):
            profile.detected_formats, profile.mixed_formats = (
                DatetimeProfiler._detect_formats(series.dropna())
            )

        # ── Range ──
        earliest = non_null.min()
        latest = non_null.max()
        profile.earliest = str(earliest)
        profile.latest = str(latest)
        profile.time_span_days = (latest - earliest).total_seconds() / 86400

        # Coverage density
        if profile.time_span_days and profile.time_span_days > 0:
            profile.coverage_density = n / profile.time_span_days

        # ── Frequency Detection ──
        if n >= 3:
            profile.frequency, profile.frequency_justification = (
                DatetimeProfiler._detect_frequency(non_null)
            )

        # ── Gap Detection ──
        if n >= 2:
            profile.gaps, profile.gap_count = DatetimeProfiler._detect_gaps(
                non_null, profile.frequency
            )

        # ── Timezone ──
        profile.timezone_info, profile.mixed_timezones = (
            DatetimeProfiler._detect_timezone(non_null)
        )

        # ── Future Dates ──
        now = pd.Timestamp.now()
        future_mask = non_null > now
        profile.future_dates_count = int(future_mask.sum())

        # ── Implausible Dates ──
        implausible = (non_null.dt.year < 1900) | (non_null.dt.year > 2100)
        profile.implausible_dates_count = int(implausible.sum())

        # ── Seasonality ──
        if profile.time_span_days and profile.time_span_days > 365:
            profile.seasonality_indicator = DatetimeProfiler._detect_seasonality(
                non_null
            )

        return profile

    @staticmethod
    def _detect_formats(series: pd.Series) -> tuple[list[str], bool]:
        """Detect datetime format patterns in strings."""
        format_patterns = [
            (r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "ISO 8601"),
            (r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "YYYY-MM-DD HH:MM:SS"),
            (r"\d{4}-\d{2}-\d{2}", "YYYY-MM-DD"),
            (r"\d{2}/\d{2}/\d{4}", "MM/DD/YYYY or DD/MM/YYYY"),
            (r"\d{2}-\d{2}-\d{4}", "MM-DD-YYYY or DD-MM-YYYY"),
            (r"\d{2}\.\d{2}\.\d{4}", "DD.MM.YYYY"),
            (r"\d{4}/\d{2}/\d{2}", "YYYY/MM/DD"),
            (r"\w+ \d{1,2}, \d{4}", "Month DD, YYYY"),
            (r"\d{1,2} \w+ \d{4}", "DD Month YYYY"),
        ]

        import re
        detected = set()
        sample = series.astype(str).head(200)

        for pattern, label in format_patterns:
            matches = sample.str.match(pattern, na=False).sum()
            if matches > len(sample) * 0.1:
                detected.add(label)

        formats = sorted(detected) if detected else ["auto-detected"]
        return formats, len(formats) > 1

    @staticmethod
    def _detect_frequency(
        sorted_series: pd.Series,
    ) -> tuple[Optional[FrequencyPattern], str]:
        """Detect the dominant frequency of time series data."""
        diffs = sorted_series.diff().dropna()
        if len(diffs) == 0:
            return None, "Insufficient data for frequency detection"

        # Get median diff in seconds
        median_diff = diffs.median().total_seconds()

        # Classify
        thresholds = [
            (3600, FrequencyPattern.HOURLY, "~1 hour"),
            (86400, FrequencyPattern.DAILY, "~1 day"),
            (604800, FrequencyPattern.WEEKLY, "~1 week"),
            (2592000, FrequencyPattern.MONTHLY, "~1 month"),
            (31536000, FrequencyPattern.YEARLY, "~1 year"),
        ]

        for threshold, pattern, label in thresholds:
            if median_diff < threshold * 1.5:
                # Check consistency: what percentage of diffs are near this frequency?
                tolerance = threshold * 0.3
                near_count = ((diffs.dt.total_seconds() - threshold).abs() < tolerance).sum()
                consistency = near_count / len(diffs) if len(diffs) > 0 else 0

                if consistency > 0.5:
                    return pattern, (
                        f"Median interval: {label}. "
                        f"{consistency:.0%} of intervals are within 30% of this frequency."
                    )
                elif consistency > 0.2:
                    return pattern, (
                        f"Loosely {label} frequency. "
                        f"Only {consistency:.0%} of intervals match closely."
                    )

        return FrequencyPattern.IRREGULAR, (
            f"Irregular frequency. Median interval: {median_diff:.0f}s "
            f"({median_diff/86400:.1f} days). No dominant pattern found."
        )

    @staticmethod
    def _detect_gaps(
        sorted_series: pd.Series,
        frequency: Optional[FrequencyPattern],
    ) -> tuple[list[dict[str, Any]], int]:
        """Detect significant gaps in the time series."""
        diffs = sorted_series.diff().dropna()
        if len(diffs) == 0:
            return [], 0

        median_diff = diffs.median()
        # A gap is defined as an interval > 3x the median
        threshold = median_diff * 3

        gap_mask = diffs > threshold
        gaps = []
        gap_indices = gap_mask[gap_mask].index

        for idx in gap_indices[:20]:  # Cap at 20 gaps
            pos = sorted_series.index.get_loc(idx)
            if pos > 0:
                gap_start = sorted_series.iloc[pos - 1]
                gap_end = sorted_series.iloc[pos]
                gap_duration = (gap_end - gap_start).total_seconds() / 86400

                gaps.append({
                    "start": str(gap_start),
                    "end": str(gap_end),
                    "duration_days": round(gap_duration, 2),
                })

        return gaps, int(gap_mask.sum())

    @staticmethod
    def _detect_timezone(series: pd.Series) -> tuple[str, bool]:
        """Detect timezone information."""
        if hasattr(series.dtype, "tz") and series.dtype.tz is not None:
            return str(series.dtype.tz), False

        # Check for mixed timezone offsets in string data
        return "none (naive)", False

    @staticmethod
    def _detect_seasonality(sorted_series: pd.Series) -> Optional[str]:
        """Preliminary seasonality detection using monthly distribution."""
        months = sorted_series.dt.month
        month_counts = months.value_counts().sort_index()

        if len(month_counts) < 4:
            return None

        # Check if distribution is significantly uneven
        expected = len(sorted_series) / 12
        chi_sq = sum((cnt - expected) ** 2 / expected for cnt in month_counts.values)

        if chi_sq > 21.0:  # χ² threshold for p<0.05 with 11 df
            peak_months = month_counts.nlargest(3).index.tolist()
            month_names = {
                1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
            }
            peak_names = [month_names.get(m, str(m)) for m in peak_months]
            return f"Potential seasonality detected. Peak months: {', '.join(peak_names)}"

        return "No significant seasonal pattern detected"
