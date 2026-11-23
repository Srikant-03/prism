"""
profiling/temporal_analyzer.py
==============================
Analyzes temporal dynamics and time-series patterns in datasets:
1. Detects timestamp regularity, sortedness, and gaps.
2. Detects duplicate timestamps (multiple events per timestamp).
3. Classifies whether rows represent event-level transactions vs regular time series.
4. Metric-aware aggregation when multiple rows exist per timestamp (SUM for volumes, MEAN for rates).
5. Evaluates classical decomposition assumptions (regular frequency, low missingness, >= 2 seasonal cycles).
6. When assumptions are met: performs classical STL decomposition with documented transformation.
7. Otherwise: provides robust Event-Level Temporal Trend Analysis (rolling moving averages, volatility bands).

Never silently applies decomposition to irregular raw transaction rows.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

from profiling.cross_column_models import (
    TemporalAnalysis,
    TemporalDiagnostic,
    EventTrendComponent,
    TimeSeriesComponent,
)
from profiling.profiling_models import DatasetProfile


# Keywords indicating volume/flow metrics (appropriate for SUM aggregation)
_VOLUME_KEYWORDS = (
    "revenue", "cost", "profit", "units_sold", "units", "sales", "prospect_count",
    "volume", "quantity", "count", "impressions", "clicks", "total", "amount", "spend"
)

# Keywords indicating rate/intensity/ordinal metrics (appropriate for MEAN aggregation)
_RATE_KEYWORDS = (
    "avg", "average", "rate", "pct", "percent", "percentage", "satisfaction",
    "score", "index", "ratio", "price", "margin", "days", "temperature", "duration"
)


def _get_metric_agg_function(col_name: str) -> str:
    """Determine whether a column should be aggregated via sum or mean."""
    c_lower = col_name.lower()
    for kw in _RATE_KEYWORDS:
        if kw in c_lower:
            return "mean"
    for kw in _VOLUME_KEYWORDS:
        if kw in c_lower:
            return "sum"
    return "mean"


class TemporalAnalyzer:
    """
    Detects higher-order temporal patterns and evaluates time-series regularity
    vs transaction-event structures.
    """

    _get_metric_agg_function = staticmethod(_get_metric_agg_function)

    def analyze(self, df: pd.DataFrame, dataset_profile: Any = None) -> TemporalAnalysis:
        if df.empty or len(df) < 5:
            return TemporalAnalysis(has_temporal_patterns=False)

        # 1. Identify primary datetime column
        time_keywords = ["year", "date", "month", "timestamp", "period", "time"]
        dt_cols: List[str] = []

        if dataset_profile is not None and hasattr(dataset_profile, "columns"):
            dt_cols = [
                c.name for c in dataset_profile.columns
                if (c.semantic_type == "datetime" or any(k in c.name.lower() for k in time_keywords))
                and c.name in df.columns
            ]
        elif isinstance(dataset_profile, dict):
            dt_cols = [
                col for col, st in dataset_profile.items()
                if (st == "datetime" or any(k in col.lower() for k in time_keywords))
                and col in df.columns
            ]
        else:
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]) or any(k in str(c).lower() for k in time_keywords):
                    dt_cols.append(c)

        if not dt_cols:
            return TemporalAnalysis(has_temporal_patterns=False)

        # Score datetime candidates:
        # - Explicit datetime semantic type or datetime64 dtype (highest weight)
        # - High unique timestamp cardinality (avoids choosing single integer year over full date)
        # - High parseability
        # - Lowest nulls
        def _score_candidate(c: str) -> tuple:
            is_explicit = False
            if dataset_profile is not None and hasattr(dataset_profile, "columns"):
                for col_prof in dataset_profile.columns:
                    if col_prof.name == c and col_prof.semantic_type == "datetime":
                        is_explicit = True
                        break
            elif isinstance(dataset_profile, dict):
                is_explicit = (dataset_profile.get(c) == "datetime")
            elif pd.api.types.is_datetime64_any_dtype(df[c]):
                is_explicit = True

            parsed = pd.to_datetime(df[c].dropna().iloc[:50], errors="coerce")
            valid_pct = float(parsed.notna().mean()) if len(parsed) > 0 else 0.0
            n_unique = int(df[c].nunique())
            null_count = int(df[c].isnull().sum())

            # Columns named 'date' or 'timestamp' have higher priority than single 'year'
            name_bonus = 2 if any(k in c.lower() for k in ("date", "timestamp")) else (1 if "time" in c.lower() else 0)

            return (1 if is_explicit else 0, name_bonus, 1 if n_unique >= 5 else 0, valid_pct, n_unique, -null_count)

        primary_dt = max(dt_cols, key=_score_candidate)

        if not primary_dt or df[primary_dt].notna().sum() < 5:
            return TemporalAnalysis(has_temporal_patterns=False)

        # Parse datetime series
        df_work = df.copy()
        raw_series = df_work[primary_dt].dropna()
        parsed_dt = pd.to_datetime(raw_series, errors="coerce")
        valid_mask = parsed_dt.notna()

        if valid_mask.sum() < 5:
            return TemporalAnalysis(has_temporal_patterns=False)

        df_work = df_work.loc[raw_series[valid_mask].index].copy()
        df_work[primary_dt] = parsed_dt[valid_mask]

        # 2. Timestamp Diagnostic
        total_records = len(df_work)
        unique_timestamps = int(df_work[primary_dt].nunique())
        duplicate_count = total_records - unique_timestamps
        has_duplicates = duplicate_count > 0
        is_monotonically_sorted = bool(df_work[primary_dt].is_monotonic_increasing)

        # Sort chronologically for all temporal analysis
        df_sorted = df_work.sort_values(by=primary_dt).reset_index(drop=True)

        # Calculate time diff distribution on unique dates
        unique_dates = pd.Series(df_sorted[primary_dt].unique()).sort_values()
        date_diffs = unique_dates.diff().dropna()
        diff_days = date_diffs.dt.total_seconds() / 86400.0

        # Regularity check
        is_regular = False
        sampling_summary = ""
        if not diff_days.empty:
            dominant_diff = diff_days.mode().iloc[0] if not diff_days.mode().empty else diff_days.median()
            match_ratio = float((diff_days == dominant_diff).mean())
            if not has_duplicates and match_ratio >= 0.95:
                is_regular = True
                sampling_summary = f"Regularly sampled with uniform interval of {dominant_diff:.1f} days."
            else:
                is_regular = False
                min_gap = float(diff_days.min())
                max_gap = float(diff_days.max())
                sampling_summary = (
                    f"Irregular intervals spanning {len(unique_dates)} unique dates "
                    f"(gaps range from {min_gap:.0f} to {max_gap:.0f} days; dominant interval: {dominant_diff:.0f} days)."
                )

        # Determine series type
        if has_duplicates or not is_regular:
            series_type = "event_transactions"
        else:
            series_type = "regular_time_series"

        # Continuous numeric candidate columns
        num_cols: List[str] = []
        if dataset_profile is not None and hasattr(dataset_profile, "columns"):
            num_cols = [
                c.name for c in dataset_profile.columns
                if c.semantic_type in ("numeric_continuous", "currency", "percentage", "numeric_discrete")
                and c.name in df_sorted.columns
            ]
        elif isinstance(dataset_profile, dict):
            num_cols = [
                col for col, st in dataset_profile.items()
                if st in ("numeric_continuous", "currency", "percentage", "numeric_discrete", "numeric")
                and col in df_sorted.columns
            ]
        else:
            num_cols = [
                c for c in df_sorted.columns
                if c != primary_dt and pd.api.types.is_numeric_dtype(df_sorted[c])
            ]

        # 3. Compute Event-Level Temporal Trend Analysis
        event_trends: Dict[str, EventTrendComponent] = {}
        for col in num_cols[:5]:
            try:
                sub = df_sorted[[primary_dt, col]].dropna()
                if len(sub) < 5:
                    continue

                ts_strings = sub[primary_dt].dt.strftime("%Y-%m-%d").tolist()
                vals = pd.to_numeric(sub[col], errors="coerce").fillna(0.0).tolist()

                # Rolling window trend (window size adaptive to sample size)
                window = min(14, max(5, len(vals) // 10))
                s_vals = pd.Series(vals)
                roll_mean = s_vals.rolling(window=window, min_periods=1, center=True).mean().tolist()
                roll_std = s_vals.rolling(window=window, min_periods=1, center=True).std().fillna(0.0).tolist()

                # Direction heuristic
                first_avg = float(np.mean(vals[:max(3, len(vals) // 4)]))
                last_avg = float(np.mean(vals[-max(3, len(vals) // 4):]))
                direction = "upward" if last_avg > first_avg * 1.05 else ("downward" if last_avg < first_avg * 0.95 else "stable")

                event_trends[col] = EventTrendComponent(
                    feature=col,
                    timestamps=ts_strings,
                    values=[round(float(v), 2) for v in vals],
                    rolling_mean=[round(float(v), 2) for v in roll_mean],
                    rolling_std=[round(float(v), 2) for v in roll_std],
                    aggregation_used=None,
                    explanation=(
                        f"Event-level temporal trend for '{col}' showing {direction} trajectory "
                        f"over {len(vals)} recorded transactions (window={window} rolling average)."
                    ),
                )
            except Exception:
                pass

        # 4. Evaluate Classical Decomposition Assumptions
        decompositions: Dict[str, TimeSeriesComponent] = {}
        detected_periodicities: List[str] = []
        decomposition_eligible = False
        ineligibility_reason: Optional[str] = None
        transformation_explanation: Optional[str] = None

        date_span_days = int((unique_dates.max() - unique_dates.min()).days + 1) if len(unique_dates) > 1 else 1
        missing_date_ratio = 1.0 - (len(unique_dates) / max(date_span_days, 1))

        if series_type == "event_transactions":
            # Gaps check: if over 25% of dates in span are missing, interpolating would fabricate data
            if missing_date_ratio > 0.25:
                decomposition_eligible = False
                ineligibility_reason = (
                    f"Dataset contains irregular event-level transactions across a {date_span_days}-day span "
                    f"with {missing_date_ratio * 100:.1f}% missing dates and {duplicate_count} duplicate timestamp records. "
                    f"Classical decomposition assumptions (regular sampling without severe gap imputation) are not satisfied. "
                    f"Presenting Event-Level Temporal Trend Analysis instead."
                )
                transformation_explanation = (
                    f"Raw data consists of {total_records} transaction rows ({duplicate_count} repeated dates). "
                    f"Classical decomposition was omitted to prevent artificial seasonal fitting on irregular gaps. "
                    f"Visualizing event-level temporal trends."
                )
            else:
                # Gaps are minor (< 25%), attempt aggregation and regularization
                decomposition_eligible, ineligibility_reason, decompositions, transformation_explanation = (
                    self._attempt_aggregated_decomposition(
                        df_sorted, primary_dt, num_cols, total_records, duplicate_count
                    )
                )
        else:
            # Regular time series
            decomposition_eligible, ineligibility_reason, decompositions, transformation_explanation = (
                self._attempt_regular_decomposition(df_sorted, primary_dt, num_cols)
            )

        diagnostic = TemporalDiagnostic(
            is_regular=is_regular,
            has_duplicates=has_duplicates,
            duplicate_count=duplicate_count,
            total_records=total_records,
            unique_timestamps=unique_timestamps,
            is_monotonically_sorted=is_monotonically_sorted,
            series_type=series_type,
            sampling_summary=sampling_summary,
            decomposition_eligible=decomposition_eligible,
            decomposition_ineligibility_reason=ineligibility_reason,
            transformation_explanation=transformation_explanation,
        )

        analysis_mode = "classical_decomposition" if decomposition_eligible and decompositions else "event_trend"

        return TemporalAnalysis(
            has_temporal_patterns=bool(event_trends or decompositions),
            primary_time_col=primary_dt,
            decompositions=decompositions,
            detected_periodicities=detected_periodicities,
            diagnostic=diagnostic,
            event_trends=event_trends,
            analysis_mode=analysis_mode,
            transformation_used=transformation_explanation,
        )

    def _attempt_aggregated_decomposition(
        self,
        df_sorted: pd.DataFrame,
        primary_dt: str,
        num_cols: List[str],
        total_records: int,
        duplicate_count: int,
    ) -> Tuple[bool, Optional[str], Dict[str, TimeSeriesComponent], str]:
        """Aggregate duplicate timestamps and run STL only if regular frequency holds."""
        decompositions = {}
        trans_notes = []

        for col in num_cols[:3]:
            agg_func = _get_metric_agg_function(col)
            # Group by date with metric-aware aggregation
            df_agg = df_sorted.groupby(df_sorted[primary_dt].dt.normalize())[col].agg(agg_func)
            if len(df_agg) < 14:
                continue

            # Resample daily and interpolate small gaps
            s_daily = df_agg.resample("D").asfreq()
            missing_pct = s_daily.isna().mean()
            if missing_pct > 0.25:
                continue

            s_filled = s_daily.interpolate(method="linear").bfill().ffill()
            period = 7
            if len(s_filled) >= 2 * period:
                try:
                    stl = STL(s_filled.values, period=period, robust=True)
                    res = stl.fit()
                    t = res.trend.tolist()
                    season = res.seasonal.tolist()
                    resid = res.resid.tolist()
                    idx = s_filled.index.strftime("%Y-%m-%d").tolist()

                    def clean(arr):
                        return [round(float(x), 2) if not np.isnan(x) else 0.0 for x in arr]

                    decompositions[col] = TimeSeriesComponent(
                        trend=clean(t),
                        seasonal=clean(season),
                        residual=clean(resid),
                        timestamps=idx,
                    )
                    trans_notes.append(f"'{col}' aggregated by date ({agg_func.upper()}) and regularized to daily")
                except Exception:
                    pass

        if decompositions:
            explanation = (
                f"Aggregated {total_records} rows ({duplicate_count} duplicates) to daily observations: "
                + "; ".join(trans_notes) + "."
            )
            return True, None, decompositions, explanation
        else:
            return False, "Aggregated series has insufficient points (requires at least 14 points for 2 seasonal periods) or excessive missingness for STL.", {}, ""

    def _attempt_regular_decomposition(
        self,
        df_sorted: pd.DataFrame,
        primary_dt: str,
        num_cols: List[str],
    ) -> Tuple[bool, Optional[str], Dict[str, TimeSeriesComponent], str]:
        """Decompose cleanly sampled regular time series."""
        decompositions = {}
        for col in num_cols[:3]:
            s = df_sorted.set_index(primary_dt)[col].dropna()
            if len(s) < 14:
                continue
            period = 7 if len(s) >= 14 else max(2, len(s) // 4)
            if len(s) >= 2 * period:
                try:
                    stl = STL(s.values, period=period, robust=True)
                    res = stl.fit()
                    idx = s.index.strftime("%Y-%m-%d").tolist()
                    def clean(arr):
                        return [round(float(x), 2) if not np.isnan(x) else 0.0 for x in arr]

                    decompositions[col] = TimeSeriesComponent(
                        trend=clean(res.trend.tolist()),
                        seasonal=clean(res.seasonal.tolist()),
                        residual=clean(res.resid.tolist()),
                        timestamps=idx,
                    )
                except Exception:
                    pass

        if decompositions:
            return True, None, decompositions, "Analyzed at regular frequency without aggregation."
        else:
            return False, "Insufficient observations for seasonal decomposition (minimum 2 periods required).", {}, ""
