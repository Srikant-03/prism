"""
Datetime Handler — Feature engineering for datetime columns.
Extracts temporal components, derives flags, computes elapsed time,
pairwise deltas, and detects time-series patterns.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
)


class DatetimeHandler:
    """Datetime feature engineering engine."""

    def __init__(
        self, df: pd.DataFrame, file_id: str,
        holidays_country: str = "US",
        profile: Any = None,
    ):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.holidays_country = holidays_country
        self.profile = profile

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], dict]:
        actions: list[CleaningAction] = []
        report: dict[str, Any] = {
            "datetime_columns": [],
            "time_series_candidates": [],
            "pairwise_deltas": [],
        }

        dt_cols = self._find_datetime_columns()
        report["datetime_columns"] = dt_cols

        for col in dt_cols:
            series = self._to_datetime(col)
            if series is None:
                continue

            # 1. Component extraction
            components, features_count = self._analyze_components(col, series)
            if features_count > 0:
                actions.append(self._build_extract_action(col, components, features_count))

            # 2. Derived flags
            flags = self._analyze_flags(col, series)
            if flags:
                actions.append(self._build_flags_action(col, flags))

            # 3. Elapsed time features
            actions.append(self._build_elapsed_action(col, series))

        # 4. Pairwise time deltas
        if len(dt_cols) >= 2:
            delta_actions = self._analyze_pairwise_deltas(dt_cols)
            actions.extend(delta_actions)
            report["pairwise_deltas"] = [
                {"col_a": a.metadata.get("col_a"), "col_b": a.metadata.get("col_b")}
                for a in delta_actions
            ]

        # 5. Time series detection
        for col in dt_cols:
            ts_info = self._detect_time_series(col)
            if ts_info:
                report["time_series_candidates"].append(ts_info)
                actions.append(self._build_timeseries_action(col, ts_info))

        return actions, report

    # ── Identify datetime columns ─────────────────────────────────────
    def _find_datetime_columns(self) -> list[str]:
        dt_cols: list[str] = []
        for col in self.df.columns:
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                dt_cols.append(col)
                continue
            # Try to parse string columns as dates
            if self.df[col].dtype == object:
                sample = self.df[col].dropna().head(20)
                try:
                    parsed = pd.to_datetime(sample, errors="coerce")
                    if parsed.notna().mean() > 0.8:
                        dt_cols.append(col)
                except Exception:
                    pass
        return dt_cols

    def _to_datetime(self, col: str) -> Optional[pd.Series]:
        """Convert column to datetime if needed."""
        if pd.api.types.is_datetime64_any_dtype(self.df[col]):
            return self.df[col]
        try:
            return pd.to_datetime(self.df[col], errors="coerce")
        except Exception:
            return None

    # ── Component analysis ────────────────────────────────────────────
    def _analyze_components(self, col: str, series: pd.Series) -> tuple[list[str], int]:
        """Determine which datetime components have meaningful variance."""
        components: list[str] = []
        s = series.dropna()
        if len(s) < 2:
            return components, 0

        checks = [
            ("year", s.dt.year),
            ("month", s.dt.month),
            ("quarter", s.dt.quarter),
            ("week", s.dt.isocalendar().week.astype(int) if hasattr(s.dt, 'isocalendar') else s.dt.week),
            ("day", s.dt.day),
            ("dayofweek", s.dt.dayofweek),
            ("hour", s.dt.hour),
            ("minute", s.dt.minute),
            ("second", s.dt.second),
        ]

        for name, values in checks:
            try:
                if values.nunique() > 1:
                    components.append(name)
            except Exception:
                pass

        # Also add day_name and month_name if month/day have variance
        if "month" in components:
            components.append("month_name")
        if "dayofweek" in components:
            components.append("day_name")

        return components, len(components)

    # ── Flag analysis ─────────────────────────────────────────────────
    def _analyze_flags(self, col: str, series: pd.Series) -> list[str]:
        """Determine which derived flags are meaningful."""
        flags: list[str] = []
        s = series.dropna()
        if len(s) < 2:
            return flags

        # Check if data spans weekdays and weekends
        dow = s.dt.dayofweek
        has_weekend = (dow >= 5).any()
        has_weekday = (dow < 5).any()
        if has_weekend and has_weekday:
            flags.append("is_weekend")

        # Month boundary flags (only if data spans multiple months)
        if s.dt.month.nunique() > 1:
            flags.extend(["is_month_start", "is_month_end"])

        # Quarter boundary flags (only if data spans multiple quarters)
        if s.dt.quarter.nunique() > 1:
            flags.extend(["is_quarter_start", "is_quarter_end"])

        # Year boundary flags (only if data spans multiple years)
        if s.dt.year.nunique() > 1:
            flags.extend(["is_year_start", "is_year_end"])

        return flags

    # ── Pairwise deltas ───────────────────────────────────────────────
    def _analyze_pairwise_deltas(self, dt_cols: list[str]) -> list[CleaningAction]:
        actions: list[CleaningAction] = []

        for i in range(len(dt_cols)):
            for j in range(i + 1, len(dt_cols)):
                col_a, col_b = dt_cols[i], dt_cols[j]
                sa = self._to_datetime(col_a)
                sb = self._to_datetime(col_b)
                if sa is None or sb is None:
                    continue

                # Check if delta is meaningful (not all same)
                delta = (sb - sa).dt.total_seconds()
                if delta.dropna().nunique() < 2:
                    continue

                mean_delta = delta.mean()
                unit = "seconds"
                if abs(mean_delta) > 86400:
                    mean_delta /= 86400
                    unit = "days"
                elif abs(mean_delta) > 3600:
                    mean_delta /= 3600
                    unit = "hours"
                elif abs(mean_delta) > 60:
                    mean_delta /= 60
                    unit = "minutes"

                delta_name = f"{col_b}_minus_{col_a}"

                actions.append(CleaningAction(
                    category=ActionCategory.DATETIME_ENGINEERING,
                    action_type=ActionType.COMPUTE_TIME_DELTAS,
                    confidence=ActionConfidence.JUDGMENT_CALL,
                    evidence=(
                        f"Two datetime columns '{col_a}' and '{col_b}' found. "
                        f"Mean difference: {mean_delta:.1f} {unit}."
                    ),
                    recommendation=f"Compute time delta '{delta_name}' in {unit}.",
                    reasoning=(
                        "Pairwise time deltas between datetime columns often capture "
                        "important business durations (e.g., processing time, age, wait time)."
                    ),
                    target_columns=[col_a, col_b],
                    impact=ImpactEstimate(
                        rows_before=self.n_rows, rows_after=self.n_rows,
                        columns_before=self.n_cols, columns_after=self.n_cols + 1,
                        columns_affected=1,
                        description=f"Adds column '{delta_name}' ({unit}).",
                    ),
                    options=[
                        UserOption(key="days", label="Delta in Days", is_default=(unit == "days")),
                        UserOption(key="hours", label="Delta in Hours", is_default=(unit == "hours")),
                        UserOption(key="minutes", label="Delta in Minutes", is_default=(unit == "minutes")),
                        UserOption(key="seconds", label="Delta in Seconds", is_default=(unit == "seconds")),
                        UserOption(key="skip", label="Skip"),
                    ],
                    metadata={"col_a": col_a, "col_b": col_b, "unit": unit},
                ))

        return actions

    # ── Time series detection ─────────────────────────────────────────
    def _detect_time_series(self, col: str) -> Optional[dict]:
        """Check if dataset qualifies as a time series."""
        series = self._to_datetime(col)
        if series is None:
            return None

        s = series.dropna().sort_values()
        if len(s) < 20:
            return None

        # Check if datetime is monotonic or mostly sorted
        is_sorted = s.is_monotonic_increasing
        sort_ratio = (s.diff().dropna() > pd.Timedelta(0)).mean()

        if sort_ratio < 0.8:
            return None

        # Check for regular intervals
        diffs = s.diff().dropna().dt.total_seconds()
        median_diff = float(diffs.median())
        std_diff = float(diffs.std())
        cv = std_diff / median_diff if median_diff > 0 else float("inf")

        # Determine granularity
        if median_diff < 2:
            granularity = "sub-second"
        elif median_diff < 120:
            granularity = "minutes"
        elif median_diff < 7200:
            granularity = "hours"
        elif median_diff < 172800:
            granularity = "daily"
        elif median_diff < 1209600:
            granularity = "weekly"
        else:
            granularity = "monthly+"

        # Count numeric columns (potential time series values)
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()

        if len(numeric_cols) < 1:
            return None

        return {
            "column": col,
            "is_sorted": is_sorted,
            "sort_ratio": round(sort_ratio, 3),
            "granularity": granularity,
            "regularity_cv": round(cv, 3),
            "is_regular": cv < 0.1,
            "numeric_columns": numeric_cols,
            "n_observations": len(s),
        }

    # ── Action builders ───────────────────────────────────────────────
    def _build_extract_action(self, col: str, components: list[str], n: int) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATETIME_ENGINEERING,
            action_type=ActionType.EXTRACT_DATETIME,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Datetime column '{col}' has {n} components with meaningful variance: "
                f"{', '.join(components)}."
            ),
            recommendation=f"Extract {n} temporal features from '{col}'.",
            reasoning=(
                "Only components with actual variance in the data are extracted. "
                "Constant components (e.g., all same year) are excluded to avoid "
                "zero-variance features."
            ),
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols + n,
                columns_affected=n,
                description=f"Adds {n} temporal feature columns from '{col}'.",
            ),
            options=[
                UserOption(key="all", label=f"Extract All {n} Components", is_default=True),
                UserOption(key="date_only", label="Date Components Only (year, month, day, quarter)"),
                UserOption(key="time_only", label="Time Components Only (hour, minute, second)"),
                UserOption(key="skip", label="Skip"),
            ],
            metadata={"components": components},
        )

    def _build_flags_action(self, col: str, flags: list[str]) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.DATETIME_ENGINEERING,
            action_type=ActionType.DERIVE_DATETIME_FLAGS,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=f"Datetime column '{col}' spans sufficient range to derive {len(flags)} flags: {', '.join(flags)}.",
            recommendation=f"Add {len(flags)} derived boolean flags from '{col}'.",
            reasoning="Boolean flags enable partitioning by business-relevant periods.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols + len(flags),
                columns_affected=len(flags),
                description=f"Adds {len(flags)} boolean flag columns.",
            ),
            options=[
                UserOption(key="all", label=f"All {len(flags)} Flags", is_default=True),
                UserOption(key="skip", label="Skip"),
            ],
            metadata={"flags": flags},
        )

    def _build_elapsed_action(self, col: str, series: pd.Series) -> CleaningAction:
        s = series.dropna()
        min_dt = s.min()
        max_dt = s.max()
        span = (max_dt - min_dt).days

        return CleaningAction(
            category=ActionCategory.DATETIME_ENGINEERING,
            action_type=ActionType.COMPUTE_ELAPSED_TIME,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=f"Datetime column '{col}' spans {span} days (from {min_dt.date()} to {max_dt.date()}).",
            recommendation=f"Compute elapsed time since minimum value in '{col}'.",
            reasoning="Elapsed time features capture temporal distance and aging patterns.",
            target_columns=[col],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols + 1,
                columns_affected=1,
                description=f"Adds '{col}_elapsed_days' column.",
            ),
            options=[
                UserOption(key="days", label="Elapsed in Days", is_default=True),
                UserOption(key="hours", label="Elapsed in Hours"),
                UserOption(key="skip", label="Skip"),
            ],
            metadata={"min_date": str(min_dt), "max_date": str(max_dt), "span_days": span},
        )

    def _build_timeseries_action(self, col: str, ts_info: dict) -> CleaningAction:
        n = len(ts_info["numeric_columns"])
        return CleaningAction(
            category=ActionCategory.DATETIME_ENGINEERING,
            action_type=ActionType.TIME_SERIES_FEATURES,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Dataset qualifies as time series: '{col}' is {ts_info['granularity']} granularity, "
                f"{'regular' if ts_info['is_regular'] else 'irregular'} intervals, "
                f"{ts_info['n_observations']} observations, {n} numeric columns."
            ),
            recommendation=f"Generate time-series features (lags, rolling stats) for {n} numeric columns.",
            reasoning=(
                "Time-series features capture temporal dependencies. "
                "Rolling statistics smooth noise, lag features capture autocorrelation, "
                "and lead features can serve as forecasting targets."
            ),
            target_columns=[col] + ts_info["numeric_columns"],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols + n * 6,
                columns_affected=n * 6,
                description=f"Adds ~{n * 6} time-series feature columns (3 lags + 3 rolling stats per numeric col).",
            ),
            options=[
                UserOption(key="full", label="Full (lags + rolling + lead)", is_default=True),
                UserOption(key="lags_only", label="Lag Features Only"),
                UserOption(key="rolling_only", label="Rolling Statistics Only"),
                UserOption(key="resample", label="Resample to Regular Intervals"),
                UserOption(key="skip", label="Skip"),
            ],
            metadata=ts_info,
        )

    # ── Static execution methods ──────────────────────────────────────

    @staticmethod
    def extract_components(df: pd.DataFrame, col: str, components: list[str]) -> pd.DataFrame:
        """Extract datetime components as new columns."""
        s = pd.to_datetime(df[col], errors="coerce")
        for comp in components:
            if comp == "year":
                df[f"{col}_year"] = s.dt.year
            elif comp == "month":
                df[f"{col}_month"] = s.dt.month
            elif comp == "month_name":
                df[f"{col}_month_name"] = s.dt.month_name()
            elif comp == "quarter":
                df[f"{col}_quarter"] = s.dt.quarter
            elif comp == "week":
                try:
                    df[f"{col}_week"] = s.dt.isocalendar().week.astype(int)
                except Exception:
                    df[f"{col}_week"] = s.dt.week
            elif comp == "day":
                df[f"{col}_day"] = s.dt.day
            elif comp == "dayofweek":
                df[f"{col}_dayofweek"] = s.dt.dayofweek
            elif comp == "day_name":
                df[f"{col}_day_name"] = s.dt.day_name()
            elif comp == "hour":
                df[f"{col}_hour"] = s.dt.hour
            elif comp == "minute":
                df[f"{col}_minute"] = s.dt.minute
            elif comp == "second":
                df[f"{col}_second"] = s.dt.second
        return df

    @staticmethod
    def derive_flags(df: pd.DataFrame, col: str, flags: list[str]) -> pd.DataFrame:
        """Derive boolean flag columns."""
        s = pd.to_datetime(df[col], errors="coerce")
        for flag in flags:
            if flag == "is_weekend":
                df[f"{col}_is_weekend"] = (s.dt.dayofweek >= 5).astype(int)
            elif flag == "is_month_start":
                df[f"{col}_is_month_start"] = s.dt.is_month_start.astype(int)
            elif flag == "is_month_end":
                df[f"{col}_is_month_end"] = s.dt.is_month_end.astype(int)
            elif flag == "is_quarter_start":
                df[f"{col}_is_quarter_start"] = s.dt.is_quarter_start.astype(int)
            elif flag == "is_quarter_end":
                df[f"{col}_is_quarter_end"] = s.dt.is_quarter_end.astype(int)
            elif flag == "is_year_start":
                df[f"{col}_is_year_start"] = s.dt.is_year_start.astype(int)
            elif flag == "is_year_end":
                df[f"{col}_is_year_end"] = s.dt.is_year_end.astype(int)
        return df

    @staticmethod
    def compute_elapsed(df: pd.DataFrame, col: str, unit: str = "days") -> pd.DataFrame:
        """Compute elapsed time since min datetime."""
        s = pd.to_datetime(df[col], errors="coerce")
        min_dt = s.min()
        delta = s - min_dt

        if unit == "hours":
            df[f"{col}_elapsed_hours"] = delta.dt.total_seconds() / 3600
        elif unit == "minutes":
            df[f"{col}_elapsed_minutes"] = delta.dt.total_seconds() / 60
        else:
            df[f"{col}_elapsed_days"] = delta.dt.total_seconds() / 86400

        return df

    @staticmethod
    def compute_time_delta(df: pd.DataFrame, col_a: str, col_b: str, unit: str = "days") -> pd.DataFrame:
        """Compute time delta between two datetime columns."""
        sa = pd.to_datetime(df[col_a], errors="coerce")
        sb = pd.to_datetime(df[col_b], errors="coerce")
        delta = (sb - sa).dt.total_seconds()

        divisor = {"days": 86400, "hours": 3600, "minutes": 60, "seconds": 1}.get(unit, 86400)
        col_name = f"{col_b}_minus_{col_a}_{unit}"
        df[col_name] = delta / divisor
        return df

    @staticmethod
    def generate_ts_features(
        df: pd.DataFrame, dt_col: str, numeric_cols: list[str],
        mode: str = "full",
    ) -> pd.DataFrame:
        """Generate time series features (lags, rolling stats)."""
        df = df.sort_values(dt_col).reset_index(drop=True)

        for col in numeric_cols:
            if col not in df.columns:
                continue

            if mode in ("full", "lags_only"):
                for lag in [1, 3, 7]:
                    df[f"{col}_lag{lag}"] = df[col].shift(lag)

            if mode in ("full", "rolling_only"):
                for window in [7, 14, 30]:
                    if len(df) > window:
                        df[f"{col}_roll_mean_{window}"] = df[col].rolling(window, min_periods=1).mean()

            if mode == "full":
                # Lead features
                df[f"{col}_lead1"] = df[col].shift(-1)

        return df
