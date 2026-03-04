"""
Outlier Handler — Multi-method outlier detection with distribution-aware
method selection.  Each outlier is contextualized with error likelihood.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
    OutlierReport, OutlierDetail, ColumnOutlierSummary, OutlierMethod,
)


# Business-rule patterns: column name → expected domain
_BUSINESS_RULES: list[dict[str, Any]] = [
    {"pattern": r"(?i)(age|years_old)", "min": 0, "max": 150, "label": "age"},
    {"pattern": r"(?i)(percent|pct|ratio)", "min": 0, "max": 100, "label": "percentage"},
    {"pattern": r"(?i)(price|cost|amount|salary|income|revenue|fee)", "min": 0, "max": None, "label": "monetary (non-negative)"},
    {"pattern": r"(?i)(count|quantity|qty|num_|number_of)", "min": 0, "max": None, "label": "count (non-negative)"},
    {"pattern": r"(?i)(weight|mass)", "min": 0, "max": None, "label": "weight (non-negative)"},
    {"pattern": r"(?i)(height|length|width|distance)", "min": 0, "max": None, "label": "dimension (non-negative)"},
    {"pattern": r"(?i)(temperature|temp)", "min": -100, "max": 60, "label": "temperature (°C)"},
    {"pattern": r"(?i)(latitude|lat)", "min": -90, "max": 90, "label": "latitude"},
    {"pattern": r"(?i)(longitude|lon|lng)", "min": -180, "max": 180, "label": "longitude"},
    {"pattern": r"(?i)(rating|score|grade)", "min": 0, "max": None, "label": "rating (non-negative)"},
    {"pattern": r"(?i)(probability|prob)", "min": 0, "max": 1, "label": "probability"},
]


class OutlierHandler:
    """Multi-method outlier detection engine."""

    def __init__(self, df: pd.DataFrame, file_id: str, profile: Any = None):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile
        self.numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], OutlierReport]:
        actions: list[CleaningAction] = []
        report = OutlierReport(columns_analyzed=len(self.numeric_cols))

        if not self.numeric_cols:
            return actions, report

        # Per-column detection
        for col in self.numeric_cols:
            series = self.df[col].dropna()
            if len(series) < 10:
                continue

            col_outliers: list[OutlierDetail] = []
            methods_used: list[OutlierMethod] = []
            stats = self._compute_stats(series)

            # Select methods based on distribution
            skewness = abs(stats.get("skewness", 0))
            is_normal = skewness < 1.0

            # 1. IQR (always)
            iqr_outliers = self._iqr_detect(col, series, stats)
            col_outliers.extend(iqr_outliers)
            if iqr_outliers:
                methods_used.append(OutlierMethod.IQR_MILD)

            # 2. Z-score (for normal)
            if is_normal:
                z_outliers = self._zscore_detect(col, series, stats)
                col_outliers.extend(z_outliers)
                if z_outliers:
                    methods_used.append(OutlierMethod.ZSCORE)

            # 3. Modified Z-score / MAD (for non-normal)
            if not is_normal:
                mad_outliers = self._modified_zscore_detect(col, series)
                col_outliers.extend(mad_outliers)
                if mad_outliers:
                    methods_used.append(OutlierMethod.MODIFIED_ZSCORE)

            # 4. Time-series envelope (for temporal data)
            if self._is_temporal_column(col):
                ts_outliers = self._timeseries_envelope(col, series)
                col_outliers.extend(ts_outliers)
                if ts_outliers:
                    methods_used.append(OutlierMethod.TIMESERIES_ENVELOPE)

            # 5. Business rule violations
            br_outliers, br_violations = self._business_rule_check(col, series)
            col_outliers.extend(br_outliers)
            if br_outliers:
                methods_used.append(OutlierMethod.BUSINESS_RULE)
                report.business_rule_violations.extend(br_violations)

            # Deduplicate outliers by row index (same row flagged by multiple methods)
            col_outliers = self._merge_outlier_details(col_outliers)

            # Compute error likelihood
            for od in col_outliers:
                od.error_likelihood = self._estimate_error_likelihood(od, stats)
                od.is_likely_error = od.error_likelihood > 0.6
                od.error_reasoning = self._explain_error_likelihood(od, stats)

            if col_outliers:
                total_outs = len(col_outliers)
                likely_errors = sum(1 for o in col_outliers if o.is_likely_error)
                treatment, reason = self._recommend_treatment(col, col_outliers, stats)

                summary = ColumnOutlierSummary(
                    column=col,
                    total_outliers=total_outs,
                    outlier_pct=round(total_outs / len(series) * 100, 2),
                    methods_used=methods_used,
                    likely_errors=likely_errors,
                    sample_outliers=col_outliers[:10],
                    distribution_stats=stats,
                    recommended_treatment=treatment,
                    treatment_reasoning=reason,
                )
                report.column_summaries.append(summary)
                report.total_outlier_values += total_outs

                # Build CleaningAction
                action = self._build_action(col, summary)
                actions.append(action)

        # Multivariate outlier detection (Isolation Forest + DBSCAN + LOF)
        if len(self.numeric_cols) >= 3:
            mv_actions, mv_outliers = self._multivariate_detect()
            actions.extend(mv_actions)
            report.multivariate_outliers = mv_outliers

        # Count unique outlier rows
        outlier_rows = set()
        for cs in report.column_summaries:
            for od in cs.sample_outliers:
                outlier_rows.add(od.row_index)
        report.total_outlier_rows = len(outlier_rows)

        return actions, report

    # ── IQR Detection ────────────────────────────────────────────────
    def _iqr_detect(self, col: str, series: pd.Series, stats: dict) -> list[OutlierDetail]:
        outliers: list[OutlierDetail] = []
        q1, q3 = stats["q1"], stats["q3"]
        iqr = q3 - q1
        if iqr == 0:
            return outliers

        lower_mild = q1 - 1.5 * iqr
        upper_mild = q3 + 1.5 * iqr
        lower_extreme = q1 - 3.0 * iqr
        upper_extreme = q3 + 3.0 * iqr

        for idx in series.index:
            val = float(series[idx])
            if val < lower_extreme or val > upper_extreme:
                multiple = abs(val - stats["median"]) / iqr if iqr > 0 else 0
                outliers.append(OutlierDetail(
                    column=col, row_index=int(idx), value=val,
                    detection_methods=[OutlierMethod.IQR_EXTREME],
                    iqr_multiple=round(multiple, 2),
                ))
            elif val < lower_mild or val > upper_mild:
                multiple = abs(val - stats["median"]) / iqr if iqr > 0 else 0
                outliers.append(OutlierDetail(
                    column=col, row_index=int(idx), value=val,
                    detection_methods=[OutlierMethod.IQR_MILD],
                    iqr_multiple=round(multiple, 2),
                ))

        return outliers[:200]  # cap for performance

    # ── Z-Score Detection ────────────────────────────────────────────
    def _zscore_detect(self, col: str, series: pd.Series, stats: dict) -> list[OutlierDetail]:
        outliers: list[OutlierDetail] = []
        mean, std = stats["mean"], stats["std"]
        if std == 0:
            return outliers

        z_scores = (series - mean) / std
        mask = z_scores.abs() > 3.0

        for idx in series[mask].index:
            val = float(series[idx])
            zs = float(z_scores[idx])
            outliers.append(OutlierDetail(
                column=col, row_index=int(idx), value=val,
                detection_methods=[OutlierMethod.ZSCORE],
                z_score=round(zs, 2),
            ))

        return outliers[:200]

    # ── Modified Z-Score (MAD) ───────────────────────────────────────
    def _modified_zscore_detect(self, col: str, series: pd.Series) -> list[OutlierDetail]:
        outliers: list[OutlierDetail] = []
        median = float(series.median())
        mad = float(np.median(np.abs(series - median)))
        if mad == 0:
            return outliers

        modified_z = 0.6745 * (series - median) / mad
        mask = modified_z.abs() > 3.5

        for idx in series[mask].index:
            val = float(series[idx])
            mz = float(modified_z[idx])
            outliers.append(OutlierDetail(
                column=col, row_index=int(idx), value=val,
                detection_methods=[OutlierMethod.MODIFIED_ZSCORE],
                modified_z_score=round(mz, 2),
            ))

        return outliers[:200]

    # ── Time-Series Envelope ─────────────────────────────────────────
    def _timeseries_envelope(self, col: str, series: pd.Series) -> list[OutlierDetail]:
        outliers: list[OutlierDetail] = []
        try:
            # Rolling IQR envelope
            window = max(7, len(series) // 20)
            rolling_median = series.rolling(window, center=True, min_periods=3).median()
            rolling_std = series.rolling(window, center=True, min_periods=3).std()

            if rolling_std is None or rolling_median is None:
                return outliers

            upper = rolling_median + 2.5 * rolling_std
            lower = rolling_median - 2.5 * rolling_std

            for idx in series.index:
                val = float(series[idx])
                u = upper.get(idx)
                l = lower.get(idx)
                if u is not None and l is not None and (val > u or val < l):
                    outliers.append(OutlierDetail(
                        column=col, row_index=int(idx), value=val,
                        detection_methods=[OutlierMethod.TIMESERIES_ENVELOPE],
                    ))
        except Exception:
            pass

        return outliers[:100]

    # ── Business Rule Violations ─────────────────────────────────────
    def _business_rule_check(self, col: str, series: pd.Series) -> tuple[list[OutlierDetail], list[dict]]:
        outliers: list[OutlierDetail] = []
        violations: list[dict] = []

        for rule in _BUSINESS_RULES:
            if not re.search(rule["pattern"], col):
                continue

            rule_min = rule.get("min")
            rule_max = rule.get("max")

            violating = pd.Series(False, index=series.index)
            reason_parts = []

            if rule_min is not None:
                below = series < rule_min
                violating |= below
                if below.any():
                    reason_parts.append(f"values below {rule_min}")

            if rule_max is not None:
                above = series > rule_max
                violating |= above
                if above.any():
                    reason_parts.append(f"values above {rule_max}")

            if violating.any():
                violations.append({
                    "column": col,
                    "rule": rule["label"],
                    "expected_range": f"[{rule_min}, {rule_max}]",
                    "violation_count": int(violating.sum()),
                })

                for idx in series[violating].head(50).index:
                    val = float(series[idx])
                    outliers.append(OutlierDetail(
                        column=col, row_index=int(idx), value=val,
                        detection_methods=[OutlierMethod.BUSINESS_RULE],
                        error_reasoning=f"Violates {rule['label']} domain: {', '.join(reason_parts)}",
                    ))
            break  # Only match first rule per column

        return outliers, violations

    # ── Multivariate Detection (sklearn) ─────────────────────────────
    def _multivariate_detect(self) -> tuple[list[CleaningAction], list[dict]]:
        actions: list[CleaningAction] = []
        mv_outliers: list[dict] = []

        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.neighbors import LocalOutlierFactor
            from sklearn.cluster import DBSCAN
            from sklearn.preprocessing import StandardScaler

            # Prepare data — sample for speed
            df_num = self.df[self.numeric_cols].dropna()
            if len(df_num) < 20:
                return actions, mv_outliers
            if len(df_num) > 10_000:
                df_num = df_num.sample(10_000, random_state=42)

            scaler = StandardScaler()
            X = scaler.fit_transform(df_num)

            # Isolation Forest
            iso = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
            iso_labels = iso.fit_predict(X)
            iso_outlier_idx = df_num.index[iso_labels == -1].tolist()

            # LOF
            lof = LocalOutlierFactor(n_neighbors=20, contamination=0.05)
            lof_labels = lof.fit_predict(X)
            lof_outlier_idx = df_num.index[lof_labels == -1].tolist()

            # DBSCAN
            db = DBSCAN(eps=2.0, min_samples=5)
            db_labels = db.fit_predict(X)
            db_outlier_idx = df_num.index[db_labels == -1].tolist()

            # Consensus: flagged by ≥2 methods
            from collections import Counter
            all_flagged: list[int] = iso_outlier_idx + lof_outlier_idx + db_outlier_idx
            counts = Counter(all_flagged)
            consensus_idx = [idx for idx, cnt in counts.items() if cnt >= 2]

            if consensus_idx:
                n_mv = len(consensus_idx)
                mv_pct = round(n_mv / len(df_num) * 100, 2)

                for idx in consensus_idx[:20]:
                    methods = []
                    if idx in iso_outlier_idx:
                        methods.append("isolation_forest")
                    if idx in lof_outlier_idx:
                        methods.append("lof")
                    if idx in db_outlier_idx:
                        methods.append("dbscan")
                    mv_outliers.append({
                        "row_index": int(idx),
                        "methods": methods,
                        "values": self.df.loc[idx, self.numeric_cols].to_dict(),
                    })

                action = CleaningAction(
                    category=ActionCategory.OUTLIERS,
                    action_type=ActionType.FLAG_OUTLIER,
                    confidence=ActionConfidence.JUDGMENT_CALL,
                    evidence=(
                        f"Multivariate analysis detected {n_mv} rows ({mv_pct}%) as outliers. "
                        f"Methods: Isolation Forest, LOF, DBSCAN — flagged rows confirmed by ≥2 methods."
                    ),
                    recommendation=f"Review {n_mv} multivariate outlier rows across {len(self.numeric_cols)} numeric columns.",
                    reasoning=(
                        "Multivariate outliers are points that look normal in any single dimension "
                        "but are unusual when all features are considered together. Consensus "
                        "across multiple algorithms increases confidence."
                    ),
                    target_columns=self.numeric_cols,
                    impact=ImpactEstimate(
                        rows_before=self.n_rows, rows_after=self.n_rows,
                        rows_affected=n_mv, rows_affected_pct=mv_pct,
                        description=f"{n_mv} rows flagged as multivariate outliers ({mv_pct}%).",
                    ),
                    options=[
                        UserOption(key="flag", label="Flag and Keep", is_default=True),
                        UserOption(key="remove", label="Remove Outlier Rows"),
                        UserOption(key="ignore", label="Leave Unchanged"),
                    ],
                    metadata={"consensus_count": n_mv, "sample_outliers": mv_outliers[:5]},
                )
                actions.append(action)

        except ImportError:
            pass  # sklearn not installed
        except Exception:
            pass

        return actions, mv_outliers

    # ── Helpers ───────────────────────────────────────────────────────
    def _compute_stats(self, series: pd.Series) -> dict:
        return {
            "mean": float(series.mean()),
            "median": float(series.median()),
            "std": float(series.std()),
            "min": float(series.min()),
            "max": float(series.max()),
            "q1": float(series.quantile(0.25)),
            "q3": float(series.quantile(0.75)),
            "skewness": float(series.skew()),
            "kurtosis": float(series.kurtosis()),
            "count": len(series),
        }

    def _merge_outlier_details(self, outliers: list[OutlierDetail]) -> list[OutlierDetail]:
        """Merge multiple detections of the same row into one detail."""
        by_row: dict[int, OutlierDetail] = {}
        for od in outliers:
            if od.row_index in by_row:
                existing = by_row[od.row_index]
                for m in od.detection_methods:
                    if m not in existing.detection_methods:
                        existing.detection_methods.append(m)
                if od.z_score is not None and existing.z_score is None:
                    existing.z_score = od.z_score
                if od.modified_z_score is not None and existing.modified_z_score is None:
                    existing.modified_z_score = od.modified_z_score
                if od.iqr_multiple is not None and existing.iqr_multiple is None:
                    existing.iqr_multiple = od.iqr_multiple
            else:
                by_row[od.row_index] = od
        return list(by_row.values())

    def _estimate_error_likelihood(self, od: OutlierDetail, stats: dict) -> float:
        """Estimate 0–1 probability that this outlier is a data entry error."""
        score = 0.0
        count = 0

        # Factor 1: deviation magnitude
        if od.z_score is not None:
            dev_score = min(abs(od.z_score) / 10, 1.0)
            score += dev_score
            count += 1
        if od.iqr_multiple is not None:
            dev_score = min(od.iqr_multiple / 10, 1.0)
            score += dev_score
            count += 1
        if od.modified_z_score is not None:
            dev_score = min(abs(od.modified_z_score) / 10, 1.0)
            score += dev_score
            count += 1

        # Factor 2: number of methods that flagged it
        method_score = min(len(od.detection_methods) / 4, 1.0)
        score += method_score
        count += 1

        # Factor 3: business rule violation is strong evidence
        if OutlierMethod.BUSINESS_RULE in od.detection_methods:
            score += 0.8
            count += 1

        # Factor 4: isolation (is the value far from any cluster?)
        data_range = stats["max"] - stats["min"]
        if data_range > 0:
            normalized_distance = abs(od.value - stats["median"]) / data_range
            score += min(normalized_distance * 2, 1.0)
            count += 1

        return round(score / max(count, 1), 3)

    def _explain_error_likelihood(self, od: OutlierDetail, stats: dict) -> str:
        """Generate human-readable error likelihood explanation."""
        parts: list[str] = []
        if od.z_score is not None:
            parts.append(f"{abs(od.z_score):.1f} standard deviations from mean")
        if od.iqr_multiple is not None:
            parts.append(f"{od.iqr_multiple:.1f}× IQR from median")
        if len(od.detection_methods) > 1:
            parts.append(f"flagged by {len(od.detection_methods)} independent methods")
        if OutlierMethod.BUSINESS_RULE in od.detection_methods:
            parts.append("violates expected domain range")
        if od.error_likelihood > 0.7:
            parts.append("high probability of data entry error")
        elif od.error_likelihood > 0.4:
            parts.append("moderate probability — could be legitimate extreme")
        else:
            parts.append("likely a legitimate extreme value")
        return "; ".join(parts)

    def _recommend_treatment(
        self, col: str, outliers: list[OutlierDetail], stats: dict
    ) -> tuple[ActionType, str]:
        """Recommend treatment based on outlier characteristics."""
        total = len(outliers)
        likely_errors = sum(1 for o in outliers if o.is_likely_error)
        error_ratio = likely_errors / total if total > 0 else 0

        has_business_violations = any(
            OutlierMethod.BUSINESS_RULE in o.detection_methods for o in outliers
        )

        # Business rule violations → cap at boundary
        if has_business_violations and error_ratio > 0.8:
            return ActionType.REPLACE_BOUNDARY, (
                "Most outliers violate business rules suggesting data entry errors. "
                "Replacing with boundary values corrects implausible data."
            )

        # High error ratio + few outliers → remove rows
        if error_ratio > 0.7 and total < self.n_rows * 0.02:
            return ActionType.REMOVE_OUTLIER_ROWS, (
                f"Only {total} rows affected ({total / self.n_rows * 100:.1f}%) and most appear "
                "to be errors. Removal has minimal impact on dataset size."
            )

        # Moderate outliers → winsorize
        if error_ratio > 0.3:
            return ActionType.WINSORIZE, (
                "Mix of likely errors and extreme values. Winsorizing caps at 1st/99th "
                "percentile — preserving the observation while limiting distortion."
            )

        # Highly skewed data → log transform
        if stats.get("skewness", 0) > 2.0 and stats.get("min", 0) > 0:
            return ActionType.LOG_TRANSFORM, (
                "Highly right-skewed distribution with all positive values. Log transform "
                "compresses the range and reduces outlier leverage."
            )

        # Default → flag and keep
        return ActionType.FLAG_OUTLIER, (
            "Outliers appear to be legitimate extreme values. Flagging preserves them "
            "while enabling downstream filtering."
        )

    def _build_action(self, col: str, summary: ColumnOutlierSummary) -> CleaningAction:
        """Build a CleaningAction from a column outlier summary."""
        stats = summary.distribution_stats
        methods_str = ", ".join(m.value for m in summary.methods_used)

        # Preview: show some outlier rows
        preview_indices = [od.row_index for od in summary.sample_outliers[:5]]
        before_rows = []
        for idx in preview_indices:
            if idx < len(self.df):
                row_data = {col: str(self.df.iloc[idx][col]) if col in self.df.columns else ""}
                before_rows.append(row_data)

        options = [
            UserOption(key="flag", label="Flag and Keep (add _is_outlier column)", is_default=True),
            UserOption(key="winsorize", label="Winsorize at 1st/99th percentile"),
            UserOption(key="cap_iqr", label="Cap at IQR fence"),
            UserOption(key="remove", label="Remove outlier rows"),
            UserOption(key="log", label="Log transform column"),
            UserOption(key="ignore", label="Leave unchanged"),
        ]

        return CleaningAction(
            category=ActionCategory.OUTLIERS,
            action_type=summary.recommended_treatment,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Column '{col}': {summary.total_outliers} outliers detected ({summary.outlier_pct}%) "
                f"using [{methods_str}]. {summary.likely_errors} appear to be data entry errors."
            ),
            recommendation=(
                f"Apply {summary.recommended_treatment.value.replace('_', ' ')} to '{col}'."
            ),
            reasoning=summary.treatment_reasoning,
            target_columns=[col],
            preview=PreviewSample(
                before=before_rows,
                columns_before=[col],
                columns_after=[col],
            ),
            impact=ImpactEstimate(
                rows_before=self.n_rows,
                rows_after=self.n_rows - (summary.total_outliers if summary.recommended_treatment == ActionType.REMOVE_OUTLIER_ROWS else 0),
                rows_affected=summary.total_outliers,
                rows_affected_pct=summary.outlier_pct,
                description=(
                    f"{summary.total_outliers} outlier values in '{col}' "
                    f"({summary.likely_errors} likely errors)."
                ),
            ),
            options=options,
            metadata={
                "total_outliers": summary.total_outliers,
                "likely_errors": summary.likely_errors,
                "methods": [m.value for m in summary.methods_used],
                "stats": stats,
            },
        )

    def _is_temporal_column(self, col: str) -> bool:
        """Check if column has temporal context."""
        if self.profile and hasattr(self.profile, "temporal_columns"):
            return col in self.profile.temporal_columns
        return False
