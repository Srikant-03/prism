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
    OutlierClassification,
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

    def __init__(
        self,
        df: pd.DataFrame,
        file_id: str,
        profile: Any = None,
        target_column: Optional[str] = None,
    ):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.profile = profile
        self.target_column = target_column

        if not self.target_column:
            try:
                from state import get_target_state
                ts = get_target_state(file_id)
                if ts and ts.get("target_column"):
                    self.target_column = ts.get("target_column")
            except Exception:
                pass

        if not self.target_column and self.profile:
            self.target_column = getattr(self.profile, "target_column", None)

        self.numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        self.predictor_numeric_cols = [
            c for c in self.numeric_cols if c != self.target_column
        ]

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

            # LEVEL 2: Within-group / contextual outlier evaluation
            grouping_dim, grouping_cols = self._find_meaningful_grouping_dimension(col, series)
            subgroup_normal_count = 0
            within_group_outliers_count = 0
            if grouping_dim and grouping_cols:
                subgroup_normal_count, within_group_outliers_count = self._evaluate_group_outliers(
                    col, series, col_outliers, grouping_dim, grouping_cols
                )

            # Classify outliers into separated concepts (statistical, contextual, quality, confirmed invalid)
            for od in col_outliers:
                self._classify_outlier(od, stats)

            if col_outliers:
                total_outs = len(col_outliers)
                stat_count = sum(1 for o in col_outliers if o.classification == OutlierClassification.STATISTICAL_OUTLIER)
                context_count = sum(1 for o in col_outliers if o.classification == OutlierClassification.CONTEXTUAL_OUTLIER)
                qual_count = sum(1 for o in col_outliers if o.classification == OutlierClassification.POTENTIAL_DATA_QUALITY)
                invalid_count = sum(1 for o in col_outliers if o.classification == OutlierClassification.CONFIRMED_INVALID)
                likely_errors = invalid_count  # Strictly evidence-based confirmed invalid values
                treatment, reason = self._recommend_treatment(
                    col, col_outliers, stats,
                    grouping_dimension=grouping_dim,
                    subgroup_normal_count=subgroup_normal_count
                )

                summary = ColumnOutlierSummary(
                    column=col,
                    total_outliers=total_outs,
                    outlier_pct=round(total_outs / len(series) * 100, 2),
                    methods_used=methods_used,
                    likely_errors=likely_errors,
                    statistical_outliers_count=stat_count,
                    contextual_outliers_count=context_count,
                    potential_quality_issues_count=qual_count,
                    confirmed_invalid_count=invalid_count,
                    grouping_dimension=grouping_dim,
                    subgroup_normal_count=subgroup_normal_count,
                    within_group_outliers_count=within_group_outliers_count,
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

        # Multivariate outlier detection on predictor features X (Isolation Forest + DBSCAN + LOF)
        if len(self.predictor_numeric_cols) >= 3:
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

        for i in range(len(series)):
            val = float(series.iloc[i])
            idx_label = series.index[i]
            if val < lower_extreme or val > upper_extreme:
                multiple = abs(val - stats["median"]) / iqr if iqr > 0 else 0
                outliers.append(OutlierDetail(
                    column=col, row_index=int(idx_label), value=val,
                    detection_methods=[OutlierMethod.IQR_EXTREME],
                    iqr_multiple=round(multiple, 2),
                ))
            elif val < lower_mild or val > upper_mild:
                multiple = abs(val - stats["median"]) / iqr if iqr > 0 else 0
                outliers.append(OutlierDetail(
                    column=col, row_index=int(idx_label), value=val,
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

        outlier_positions = np.where(mask)[0]
        for i in outlier_positions:
            val = float(series.iloc[i])
            zs = float(z_scores.iloc[i])
            idx_label = series.index[i]
            outliers.append(OutlierDetail(
                column=col, row_index=int(idx_label), value=val,
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

        outlier_positions = np.where(mask)[0]
        for i in outlier_positions:
            val = float(series.iloc[i])
            mz = float(modified_z.iloc[i])
            idx_label = series.index[i]
            outliers.append(OutlierDetail(
                column=col, row_index=int(idx_label), value=val,
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

            for i in range(len(series)):
                val = float(series.iloc[i])
                idx_label = series.index[i]
                u = upper.iloc[i]
                l = lower.iloc[i]
                if pd.notna(u) and pd.notna(l) and (val > u or val < l):
                    outliers.append(OutlierDetail(
                        column=col, row_index=int(idx_label), value=val,
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

                outlier_positions = np.where(violating)[0][:50]
                for i in outlier_positions:
                    val = float(series.iloc[i])
                    idx_label = series.index[i]
                    outliers.append(OutlierDetail(
                        column=col, row_index=int(idx_label), value=val,
                        detection_methods=[OutlierMethod.BUSINESS_RULE],
                        error_reasoning=f"Violates {rule['label']} domain: {', '.join(reason_parts)}",
                    ))
            break  # Only match first rule per column

        return outliers, violations

    # ── Multivariate Detection (sklearn) ─────────────────────────────
    def _multivariate_detect(self) -> tuple[list[CleaningAction], list[dict]]:
        assert self.target_column is None or self.target_column not in self.predictor_numeric_cols, (
            f"Target '{self.target_column}' must not enter predictor multivariate outlier detection!"
        )
        actions: list[CleaningAction] = []
        mv_outliers: list[dict] = []

        if len(self.predictor_numeric_cols) < 3:
            return actions, mv_outliers

        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.neighbors import LocalOutlierFactor
            from sklearn.cluster import DBSCAN
            from sklearn.preprocessing import StandardScaler

            # Prepare predictor feature data -- strictly excluding target y
            df_num = self.df[self.predictor_numeric_cols].dropna()
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

            # Consensus: flagged by >=2 methods
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
                        "values": self.df.loc[idx, self.predictor_numeric_cols].to_dict(),
                    })

                action = CleaningAction(
                    category=ActionCategory.OUTLIERS,
                    action_type=ActionType.FLAG_OUTLIER,
                    confidence=ActionConfidence.JUDGMENT_CALL,
                    evidence=(
                        f"Multivariate analysis detected {n_mv} rows ({mv_pct}%) as outliers. "
                        f"Methods: Isolation Forest, LOF, DBSCAN -- flagged rows confirmed by >=2 methods."
                    ),
                    recommendation=f"Review {n_mv} multivariate outlier rows across {len(self.predictor_numeric_cols)} numeric predictor columns.",
                    reasoning=(
                        "Multivariate outliers are points that look normal in any single dimension "
                        "but are unusual when all predictor features are considered together. Consensus "
                        "across multiple algorithms increases confidence."
                    ),
                    target_columns=self.predictor_numeric_cols,
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

    def _classify_outlier(self, od: OutlierDetail, stats: dict) -> None:
        """
        Classify an outlier observation into one of the 4 mutually distinct concepts:
        1. Confirmed invalid value / data-entry error (requires concrete evidence: violated business rules, impossible range)
        2. Contextual/group outlier (unusual only relative to temporal envelope or subgroup)
        3. Potential data-quality issue (multivariate density anomalies)
        4. Statistical outlier (distributional rule: IQR, Z-score, modified Z-score)
        """
        # Concept 4: Confirmed invalid value / data-entry error
        # Requires concrete additional evidence (e.g. impossible domain range, violated business constraint)
        if OutlierMethod.BUSINESS_RULE in od.detection_methods:
            od.classification = OutlierClassification.CONFIRMED_INVALID
            od.is_likely_error = True
            od.error_likelihood = 1.0
            od.classification_reason = "Violates business constraint / domain range: confirmed invalid value."
            od.error_reasoning = od.classification_reason
            return

        # Concept 2: Contextual / group outlier
        # Case A: Level 2 evaluation established it is normal within its peer group
        if od.is_group_outlier is False:
            od.classification = OutlierClassification.CONTEXTUAL_OUTLIER
            od.is_likely_error = False
            od.error_likelihood = 0.0
            od.contextual_explanation = "Globally unusual but normal for this product group"
            od.classification_reason = "Globally unusual but normal for this product group"
            od.error_reasoning = "Globally unusual but normal for this product group"
            return

        # Case B: Level 2 evaluation established it is unusual even within its peer group
        if od.is_group_outlier is True:
            od.classification = OutlierClassification.STATISTICAL_OUTLIER
            od.is_likely_error = False
            od.error_likelihood = 0.0
            od.contextual_explanation = "Unusual even within its peer group."
            od.classification_reason = "Unusual even within its peer group."
            od.error_reasoning = "Unusual even within its peer group."
            return

        if OutlierMethod.TIMESERIES_ENVELOPE in od.detection_methods:
            od.classification = OutlierClassification.CONTEXTUAL_OUTLIER
            od.is_likely_error = False
            od.error_likelihood = 0.0
            od.classification_reason = "Contextual outlier: unusual relative to its subgroup/temporal context; manual review recommended."
            od.error_reasoning = od.classification_reason
            return

        # Concept 3: Potential data-quality issue
        if OutlierMethod.DBSCAN in od.detection_methods or OutlierMethod.LOF in od.detection_methods:
            od.classification = OutlierClassification.POTENTIAL_DATA_QUALITY
            od.is_likely_error = False
            od.error_likelihood = 0.0
            od.classification_reason = "Potential data-quality issue: suspicious density deviation; manual review recommended."
            od.error_reasoning = od.classification_reason
            return

        # Concept 1: Statistical outlier (IQR, Z-score, modified Z-score, Isolation Forest)
        # Statistical outlier algorithms only establish that an observation is unusual under a distributional rule.
        # It does NOT prove that the value is an erroneous data entry.
        od.classification = OutlierClassification.STATISTICAL_OUTLIER
        od.is_likely_error = False
        od.error_likelihood = 0.0
        od.classification_reason = "Statistically unusual observation; manual review recommended."
        od.error_reasoning = "Statistically unusual observation; manual review recommended."

    def _estimate_error_likelihood(self, od: OutlierDetail, stats: dict) -> float:
        """
        Return error likelihood. Statistical algorithms alone NEVER invent an error probability.
        Only confirmed invalid values (e.g. business rule violations) have non-zero error likelihood.
        """
        if OutlierMethod.BUSINESS_RULE in od.detection_methods or od.classification == OutlierClassification.CONFIRMED_INVALID:
            return 1.0
        return 0.0

    def _explain_error_likelihood(self, od: OutlierDetail, stats: dict) -> str:
        """Return standardized explanation for the observation."""
        if OutlierMethod.BUSINESS_RULE in od.detection_methods or od.classification == OutlierClassification.CONFIRMED_INVALID:
            return "Violates business constraint / domain range: confirmed invalid value."
        if od.is_group_outlier is False:
            return "Globally unusual but normal for this product group"
        if od.is_group_outlier is True:
            return "Unusual even within its peer group."
        if OutlierMethod.TIMESERIES_ENVELOPE in od.detection_methods:
            return "Contextual outlier: unusual relative to its subgroup/temporal context; manual review recommended."
        if OutlierMethod.DBSCAN in od.detection_methods or OutlierMethod.LOF in od.detection_methods:
            return "Potential data-quality issue: suspicious density deviation; manual review recommended."
        return "Statistically unusual observation; manual review recommended."

    def _find_meaningful_grouping_dimension(
        self, col: str, series: pd.Series
    ) -> tuple[Optional[str], Optional[list[str]]]:
        """
        Identify the most meaningful grouping dimension for a numeric column.
        Evaluates compound (category + subcategory), subcategory, category,
        channel, region, and other categorical dimensions.
        Calculates ANOVA eta-squared (proportion of variance explained).
        Returns (dimension_label, list_of_column_names) if eta_sq >= 0.10.
        """
        candidate_specs: list[tuple[str, list[str]]] = []

        # 1. Compound dimension if both category and subcategory exist
        if "category" in self.df.columns and "subcategory" in self.df.columns and col not in ("category", "subcategory"):
            candidate_specs.append(("category + subcategory", ["category", "subcategory"]))

        # 2. Individual domain dimensions
        for dim in ["subcategory", "category", "channel", "region"]:
            if dim in self.df.columns and dim != col:
                candidate_specs.append((dim, [dim]))

        # 3. Other categorical columns with moderate cardinality
        for c in self.df.columns:
            if c != col and c not in ("category", "subcategory", "channel", "region"):
                if self.df[c].dtype == "object" or self.df[c].dtype.name == "category":
                    nunique = int(self.df[c].nunique())
                    if 2 <= nunique <= min(30, max(2, len(self.df) // 3)):
                        candidate_specs.append((c, [c]))

        best_label: Optional[str] = None
        best_cols: Optional[list[str]] = None
        best_score = -1.0

        for label, cols in candidate_specs:
            try:
                if len(cols) == 1:
                    grp_series = self.df[cols[0]].astype(str)
                else:
                    grp_series = self.df[cols[0]].astype(str) + " / " + self.df[cols[1]].astype(str)

                valid_idx = series.index.intersection(grp_series.dropna().index)
                if len(valid_idx) < 10:
                    continue

                vals = series.loc[valid_idx]
                grps = grp_series.loc[valid_idx]

                n_grps = grps.nunique()
                if n_grps < 2 or n_grps > len(vals) // 2:
                    continue

                grand_mean = float(vals.mean())
                ss_tot = float(((vals - grand_mean) ** 2).sum())
                if ss_tot <= 0:
                    continue

                grouped = vals.groupby(grps)
                counts = grouped.count()
                if counts.mean() < 2.5:
                    continue

                ss_between = float((counts * (grouped.mean() - grand_mean) ** 2).sum())
                eta_sq = ss_between / ss_tot

                if eta_sq >= 0.10:
                    # Give compound dimension slight preference if both category and subcategory are informative
                    score = eta_sq + (0.05 if len(cols) > 1 else 0.0)
                    if score > best_score:
                        best_score = score
                        best_label = label
                        best_cols = cols
            except Exception:
                continue

        return best_label, best_cols

    def _evaluate_group_outliers(
        self,
        col: str,
        series: pd.Series,
        global_outliers: list[OutlierDetail],
        grouping_dim_label: str,
        grouping_cols: list[str],
    ) -> tuple[int, int]:
        """
        LEVEL 2: Evaluate global outliers within peer groups.
        Distinguishes:
        - 'Globally unusual but normal for this product group'
        - 'Unusual even within its peer group.'
        Returns (subgroup_normal_count, within_group_outliers_count).
        """
        subgroup_normal_count = 0
        within_group_outliers_count = 0

        if len(grouping_cols) == 1:
            grp_series = self.df[grouping_cols[0]].astype(str)
        else:
            grp_series = self.df[grouping_cols[0]].astype(str) + " / " + self.df[grouping_cols[1]].astype(str)

        # Precompute group statistics
        group_stats: dict[str, dict[str, float]] = {}
        for g_name, group_data in series.groupby(grp_series):
            vals = group_data.dropna()
            n = len(vals)
            if n < 3:
                continue
            q1 = float(vals.quantile(0.25))
            q3 = float(vals.quantile(0.75))
            iqr = q3 - q1
            med = float(vals.median())
            std = float(vals.std()) if n > 1 else 0.0
            mean = float(vals.mean())

            if iqr > 0:
                lower_fence = q1 - 1.5 * iqr
                upper_fence = q3 + 1.5 * iqr
            elif std > 0:
                lower_fence = mean - 2.5 * std
                upper_fence = mean + 2.5 * std
            else:
                lower_fence = float(vals.min())
                upper_fence = float(vals.max())

            group_stats[str(g_name)] = {
                "count": n,
                "q1": round(q1, 2),
                "q3": round(q3, 2),
                "median": round(med, 2),
                "iqr": round(iqr, 2),
                "lower_fence": round(lower_fence, 2),
                "upper_fence": round(upper_fence, 2),
            }

        # Evaluate each global outlier within its group
        for od in global_outliers:
            od.is_global_outlier = True
            od.grouping_dimension = grouping_dim_label

            if od.row_index in grp_series.index:
                g_val = str(grp_series.loc[od.row_index])
                od.group_name = g_val
                g_stat = group_stats.get(g_val)

                if g_stat:
                    od.group_stats = g_stat
                    val = od.value
                    is_within = (val >= g_stat["lower_fence"] and val <= g_stat["upper_fence"])

                    if is_within:
                        od.is_group_outlier = False
                        od.contextual_explanation = "Globally unusual but normal for this product group"
                        subgroup_normal_count += 1
                    else:
                        od.is_group_outlier = True
                        od.contextual_explanation = "Unusual even within its peer group."
                        within_group_outliers_count += 1
                else:
                    od.is_group_outlier = True
                    od.contextual_explanation = "Unusual even within its peer group."
                    within_group_outliers_count += 1

        return subgroup_normal_count, within_group_outliers_count

    def _recommend_treatment(
        self,
        col: str,
        outliers: list[OutlierDetail],
        stats: dict,
        grouping_dimension: Optional[str] = None,
        subgroup_normal_count: int = 0,
    ) -> tuple[ActionType, str]:
        """Recommend treatment based on outlier characteristics."""
        total = len(outliers)
        confirmed_invalids = sum(1 for o in outliers if o.classification == OutlierClassification.CONFIRMED_INVALID)
        has_business_violations = any(
            OutlierMethod.BUSINESS_RULE in o.detection_methods for o in outliers
        )

        # Business rule violations → cap at boundary or replace
        if has_business_violations and confirmed_invalids > 0:
            return ActionType.REPLACE_BOUNDARY, (
                f"{confirmed_invalids} value(s) violate business constraints / domain ranges. "
                "Replacing with boundary values corrects invalid data."
            )

        # Distinct subgroup scale differences → DO NOT automatically winsorize or log transform
        if subgroup_normal_count > 0:
            dim_str = f" across '{grouping_dimension}'" if grouping_dimension else ""
            return ActionType.FLAG_OUTLIER, (
                f"Column '{col}' exhibits distinct subgroup scale behavior{dim_str}. "
                f"{subgroup_normal_count} observation(s) are globally unusual but normal for their product group. "
                "Do not automatically winsorize observations that are merely extreme because different groups have different scales. "
                "Flagging or group-stratified modeling recommended to preserve subgroup distributions."
            )

        # Highly skewed data → log transform
        if stats.get("skewness", 0) > 2.0 and stats.get("min", 0) > 0:
            return ActionType.LOG_TRANSFORM, (
                "Statistically unusual observation; manual review recommended. "
                "Highly right-skewed distribution with all positive values: log transform "
                "compresses the range and reduces outlier leverage without discarding data."
            )

        # Moderate outliers in heavy tail → winsorize
        if total > self.n_rows * 0.05:
            return ActionType.WINSORIZE, (
                "Statistically unusual observation; manual review recommended. "
                "Winsorizing caps at 1st/99th percentile, limiting extreme leverage "
                "while preserving all observations."
            )

        # Default → flag and keep
        return ActionType.FLAG_OUTLIER, (
            "Statistically unusual observation; manual review recommended. "
            "Flagging preserves records while enabling downstream filtering."
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

        if summary.subgroup_normal_count > 0:
            options = [
                UserOption(key="flag", label="Flag Outliers and Keep (preserve subgroup scales)", is_default=True),
                UserOption(key="stratified_scale", label="Apply Group-Stratified Modeling / Scaling"),
                UserOption(key="ignore", label="Leave unchanged"),
                UserOption(key="winsorize", label="Winsorize globally (Not recommended: distorts subgroup scale)"),
                UserOption(key="cap_iqr", label="Cap at IQR fence"),
                UserOption(key="remove", label="Remove outlier rows"),
            ]
        else:
            options = [
                UserOption(key="flag", label="Flag and Keep (add _is_outlier column)", is_default=True),
                UserOption(key="winsorize", label="Winsorize at 1st/99th percentile"),
                UserOption(key="cap_iqr", label="Cap at IQR fence"),
                UserOption(key="remove", label="Remove outlier rows"),
                UserOption(key="log", label="Log transform column"),
                UserOption(key="ignore", label="Leave unchanged"),
            ]

        if summary.likely_errors > 0:
            evidence_str = (
                f"Column '{col}': {summary.total_outliers} outliers detected ({summary.outlier_pct}%) "
                f"using [{methods_str}], including {summary.likely_errors} confirmed invalid value(s) "
                f"violating business constraints."
            )
            desc_str = (
                f"{summary.total_outliers} outlier values in '{col}' "
                f"({summary.likely_errors} confirmed invalid / data-entry error(s))."
            )
        elif summary.subgroup_normal_count > 0:
            dim_str = f" across '{summary.grouping_dimension}'" if summary.grouping_dimension else ""
            evidence_str = (
                f"Column '{col}': {summary.total_outliers} global statistical outliers evaluated{dim_str}. "
                f"{summary.subgroup_normal_count} observation(s) are globally unusual but normal for this product group; "
                f"{summary.within_group_outliers_count} are unusual even within their peer group. "
                "Statistically unusual observation; manual review recommended."
            )
            desc_str = (
                f"{summary.total_outliers} global outliers in '{col}' ({summary.subgroup_normal_count} normal within product group). "
                "Do not automatically winsorize observations that are merely extreme because different groups have different scales."
            )
        else:
            evidence_str = (
                f"Column '{col}': {summary.total_outliers} statistical outliers detected ({summary.outlier_pct}%) "
                f"using [{methods_str}]. Statistically unusual observation; manual review recommended."
            )
            desc_str = (
                f"{summary.total_outliers} statistical outliers in '{col}'. "
                "Statistically unusual observation; manual review recommended."
            )

        return CleaningAction(
            category=ActionCategory.OUTLIERS,
            action_type=summary.recommended_treatment,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=evidence_str,
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
                description=desc_str,
            ),
            options=options,
            metadata={
                "total_outliers": summary.total_outliers,
                "likely_errors": summary.likely_errors,
                "statistical_outliers": summary.statistical_outliers_count,
                "contextual_outliers": summary.contextual_outliers_count,
                "potential_quality_issues": summary.potential_quality_issues_count,
                "confirmed_invalid": summary.confirmed_invalid_count,
                "grouping_dimension": summary.grouping_dimension,
                "subgroup_normal_count": summary.subgroup_normal_count,
                "within_group_outliers_count": summary.within_group_outliers_count,
                "methods": [m.value for m in summary.methods_used],
                "stats": stats,
            },
        )

    def _is_temporal_column(self, col: str) -> bool:
        """Check if column has temporal context."""
        if self.profile and hasattr(self.profile, "temporal_columns"):
            return col in self.profile.temporal_columns
        return False
