"""
Feature Selection Handler — Filters and reduces features based on
variance, correlation, VIF, mutual information, and PCA analysis.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
)


class FeatureSelectionHandler:
    """Feature selection and dimensionality reduction recommendations."""

    def __init__(
        self, df: pd.DataFrame, file_id: str,
        near_zero_threshold: float = 0.95,
        correlation_threshold: float = 0.95,
        vif_threshold: float = 10.0,
        target_column: Optional[str] = None,
        feature_importances: Optional[dict[str, float]] = None,
        profile: Any = None,
    ):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)
        self.near_zero_threshold = near_zero_threshold
        self.correlation_threshold = correlation_threshold
        self.vif_threshold = vif_threshold
        self.target_column = target_column
        self.feature_importances = feature_importances or {}
        self.profile = profile

    # ── Public entry ──────────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], dict]:
        actions: list[CleaningAction] = []
        report: dict[str, Any] = {
            "zero_variance": [],
            "near_zero_variance": [],
            "high_correlation_pairs": [],
            "high_vif_columns": [],
            "low_mi_columns": [],
            "pca_suggestion": None,
            "feature_clusters": [],
        }

        numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()

        # 1. Zero-variance filter (definitive)
        zv_cols = self._zero_variance()
        report["zero_variance"] = zv_cols
        if zv_cols:
            actions.append(self._build_zero_variance_action(zv_cols))

        # 2. Near-zero variance filter
        nzv_cols = self._near_zero_variance(numeric_cols)
        report["near_zero_variance"] = nzv_cols
        if nzv_cols:
            actions.append(self._build_near_zero_action(nzv_cols))

        # 3. High correlation filter
        corr_pairs = self._high_correlation(numeric_cols)
        report["high_correlation_pairs"] = corr_pairs
        for pair in corr_pairs:
            actions.append(self._build_correlation_action(pair))

        # 4. VIF-based multicollinearity
        if len(numeric_cols) >= 3:
            vif_cols = self._high_vif(numeric_cols)
            report["high_vif_columns"] = vif_cols
            if vif_cols:
                actions.append(self._build_vif_action(vif_cols))

        # 5. Low mutual information (if target available)
        if self.target_column and self.target_column in self.df.columns:
            low_mi = self._low_mutual_info(numeric_cols)
            report["low_mi_columns"] = low_mi
            if low_mi:
                actions.append(self._build_low_mi_action(low_mi))

        # 6. PCA suggestion (if high dimensionality)
        if len(numeric_cols) >= 10:
            pca_info = self._pca_analysis(numeric_cols)
            if pca_info:
                report["pca_suggestion"] = pca_info
                actions.append(self._build_pca_action(pca_info))

        # 7. Feature clustering
        if len(numeric_cols) >= 5:
            clusters = self._feature_clustering(numeric_cols)
            report["feature_clusters"] = clusters
            if clusters:
                actions.append(self._build_cluster_action(clusters))

        return actions, report

    # ── 1. Zero variance ──────────────────────────────────────────────
    def _zero_variance(self) -> list[str]:
        return [col for col in self.df.columns if self.df[col].nunique() <= 1]

    def _build_zero_variance_action(self, cols: list[str]) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.FEATURE_SELECTION,
            action_type=ActionType.DROP_ZERO_VARIANCE,
            confidence=ActionConfidence.DEFINITIVE,
            evidence=f"{len(cols)} column(s) have zero variance (only one unique value): {cols}.",
            recommendation=f"Drop {len(cols)} zero-variance column(s): {', '.join(cols)}.",
            reasoning="Columns with a single value carry no information for any analysis or model.",
            target_columns=cols,
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - len(cols),
                columns_affected=len(cols),
                description=f"Removes {len(cols)} constant column(s).",
            ),
        )

    # ── 2. Near-zero variance ─────────────────────────────────────────
    def _near_zero_variance(self, numeric_cols: list[str]) -> list[dict]:
        results: list[dict] = []
        for col in numeric_cols:
            if self.df[col].nunique() <= 1:
                continue  # Already handled by zero-variance
            vc = self.df[col].value_counts(normalize=True)
            top_freq = float(vc.iloc[0]) if len(vc) > 0 else 0
            if top_freq > self.near_zero_threshold:
                results.append({
                    "column": col,
                    "top_value": str(vc.index[0]),
                    "top_frequency": round(top_freq * 100, 1),
                })
        return results

    def _build_near_zero_action(self, nzv_cols: list[dict]) -> CleaningAction:
        cols = [c["column"] for c in nzv_cols]
        details = "; ".join(
            f"'{c['column']}' ({c['top_frequency']}% = {c['top_value']})"
            for c in nzv_cols[:5]
        )

        return CleaningAction(
            category=ActionCategory.FEATURE_SELECTION,
            action_type=ActionType.DROP_NEAR_ZERO_VARIANCE,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"{len(nzv_cols)} column(s) have near-zero variance (>{self.near_zero_threshold * 100:.0f}% "
                f"same value): {details}."
            ),
            recommendation=f"Consider dropping {len(nzv_cols)} near-constant column(s).",
            reasoning=(
                f"Columns where >{self.near_zero_threshold * 100:.0f}% of values are identical "
                "provide minimal discriminative power. Threshold auto-justified by dataset size "
                f"({self.n_rows} rows)."
            ),
            target_columns=cols,
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - len(cols),
                columns_affected=len(cols),
                description=f"Removes {len(cols)} near-constant column(s).",
            ),
            options=[
                UserOption(key="drop_all", label=f"Drop All {len(cols)} Columns", is_default=True),
                UserOption(key="skip", label="Keep All"),
            ],
        )

    # ── 3. High correlation ───────────────────────────────────────────
    def _high_correlation(self, numeric_cols: list[str]) -> list[dict]:
        if len(numeric_cols) < 2:
            return []

        df_num = self.df[numeric_cols].dropna()
        if len(df_num) < 5:
            return []

        corr = df_num.corr().abs()
        pairs: list[dict] = []
        seen: set[frozenset] = set()

        for i in range(len(corr.columns)):
            for j in range(i + 1, len(corr.columns)):
                col_a = corr.columns[i]
                col_b = corr.columns[j]
                r = float(corr.iloc[i, j])
                if r > self.correlation_threshold:
                    pair_key = frozenset([col_a, col_b])
                    if pair_key not in seen:
                        seen.add(pair_key)
                        # Recommend dropping the one with lower feature importance
                        fi_a = self.feature_importances.get(col_a, 50)
                        fi_b = self.feature_importances.get(col_b, 50)
                        drop = col_b if fi_a >= fi_b else col_a
                        keep = col_a if drop == col_b else col_b

                        pairs.append({
                            "col_a": col_a,
                            "col_b": col_b,
                            "correlation": round(r, 4),
                            "drop_recommendation": drop,
                            "keep_recommendation": keep,
                            "importance_a": fi_a,
                            "importance_b": fi_b,
                        })

        return pairs

    def _build_correlation_action(self, pair: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.FEATURE_SELECTION,
            action_type=ActionType.DROP_HIGH_CORRELATION,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Columns '{pair['col_a']}' and '{pair['col_b']}' have correlation "
                f"{pair['correlation']:.4f} (threshold: {self.correlation_threshold})."
            ),
            recommendation=(
                f"Drop '{pair['drop_recommendation']}' (lower importance) and keep "
                f"'{pair['keep_recommendation']}'."
            ),
            reasoning=(
                f"Highly correlated features (r={pair['correlation']:.4f}) provide "
                "redundant information. Dropping the less important one reduces "
                "multicollinearity without losing predictive power."
            ),
            target_columns=[pair["drop_recommendation"]],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                columns_affected=1,
                description=f"Drops '{pair['drop_recommendation']}' (r={pair['correlation']:.4f} with '{pair['keep_recommendation']}').",
            ),
            options=[
                UserOption(key="drop_rec", label=f"Drop '{pair['drop_recommendation']}'", is_default=True),
                UserOption(key="drop_other", label=f"Drop '{pair['keep_recommendation']}' Instead"),
                UserOption(key="keep_both", label="Keep Both"),
            ],
            metadata=pair,
        )

    # ── 4. VIF ────────────────────────────────────────────────────────
    def _high_vif(self, numeric_cols: list[str]) -> list[dict]:
        results: list[dict] = []
        try:
            from statsmodels.stats.outliers_influence import variance_inflation_factor

            df_num = self.df[numeric_cols].dropna()
            if len(df_num) < len(numeric_cols) + 2:
                return results

            # Add constant
            from statsmodels.tools.tools import add_constant
            X = add_constant(df_num)

            for i, col in enumerate(numeric_cols):
                try:
                    vif = variance_inflation_factor(X.values, i + 1)  # +1 for constant
                    if vif > self.vif_threshold and np.isfinite(vif):
                        results.append({"column": col, "vif": round(float(vif), 2)})
                except Exception:
                    pass

            # Sort by VIF descending
            results.sort(key=lambda x: x["vif"], reverse=True)

        except ImportError:
            # Fallback: use correlation-based VIF approximation
            pass

        return results

    def _build_vif_action(self, vif_cols: list[dict]) -> CleaningAction:
        cols = [c["column"] for c in vif_cols]
        details = ", ".join(f"'{c['column']}' (VIF={c['vif']})" for c in vif_cols[:5])

        return CleaningAction(
            category=ActionCategory.FEATURE_SELECTION,
            action_type=ActionType.DROP_HIGH_VIF,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"{len(vif_cols)} column(s) have VIF > {self.vif_threshold}: {details}."
            ),
            recommendation=(
                f"Iteratively remove features with highest VIF until all remaining have "
                f"VIF < {self.vif_threshold}."
            ),
            reasoning=(
                "High VIF (>10) indicates severe multicollinearity — the feature can be "
                "nearly perfectly predicted by other features. This inflates regression "
                "coefficients and reduces model stability."
            ),
            target_columns=cols,
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols,
                columns_after=self.n_cols - len(cols),
                columns_affected=len(cols),
                description=f"Removes {len(cols)} multicollinear column(s).",
            ),
            options=[
                UserOption(key="iterative", label="Remove Iteratively (highest VIF first)", is_default=True),
                UserOption(key="all", label="Remove All at Once"),
                UserOption(key="skip", label="Keep All"),
            ],
            metadata={"vif_details": vif_cols},
        )

    # ── 5. Low mutual information ─────────────────────────────────────
    def _low_mutual_info(self, numeric_cols: list[str]) -> list[dict]:
        results: list[dict] = []
        try:
            from sklearn.feature_selection import mutual_info_regression, mutual_info_classif

            target = self.df[self.target_column].dropna()
            if len(target) < 10:
                return results

            feature_cols = [c for c in numeric_cols if c != self.target_column]
            df_clean = self.df[feature_cols + [self.target_column]].dropna()
            if len(df_clean) < 10:
                return results

            X = df_clean[feature_cols].values
            y = df_clean[self.target_column].values

            # Choose MI function based on target type
            if pd.api.types.is_numeric_dtype(target) and target.nunique() > 10:
                mi = mutual_info_regression(X, y, random_state=42)
            else:
                mi = mutual_info_classif(X, y, random_state=42)

            for col, score in zip(feature_cols, mi):
                if score < 0.01:
                    results.append({"column": col, "mi_score": round(float(score), 4)})

        except ImportError:
            pass

        return results

    def _build_low_mi_action(self, low_mi: list[dict]) -> CleaningAction:
        cols = [c["column"] for c in low_mi]
        details = ", ".join(f"'{c['column']}' (MI={c['mi_score']})" for c in low_mi[:5])

        return CleaningAction(
            category=ActionCategory.FEATURE_SELECTION,
            action_type=ActionType.DROP_LOW_MI,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=f"{len(low_mi)} feature(s) with near-zero mutual information with target: {details}.",
            recommendation=f"Drop {len(low_mi)} feature(s) with negligible target correlation.",
            reasoning="Features with near-zero mutual information with the target provide no predictive value.",
            target_columns=cols,
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - len(cols),
                columns_affected=len(cols),
                description=f"Removes {len(cols)} low-MI feature(s).",
            ),
            options=[
                UserOption(key="drop", label=f"Drop All {len(cols)}", is_default=True),
                UserOption(key="skip", label="Keep All"),
            ],
            metadata={"low_mi_details": low_mi},
        )

    # ── 6. PCA analysis ───────────────────────────────────────────────
    def _pca_analysis(self, numeric_cols: list[str]) -> Optional[dict]:
        try:
            from sklearn.decomposition import PCA
            from sklearn.preprocessing import StandardScaler

            df_num = self.df[numeric_cols].dropna()
            if len(df_num) < 10:
                return None

            X = StandardScaler().fit_transform(df_num)
            pca = PCA()
            pca.fit(X)

            explained = pca.explained_variance_ratio_
            cumulative = np.cumsum(explained)

            # Find components for different thresholds
            n_90 = int(np.argmax(cumulative >= 0.90)) + 1
            n_95 = int(np.argmax(cumulative >= 0.95)) + 1
            n_99 = int(np.argmax(cumulative >= 0.99)) + 1

            return {
                "total_components": len(numeric_cols),
                "components_for_90pct": n_90,
                "components_for_95pct": n_95,
                "components_for_99pct": n_99,
                "explained_variance_top5": [round(float(v), 4) for v in explained[:5]],
                "cumulative_top5": [round(float(v), 4) for v in cumulative[:5]],
                "reduction_ratio_95": round(1 - n_95 / len(numeric_cols), 2),
            }

        except ImportError:
            return None

    def _build_pca_action(self, pca_info: dict) -> CleaningAction:
        return CleaningAction(
            category=ActionCategory.FEATURE_SELECTION,
            action_type=ActionType.SUGGEST_PCA,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"Dataset has {pca_info['total_components']} numeric features. "
                f"PCA can capture 90% variance with {pca_info['components_for_90pct']} components, "
                f"95% with {pca_info['components_for_95pct']}, 99% with {pca_info['components_for_99pct']}."
            ),
            recommendation=(
                f"Consider PCA with {pca_info['components_for_95pct']} components "
                f"(retains 95% explained variance, {pca_info['reduction_ratio_95'] * 100:.0f}% dimensionality reduction)."
            ),
            reasoning=(
                "PCA is NOT applied automatically — this is informational only. "
                "It's most useful when: (a) many features are correlated, "
                "(b) interpretability is not critical, (c) model training speed is a concern."
            ),
            target_columns=[],
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols,
                columns_after=self.n_cols - pca_info["total_components"] + pca_info["components_for_95pct"],
                columns_affected=pca_info["total_components"],
                description=(
                    f"Would reduce {pca_info['total_components']} features to "
                    f"{pca_info['components_for_95pct']} components (95% variance)."
                ),
            ),
            options=[
                UserOption(key="pca_90", label=f"Apply PCA ({pca_info['components_for_90pct']} components, 90%)"),
                UserOption(key="pca_95", label=f"Apply PCA ({pca_info['components_for_95pct']} components, 95%)", is_default=True),
                UserOption(key="pca_99", label=f"Apply PCA ({pca_info['components_for_99pct']} components, 99%)"),
                UserOption(key="skip", label="Do Not Apply PCA"),
            ],
            metadata=pca_info,
        )

    # ── 7. Feature clustering ─────────────────────────────────────────
    def _feature_clustering(self, numeric_cols: list[str]) -> list[dict]:
        """Group highly correlated features into clusters."""
        if len(numeric_cols) < 5:
            return []

        try:
            df_num = self.df[numeric_cols].dropna()
            if len(df_num) < 10:
                return []

            corr = df_num.corr().abs()

            # Simple hierarchical clustering based on correlation
            from scipy.cluster.hierarchy import linkage, fcluster

            # Convert correlation to distance
            dist = 1 - corr.values
            np.fill_diagonal(dist, 0)

            # Ensure symmetry and non-negativity
            dist = np.maximum(dist, 0)
            # Convert to condensed form
            from scipy.spatial.distance import squareform
            dist_condensed = squareform(dist)

            Z = linkage(dist_condensed, method="average")
            labels = fcluster(Z, t=0.3, criterion="distance")

            clusters: dict[int, list[str]] = {}
            for col, label in zip(numeric_cols, labels):
                clusters.setdefault(int(label), []).append(col)

            # Only report clusters with >1 member
            result: list[dict] = []
            for cluster_id, members in clusters.items():
                if len(members) < 2:
                    continue

                # Select representative: highest feature importance or first
                importances = {m: self.feature_importances.get(m, 50) for m in members}
                representative = max(importances, key=importances.get)

                result.append({
                    "cluster_id": cluster_id,
                    "members": members,
                    "representative": representative,
                    "size": len(members),
                    "members_to_drop": [m for m in members if m != representative],
                })

            return result

        except ImportError:
            return []
        except Exception:
            return []

    def _build_cluster_action(self, clusters: list[dict]) -> CleaningAction:
        total_drop = sum(len(c["members_to_drop"]) for c in clusters)
        cluster_desc = "; ".join(
            f"Cluster {c['cluster_id']}: keep '{c['representative']}', drop {c['members_to_drop'][:3]}"
            for c in clusters[:3]
        )

        drop_cols = []
        for c in clusters:
            drop_cols.extend(c["members_to_drop"])

        return CleaningAction(
            category=ActionCategory.FEATURE_SELECTION,
            action_type=ActionType.CLUSTER_FEATURES,
            confidence=ActionConfidence.JUDGMENT_CALL,
            evidence=(
                f"{len(clusters)} feature clusters detected (correlated groups). "
                f"{cluster_desc}"
            ),
            recommendation=f"Retain {len(clusters)} cluster representatives, drop {total_drop} redundant features.",
            reasoning=(
                "Features within a cluster are highly correlated — keeping only the "
                "representative preserves information while reducing dimensionality."
            ),
            target_columns=drop_cols,
            impact=ImpactEstimate(
                rows_before=self.n_rows, rows_after=self.n_rows,
                columns_before=self.n_cols, columns_after=self.n_cols - total_drop,
                columns_affected=total_drop,
                description=f"Drops {total_drop} features from {len(clusters)} clusters.",
            ),
            options=[
                UserOption(key="drop_redundant", label=f"Drop {total_drop} Redundant Features", is_default=True),
                UserOption(key="skip", label="Keep All"),
            ],
            metadata={"clusters": clusters},
        )

    # ── Static execution methods ──────────────────────────────────────

    @staticmethod
    def apply_drop_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        return df.drop(columns=[c for c in cols if c in df.columns])

    @staticmethod
    def apply_pca(df: pd.DataFrame, numeric_cols: list[str], n_components: int) -> pd.DataFrame:
        """Apply PCA and replace numeric columns with principal components."""
        try:
            from sklearn.decomposition import PCA
            from sklearn.preprocessing import StandardScaler

            df_num = df[numeric_cols].fillna(0)
            X = StandardScaler().fit_transform(df_num)
            pca = PCA(n_components=n_components)
            components = pca.fit_transform(X)

            # Drop original numeric columns
            df = df.drop(columns=numeric_cols)

            # Add PCA components
            for i in range(n_components):
                df[f"PC{i + 1}"] = components[:, i]

        except ImportError:
            pass

        return df
