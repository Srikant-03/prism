"""
Duplicate Handler — detects exact, subset, near-duplicate, column-level,
and derived-column duplicates.  Returns CleaningActions with evidence.
"""

from __future__ import annotations

import itertools
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

from cleaning.cleaning_models import (
    CleaningAction, ActionType, ActionConfidence, ActionCategory,
    ImpactEstimate, PreviewSample, UserOption,
    DuplicateReport, DuplicateGroup, DuplicateType,
)


class DuplicateHandler:
    """Detect all flavours of duplication and return evidence-based actions."""

    def __init__(self, df: pd.DataFrame, file_id: str):
        self.df = df
        self.file_id = file_id
        self.n_rows = len(df)
        self.n_cols = len(df.columns)

    # ── public entry point ────────────────────────────────────────────
    def analyze(self) -> tuple[list[CleaningAction], DuplicateReport]:
        actions: list[CleaningAction] = []
        report = DuplicateReport()

        # 1. Exact row duplicates
        exact_actions, exact_count, exact_pct = self._exact_duplicates()
        actions.extend(exact_actions)
        report.exact_count = exact_count
        report.exact_pct = exact_pct

        # 2. Duplicate columns
        dup_col_actions, dup_col_pairs = self._duplicate_columns()
        actions.extend(dup_col_actions)
        report.duplicate_column_pairs = dup_col_pairs

        # 3. Derived columns
        derived_actions, derived_pairs = self._derived_columns()
        actions.extend(derived_actions)
        report.derived_column_pairs = derived_pairs

        # 4. Near duplicates (only on smaller datasets — expensive)
        if self.n_rows <= 50_000:
            near_actions, near_clusters = self._near_duplicates()
            actions.extend(near_actions)
            report.near_duplicate_clusters = near_clusters

        return actions, report

    # ── 1. Exact row duplicates ───────────────────────────────────────
    def _exact_duplicates(self) -> tuple[list[CleaningAction], int, float]:
        mask = self.df.duplicated(keep=False)
        dup_count = int(mask.sum())
        if dup_count == 0:
            return [], 0, 0.0

        removable = int(self.df.duplicated(keep="first").sum())
        dup_pct = round(removable / self.n_rows * 100, 2)

        # Build preview — up to 5 sample duplicate rows
        sample_indices = self.df[self.df.duplicated(keep="first")].head(5).index.tolist()
        sample_rows = self.df.loc[sample_indices].head(5)
        before_sample = sample_rows.fillna("").astype(str).to_dict(orient="records")
        after_sample: list[dict] = []  # effectively removed

        action = CleaningAction(
            category=ActionCategory.DUPLICATES,
            action_type=ActionType.REMOVE_EXACT_DUPLICATES,
            confidence=ActionConfidence.DEFINITIVE if dup_pct < 5 else ActionConfidence.JUDGMENT_CALL,
            evidence=f"Found {removable:,} exact duplicate rows ({dup_pct}% of dataset).",
            recommendation=f"Remove duplicate rows, keeping the first occurrence of each.",
            reasoning=(
                "Exact duplicate rows carry no additional information and inflate dataset "
                "statistics.  Removing them is safe unless duplicates are semantically "
                "meaningful (e.g. repeated transactions)."
            ),
            target_columns=list(self.df.columns),
            preview=PreviewSample(
                before=before_sample,
                after=after_sample,
                columns_before=list(self.df.columns),
                columns_after=list(self.df.columns),
            ),
            impact=ImpactEstimate(
                rows_before=self.n_rows,
                rows_after=self.n_rows - removable,
                rows_affected=removable,
                rows_affected_pct=dup_pct,
                columns_before=self.n_cols,
                columns_after=self.n_cols,
                description=f"Removes {removable:,} rows, reducing the dataset by {dup_pct}%.",
            ),
            options=[
                UserOption(key="keep_first", label="Remove All, Keep First", is_default=True),
                UserOption(key="keep_last", label="Remove All, Keep Last"),
                UserOption(key="keep_all", label="Keep All (Flag Only)"),
            ],
        )
        return [action], dup_count, dup_pct

    # ── 2. Duplicate columns ──────────────────────────────────────────
    def _duplicate_columns(self) -> tuple[list[CleaningAction], list[dict]]:
        actions: list[CleaningAction] = []
        pairs: list[dict] = []

        cols = list(self.df.columns)
        checked: set[tuple[str, str]] = set()

        for i, c1 in enumerate(cols):
            for c2 in cols[i + 1:]:
                if (c1, c2) in checked:
                    continue
                checked.add((c1, c2))

                try:
                    if self.df[c1].dtype != self.df[c2].dtype:
                        continue
                    # Fast equality check
                    equal_mask = self.df[c1].fillna("__NULL__") == self.df[c2].fillna("__NULL__")
                    similarity = equal_mask.mean()

                    if similarity >= 0.98:
                        # Prefer keeping the column with the more descriptive name
                        drop_col = c2 if len(c1) >= len(c2) else c1
                        keep_col = c1 if drop_col == c2 else c2

                        pair_info = {
                            "col1": c1, "col2": c2,
                            "similarity": round(similarity, 4),
                            "drop_recommended": drop_col,
                        }
                        pairs.append(pair_info)

                        action = CleaningAction(
                            category=ActionCategory.DUPLICATES,
                            action_type=ActionType.DROP_DUPLICATE_COLUMN,
                            confidence=ActionConfidence.DEFINITIVE if similarity == 1.0 else ActionConfidence.JUDGMENT_CALL,
                            evidence=f"Columns '{c1}' and '{c2}' are {similarity * 100:.1f}% identical.",
                            recommendation=f"Drop column '{drop_col}' (keeping '{keep_col}').",
                            reasoning=(
                                f"Keeping both columns is redundant.  "
                                f"'{keep_col}' is retained as the more descriptive name."
                            ),
                            target_columns=[drop_col],
                            impact=ImpactEstimate(
                                rows_before=self.n_rows, rows_after=self.n_rows,
                                columns_before=self.n_cols, columns_after=self.n_cols - 1,
                                columns_affected=1,
                                description=f"Drops 1 redundant column '{drop_col}'.",
                            ),
                        )
                        actions.append(action)
                except Exception:
                    continue

        return actions, pairs

    # ── 3. Derived columns (linear transforms) ───────────────────────
    def _derived_columns(self) -> tuple[list[CleaningAction], list[dict]]:
        actions: list[CleaningAction] = []
        pairs: list[dict] = []

        numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) < 2:
            return actions, pairs

        # Sample for speed
        sample = self.df[numeric_cols].dropna()
        if len(sample) < 20:
            return actions, pairs
        if len(sample) > 10_000:
            sample = sample.sample(10_000, random_state=42)

        checked: set[tuple[str, str]] = set()
        for c1, c2 in itertools.combinations(numeric_cols, 2):
            if (c1, c2) in checked:
                continue
            checked.add((c1, c2))

            s1 = sample[c1].values
            s2 = sample[c2].values

            # Check if c2 ≈ a*c1 + b via correlation
            if np.std(s1) == 0 or np.std(s2) == 0:
                continue
            corr = np.corrcoef(s1, s2)[0, 1]
            if abs(corr) < 0.999:
                continue

            # Fit linear relationship
            try:
                coeffs = np.polyfit(s1, s2, 1)
                predicted = np.polyval(coeffs, s1)
                ss_res = np.sum((s2 - predicted) ** 2)
                ss_tot = np.sum((s2 - np.mean(s2)) ** 2)
                r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

                if r_squared >= 0.99:
                    a, b = round(coeffs[0], 4), round(coeffs[1], 4)
                    pair_info = {
                        "col1": c1, "col2": c2,
                        "relationship": f"{c2} ≈ {a} × {c1} + {b}",
                        "r_squared": round(r_squared, 6),
                    }
                    pairs.append(pair_info)

                    action = CleaningAction(
                        category=ActionCategory.DUPLICATES,
                        action_type=ActionType.DROP_DERIVED_COLUMN,
                        confidence=ActionConfidence.JUDGMENT_CALL,
                        evidence=f"'{c2}' is a near-perfect linear transform of '{c1}' (R²={r_squared:.4f}, {c2} ≈ {a}×{c1} + {b}).",
                        recommendation=f"Consider dropping '{c2}' as it is derivable from '{c1}'.",
                        reasoning="Derived columns add multicollinearity without new information. However, the derived column may carry business meaning.",
                        target_columns=[c2],
                        impact=ImpactEstimate(
                            rows_before=self.n_rows, rows_after=self.n_rows,
                            columns_before=self.n_cols, columns_after=self.n_cols - 1,
                            columns_affected=1,
                            description=f"Drops 1 derived column '{c2}'.",
                        ),
                        metadata={"relationship": pair_info["relationship"], "r_squared": r_squared},
                    )
                    actions.append(action)
            except Exception:
                continue

        return actions, pairs

    # ── 4. Near-duplicates (fuzzy string matching) ────────────────────
    def _near_duplicates(self) -> tuple[list[CleaningAction], list[DuplicateGroup]]:
        actions: list[CleaningAction] = []
        clusters: list[DuplicateGroup] = []

        try:
            import jellyfish
        except ImportError:
            return actions, clusters

        # Find string columns with moderate cardinality
        string_cols = [
            c for c in self.df.columns
            if self.df[c].dtype == object and 2 < self.df[c].nunique() < 1000
        ]
        if not string_cols:
            return actions, clusters

        # Use the first suitable string column for near-dup detection
        for col in string_cols[:3]:
            unique_vals = self.df[col].dropna().unique()
            if len(unique_vals) > 500:
                unique_vals = np.random.choice(unique_vals, 500, replace=False)

            # Pairwise similarity — O(n²) on limited unique values
            near_pairs: list[tuple[str, str, float]] = []
            vals_list = [str(v) for v in unique_vals]
            for i in range(len(vals_list)):
                for j in range(i + 1, len(vals_list)):
                    sim = jellyfish.jaro_winkler_similarity(vals_list[i], vals_list[j])
                    if 0.85 <= sim < 1.0:
                        near_pairs.append((vals_list[i], vals_list[j], sim))

            if near_pairs:
                # Group into clusters (simple single-linkage)
                cluster_map: dict[str, int] = {}
                cluster_id = 0
                for v1, v2, sim in near_pairs:
                    c_id1 = cluster_map.get(v1)
                    c_id2 = cluster_map.get(v2)
                    if c_id1 is None and c_id2 is None:
                        cluster_map[v1] = cluster_id
                        cluster_map[v2] = cluster_id
                        cluster_id += 1
                    elif c_id1 is not None and c_id2 is None:
                        cluster_map[v2] = c_id1
                    elif c_id2 is not None and c_id1 is None:
                        cluster_map[v1] = c_id2
                    # both assigned — skip merging for simplicity

                # Build clusters
                from collections import defaultdict
                inv_map: dict[int, list[str]] = defaultdict(list)
                for val, cid in cluster_map.items():
                    inv_map[cid].append(val)

                for cid, members in inv_map.items():
                    if len(members) < 2:
                        continue
                    # Count affected rows
                    affected = int(self.df[col].isin(members).sum())
                    cluster = DuplicateGroup(
                        duplicate_type=DuplicateType.NEAR_DUPLICATE,
                        group_id=cid,
                        size=len(members),
                        columns_involved=[col],
                        description=f"Near-duplicate values in '{col}': {members[:5]}",
                        sample_rows=[{"value": m} for m in members[:10]],
                    )
                    clusters.append(cluster)

                if clusters:
                    total_affected = sum(
                        int(self.df[col].isin(
                            [m for c in clusters for m in c.sample_rows]
                        ).sum())
                        for _ in [1]  # single pass
                    )
                    action = CleaningAction(
                        category=ActionCategory.DUPLICATES,
                        action_type=ActionType.MERGE_NEAR_DUPLICATES,
                        confidence=ActionConfidence.JUDGMENT_CALL,
                        evidence=f"Found {len(clusters)} clusters of near-duplicate values in column '{col}'.",
                        recommendation=f"Review and standardize near-duplicate values in '{col}'.",
                        reasoning="Inconsistent representations of the same entity inflate cardinality and confuse downstream models.",
                        target_columns=[col],
                        impact=ImpactEstimate(
                            rows_before=self.n_rows, rows_after=self.n_rows,
                            description=f"{len(clusters)} value clusters to review in '{col}'.",
                        ),
                        metadata={"clusters": [c.model_dump() for c in clusters[:10]]},
                    )
                    actions.append(action)

        return actions, clusters
