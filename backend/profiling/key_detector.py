"""
Key detector — identifies primary key candidates, foreign key candidates,
and ID columns (high uniqueness, no analytical value).
"""

from __future__ import annotations

from itertools import combinations
from typing import Any

import pandas as pd
import numpy as np

from profiling.profiling_models import KeyCandidate


class KeyDetector:
    """
    Detects:
    - Primary key candidates: columns/combos with 100% uniqueness
    - Foreign key candidates: columns whose values are a subset of another column's values
    - ID columns: high uniqueness, no analytical value (flagged for exclusion)
    """

    @staticmethod
    def detect_primary_keys(df: pd.DataFrame) -> list[KeyCandidate]:
        """Find columns or column combinations with 100% uniqueness."""
        if df.empty:
            return []

        candidates: list[KeyCandidate] = []
        n = len(df)

        # Single-column primary keys
        for col in df.columns:
            if df[col].nunique() == n and df[col].notna().all():
                candidates.append(KeyCandidate(
                    columns=[col],
                    uniqueness=1.0,
                    justification=(
                        f"Column '{col}' has 100% unique values with no nulls "
                        f"across all {n:,} rows — strong primary key candidate."
                    ),
                ))

        # If no single-column PK, try 2-column combinations (capped)
        if not candidates and len(df.columns) <= 30:
            non_null_cols = [c for c in df.columns if df[c].notna().all()]
            for combo in combinations(non_null_cols[:15], 2):
                combined = df[list(combo)].apply(tuple, axis=1)
                if combined.nunique() == n:
                    candidates.append(KeyCandidate(
                        columns=list(combo),
                        uniqueness=1.0,
                        justification=(
                            f"Combination of ({', '.join(combo)}) provides "
                            f"100% unique values — composite primary key candidate."
                        ),
                    ))
                    if len(candidates) >= 5:
                        break

        return candidates

    @staticmethod
    def detect_foreign_keys(df: pd.DataFrame) -> list[KeyCandidate]:
        """
        Find columns whose values are a subset of another column's values.
        These are potential join keys across related tables.
        """
        if df.empty or len(df.columns) < 2:
            return []

        candidates: list[KeyCandidate] = []

        # Only check columns with reasonable cardinality
        eligible = []
        for col in df.columns:
            nunique = df[col].nunique()
            if 2 < nunique < len(df) * 0.8:
                eligible.append(col)

        # Compare each pair
        for i, col_a in enumerate(eligible[:20]):
            values_a = set(df[col_a].dropna().unique())
            if len(values_a) == 0:
                continue

            for col_b in eligible[i + 1:20]:
                values_b = set(df[col_b].dropna().unique())
                if len(values_b) == 0:
                    continue

                # Check if A is subset of B
                if values_a < values_b:
                    overlap = len(values_a) / len(values_b) if values_b else 0
                    candidates.append(KeyCandidate(
                        columns=[col_a, col_b],
                        uniqueness=overlap,
                        justification=(
                            f"All {len(values_a)} unique values in '{col_a}' exist in "
                            f"'{col_b}' ({len(values_b)} unique values). "
                            f"Potential join key relationship."
                        ),
                    ))

                # Check if B is subset of A
                elif values_b < values_a:
                    overlap = len(values_b) / len(values_a) if values_a else 0
                    candidates.append(KeyCandidate(
                        columns=[col_b, col_a],
                        uniqueness=overlap,
                        justification=(
                            f"All {len(values_b)} unique values in '{col_b}' exist in "
                            f"'{col_a}' ({len(values_a)} unique values). "
                            f"Potential join key relationship."
                        ),
                    ))

        return candidates[:10]  # Cap results

    @staticmethod
    def detect_id_columns(df: pd.DataFrame) -> list[str]:
        """
        Detect columns that are likely identifiers with no analytical value.
        Criteria: high uniqueness, no meaningful statistical distribution.
        """
        if df.empty:
            return []

        id_columns: list[str] = []
        n = len(df)

        for col in df.columns:
            col_lower = col.lower()
            nunique = df[col].nunique()
            distinct_ratio = nunique / n if n > 0 else 0

            # Name-based ID signals
            is_id_name = any(h in col_lower for h in [
                "_id", "id_", "guid", "uuid", "key", "index",
            ]) or col_lower in {"id", "pk", "rowid", "row_id"}

            # High uniqueness
            is_high_unique = distinct_ratio > 0.95

            # Sequential integer check
            is_sequential = False
            if pd.api.types.is_integer_dtype(df[col]):
                sorted_vals = df[col].dropna().sort_values().values
                if len(sorted_vals) > 10:
                    diffs = np.diff(sorted_vals)
                    if len(diffs) > 0 and np.median(diffs) == 1:
                        is_sequential = True

            # Classify as ID if multiple signals match
            if is_id_name and (is_high_unique or is_sequential):
                id_columns.append(col)
            elif is_high_unique and is_sequential and n > 20:
                id_columns.append(col)

        return id_columns
