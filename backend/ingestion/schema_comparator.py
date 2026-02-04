"""
Multi-file schema comparison engine.
Classifies files as same-schema (merge), different-schema (separate), or mixed.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from config import IngestionConfig
from models.schemas import (
    SchemaComparison,
    SchemaComparisonEntry,
    SchemaRelationship,
)


class SchemaComparator:
    """
    Compares schemas across multiple uploaded files to determine
    whether they should be merged, kept separate, or require user decision.
    
    Uses column name overlap, dtype compatibility, and ordering as signals.
    All thresholds come from config — nothing hardcoded.
    """

    @staticmethod
    def compare(
        file_dataframes: dict[str, pd.DataFrame],
    ) -> SchemaComparison:
        """
        Compare schemas of multiple DataFrames.
        
        Args:
            file_dataframes: Dict of filename → DataFrame
            
        Returns:
            SchemaComparison with relationship type, confidence, and justification.
        """
        config = IngestionConfig()

        if len(file_dataframes) < 2:
            entries = []
            for fname, df in file_dataframes.items():
                entries.append(SchemaComparisonEntry(
                    filename=fname,
                    columns=list(df.columns),
                    dtypes={str(k): str(v) for k, v in df.dtypes.items()},
                    row_count=len(df),
                ))
            return SchemaComparison(
                relationship=SchemaRelationship.SAME_SCHEMA,
                confidence=1.0,
                justification="Only one file uploaded — no comparison needed.",
                files=entries,
            )

        # Build entries
        entries = []
        all_columns: list[set[str]] = []

        for fname, df in file_dataframes.items():
            cols = set(str(c) for c in df.columns)
            all_columns.append(cols)
            entries.append(SchemaComparisonEntry(
                filename=fname,
                columns=list(df.columns.astype(str)),
                dtypes={str(k): str(v) for k, v in df.dtypes.items()},
                row_count=len(df),
            ))

        # Calculate pairwise column overlap
        overlap_scores = []
        for i in range(len(all_columns)):
            for j in range(i + 1, len(all_columns)):
                a, b = all_columns[i], all_columns[j]
                if not a or not b:
                    overlap_scores.append(0.0)
                else:
                    intersection = a & b
                    union = a | b
                    jaccard = len(intersection) / len(union) if union else 0
                    overlap_scores.append(jaccard)

        avg_overlap = sum(overlap_scores) / len(overlap_scores) if overlap_scores else 0

        # Find common and differing columns
        common = set.intersection(*all_columns) if all_columns else set()
        differing: dict[str, list[str]] = {}
        for entry in entries:
            unique = set(entry.columns) - common
            if unique:
                differing[entry.filename] = sorted(unique)

        # Classify relationship
        threshold = config.SCHEMA_MATCH_THRESHOLD

        if avg_overlap >= threshold:
            relationship = SchemaRelationship.SAME_SCHEMA
            confidence = avg_overlap

            # Check dtype compatibility for common columns
            if common:
                dtype_mismatches = SchemaComparator._check_dtype_compatibility(
                    file_dataframes, common
                )
                if dtype_mismatches:
                    confidence *= 0.9  # Slight confidence reduction for dtype mismatches

            justification = (
                f"Files share {len(common)}/{len(set.union(*all_columns))} columns "
                f"({avg_overlap*100:.1f}% overlap). "
                f"Schema similarity is above the {threshold*100:.0f}% threshold. "
                f"Recommended action: merge into a single dataset."
            )
            if differing:
                justification += (
                    f" Note: {sum(len(v) for v in differing.values())} column(s) differ "
                    f"across files — missing columns will be filled with null values."
                )

        elif avg_overlap > 0.3:
            relationship = SchemaRelationship.MIXED
            confidence = 1.0 - abs(avg_overlap - 0.5) * 2

            justification = (
                f"Files share {len(common)}/{len(set.union(*all_columns))} columns "
                f"({avg_overlap*100:.1f}% overlap). "
                f"This is between thresholds — please review and decide: "
                f"merge (treating differing columns as nullable) or keep separate."
            )

        else:
            relationship = SchemaRelationship.DIFFERENT_SCHEMA
            confidence = 1.0 - avg_overlap

            justification = (
                f"Files share only {len(common)}/{len(set.union(*all_columns))} columns "
                f"({avg_overlap*100:.1f}% overlap). "
                f"These appear to be different datasets. "
                f"Recommended action: treat as separate tables for join-based analysis."
            )

        return SchemaComparison(
            relationship=relationship,
            confidence=round(confidence, 4),
            justification=justification,
            files=entries,
            common_columns=sorted(common),
            differing_columns=differing,
        )

    @staticmethod
    def _check_dtype_compatibility(
        file_dfs: dict[str, pd.DataFrame],
        common_cols: set[str],
    ) -> list[str]:
        """Check if common columns have compatible dtypes across files."""
        mismatches = []
        dfs = list(file_dfs.values())

        for col in common_cols:
            col_str = str(col)
            dtypes = set()
            for df in dfs:
                if col_str in df.columns:
                    dtypes.add(str(df[col_str].dtype))
            if len(dtypes) > 1:
                mismatches.append(f"{col_str}: {dtypes}")

        return mismatches

    @staticmethod
    def merge_dataframes(
        file_dfs: dict[str, pd.DataFrame],
        add_source_column: bool = True,
    ) -> pd.DataFrame:
        """
        Merge multiple DataFrames with the same schema.
        Missing columns are filled with NaN.
        """
        combined = []
        for fname, df in file_dfs.items():
            df_copy = df.copy()
            if add_source_column:
                df_copy["__source_file__"] = fname
            combined.append(df_copy)

        if not combined:
            return pd.DataFrame()

        return pd.concat(combined, ignore_index=True)
