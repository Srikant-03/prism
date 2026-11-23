"""
profiling/functional_dependency.py
===================================
Detects deterministic functional dependencies and categorical redundancies
between features in a dataset.

Mathematical Definition:
A functional dependency X -> Y holds on dataset D if and only if every value of
X is associated with exactly one value of Y:
    nunique(Y | X) == 1 for all distinct values of X.

Types:
1. Bijective (1-to-1): X -> Y AND Y -> X
   e.g. region <-> country (East -> Canada, North -> USA, South -> Mexico, West -> UK)
2. Directional (Many-to-1): X -> Y, but NOT Y -> X
   e.g. city -> country, subcategory -> category

Crucial Distinction:
A functional dependency between two features is NOT data leakage.
It is deterministic redundancy, useful for feature reduction and dimensionality reduction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List
import pandas as pd


@dataclass
class FunctionalDependencyResult:
    determinant: str        # X: the determining feature (e.g. "region")
    dependent: str          # Y: the determined feature (e.g. "country")
    is_bijective: bool      # True if 1-to-1 (both X -> Y and Y -> X hold)
    description: str
    recommendation: str
    purity: float = 1.0     # Purity of dependency (1.0 for deterministic)
    cardinality_det: int = 0
    cardinality_dep: int = 0


def detect_functional_dependency(
    df: pd.DataFrame,
    col1: str,
    col2: str,
) -> Optional[FunctionalDependencyResult]:
    """
    Check if a deterministic functional dependency exists between col1 and col2.

    Returns:
        FunctionalDependencyResult if a dependency exists, else None.
    """
    if col1 == col2 or col1 not in df.columns or col2 not in df.columns:
        return None

    # Drop rows where either column is null
    sub = df[[col1, col2]].dropna()
    if len(sub) < 2:
        return None

    # Continuous floats are continuous variables analyzed via correlation, not discrete functional dependencies
    c1_is_float = pd.api.types.is_float_dtype(sub[col1])
    c2_is_float = pd.api.types.is_float_dtype(sub[col2])
    if c1_is_float and c2_is_float:
        return None

    n_c1 = sub[col1].nunique()
    n_c2 = sub[col2].nunique()

    # Avoid constant columns (carried by zero-variance filter)
    if n_c1 <= 1 or n_c2 <= 1:
        return None

    # If every row is uniquely numbered/valued, any pair trivially maps 1-to-1.
    # Meaningful redundancy requires repetitions or categorical types.
    if n_c1 == len(sub) and n_c2 == len(sub):
        if pd.api.types.is_numeric_dtype(sub[col1]) or pd.api.types.is_numeric_dtype(sub[col2]):
            return None

    # Check col1 -> col2
    c1_determines_c2 = bool((sub.groupby(col1)[col2].nunique() <= 1).all())
    # Check col2 -> col1
    c2_determines_c1 = bool((sub.groupby(col2)[col1].nunique() <= 1).all())

    if not c1_determines_c2 and not c2_determines_c1:
        return None

    if c1_determines_c2 and c2_determines_c1:
        # Bijective (1-to-1) mapping.
        # Preserve caller's requested direction (col1 -> col2) or canonical order.
        determinant = col1
        dependent = col2
        is_bijective = True
    elif c1_determines_c2:
        # Many-to-1: col1 -> col2
        determinant = col1
        dependent = col2
        is_bijective = False
    else:
        # Many-to-1: col2 -> col1
        determinant = col2
        dependent = col1
        is_bijective = False

    desc = f"`{dependent}` is deterministically determined by `{determinant}` in this dataset. Keeping both may introduce redundant information."
    rec = f"Consider dropping `{dependent}` (or `{determinant}`) to eliminate redundant information and reduce dimensionality."

    return FunctionalDependencyResult(
        determinant=determinant,
        dependent=dependent,
        is_bijective=is_bijective,
        description=desc,
        recommendation=rec,
        purity=1.0,
        cardinality_det=n_c1 if determinant == col1 else n_c2,
        cardinality_dep=n_c2 if dependent == col2 else n_c1,
    )


def find_all_functional_dependencies(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
) -> List[FunctionalDependencyResult]:
    """
    Scan all pairs among given columns (or all categorical/discrete columns)
    and return all detected functional dependencies.
    """
    if columns is None:
        # Select categorical, object, and discrete columns
        columns = [
            c for c in df.columns
            if df[c].dtype == object or str(df[c].dtype) == "category" or df[c].nunique() <= 100
        ]

    results: List[FunctionalDependencyResult] = []
    seen_pairs = set()

    for i in range(len(columns)):
        col1 = columns[i]
        for j in range(i + 1, len(columns)):
            col2 = columns[j]
            pair_key = frozenset([col1, col2])
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            fd = detect_functional_dependency(df, col1, col2)
            if fd:
                results.append(fd)

    return results
