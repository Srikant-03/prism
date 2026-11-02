"""
profiling/formula_detector.py
==============================
Detects deterministic mathematical formula dependencies between numeric columns
in a dataset (e.g., profit = revenue - cost, revenue = units_sold * avg_order_value,
tax = subtotal * tax_pct / 100).

Relationships detected:
- A = B + C  (Addition)
- A = B - C  (Subtraction)
- A = B * C  (Multiplication)
- A = B / C  (Division)
- Common percentage formulas:
  - A = B * C / 100
  - A = B * (1 - C / 100)
  - A = B * (1 + C / 100)
  - A = (B / C) * 100
  - A = ((B - C) / C) * 100
  - A = ((B - C) / B) * 100

Terminology & Target Interpretation:
- If target == dependent (e.g., target == profit):
  Classify using operands to predict target as DIRECT TARGET LEAKAGE.
- If target != dependent (e.g., target == prospect_count or None):
  Classify as DETERMINISTIC REDUNDANCY / MATHEMATICAL DEPENDENCY (never data leakage).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple
import numpy as np
import pandas as pd


@dataclass
class DeterministicFormula:
    dependent: str            # Reconstructed column (e.g., "profit")
    operands: List[str]       # Component columns (e.g., ["revenue", "cost"])
    operation: str           # "subtraction", "addition", "multiplication", "division", "percentage"
    formula_expr: str        # Human-readable formula (e.g., "profit = revenue - cost")
    description: str         # Canonical description: "`profit` is deterministically derived from `revenue` and `cost`."
    match_ratio: float       # Fraction of valid rows satisfying formula (e.g., 1.0)
    sample_size: int         # Number of valid non-null rows evaluated
    tolerance: float = 1e-2  # Absolute tolerance applied for float comparisons


# Keywords suggesting a column is typically a derived/calculated metric
_DERIVED_KEYWORDS = (
    "profit", "net", "margin", "tax", "pct", "percent", "discount",
    "fee", "total", "sum", "balance", "delta", "diff", "remainder",
    "growth", "yield", "markup", "subtotal"
)


def _score_derived_name(col: str) -> int:
    """Return a score indicating how likely a column name represents a derived metric."""
    c_lower = col.lower()
    score = 0
    for kw in _DERIVED_KEYWORDS:
        if kw in c_lower:
            score += 2
    return score


def _clean_numeric_series(series: pd.Series) -> pd.Series:
    """Convert a series to clean numeric floats."""
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    cleaned = series.astype(str).str.replace(r"[$,€£%\s]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


def check_formula_match(
    s_a: pd.Series,
    expected_a: pd.Series,
    atol: float = 1e-2,
    rtol: float = 1e-3,
    min_match_ratio: float = 0.98,
    min_rows: int = 5,
) -> Tuple[bool, float, int]:
    """
    Check if series s_a matches expected_a within floating-point tolerance.
    """
    valid = s_a.notna() & expected_a.notna() & np.isfinite(s_a) & np.isfinite(expected_a)
    n_valid = int(valid.sum())
    if n_valid < min_rows:
        return False, 0.0, n_valid

    a_vals = s_a[valid].to_numpy(dtype=float)
    exp_vals = expected_a[valid].to_numpy(dtype=float)

    # 1. Non-zero values of a_vals must actually match exp_vals
    a_nonzero = (a_vals != 0.0)
    if np.sum(a_nonzero) > 0:
        nonzero_matches = np.isclose(a_vals[a_nonzero], exp_vals[a_nonzero], atol=atol, rtol=rtol)
        if np.mean(nonzero_matches) < min_match_ratio:
            return False, float(np.mean(nonzero_matches)), n_valid

    # 2. Guard against zero-dominated false positives:
    # If a column is >80% zeros, expected values for zero rows must also be actually zero
    a_zero = (a_vals == 0.0)
    if np.mean(a_zero) > 0.80 and np.sum(a_zero) > 0:
        zero_matches = np.isclose(exp_vals[a_zero], 0.0, atol=1e-4)
        if np.mean(zero_matches) < min_match_ratio:
            return False, float(np.mean(zero_matches)), n_valid

    matches = np.isclose(a_vals, exp_vals, atol=atol, rtol=rtol)
    match_ratio = float(np.mean(matches))

    return match_ratio >= min_match_ratio, match_ratio, n_valid


def detect_three_variable_formulas(
    df: pd.DataFrame,
    col_a: str,
    col_b: str,
    col_c: str,
    atol: float = 1e-2,
    rtol: float = 1e-3,
    min_match_ratio: float = 0.98,
    target_column: Optional[str] = None,
) -> List[DeterministicFormula]:
    """
    Check all permutations of 3 columns (col_a, col_b, col_c) for deterministic formulas.
    """
    cols = [col_a, col_b, col_c]
    if len(set(cols)) != 3 or any(c not in df.columns for c in cols):
        return []

    s_map = {c: _clean_numeric_series(df[c]) for c in cols}
    # Check that columns have variance
    for c, s in s_map.items():
        if s.dropna().nunique() <= 1:
            return []

    found: List[DeterministicFormula] = []

    # Permutations: choose one as dependent (A), and the other two as operands (B, C)
    for a in cols:
        b_c_candidates = [c for c in cols if c != a]
        b, c = b_c_candidates[0], b_c_candidates[1]
        s_a = s_map[a]

        # Try both (b, c) and (c, b) for non-commutative operations
        operand_orderings = [(b, c), (c, b)]

        # 1. Addition: A = B + C (commutative)
        exp_add = s_map[b] + s_map[c]
        ok, ratio, count = check_formula_match(s_a, exp_add, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
        if ok:
            found.append(DeterministicFormula(
                dependent=a,
                operands=sorted([b, c]),
                operation="addition",
                formula_expr=f"{a} = {b} + {c}",
                description=f"`{a}` is deterministically derived from `{b}` and `{c}`.",
                match_ratio=ratio,
                sample_size=count,
                tolerance=atol,
            ))

        # 2. Subtraction: A = B - C and A = C - B
        for op1, op2 in operand_orderings:
            exp_sub = s_map[op1] - s_map[op2]
            ok, ratio, count = check_formula_match(s_a, exp_sub, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="subtraction",
                    formula_expr=f"{a} = {op1} - {op2}",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

        # 3. Multiplication: A = B * C (commutative)
        exp_mul = s_map[b] * s_map[c]
        ok, ratio, count = check_formula_match(s_a, exp_mul, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
        if ok:
            found.append(DeterministicFormula(
                dependent=a,
                operands=sorted([b, c]),
                operation="multiplication",
                formula_expr=f"{a} = {b} * {c}",
                description=f"`{a}` is deterministically derived from `{b}` and `{c}`.",
                match_ratio=ratio,
                sample_size=count,
                tolerance=atol,
            ))

        # 4. Division: A = B / C and A = C / B
        for op1, op2 in operand_orderings:
            s_op2 = s_map[op2].replace(0, np.nan)
            with np.errstate(divide="ignore", invalid="ignore"):
                exp_div = s_map[op1] / s_op2
            ok, ratio, count = check_formula_match(s_a, exp_div, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="division",
                    formula_expr=f"{a} = {op1} / {op2}",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

        # 5. Percentage formulas
        for op1, op2 in operand_orderings:
            # 5a. A = op1 * (op2 / 100)
            exp_pct1 = s_map[op1] * (s_map[op2] / 100.0)
            ok, ratio, count = check_formula_match(s_a, exp_pct1, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="percentage",
                    formula_expr=f"{a} = {op1} * ({op2} / 100)",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

            # 5b. A = op1 * (1 - op2 / 100) (Discounted price)
            exp_pct2 = s_map[op1] * (1.0 - s_map[op2] / 100.0)
            ok, ratio, count = check_formula_match(s_a, exp_pct2, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="percentage",
                    formula_expr=f"{a} = {op1} * (1 - {op2} / 100)",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

            # 5b_dec. A = op1 * (1 - op2) (Discounted price with decimal rate, e.g. discount_rate = 0.15)
            exp_pct2_dec = s_map[op1] * (1.0 - s_map[op2])
            ok, ratio, count = check_formula_match(s_a, exp_pct2_dec, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="percentage",
                    formula_expr=f"{a} = {op1} * (1 - {op2})",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

            # 5c. A = op1 * (1 + op2 / 100) (Tax / Markup)
            exp_pct3 = s_map[op1] * (1.0 + s_map[op2] / 100.0)
            ok, ratio, count = check_formula_match(s_a, exp_pct3, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="percentage",
                    formula_expr=f"{a} = {op1} * (1 + {op2} / 100)",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

            # 5c_dec. A = op1 * (1 + op2) (Tax / Markup with decimal rate)
            exp_pct3_dec = s_map[op1] * (1.0 + s_map[op2])
            ok, ratio, count = check_formula_match(s_a, exp_pct3_dec, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="percentage",
                    formula_expr=f"{a} = {op1} * (1 + {op2})",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

            # 5d. A = (op1 / op2) * 100 (Ratio/Percentage)
            s_op2 = s_map[op2].replace(0, np.nan)
            with np.errstate(divide="ignore", invalid="ignore"):
                exp_pct4 = (s_map[op1] / s_op2) * 100.0
            ok, ratio, count = check_formula_match(s_a, exp_pct4, atol=atol, rtol=rtol, min_match_ratio=min_match_ratio)
            if ok:
                found.append(DeterministicFormula(
                    dependent=a,
                    operands=[op1, op2],
                    operation="percentage",
                    formula_expr=f"{a} = ({op1} / {op2}) * 100",
                    description=f"`{a}` is deterministically derived from `{op1}` and `{op2}`.",
                    match_ratio=ratio,
                    sample_size=count,
                    tolerance=atol,
                ))

    if not found:
        return []

    # Filter/sort to canonical representation
    return _pick_canonical_formulas(found, target_column=target_column)


def _pick_canonical_formulas(
    formulas: List[DeterministicFormula],
    target_column: Optional[str] = None,
) -> List[DeterministicFormula]:
    """
    Select the canonical formula from equivalent algebraic expressions
    (e.g., profit = revenue - cost vs revenue = profit + cost).
    """
    if len(formulas) <= 1:
        return formulas

    # If target_column matches one formula's dependent, prioritize that one!
    if target_column:
        target_matches = [f for f in formulas if f.dependent == target_column]
        if target_matches:
            # Subtraction/multiplication preferred if multiple
            target_matches.sort(
                key=lambda f: (
                    0 if f.operation in ("subtraction", "percentage") else 1,
                    -f.match_ratio
                )
            )
            return [target_matches[0]]

    # Otherwise, score by semantic naming and operation type:
    def formula_priority(f: DeterministicFormula) -> Tuple[int, int, float]:
        dep_l = f.dependent.lower()
        score = 0

        # High-affinity semantic matches for specific mathematical operations
        if f.operation == "subtraction" and any(k in dep_l for k in ["profit", "net", "diff", "difference", "margin", "loss", "rem"]):
            score += 20
        elif f.operation == "addition" and any(k in dep_l for k in ["total", "sum", "gross", "overall", "aggregate"]) and "sub" not in dep_l:
            score += 20
        elif f.operation == "division" and any(k in dep_l for k in ["speed", "rate", "avg", "average", "ratio", "per", "density"]):
            score += 20
        elif f.operation == "percentage" and any(k in dep_l for k in ["amount", "disc", "discount", "tax", "fee", "tip", "interest"]) and "pct" not in dep_l and "rate" not in dep_l:
            score += 20
        elif f.operation == "percentage" and any(k in dep_l for k in ["net", "final", "total", "pay"]):
            score += 18
        elif f.operation == "multiplication" and any(k in dep_l for k in ["revenue", "sales", "volume", "area"]):
            score += 15
        elif f.operation == "percentage":
            score += 12
        elif f.operation == "subtraction":
            score += 8
        elif f.operation == "multiplication":
            score += 6
        elif f.operation == "addition":
            score += 4
        elif f.operation == "division":
            score += 3

        name_score = _score_derived_name(f.dependent)
        return (score + name_score, int(f.sample_size), f.match_ratio)

    formulas.sort(key=formula_priority, reverse=True)
    return [formulas[0]]


def detect_formula_dependencies(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    atol: float = 1e-2,
    rtol: float = 1e-3,
    min_match_ratio: float = 0.98,
    target_column: Optional[str] = None,
) -> List[DeterministicFormula]:
    """
    Scan numeric columns in df for deterministic mathematical dependencies.

    Returns:
        List of unique DeterministicFormula instances found.
    """
    if columns is None:
        num_cols = []
        for c in df.columns:
            if pd.api.types.is_numeric_dtype(df[c]):
                if df[c].dropna().nunique() > 1:
                    num_cols.append(c)
            else:
                # Check if parseable as numeric
                sample = df[c].dropna().head(30)
                if len(sample) >= 5:
                    cleaned = sample.astype(str).str.replace(r"[$,€£%\s]", "", regex=True)
                    try:
                        parsed = pd.to_numeric(cleaned, errors="coerce")
                        if parsed.notna().mean() >= 0.8 and parsed.nunique() > 1:
                            num_cols.append(c)
                    except Exception:
                        pass
        columns = num_cols

    if len(columns) < 3:
        return []

    results: List[DeterministicFormula] = []
    seen_triplets = set()

    # Prioritize triplets containing the target_column first if specified
    all_triplets = []
    n = len(columns)
    for i in range(n):
        for j in range(i + 1, n):
            for k in range(j + 1, n):
                triplet = frozenset([columns[i], columns[j], columns[k]])
                if triplet not in seen_triplets:
                    seen_triplets.add(triplet)
                    all_triplets.append((columns[i], columns[j], columns[k]))

    if target_column:
        all_triplets.sort(key=lambda t: 0 if target_column in t else 1)

    seen_dependent_operand_pairs = set()

    for col_a, col_b, col_c in all_triplets:
        formulas = detect_three_variable_formulas(
            df, col_a, col_b, col_c,
            atol=atol, rtol=rtol,
            min_match_ratio=min_match_ratio,
            target_column=target_column,
        )
        for f in formulas:
            key = (f.dependent, frozenset(f.operands))
            if key not in seen_dependent_operand_pairs:
                seen_dependent_operand_pairs.add(key)
                results.append(f)

    return results
