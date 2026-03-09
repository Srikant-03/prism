"""
Hypothesis Testing API — Surfaces the top statistically interesting
column-pair relationships with p-values, effect sizes, and plain-English interpretations.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import numpy as np
import pandas as pd

router = APIRouter(prefix="/api/hypothesis-testing", tags=["hypothesis-testing"])


class TestRequest(BaseModel):
    file_id: str
    limit: int = 10


@router.post("/auto-discover")
async def auto_discover(request: TestRequest):
    """Auto-discover the top statistically interesting column-pair relationships."""
    from state import get_df

    df = get_df(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    results = []
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    # ── 1. Numeric × Numeric: Pearson correlation + significance ──
    if len(numeric_cols) >= 2:
        from scipy.stats import pearsonr, spearmanr

        for i, col_a in enumerate(numeric_cols[:15]):
            for col_b in numeric_cols[i + 1:15]:
                valid = df[[col_a, col_b]].dropna()
                if len(valid) < 10:
                    continue

                r_val, p_val = pearsonr(valid[col_a], valid[col_b])
                rho, _ = spearmanr(valid[col_a], valid[col_b])

                if abs(r_val) < 0.2:
                    continue  # Skip weak relationships

                strength = "strong" if abs(r_val) > 0.7 else "moderate" if abs(r_val) > 0.4 else "weak"
                direction = "positive" if r_val > 0 else "negative"

                results.append({
                    "test": "Pearson Correlation",
                    "columns": [col_a, col_b],
                    "statistic": round(float(r_val), 4),
                    "p_value": round(float(p_val), 6),
                    "effect_size": round(float(r_val ** 2), 4),
                    "significant": p_val < 0.05,
                    "interpretation": (
                        f"There is a {strength} {direction} linear relationship between "
                        f"'{col_a}' and '{col_b}' (r={r_val:.3f}, p={p_val:.4f}). "
                        f"This means that as '{col_a}' increases, '{col_b}' tends to "
                        f"{'increase' if r_val > 0 else 'decrease'}."
                    ),
                    "spearman_rho": round(float(rho), 4),
                })

    # ── 2. Cat × Numeric: ANOVA / Kruskal-Wallis ──────────────────
    if cat_cols and numeric_cols:
        from scipy.stats import f_oneway, kruskal

        for cat_col in cat_cols[:8]:
            n_groups = df[cat_col].nunique()
            if n_groups < 2 or n_groups > 20:
                continue

            for num_col in numeric_cols[:10]:
                groups = [g[num_col].dropna().values for _, g in df.groupby(cat_col)]
                groups = [g for g in groups if len(g) >= 3]

                if len(groups) < 2:
                    continue

                try:
                    f_stat, p_val = f_oneway(*groups)

                    if p_val > 0.1:
                        continue

                    # Effect size: eta-squared
                    ss_between = sum(len(g) * (np.mean(g) - df[num_col].mean()) ** 2 for g in groups)
                    ss_total = sum((v - df[num_col].mean()) ** 2 for g in groups for v in g)
                    eta_sq = ss_between / ss_total if ss_total > 0 else 0

                    effect = "large" if eta_sq > 0.14 else "medium" if eta_sq > 0.06 else "small"

                    results.append({
                        "test": "One-Way ANOVA",
                        "columns": [cat_col, num_col],
                        "statistic": round(float(f_stat), 4),
                        "p_value": round(float(p_val), 6),
                        "effect_size": round(float(eta_sq), 4),
                        "significant": p_val < 0.05,
                        "interpretation": (
                            f"The average '{num_col}' differs significantly across categories "
                            f"of '{cat_col}' (F={f_stat:.2f}, p={p_val:.4f}). "
                            f"The effect size is {effect} (η²={eta_sq:.3f}), meaning "
                            f"'{cat_col}' explains ~{eta_sq * 100:.1f}% of the variance in '{num_col}'."
                        ),
                    })
                except Exception:
                    continue

    # ── 3. Cat × Cat: Chi-squared independence ────────────────────
    if len(cat_cols) >= 2:
        from scipy.stats import chi2_contingency

        for i, col_a in enumerate(cat_cols[:8]):
            if df[col_a].nunique() > 30:
                continue
            for col_b in cat_cols[i + 1:8]:
                if df[col_b].nunique() > 30:
                    continue

                try:
                    contingency = pd.crosstab(df[col_a], df[col_b])
                    if contingency.shape[0] < 2 or contingency.shape[1] < 2:
                        continue

                    chi2, p_val, dof, _ = chi2_contingency(contingency)
                    if p_val > 0.1:
                        continue

                    # Cramér's V as effect size
                    n = contingency.sum().sum()
                    k = min(contingency.shape) - 1
                    cramers_v = np.sqrt(chi2 / (n * k)) if n * k > 0 else 0

                    effect = "strong" if cramers_v > 0.5 else "moderate" if cramers_v > 0.3 else "weak"

                    results.append({
                        "test": "Chi-Squared Independence",
                        "columns": [col_a, col_b],
                        "statistic": round(float(chi2), 4),
                        "p_value": round(float(p_val), 6),
                        "effect_size": round(float(cramers_v), 4),
                        "significant": p_val < 0.05,
                        "interpretation": (
                            f"There is a statistically significant association between "
                            f"'{col_a}' and '{col_b}' (χ²={chi2:.2f}, p={p_val:.4f}). "
                            f"The {effect} association (Cramér's V={cramers_v:.3f}) suggests "
                            f"these categories are not independent."
                        ),
                    })
                except Exception:
                    continue

    # Sort by significance and effect size
    results.sort(key=lambda r: (-int(r["significant"]), -abs(r["effect_size"])))

    return {
        "file_id": request.file_id,
        "tests": results[:request.limit],
        "total_found": len(results),
        "columns_analyzed": {
            "numeric": len(numeric_cols),
            "categorical": len(cat_cols),
        },
    }


class ManualTestRequest(BaseModel):
    file_id: str
    test_name: str
    columns: list[str]


@router.post("/run")
async def run_test(request: ManualTestRequest):
    """Run a specific statistical test on specified columns."""
    from state import get_df
    from insights.stat_tests import run_test as execute_test

    df = get_df(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Build data dict expected by stat_tests module
    data: dict = {}
    for col in request.columns:
        if col not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{col}' not found")
        data[col] = df[col].dropna().values.tolist()

    try:
        result = execute_test(request.test_name, data)
        return {"test": request.test_name, "columns": request.columns, **result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/suggest/{file_id}")
async def suggest_tests(file_id: str):
    """Suggest appropriate statistical tests based on column types."""
    from state import get_df
    from insights.stat_tests import suggest_tests as get_suggestions

    df = get_df(file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        col_type = "numeric" if pd.api.types.is_numeric_dtype(df[col]) else "categorical"
        columns.append({"name": col, "dtype": dtype, "type": col_type})

    suggestions = get_suggestions(columns)
    return {"file_id": file_id, "suggestions": suggestions}
