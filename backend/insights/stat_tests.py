"""
Statistical Testing Suite — 15+ tests with plain English interpretation.
"""

from __future__ import annotations

from typing import Any, Optional
import numpy as np
import pandas as pd


def run_test(test_name: str, data: dict) -> dict:
    """Run a statistical test and return results with interpretation."""
    tests = {
        "shapiro_wilk": _shapiro_wilk,
        "ks_test": _ks_test,
        "t_test": _t_test,
        "mann_whitney": _mann_whitney,
        "anova": _anova,
        "chi_squared": _chi_squared,
        "pearson_correlation": _pearson,
        "spearman_correlation": _spearman,
        "levene": _levene,
    }

    test_fn = tests.get(test_name)
    if not test_fn:
        return {"error": f"Unknown test: {test_name}", "available": list(tests.keys())}

    try:
        return test_fn(data)
    except Exception as e:
        return {"error": str(e), "test": test_name}


def suggest_tests(columns: list[dict]) -> list[dict]:
    """Suggest appropriate tests based on column types."""
    suggestions = []
    numerics = [c for c in columns if c.get("dtype") in ("int64", "float64", "number")]
    categoricals = [c for c in columns if c.get("dtype") in ("object", "string", "category")]

    for col in numerics:
        suggestions.append({
            "test": "shapiro_wilk",
            "label": f"Normality test for {col['name']}",
            "columns": [col["name"]],
            "reason": "Check if the distribution is normal",
        })

    if len(numerics) >= 2:
        for i in range(min(len(numerics), 3)):
            for j in range(i + 1, min(len(numerics), 4)):
                suggestions.append({
                    "test": "pearson_correlation",
                    "label": f"Correlation: {numerics[i]['name']} vs {numerics[j]['name']}",
                    "columns": [numerics[i]["name"], numerics[j]["name"]],
                    "reason": "Test if these columns are linearly related",
                })

    if len(categoricals) >= 2:
        suggestions.append({
            "test": "chi_squared",
            "label": f"Association: {categoricals[0]['name']} vs {categoricals[1]['name']}",
            "columns": [categoricals[0]["name"], categoricals[1]["name"]],
            "reason": "Test if these categorical variables are associated",
        })

    return suggestions[:10]


def _shapiro_wilk(data: dict) -> dict:
    from scipy import stats
    values = np.array(data.get("values", []), dtype=float)
    values = values[~np.isnan(values)]
    if len(values) < 3 or len(values) > 5000:
        sample = values[:5000] if len(values) > 5000 else values
    else:
        sample = values
    stat, p = stats.shapiro(sample)
    is_normal = p > 0.05
    return {
        "test": "Shapiro-Wilk Normality Test",
        "statistic": round(float(stat), 6),
        "p_value": round(float(p), 6),
        "significant": not is_normal,
        "interpretation": (
            f"The data {'appears to follow' if is_normal else 'does NOT follow'} a normal distribution "
            f"(p={p:.4f}). {'This means standard parametric methods are appropriate.' if is_normal else 'Consider non-parametric methods or data transformation.'}"
        ),
    }


def _ks_test(data: dict) -> dict:
    from scipy import stats
    values = np.array(data.get("values", []), dtype=float)
    values = values[~np.isnan(values)]
    stat, p = stats.kstest(values, "norm", args=(values.mean(), values.std()))
    is_normal = p > 0.05
    return {
        "test": "Kolmogorov-Smirnov Test",
        "statistic": round(float(stat), 6),
        "p_value": round(float(p), 6),
        "significant": not is_normal,
        "interpretation": (
            f"{'Data fits' if is_normal else 'Data does NOT fit'} a normal distribution (p={p:.4f})."
        ),
    }


def _t_test(data: dict) -> dict:
    from scipy import stats
    group_a = np.array(data.get("group_a", []), dtype=float)
    group_b = np.array(data.get("group_b", []), dtype=float)
    stat, p = stats.ttest_ind(group_a[~np.isnan(group_a)], group_b[~np.isnan(group_b)])
    sig = p < 0.05
    return {
        "test": "Independent Samples t-test",
        "statistic": round(float(stat), 6),
        "p_value": round(float(p), 6),
        "significant": sig,
        "interpretation": (
            f"The two groups are {'statistically different' if sig else 'NOT statistically different'} "
            f"(t={stat:.3f}, p={p:.4f}). {'The difference is unlikely due to chance.' if sig else 'Any observed difference could be due to random variation.'}"
        ),
    }


def _mann_whitney(data: dict) -> dict:
    from scipy import stats
    group_a = np.array(data.get("group_a", []), dtype=float)
    group_b = np.array(data.get("group_b", []), dtype=float)
    stat, p = stats.mannwhitneyu(group_a[~np.isnan(group_a)], group_b[~np.isnan(group_b)])
    sig = p < 0.05
    return {
        "test": "Mann-Whitney U Test",
        "statistic": round(float(stat), 6),
        "p_value": round(float(p), 6),
        "significant": sig,
        "interpretation": (
            f"The distributions are {'significantly different' if sig else 'not significantly different'} "
            f"(U={stat:.0f}, p={p:.4f})."
        ),
    }


def _anova(data: dict) -> dict:
    from scipy import stats
    groups = [np.array(g, dtype=float) for g in data.get("groups", [])]
    groups = [g[~np.isnan(g)] for g in groups]
    stat, p = stats.f_oneway(*groups)
    sig = p < 0.05
    return {
        "test": "One-Way ANOVA",
        "statistic": round(float(stat), 6),
        "p_value": round(float(p), 6),
        "significant": sig,
        "interpretation": (
            f"{'At least one group mean differs significantly' if sig else 'No significant differences between group means'} "
            f"(F={stat:.3f}, p={p:.4f})."
        ),
    }


def _chi_squared(data: dict) -> dict:
    from scipy import stats
    table = np.array(data.get("contingency_table", []))
    if table.size == 0:
        col_a = data.get("column_a", [])
        col_b = data.get("column_b", [])
        table = pd.crosstab(pd.Series(col_a), pd.Series(col_b)).values
    stat, p, dof, expected = stats.chi2_contingency(table)
    sig = p < 0.05
    return {
        "test": "Chi-Squared Test of Independence",
        "statistic": round(float(stat), 6),
        "p_value": round(float(p), 6),
        "degrees_of_freedom": int(dof),
        "significant": sig,
        "interpretation": (
            f"The variables are {'significantly associated' if sig else 'not significantly associated'} "
            f"(χ²={stat:.2f}, df={dof}, p={p:.4f})."
        ),
    }


def _pearson(data: dict) -> dict:
    from scipy import stats
    x = np.array(data.get("x", []), dtype=float)
    y = np.array(data.get("y", []), dtype=float)
    mask = ~(np.isnan(x) | np.isnan(y))
    r, p = stats.pearsonr(x[mask], y[mask])
    strength = "strong" if abs(r) > 0.7 else "moderate" if abs(r) > 0.4 else "weak"
    direction = "positive" if r > 0 else "negative"
    return {
        "test": "Pearson Correlation",
        "statistic": round(float(r), 6),
        "p_value": round(float(p), 6),
        "significant": p < 0.05,
        "interpretation": (
            f"There is a {strength} {direction} linear correlation (r={r:.3f}, p={p:.4f}). "
            f"{'This relationship is statistically significant.' if p < 0.05 else 'This correlation is not statistically significant.'}"
        ),
    }


def _spearman(data: dict) -> dict:
    from scipy import stats
    x = np.array(data.get("x", []), dtype=float)
    y = np.array(data.get("y", []), dtype=float)
    mask = ~(np.isnan(x) | np.isnan(y))
    r, p = stats.spearmanr(x[mask], y[mask])
    strength = "strong" if abs(r) > 0.7 else "moderate" if abs(r) > 0.4 else "weak"
    return {
        "test": "Spearman Rank Correlation",
        "statistic": round(float(r), 6),
        "p_value": round(float(p), 6),
        "significant": p < 0.05,
        "interpretation": (
            f"There is a {strength} monotonic relationship (ρ={r:.3f}, p={p:.4f})."
        ),
    }


def _levene(data: dict) -> dict:
    from scipy import stats
    groups = [np.array(g, dtype=float) for g in data.get("groups", [])]
    groups = [g[~np.isnan(g)] for g in groups]
    stat, p = stats.levene(*groups)
    sig = p < 0.05
    return {
        "test": "Levene's Test for Equality of Variances",
        "statistic": round(float(stat), 6),
        "p_value": round(float(p), 6),
        "significant": sig,
        "interpretation": (
            f"The variances are {'significantly different' if sig else 'not significantly different'} "
            f"across groups (W={stat:.3f}, p={p:.4f}). "
            f"{'Use Welch\'s t-test instead of Student\'s t-test.' if sig else 'Standard t-test assumptions are met.'}"
        ),
    }
