"""
Explain API — AI-powered column deep-dive narratives.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/explain", tags=["explain"])


class ExplainRequest(BaseModel):
    file_id: str
    column: str


@router.post("/column")
async def explain_column(request: ExplainRequest):
    """Generate a comprehensive AI narrative for a column."""
    try:
        import numpy as np
        import pandas as pd
        from ingestion.orchestrator import get_stored_dataframe

        df = get_stored_dataframe(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        col = request.column
        if col not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{col}' not found")

        series = df[col]

        # Build column profile
        profile = {
            "name": col,
            "dtype": str(series.dtype),
            "count": len(series),
            "non_null": int(series.count()),
            "null_count": int(series.isnull().sum()),
            "null_pct": round(series.isnull().sum() / max(len(series), 1) * 100, 2),
            "unique": int(series.nunique()),
            "unique_ratio": round(series.nunique() / max(series.count(), 1), 4),
        }

        if pd.api.types.is_numeric_dtype(series):
            clean = series.dropna()
            profile.update({
                "mean": round(float(clean.mean()), 4),
                "median": round(float(clean.median()), 4),
                "std": round(float(clean.std()), 4),
                "min": float(clean.min()),
                "max": float(clean.max()),
                "skewness": round(float(clean.skew()), 4),
                "kurtosis": round(float(clean.kurtosis()), 4),
                "zeros": int((clean == 0).sum()),
                "negatives": int((clean < 0).sum()),
            })

            # Correlations with other numeric columns
            correlations = {}
            for other_col in df.select_dtypes(include=[np.number]).columns:
                if other_col != col:
                    corr = df[col].corr(df[other_col])
                    if not np.isnan(corr) and abs(corr) > 0.3:
                        correlations[other_col] = round(float(corr), 3)
            profile["correlations"] = dict(sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True)[:5])

        elif series.dtype == "object" or str(series.dtype) == "category":
            vc = series.value_counts().head(10)
            profile["top_values"] = {str(k): int(v) for k, v in vc.items()}
            profile["is_potential_id"] = profile["unique_ratio"] > 0.95

        # Try AI explanation
        try:
            from chat.engine import get_chat_engine
            engine = get_chat_engine()
            prompt = (
                f"You are a data analyst. Generate a comprehensive 3-5 paragraph analysis for the column '{col}'. "
                f"Here is its profile:\n{profile}\n\n"
                f"Cover: (1) What this column contains and likely represents, "
                f"(2) How complete and clean it is, "
                f"(3) What its distribution looks like and what that means, "
                f"(4) Key relationships with other columns, "
                f"(5) Whether it's useful for modeling and what should be done with it. "
                f"Be specific with numbers. Write in a clear, professional tone."
            )
            result = await engine.chat(prompt)
            explanation = result.get("response", "")
        except Exception:
            # Fallback: generate programmatic explanation
            explanation = _generate_fallback(profile)

        return {"explanation": explanation, "profile": profile}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _generate_fallback(profile: dict) -> str:
    """Generate a basic explanation if AI is unavailable.
    Uses inline heuristic templating (mirrors BriefingGenerator's column deep-dive approach).
    """
    lines = []
    name = profile["name"]
    dtype = profile["dtype"]

    lines.append(
        f"**{name}** is a {dtype} column with {profile['count']:,} total values, "
        f"of which {profile['non_null']:,} are non-null ({100 - profile['null_pct']:.1f}% completeness). "
        f"There are {profile['unique']:,} unique values "
        f"(unique ratio: {profile['unique_ratio']:.1%})."
    )

    if "mean" in profile:
        lines.append(
            f"\nThe distribution has a mean of {profile['mean']:.2f}, "
            f"median of {profile['median']:.2f}, and standard deviation of {profile['std']:.2f}. "
            f"Values range from {profile['min']:.2f} to {profile['max']:.2f}. "
            f"Skewness is {profile['skewness']:.2f} and kurtosis is {profile['kurtosis']:.2f}."
        )
        if abs(profile.get("skewness", 0)) > 2:
            lines.append("The distribution is heavily skewed — consider log transformation.")
        if profile.get("zeros", 0) > 0:
            lines.append(f"Contains {profile['zeros']} zero values.")
        if profile.get("negatives", 0) > 0:
            lines.append(f"Contains {profile['negatives']} negative values.")

    if profile.get("correlations"):
        corr_str = ", ".join(f"{k} (r={v})" for k, v in list(profile["correlations"].items())[:3])
        lines.append(f"\nKey correlations: {corr_str}.")

    if profile.get("top_values"):
        top_str = ", ".join(f"'{k}' ({v})" for k, v in list(profile["top_values"].items())[:5])
        lines.append(f"\nTop values: {top_str}.")
        if profile.get("is_potential_id"):
            lines.append("⚠️ This column has very high uniqueness — it may be an ID column with no analytical value.")

    if profile["null_pct"] > 20:
        lines.append(f"\n⚠️ High missing rate ({profile['null_pct']:.1f}%) — imputation or removal recommended.")
    elif profile["null_pct"] > 5:
        lines.append(f"\nModerate missing rate ({profile['null_pct']:.1f}%) — consider imputation strategy.")

    return "\n".join(lines)
