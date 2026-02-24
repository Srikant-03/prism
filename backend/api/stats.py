"""
Stats API — Statistical testing endpoints.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/stats", tags=["stats"])


class TestRequest(BaseModel):
    test: str
    file_id: str
    column: str
    column2: Optional[str] = None
    group_by: Optional[str] = None


@router.post("/test")
async def run_stat_test(request: TestRequest):
    """Run a statistical test on the dataset."""
    try:
        import numpy as np
        from api.upload import _storage
        from insights.stat_tests import run_test

        info = _storage.get(request.file_id)
        if info is None or "df" not in info:
            raise HTTPException(status_code=404, detail="Dataset not found")

        df = info["df"]
        data = {}

        col = request.column
        if col not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{col}' not found")

        # Single column tests
        if request.test in ("shapiro_wilk", "ks_test"):
            data["values"] = df[col].dropna().tolist()

        # Two-column tests
        elif request.test in ("pearson_correlation", "spearman_correlation"):
            if not request.column2 or request.column2 not in df.columns:
                raise HTTPException(status_code=400, detail="Second column required")
            data["x"] = df[col].tolist()
            data["y"] = df[request.column2].tolist()

        # Chi-squared
        elif request.test == "chi_squared":
            if not request.column2 or request.column2 not in df.columns:
                raise HTTPException(status_code=400, detail="Second column required")
            data["column_a"] = df[col].tolist()
            data["column_b"] = df[request.column2].tolist()

        # Group comparison tests
        elif request.test in ("t_test", "mann_whitney", "levene", "anova"):
            group_col = request.column2 or request.group_by
            if not group_col or group_col not in df.columns:
                raise HTTPException(status_code=400, detail="Group-by column required")

            groups = []
            for _, group_df in df.groupby(group_col):
                vals = group_df[col].dropna().tolist()
                if vals:
                    groups.append(vals)

            if request.test in ("t_test", "mann_whitney"):
                if len(groups) < 2:
                    raise HTTPException(status_code=400, detail="Need at least 2 groups")
                data["group_a"] = groups[0]
                data["group_b"] = groups[1]
            else:
                data["groups"] = groups

        result = run_test(request.test, data)
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggest/{file_id}")
async def suggest_tests(file_id: str):
    """Suggest appropriate tests for the current dataset."""
    try:
        from api.upload import _storage
        from insights.stat_tests import suggest_tests

        info = _storage.get(file_id)
        if info is None or "df" not in info:
            raise HTTPException(status_code=404, detail="Dataset not found")

        df = info["df"]
        columns = [
            {"name": col, "dtype": str(df[col].dtype)}
            for col in df.columns
        ]
        suggestions = suggest_tests(columns)
        return {"suggestions": suggestions}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
