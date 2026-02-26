"""
Joins API — Multi-dataset SQL join workbench.
Suggests join conditions from key_detector, warns on cardinality issues,
previews the merged result, and saves it as a virtual DuckDB table.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import pandas as pd

router = APIRouter(prefix="/api/joins", tags=["joins"])


class JoinCondition(BaseModel):
    left_column: str
    right_column: str


class JoinRequest(BaseModel):
    left_file_id: str
    right_file_id: str
    join_type: str = "INNER"  # INNER | LEFT | RIGHT | FULL | CROSS
    conditions: list[JoinCondition] = []
    save_as: Optional[str] = None  # If set, save result as virtual table


class SuggestJoinRequest(BaseModel):
    left_file_id: str
    right_file_id: str


# ── Suggest join keys ─────────────────────────────────────────────────

@router.post("/suggest")
async def suggest_joins(request: SuggestJoinRequest):
    """Use key_detector to suggest join conditions between two datasets."""
    from state import get_df

    df_left = get_df(request.left_file_id)
    df_right = get_df(request.right_file_id)

    if df_left is None:
        raise HTTPException(status_code=404, detail=f"Left dataset '{request.left_file_id}' not found")
    if df_right is None:
        raise HTTPException(status_code=404, detail=f"Right dataset '{request.right_file_id}' not found")

    suggestions = []

    # 1. Exact column name matches
    common_cols = set(df_left.columns) & set(df_right.columns)
    for col in sorted(common_cols):
        left_nunique = df_left[col].nunique()
        right_nunique = df_right[col].nunique()
        left_dtype = str(df_left[col].dtype)
        right_dtype = str(df_right[col].dtype)

        # Skip columns that are clearly not join keys
        if left_nunique < 2 or right_nunique < 2:
            continue

        # Check value overlap
        left_vals = set(df_left[col].dropna().unique())
        right_vals = set(df_right[col].dropna().unique())
        overlap = len(left_vals & right_vals)
        overlap_pct = overlap / max(len(left_vals | right_vals), 1) * 100

        if overlap < 1:
            continue

        # Cardinality analysis
        cardinality = _analyze_cardinality(df_left, df_right, col, col)

        suggestions.append({
            "left_column": col,
            "right_column": col,
            "confidence": round(min(overlap_pct / 100, 1.0), 2),
            "match_type": "exact_name",
            "overlap_count": overlap,
            "overlap_pct": round(overlap_pct, 1),
            "left_unique": left_nunique,
            "right_unique": right_nunique,
            "left_dtype": left_dtype,
            "right_dtype": right_dtype,
            "cardinality": cardinality,
        })

    # 2. Key detector foreign key analysis (cross-dataset)
    try:
        from profiling.key_detector import KeyDetector
        # Check if any column in left is a subset of a column in right (or vice versa)
        for left_col in df_left.columns:
            left_vals = set(df_left[left_col].dropna().unique())
            if len(left_vals) < 2 or len(left_vals) > len(df_left) * 0.95:
                continue

            for right_col in df_right.columns:
                if left_col == right_col:
                    continue  # Already handled above

                right_vals = set(df_right[right_col].dropna().unique())
                if len(right_vals) < 2:
                    continue

                # Check type compatibility
                left_dtype = str(df_left[left_col].dtype)
                right_dtype = str(df_right[right_col].dtype)
                if not _types_compatible(left_dtype, right_dtype):
                    continue

                overlap = len(left_vals & right_vals)
                if overlap < max(len(left_vals), len(right_vals)) * 0.3:
                    continue

                overlap_pct = overlap / max(len(left_vals | right_vals), 1) * 100
                cardinality = _analyze_cardinality(df_left, df_right, left_col, right_col)

                suggestions.append({
                    "left_column": left_col,
                    "right_column": right_col,
                    "confidence": round(overlap_pct / 200, 2),  # Lower confidence than exact name
                    "match_type": "value_overlap",
                    "overlap_count": overlap,
                    "overlap_pct": round(overlap_pct, 1),
                    "left_unique": int(df_left[left_col].nunique()),
                    "right_unique": int(df_right[right_col].nunique()),
                    "left_dtype": left_dtype,
                    "right_dtype": right_dtype,
                    "cardinality": cardinality,
                })
    except Exception:
        pass

    suggestions.sort(key=lambda s: s["confidence"], reverse=True)

    return {
        "suggestions": suggestions[:10],
        "left_columns": list(df_left.columns),
        "right_columns": list(df_right.columns),
        "left_rows": len(df_left),
        "right_rows": len(df_right),
    }


# ── Preview join ──────────────────────────────────────────────────────

@router.post("/preview")
async def preview_join(request: JoinRequest):
    """Execute a join and return a preview of the result."""
    result_df = await _execute_join(request)

    preview = result_df.head(50).fillna("").to_dict(orient="records")
    columns = [{"name": c, "dtype": str(result_df[c].dtype)} for c in result_df.columns]

    return {
        "preview": preview,
        "columns": columns,
        "row_count": len(result_df),
        "column_count": len(result_df.columns),
    }


# ── Execute and save join ─────────────────────────────────────────────

@router.post("/execute")
async def execute_join(request: JoinRequest):
    """Execute a join and optionally save the result as a virtual table."""
    from state import set_df
    from api.sql import sql_engine

    result_df = await _execute_join(request)

    response = {
        "row_count": len(result_df),
        "column_count": len(result_df.columns),
        "columns": list(result_df.columns),
        "preview": result_df.head(20).fillna("").to_dict(orient="records"),
    }

    # Save as virtual table if requested
    if request.save_as:
        table_name = request.save_as.replace(" ", "_").lower()

        # Register in state
        set_df(table_name, result_df)

        # Register in SQL engine if available
        try:
            sql_engine.register_dataframe(result_df, table_name, source="joined")
        except Exception:
            pass

        response["saved_as"] = table_name

    return response


# ── Helpers ───────────────────────────────────────────────────────────

async def _execute_join(request: JoinRequest) -> pd.DataFrame:
    """Execute the join and return the resulting DataFrame."""
    from state import get_df

    df_left = get_df(request.left_file_id)
    df_right = get_df(request.right_file_id)

    if df_left is None:
        raise HTTPException(status_code=404, detail=f"Left dataset not found")
    if df_right is None:
        raise HTTPException(status_code=404, detail=f"Right dataset not found")

    join_type = request.join_type.upper()

    if join_type == "CROSS":
        result = df_left.merge(df_right, how="cross", suffixes=("_left", "_right"))
        if len(result) > 100_000:
            raise HTTPException(status_code=400, detail="Cross join would produce too many rows (>100k). Add filters.")
        return result

    if not request.conditions:
        raise HTTPException(status_code=400, detail="At least one join condition is required for non-CROSS joins.")

    left_on = [c.left_column for c in request.conditions]
    right_on = [c.right_column for c in request.conditions]

    # Validate columns exist
    for col in left_on:
        if col not in df_left.columns:
            raise HTTPException(status_code=400, detail=f"Column '{col}' not found in left dataset")
    for col in right_on:
        if col not in df_right.columns:
            raise HTTPException(status_code=400, detail=f"Column '{col}' not found in right dataset")

    pandas_how = {
        "INNER": "inner", "LEFT": "left", "RIGHT": "right", "FULL": "outer",
        "SEMI": "inner", "ANTI": "left",
    }.get(join_type, "inner")

    result = df_left.merge(
        df_right,
        left_on=left_on,
        right_on=right_on,
        how=pandas_how,
        suffixes=("_left", "_right"),
        indicator=True if join_type in ("SEMI", "ANTI") else False,
    )

    # Handle SEMI/ANTI join filtering
    if join_type in ("SEMI", "ANTI"):
        merge_val = "both" if join_type == "SEMI" else "left_only"
        result = result[result["_merge"] == merge_val].drop(columns=["_merge"])
        
        # Reconstruct exactly the left DataFrame columns
        left_cols = []
        rename_map = {}
        for col in df_left.columns:
            if col in result.columns:
                left_cols.append(col)
            elif f"{col}_left" in result.columns:
                left_cols.append(f"{col}_left")
                rename_map[f"{col}_left"] = col
                
        result = result[left_cols].rename(columns=rename_map)

    return result


def _analyze_cardinality(df_left, df_right, left_col, right_col) -> dict:
    """Analyze the cardinality relationship between two columns."""
    left_unique = df_left[left_col].nunique()
    right_unique = df_right[right_col].nunique()
    left_has_dups = left_unique < len(df_left)
    right_has_dups = right_unique < len(df_right)

    if not left_has_dups and not right_has_dups:
        relationship = "1:1"
    elif not left_has_dups:
        relationship = "1:N"
    elif not right_has_dups:
        relationship = "N:1"
    else:
        relationship = "N:M"

    warning = None
    if relationship == "N:M":
        warning = "Many-to-many join may produce explosive row growth. Consider deduplication first."
    elif left_unique * right_unique > 10_000_000:
        warning = "Large cardinality product — join may be slow."

    return {
        "relationship": relationship,
        "left_unique": left_unique,
        "right_unique": right_unique,
        "warning": warning,
    }


def _types_compatible(dtype_a: str, dtype_b: str) -> bool:
    """Check if two dtypes are compatible for joining."""
    numeric = {"int64", "int32", "int16", "float64", "float32"}
    string = {"object", "string", "category"}

    if dtype_a in numeric and dtype_b in numeric:
        return True
    if dtype_a in string and dtype_b in string:
        return True
    return dtype_a == dtype_b
