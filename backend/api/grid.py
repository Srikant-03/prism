"""
Grid API — Server-side data grid support endpoints.
Column stats, pagination for large datasets, and cell edit tracking.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter(prefix="/api/grid", tags=["grid"])

# In-memory stores
_grid_data: dict = {}  # table_name -> DataFrame reference
_cell_edits: list[dict] = []
_row_notes: dict[str, str] = {}


class CellEdit(BaseModel):
    table: str
    row_index: int
    column: str
    old_value: Optional[str] = None
    new_value: str


class RowNote(BaseModel):
    row_index: int
    note: str


@router.get("/column-stats")
async def column_stats(column: str = Query(...)):
    """Get quick stats for a column from the active dataset."""
    try:
        from api.sql import get_engine
        engine = get_engine()
        tables = engine.list_tables()
        if not tables:
            raise HTTPException(status_code=404, detail="No tables loaded")

        # Use the first available table
        table_name = tables[0]["name"]
        result = engine.execute(f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT("{column}") as non_null,
                COUNT(*) - COUNT("{column}") as null_count,
                COUNT(DISTINCT "{column}") as unique_values,
                MIN("{column}") as min_value,
                MAX("{column}") as max_value
            FROM "{table_name}"
        """)

        if result["rows"]:
            row = result["rows"][0]
            stats = {
                "count": row.get("total_rows", 0),
                "non_null": row.get("non_null", 0),
                "nulls": row.get("null_count", 0),
                "unique": row.get("unique_values", 0),
                "min": row.get("min_value"),
                "max": row.get("max_value"),
            }

            # Try numeric stats
            try:
                num_result = engine.execute(f"""
                    SELECT
                        AVG(CAST("{column}" AS DOUBLE)) as mean,
                        MEDIAN(CAST("{column}" AS DOUBLE)) as median,
                        STDDEV(CAST("{column}" AS DOUBLE)) as std_dev
                    FROM "{table_name}"
                    WHERE "{column}" IS NOT NULL
                """)
                if num_result["rows"]:
                    nr = num_result["rows"][0]
                    stats["mean"] = nr.get("mean")
                    stats["median"] = nr.get("median")
                    stats["std_dev"] = nr.get("std_dev")
            except Exception:
                pass  # Not numeric

            return stats
        return {"error": "No data"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cell-edit")
async def track_cell_edit(edit: CellEdit):
    """Track a cell edit in the audit log."""
    entry = {
        "table": edit.table,
        "row": edit.row_index,
        "column": edit.column,
        "old_value": edit.old_value,
        "new_value": edit.new_value,
        "timestamp": __import__("time").time(),
    }
    _cell_edits.append(entry)
    return {"tracked": True, "total_edits": len(_cell_edits)}


@router.get("/cell-edits")
async def get_cell_edits():
    """Get all tracked cell edits."""
    return {"edits": _cell_edits, "count": len(_cell_edits)}


@router.post("/row-note")
async def save_row_note(note: RowNote):
    """Save a note for a specific row."""
    _row_notes[str(note.row_index)] = note.note
    return {"saved": True}


@router.get("/row-notes")
async def get_row_notes():
    """Get all row notes."""
    return {"notes": _row_notes}
