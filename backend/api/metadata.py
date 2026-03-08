"""
Metadata API — Column tagging and semantic annotation endpoints.
Supports auto-detection, custom tag persistence, and retrieval.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/metadata", tags=["metadata"])

# In-memory tag store: file_id -> { column_name -> { tag, custom, notes } }
_tag_store: dict[str, dict[str, dict]] = {}


class TagRequest(BaseModel):
    column: str
    tag: str
    notes: Optional[str] = None


@router.get("/detect/{file_id}")
async def detect_tags(file_id: str):
    """Auto-detect semantic tags for all columns."""
    try:
        from api.upload import _storage
        info = _storage.get(file_id)
        if info is None or "df" not in info:
            raise HTTPException(status_code=404, detail="Dataset not found")

        df = info["df"]
        tags = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            name_lower = col.lower()

            tag = "feature"
            confidence = 0.8
            reasoning = "Standard data column"

            if "id" in name_lower or "key" in name_lower:
                tag = "id"
                confidence = 0.9
                reasoning = "Matches ID-like naming pattern"
            elif "date" in name_lower or "time" in name_lower:
                tag = "datetime"
                confidence = 0.9
                reasoning = "Temporal column detected"
            elif any(x in name_lower for x in ["email", "phone", "address", "ssn"]):
                tag = "pii"
                confidence = 0.95
                reasoning = "Likely contains Personally Identifiable Information"
            elif df[col].nunique() < 10 and dtype == "object":
                tag = "categorical"
                confidence = 0.85
                reasoning = "Low cardinality string column"

            # Overlay any custom tags the user has saved
            custom = _tag_store.get(file_id, {}).get(col)
            if custom:
                tag = custom["tag"]
                confidence = 1.0
                reasoning = f"User-assigned tag" + (f": {custom['notes']}" if custom.get("notes") else "")

            tags.append({
                "name": col,
                "dtype": dtype,
                "autoTag": tag,
                "confidence": confidence,
                "reasoning": reasoning,
                "isCustom": custom is not None,
            })
        return {"columns": tags}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{file_id}/tag")
async def save_tag(file_id: str, request: TagRequest):
    """Save a custom tag for a column (persists across detect calls)."""
    try:
        from api.upload import _storage
        info = _storage.get(file_id)
        if info is None or "df" not in info:
            raise HTTPException(status_code=404, detail="Dataset not found")

        df = info["df"]
        if request.column not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{request.column}' not found in dataset")

        if file_id not in _tag_store:
            _tag_store[file_id] = {}

        _tag_store[file_id][request.column] = {
            "tag": request.tag,
            "notes": request.notes,
        }

        return {
            "saved": True,
            "column": request.column,
            "tag": request.tag,
            "notes": request.notes,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{file_id}/tags")
async def get_tags(file_id: str):
    """Get all custom tags for a dataset."""
    tags = _tag_store.get(file_id, {})
    return {
        "file_id": file_id,
        "tags": tags,
        "count": len(tags),
    }


@router.delete("/{file_id}/tag/{column}")
async def delete_tag(file_id: str, column: str):
    """Remove a custom tag from a column (reverts to auto-detection)."""
    tags = _tag_store.get(file_id, {})
    if column not in tags:
        raise HTTPException(status_code=404, detail=f"No custom tag for column '{column}'")
    del tags[column]
    return {"deleted": True, "column": column}
