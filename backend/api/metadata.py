"""
Metadata API — Column tagging and semantic annotation endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/metadata", tags=["metadata"])

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
            # Simple rule-based detector
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
                
            tags.append({
                "name": col,
                "dtype": dtype,
                "autoTag": tag,
                "confidence": confidence,
                "reasoning": reasoning
            })
        return {"columns": tags}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
