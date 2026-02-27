"""
Graph API — Column relationship graph endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from insights.graph_builder import build_relationship_graph

router = APIRouter(prefix="/api/graph", tags=["graph"])


@router.get("/{file_id}")
async def get_graph(file_id: str, threshold: float = 0.3):
    """Retrieve the relationship graph for a dataset."""
    try:
        from api.upload import _storage
        info = _storage.get(file_id)
        if info is None or "df" not in info:
            raise HTTPException(status_code=404, detail="Dataset not found")

        df = info["df"]
        graph_data = build_relationship_graph(df, threshold)
        return graph_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
