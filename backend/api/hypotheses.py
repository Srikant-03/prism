"""
Hypotheses API — Surfaces auto-generated data-driven hypotheses.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/hypotheses", tags=["hypotheses"])


@router.get("/{file_id}")
async def get_hypotheses(file_id: str):
    """Generate hypotheses from the profiled dataset."""
    try:
        from api.profiling import _profile_store
        from insights.hypothesis_engine import generate_hypotheses

        stored = _profile_store.get(file_id)
        if stored is None:
            raise HTTPException(status_code=404, detail="Profile not found — upload and profile a file first.")

        profile = stored.get("profile")
        if profile is None:
            raise HTTPException(status_code=404, detail="Profiling data missing for this file.")

        # Build the profile dict expected by the hypothesis engine
        profile_dict: dict = {}
        if hasattr(profile, "model_dump"):
            profile_dict = profile.model_dump()
        elif hasattr(profile, "dict"):
            profile_dict = profile.dict()
        elif isinstance(profile, dict):
            profile_dict = profile
        else:
            profile_dict = {"columns": {}, "row_count": 0}

        # Normalize: hypothesis engine expects columns as a dict keyed by name
        if "columns" in profile_dict and isinstance(profile_dict["columns"], list):
            cols_dict = {}
            for col in profile_dict["columns"]:
                col_name = col.get("name", "unknown")
                cols_dict[col_name] = col
            profile_dict["columns"] = cols_dict

        if "row_count" not in profile_dict and "total_rows" in profile_dict:
            profile_dict["row_count"] = profile_dict["total_rows"]

        quality = stored.get("quality")
        quality_dict = None
        if quality is not None:
            if hasattr(quality, "model_dump"):
                quality_dict = quality.model_dump()
            elif hasattr(quality, "dict"):
                quality_dict = quality.dict()
            elif isinstance(quality, dict):
                quality_dict = quality

        hypotheses = generate_hypotheses(profile_dict, quality_dict)
        return {"file_id": file_id, "hypotheses": hypotheses, "count": len(hypotheses)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
