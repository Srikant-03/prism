"""
Profiling API endpoints.
Auto-triggered after ingestion, also available on-demand.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Response

from profiling.engine import DataProfiler
from profiling.profiling_models import ProfilingResult
from ingestion.orchestrator import get_stored_data, get_stored_dataframe
from insights.export_service import ExportService
from api.models import SchemaOverrideRequest

router = APIRouter(prefix="/api", tags=["profiling"])

# In-memory store for profiling results
_profile_store: dict[str, ProfilingResult] = {}


def get_stored_profile(file_id: str) -> ProfilingResult | None:
    """Public accessor for profiling results. Avoids exposing _profile_store directly."""
    return _profile_store.get(file_id)


@router.get("/profile/{file_id}")
async def get_profile(file_id: str):
    """
    Get the profiling result for an ingested file.
    Auto-computes the profile if not already cached.
    """
    # Check cache
    if file_id in _profile_store:
        return _profile_store[file_id].model_dump()

    # Get the stored DataFrame from ingestion
    df = get_stored_dataframe(file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="File not found. Please ingest the file first.")

    stored = get_stored_data(file_id)
    disk_size = 0
    if stored and "metadata" in stored:
        meta = stored["metadata"]
        if hasattr(meta, "file_size_bytes"):
            disk_size = meta.file_size_bytes

    # Run profiling in thread pool to avoid blocking the event loop
    result = await asyncio.to_thread(DataProfiler.profile, df, file_id, disk_size)
    _profile_store[file_id] = result

    return result.model_dump()


@router.get("/profile/{file_id}/column/{column_name}")
async def get_column_profile(file_id: str, column_name: str):
    """Get detailed profiling for a specific column."""
    # Ensure profile exists
    if file_id not in _profile_store:
        await get_profile(file_id)

    result = _profile_store.get(file_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Profile not found.")

    for col in result.profile.columns:
        if col.name == column_name:
            return col.model_dump()

    raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found.")


@router.get("/profile/{file_id}/dataset")
async def get_dataset_overview(file_id: str):
    """Get dataset-level profiling summary (without per-column details)."""
    if file_id not in _profile_store:
        await get_profile(file_id)

    result = _profile_store.get(file_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Profile not found.")

    ds = result.profile
    return {
        "total_rows": ds.total_rows,
        "total_columns": ds.total_columns,
        "memory_size_readable": ds.memory_size_readable,
        "disk_size_readable": ds.disk_size_readable,
        "estimated_domain": ds.estimated_domain,
        "domain_confidence": ds.domain_confidence,
        "domain_justification": ds.domain_justification,
        "structural_completeness": ds.structural_completeness,
        "schema_consistency": ds.schema_consistency,
        "temporal_columns": ds.temporal_columns,
        "temporal_coverage": ds.temporal_coverage,
        "primary_key_candidates": [k.model_dump() for k in ds.primary_key_candidates],
        "foreign_key_candidates": [k.model_dump() for k in ds.foreign_key_candidates],
        "id_columns": ds.id_columns,
        "profiling_time_seconds": ds.profiling_time_seconds,
    }


async def auto_profile(file_id: str) -> ProfilingResult | None:
    """
    Auto-trigger profiling after ingestion.
    Called by the ingestion orchestrator.
    Returns the result or None if profiling fails.
    """
    df = get_stored_dataframe(file_id)
    if df is None:
        return None

    stored = get_stored_data(file_id)
    disk_size = 0
    if stored and "metadata" in stored:
        meta = stored["metadata"]
        if hasattr(meta, "file_size_bytes"):
            disk_size = meta.file_size_bytes

    result = await asyncio.to_thread(DataProfiler.profile, df, file_id, disk_size)
    _profile_store[file_id] = result
    return result

@router.post("/schema-override/{file_id}")
async def override_schema(file_id: str, request: SchemaOverrideRequest):
    """Override the inferred data type for a specific column and re-coerce it."""
    import pandas as pd

    if file_id not in _profile_store:
        raise HTTPException(status_code=404, detail="Profile not found.")

    # Get the stored DataFrame and coerce the column
    df = get_stored_dataframe(file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    if request.column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{request.column}' not found.")

    new_type = request.new_type.lower().strip()
    try:
        if new_type in ("int", "int64", "integer"):
            df[request.column] = pd.to_numeric(df[request.column], errors="coerce").astype("Int64")
        elif new_type in ("float", "float64", "numeric", "number"):
            df[request.column] = pd.to_numeric(df[request.column], errors="coerce")
        elif new_type in ("datetime", "date", "timestamp"):
            df[request.column] = pd.to_datetime(df[request.column], errors="coerce")
        elif new_type in ("category", "categorical"):
            df[request.column] = df[request.column].astype("category")
        elif new_type in ("bool", "boolean"):
            df[request.column] = df[request.column].astype(bool)
        elif new_type in ("str", "string", "text", "object"):
            df[request.column] = df[request.column].astype(str)
        else:
            df[request.column] = df[request.column].astype(new_type)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to coerce '{request.column}' to {new_type}: {str(e)}")

    # Update the stored DataFrame
    try:
        from ingestion.orchestrator import update_stored_dataframe
        update_stored_dataframe(file_id, df)
    except Exception:
        pass

    # Invalidate profile cache so next request re-profiles with the new type
    if file_id in _profile_store:
        del _profile_store[file_id]

    return {
        "status": "success",
        "message": f"Coerced '{request.column}' to {new_type} and invalidated profile cache.",
        "new_dtype": str(df[request.column].dtype),
    }

# ──────────────────────────────────────────
# Insights Exports
# ──────────────────────────────────────────

@router.get("/insights/{file_id}/pdf")
async def export_insights_pdf(file_id: str):
    """Download the Analyst Briefing as a PDF."""
    if file_id not in _profile_store:
        raise HTTPException(status_code=404, detail="Profile not found. Please profile the dataset first.")
    
    result = _profile_store[file_id]
    if not result.profile.insights:
        raise HTTPException(status_code=400, detail="Insights have not been generated for this dataset.")
        
    pdf_bytes = ExportService.generate_pdf(result.profile.insights)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="analyst_briefing_{file_id}.pdf"'}
    )

@router.get("/insights/{file_id}/docx")
async def export_insights_docx(file_id: str):
    """Download the Analyst Briefing as a DOCX."""
    if file_id not in _profile_store:
        raise HTTPException(status_code=404, detail="Profile not found. Please profile the dataset first.")
    
    result = _profile_store[file_id]
    if not result.profile.insights:
        raise HTTPException(status_code=400, detail="Insights have not been generated for this dataset.")
        
    docx_bytes = ExportService.generate_docx(result.profile.insights)
    return Response(
        content=docx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="analyst_briefing_{file_id}.docx"'}
    )
