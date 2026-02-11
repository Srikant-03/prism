"""
Profiling API endpoints.
Auto-triggered after ingestion, also available on-demand.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Response

from profiling.engine import DataProfiler
from profiling.profiling_models import ProfilingResult
from ingestion.orchestrator import get_stored_data, get_stored_dataframe
from insights.export_service import ExportService

router = APIRouter(prefix="/api", tags=["profiling"])

# In-memory store for profiling results
_profile_store: dict[str, ProfilingResult] = {}


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

    # Run profiling
    result = DataProfiler.profile(df, file_id, disk_size)
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


def auto_profile(file_id: str) -> ProfilingResult | None:
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

    result = DataProfiler.profile(df, file_id, disk_size)
    _profile_store[file_id] = result
    return result

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
