"""
Reporting API — Endpoints for full report generation, code export, and data export.
"""

from __future__ import annotations

import io
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from reporting.report_generator import ReportGenerator, ReportExporter, FullReport
from reporting.code_exporter import CodeExporter
from reporting.data_exporter import DataExporter
from ingestion.orchestrator import get_stored_dataframe, get_stored_data

router = APIRouter(prefix="/api/reporting", tags=["reporting"])


# ── Pydantic models ──────────────────────────────────────────────────

class ReportRequest(BaseModel):
    file_id: str
    format: str = "json"  # json, html, pdf, docx, notebook


class CodeExportRequest(BaseModel):
    file_id: str
    format: str = "python"  # python, notebook, json_pipeline, sql
    file_name: Optional[str] = None
    queries: Optional[list[dict]] = None


class DataExportRequest(BaseModel):
    file_id: str
    format: str = "csv"  # csv, excel, json, parquet, feather, sql
    table_name: Optional[str] = None
    use_cleaned: bool = True


# ── Report endpoints ─────────────────────────────────────────────────

def _gather_report_data(file_id: str) -> dict:
    """Gather profiling, cleaning, and insights data for a file."""
    from api.profiling import get_stored_profile
    from api.cleaning import _cleaning_store

    profile_data = None
    stored_profile = get_stored_profile(file_id)
    if stored_profile is not None:
        try:
            profile_data = stored_profile.profile.model_dump()
        except Exception:
            profile_data = stored_profile.profile.__dict__

    insights_data = None
    if stored_profile is not None:
        try:
            if hasattr(stored_profile, "insights") and stored_profile.insights:
                insights_data = stored_profile.insights.model_dump()
            elif hasattr(stored_profile, "quality_score"):
                insights_data = {
                    "quality_score": stored_profile.quality_score.__dict__ if stored_profile.quality_score else None,
                    "analyst_briefing": stored_profile.analyst_briefing.__dict__ if hasattr(stored_profile, "analyst_briefing") else None,
                    "feature_rankings": [r.__dict__ for r in (stored_profile.feature_rankings or [])] if hasattr(stored_profile, "feature_rankings") else [],
                    "anomalies": [a.__dict__ for a in (stored_profile.anomalies or [])] if hasattr(stored_profile, "anomalies") else [],
                }
        except Exception:
            pass

    audit_log = None
    cleaning_data = None
    if file_id in _cleaning_store:
        store = _cleaning_store[file_id]
        engine = store.get("engine")
        if engine and hasattr(engine, "audit_logger"):
            logger = engine.audit_logger
            audit_log = [e.to_dict() for e in logger.audit_log]
            cleaning_data = {
                "total_actions": len(logger.audit_log),
                "applied": sum(1 for e in logger.audit_log if e.status == "applied"),
                "skipped": sum(1 for e in logger.audit_log if e.status == "skipped"),
            }

    before_after = None
    if file_id in _cleaning_store:
        store = _cleaning_store[file_id]
        engine = store.get("engine")
        if engine and hasattr(engine, "audit_logger"):
            try:
                before_after = engine.audit_logger.compare_with_original()
            except Exception:
                pass

    return {
        "profile_data": profile_data,
        "insights_data": insights_data,
        "audit_log": audit_log or [],
        "cleaning_data": cleaning_data,
        "before_after": before_after,
    }


@router.post("/generate")
async def generate_report(request: ReportRequest):
    """Generate a full analysis report."""
    data = _gather_report_data(request.file_id)

    report = ReportGenerator.generate(
        profile_data=data["profile_data"],
        cleaning_data=data["cleaning_data"],
        insights_data=data["insights_data"],
        audit_log=data["audit_log"],
        before_after=data["before_after"],
    )

    fmt = request.format.lower()

    if fmt == "json":
        return report.to_dict()

    elif fmt == "html":
        html = ReportExporter.to_html(report)
        return StreamingResponse(
            io.BytesIO(html.encode("utf-8")),
            media_type="text/html",
            headers={"Content-Disposition": "attachment; filename=report.html"},
        )

    elif fmt == "pdf":
        pdf_bytes = ReportExporter.to_pdf(report)
        if not pdf_bytes:
            raise HTTPException(status_code=500, detail="PDF generation requires fpdf2. Install with: pip install fpdf2")
        return StreamingResponse(
            io.BytesIO(pdf_bytes),
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=report.pdf"},
        )

    elif fmt == "docx":
        docx_bytes = ReportExporter.to_docx(report)
        if not docx_bytes:
            raise HTTPException(status_code=500, detail="DOCX generation requires python-docx. Install with: pip install python-docx")
        return StreamingResponse(
            io.BytesIO(docx_bytes),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": "attachment; filename=report.docx"},
        )

    elif fmt == "notebook":
        nb_str = ReportExporter.to_notebook(report)
        return StreamingResponse(
            io.BytesIO(nb_str.encode("utf-8")),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=report.ipynb"},
        )

    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {fmt}")


# ── Code export endpoints ────────────────────────────────────────────

@router.post("/export-code")
async def export_code(request: CodeExportRequest):
    """Export preprocessing pipeline as code."""
    data = _gather_report_data(request.file_id)
    audit_log = data["audit_log"]
    file_name = request.file_name or f"{request.file_id}.csv"
    fmt = request.format.lower()

    if fmt == "python":
        code = CodeExporter.to_python_script(audit_log, file_name)
        return StreamingResponse(
            io.BytesIO(code.encode("utf-8")),
            media_type="text/x-python",
            headers={"Content-Disposition": "attachment; filename=pipeline.py"},
        )

    elif fmt == "notebook":
        nb = CodeExporter.to_notebook(audit_log, file_name)
        return StreamingResponse(
            io.BytesIO(nb.encode("utf-8")),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=pipeline.ipynb"},
        )

    elif fmt == "json_pipeline":
        spec = CodeExporter.to_json_pipeline(audit_log)
        return StreamingResponse(
            io.BytesIO(spec.encode("utf-8")),
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=pipeline.json"},
        )

    elif fmt == "sql":
        queries = request.queries or []
        sql = CodeExporter.to_sql_file(queries)
        return StreamingResponse(
            io.BytesIO(sql.encode("utf-8")),
            media_type="text/plain",
            headers={"Content-Disposition": "attachment; filename=queries.sql"},
        )

    else:
        raise HTTPException(status_code=400, detail=f"Unsupported code format: {fmt}")


# ── Data export endpoints ────────────────────────────────────────────

@router.post("/export-data")
async def export_data(request: DataExportRequest):
    """Export raw or cleaned data in various formats."""
    df = get_stored_dataframe(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="File not found.")

    # If use_cleaned, try to get the cleaned version
    if request.use_cleaned:
        clean_df = get_stored_dataframe(f"{request.file_id}_cleaned")
        if clean_df is not None:
            df = clean_df

    fmt = request.format.lower()
    table_name = request.table_name or request.file_id

    if fmt == "csv":
        data = DataExporter.to_csv(df)
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={table_name}.csv"},
        )

    elif fmt == "excel":
        data = DataExporter.to_excel(df, sheet_name=table_name[:31])
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={table_name}.xlsx"},
        )

    elif fmt == "json":
        data = DataExporter.to_json(df)
        return StreamingResponse(
            io.BytesIO(data.encode("utf-8")),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={table_name}.json"},
        )

    elif fmt == "parquet":
        data = DataExporter.to_parquet(df)
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={table_name}.parquet"},
        )

    elif fmt == "feather":
        data = DataExporter.to_feather(df)
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={table_name}.feather"},
        )

    elif fmt == "sql":
        data = DataExporter.to_sql_inserts(df, table_name)
        return StreamingResponse(
            io.BytesIO(data.encode("utf-8")),
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={table_name}.sql"},
        )

    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {fmt}")


@router.get("/export-formats")
async def get_export_formats():
    """List all supported export formats."""
    return {
        "report_formats": ["json", "html", "pdf", "docx", "notebook"],
        "code_formats": ["python", "notebook", "json_pipeline", "sql"],
        "data_formats": DataExporter.get_supported_formats(),
    }
