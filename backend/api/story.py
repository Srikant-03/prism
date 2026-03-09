"""
Story API — Automated data storytelling endpoints.
Generates real insight slides from profiling data and exports via ReportExporter.
"""

from __future__ import annotations

import io
import uuid
from typing import Optional
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/story", tags=["story"])


class StoryRequest(BaseModel):
    file_id: str


class ExportStoryRequest(BaseModel):
    file_id: str
    format: str = "html"  # html | pdf


@router.post("/generate")
async def generate_story(request: StoryRequest):
    """Auto-generate a data story (slide deck) from real profiling insights."""
    try:
        from state import get_stored_dataframe
        from api.profiling import get_stored_profile

        df = get_stored_dataframe(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        stored = get_stored_profile(request.file_id)

        slides = []

        # ── 1. Title Slide — real dataset stats ──────────────────────────
        null_total = int(df.isnull().sum().sum())
        dup_count = int(df.duplicated().sum())
        file_label = request.file_id.split("_", 1)[-1] if "_" in request.file_id else request.file_id

        slides.append({
            "id": str(uuid.uuid4())[:8],
            "type": "title",
            "title": f"Data Story: {file_label}",
            "content": (
                f"Dataset shape: {len(df):,} rows × {len(df.columns)} columns\n"
                f"Memory: {df.memory_usage(deep=True).sum() / (1024 * 1024):.2f} MB\n"
                f"Total nulls: {null_total:,} | Duplicate rows: {dup_count:,}"
            ),
        })

        # ── 2. Quality Slide — real scores if available ──────────────────
        quality_score = None
        quality_grade = None
        if stored and hasattr(stored, "insights") and stored.insights:
            insights = stored.insights
            q = getattr(insights, "quality", None) or (insights.get("quality") if isinstance(insights, dict) else None)
            if q:
                if hasattr(q, "overall_score"):
                    quality_score = q.overall_score
                    quality_grade = q.grade
                elif isinstance(q, dict):
                    quality_score = q.get("overall_score")
                    quality_grade = q.get("grade")

        if quality_score is not None:
            slides.append({
                "id": str(uuid.uuid4())[:8],
                "type": "kpi",
                "title": "Data Health Scoreboard",
                "kpiLabel": "Overall Quality Score",
                "kpiValue": f"{quality_score}/100",
                "content": f"Grade: {quality_grade}. "
                           f"The quality score reflects Completeness, Uniqueness, Validity, and Consistency dimensions."
            })
        else:
            # Compute a lightweight quality estimate from the raw DataFrame
            total_cells = max(df.size, 1)
            completeness = round(100.0 * (1.0 - null_total / total_cells), 1)
            dup_pct = round(100.0 * dup_count / max(len(df), 1), 1)
            slides.append({
                "id": str(uuid.uuid4())[:8],
                "type": "kpi",
                "title": "Data Health Scoreboard",
                "kpiLabel": "Completeness",
                "kpiValue": f"{completeness}%",
                "content": (
                    f"Completeness: {completeness}%\n"
                    f"Duplicate rate: {dup_pct}%\n"
                    f"Run full profiling for Uniqueness, Validity, and Consistency scores."
                ),
            })

        # ── 3. Column Breakdown ──────────────────────────────────────────
        import numpy as np
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
        datetime_cols = df.select_dtypes(include=["datetime"]).columns.tolist()

        type_summary = []
        if numeric_cols:
            type_summary.append(f"• {len(numeric_cols)} numeric: {', '.join(numeric_cols[:5])}{'…' if len(numeric_cols) > 5 else ''}")
        if cat_cols:
            type_summary.append(f"• {len(cat_cols)} categorical: {', '.join(cat_cols[:5])}{'…' if len(cat_cols) > 5 else ''}")
        if datetime_cols:
            type_summary.append(f"• {len(datetime_cols)} datetime: {', '.join(datetime_cols[:3])}")

        slides.append({
            "id": str(uuid.uuid4())[:8],
            "type": "insight",
            "title": "Column Type Breakdown",
            "content": "\n".join(type_summary) if type_summary else "No columns detected.",
        })

        # ── 4. Key Insights — top nulls + correlations ───────────────────
        findings = []

        # Top 3 columns by null percentage
        null_pcts = df.isnull().mean().sort_values(ascending=False)
        top_null = null_pcts[null_pcts > 0.05].head(3)
        if not top_null.empty:
            for col_name, pct in top_null.items():
                findings.append(f"• '{col_name}' is {pct:.0%} missing")

        # Top correlation pair among numeric columns
        if len(numeric_cols) >= 2:
            try:
                corr = df[numeric_cols].corr()
                # Zero out diagonal
                import numpy as np
                np.fill_diagonal(corr.values, 0)
                max_corr = corr.abs().max().max()
                if max_corr > 0.5:
                    idx = corr.abs().stack().idxmax()
                    findings.append(f"• Strongest correlation: '{idx[0]}' ↔ '{idx[1]}' (r={corr.loc[idx[0], idx[1]]:.3f})")
            except Exception:
                pass

        if not findings:
            findings.append("• No major data quality concerns detected.")

        slides.append({
            "id": str(uuid.uuid4())[:8],
            "type": "insight",
            "title": "Key Findings",
            "content": "\n".join(findings),
        })

        # ── 5. Recommendations ───────────────────────────────────────────
        recommendations = []
        if null_total > 0:
            recommendations.append("• Impute or investigate high-null columns before modeling.")
        if dup_count > 0:
            recommendations.append(f"• {dup_count:,} duplicate rows detected — consider deduplication.")
        if len(numeric_cols) >= 2:
            recommendations.append("• Check multicollinearity among correlated numeric features.")
        if not recommendations:
            recommendations.append("• Dataset appears clean. Proceed directly to feature engineering.")

        slides.append({
            "id": str(uuid.uuid4())[:8],
            "type": "insight",
            "title": "Recommended Next Steps",
            "content": "\n".join(recommendations),
        })

        return {"slides": slides}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export")
async def export_story(request: ExportStoryRequest):
    """Export the data story as an HTML or PDF file."""
    try:
        from reporting.report_generator import ReportGenerator, ReportExporter
        from state import get_stored_dataframe
        from api.profiling import get_stored_profile

        df = get_stored_dataframe(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Build profile data dict for report generator
        profile_data = None
        insights_data = None
        stored = get_stored_profile(request.file_id)
        if stored:
            if hasattr(stored, "profile") and stored.profile:
                p = stored.profile
                profile_data = p.model_dump() if hasattr(p, "model_dump") else (p.dict() if hasattr(p, "dict") else p)
            if hasattr(stored, "insights") and stored.insights:
                ins = stored.insights
                insights_data = ins.model_dump() if hasattr(ins, "model_dump") else (ins.dict() if hasattr(ins, "dict") else ins)

        # Generate full report
        report = ReportGenerator.generate(
            profile_data=profile_data,
            insights_data=insights_data,
        )

        fmt = request.format.lower()

        if fmt == "pdf":
            pdf_bytes = ReportExporter.to_pdf(report)
            return StreamingResponse(
                io.BytesIO(pdf_bytes),
                media_type="application/pdf",
                headers={"Content-Disposition": f"attachment; filename=data_story_{request.file_id}.pdf"},
            )
        else:
            html_str = ReportExporter.to_html(report)
            return StreamingResponse(
                io.BytesIO(html_str.encode("utf-8")),
                media_type="text/html",
                headers={"Content-Disposition": f"attachment; filename=data_story_{request.file_id}.html"},
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
