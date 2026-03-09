"""
Story API — Automated data storytelling endpoints.
Generates a 10-slide narrative deck from real profiling, quality, anomaly,
and feature-importance data.  Exports to HTML or PPTX.
"""

from __future__ import annotations

import io
import uuid
from typing import Optional

import numpy as np
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/story", tags=["story"])


class StoryRequest(BaseModel):
    file_id: str


class ExportStoryRequest(BaseModel):
    file_id: str
    format: str = "html"  # html | pdf | pptx


# ── Helpers ───────────────────────────────────────────────────────────

def _safe_profile(stored):
    """Extract profile object from stored profiling result."""
    if stored is None:
        return None
    p = getattr(stored, "profile", None)
    if p is None and isinstance(stored, dict):
        p = stored.get("profile")
    return p


def _safe_insights(stored):
    """Extract insights dict from stored profiling result."""
    if stored is None:
        return None
    ins = getattr(stored, "insights", None)
    if ins is None and isinstance(stored, dict):
        ins = stored.get("insights")
    return ins


def _uid() -> str:
    return str(uuid.uuid4())[:8]


# ── 10-Slide Generator ───────────────────────────────────────────────

@router.post("/generate")
async def generate_story(request: StoryRequest):
    """Auto-generate a 10-slide data story from real profiling insights."""
    try:
        from state import get_df, get_profile

        df = get_df(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        stored = get_profile(request.file_id)
        profile = _safe_profile(stored)
        insights = _safe_insights(stored)

        slides: list[dict] = []
        null_total = int(df.isnull().sum().sum())
        dup_count = int(df.duplicated().sum())
        total_cells = max(df.size, 1)
        file_label = request.file_id.split("_", 1)[-1] if "_" in request.file_id else request.file_id
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
        datetime_cols = df.select_dtypes(include=["datetime"]).columns.tolist()

        # ── 1. Title ─────────────────────────────────────────────────
        slides.append({
            "id": _uid(), "type": "title",
            "title": f"Data Story: {file_label}",
            "content": (
                f"Dataset shape: {len(df):,} rows × {len(df.columns)} columns\n"
                f"Memory: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB\n"
                f"Total nulls: {null_total:,} | Duplicate rows: {dup_count:,}"
            ),
        })

        # ── 2. Quality KPI ───────────────────────────────────────────
        quality_score, quality_grade = None, None
        quality_dims = {}
        if insights:
            q = getattr(insights, "quality", None) or (insights.get("quality") if isinstance(insights, dict) else None)
            if q:
                quality_score = getattr(q, "overall_score", None) or (q.get("overall_score") if isinstance(q, dict) else None)
                quality_grade = getattr(q, "grade", None) or (q.get("grade") if isinstance(q, dict) else None)
                for dim in ("completeness", "uniqueness", "validity", "consistency"):
                    quality_dims[dim] = getattr(q, dim, None) or (q.get(dim) if isinstance(q, dict) else None)

        if quality_score is not None:
            dim_lines = "\n".join(f"  {k.title()}: {v}/100" for k, v in quality_dims.items() if v is not None)
            slides.append({
                "id": _uid(), "type": "kpi",
                "title": "Data Health Scoreboard",
                "kpiLabel": "Overall Quality",
                "kpiValue": f"{quality_score}/100",
                "content": f"Grade: {quality_grade}\n{dim_lines}",
            })
        else:
            completeness = round(100.0 * (1.0 - null_total / total_cells), 1)
            slides.append({
                "id": _uid(), "type": "kpi",
                "title": "Data Health Scoreboard",
                "kpiLabel": "Completeness",
                "kpiValue": f"{completeness}%",
                "content": f"Completeness: {completeness}%\nDuplicate rate: {round(100.0 * dup_count / max(len(df), 1), 1)}%",
            })

        # ── 3. Column Breakdown ──────────────────────────────────────
        lines = []
        if numeric_cols:
            lines.append(f"• {len(numeric_cols)} numeric: {', '.join(numeric_cols[:5])}{'…' if len(numeric_cols) > 5 else ''}")
        if cat_cols:
            lines.append(f"• {len(cat_cols)} categorical: {', '.join(cat_cols[:5])}{'…' if len(cat_cols) > 5 else ''}")
        if datetime_cols:
            lines.append(f"• {len(datetime_cols)} datetime: {', '.join(datetime_cols[:3])}")
        slides.append({
            "id": _uid(), "type": "insight",
            "title": "Column Type Breakdown",
            "content": "\n".join(lines) or "No columns detected.",
        })

        # ── 4. Missing Data ──────────────────────────────────────────
        null_pcts = df.isnull().mean().sort_values(ascending=False)
        top_nulls = null_pcts[null_pcts > 0.01].head(5)
        if not top_nulls.empty:
            null_lines = [f"• '{c}': {p:.1%} missing ({int(p * len(df)):,} rows)" for c, p in top_nulls.items()]
        else:
            null_lines = ["• No significant missing data detected."]
        slides.append({
            "id": _uid(), "type": "insight",
            "title": "Missing Data Analysis",
            "content": "\n".join(null_lines),
        })

        # ── 5. Anomalies ─────────────────────────────────────────────
        anomaly_lines = []
        if insights:
            anomalies = getattr(insights, "anomalies", None) or (insights.get("anomalies") if isinstance(insights, dict) else None)
            if anomalies:
                anomaly_list = anomalies if isinstance(anomalies, list) else []
                for a in anomaly_list[:3]:
                    sev = getattr(a, "severity", None) or (a.get("severity") if isinstance(a, dict) else "")
                    desc = getattr(a, "description", None) or (a.get("description") if isinstance(a, dict) else "")
                    feat = getattr(a, "feature", None) or (a.get("feature") if isinstance(a, dict) else "")
                    sev_str = sev.value if hasattr(sev, "value") else str(sev)
                    anomaly_lines.append(f"• [{sev_str.upper()}] {feat}: {desc}")
        if not anomaly_lines:
            anomaly_lines.append("• No critical anomalies detected.")
        slides.append({
            "id": _uid(), "type": "insight",
            "title": "Anomaly Warnings",
            "content": "\n".join(anomaly_lines),
        })

        # ── 6. Correlations ──────────────────────────────────────────
        corr_lines = []
        if len(numeric_cols) >= 2:
            try:
                corr = df[numeric_cols].corr()
                np.fill_diagonal(corr.values, 0)
                abs_corr = corr.abs()
                top_pairs = abs_corr.stack().nlargest(3)
                for (c1, c2), val in top_pairs.items():
                    if val > 0.3:
                        corr_lines.append(f"• '{c1}' ↔ '{c2}': r={corr.loc[c1, c2]:.3f}")
            except Exception:
                pass
        if not corr_lines:
            corr_lines.append("• No strong correlations found among numeric columns.")
        slides.append({
            "id": _uid(), "type": "insight",
            "title": "Correlation Analysis",
            "content": "\n".join(corr_lines),
        })

        # ── 7. Feature Importance ────────────────────────────────────
        feat_lines = []
        if insights:
            rankings = getattr(insights, "feature_importance", None) or (insights.get("feature_importance") if isinstance(insights, dict) else None)
            if rankings:
                rank_list = rankings if isinstance(rankings, list) else []
                for r in rank_list[:5]:
                    name = getattr(r, "feature", None) or (r.get("feature") if isinstance(r, dict) else "")
                    score = getattr(r, "importance_score", None) or (r.get("importance_score") if isinstance(r, dict) else 0)
                    feat_lines.append(f"• {name}: {score}/100")
        if not feat_lines:
            feat_lines.append("• Run full profiling to generate feature importance rankings.")
        slides.append({
            "id": _uid(), "type": "insight",
            "title": "Feature Importance",
            "content": "\n".join(feat_lines),
        })

        # ── 8. AI Narrative ──────────────────────────────────────────
        briefing = None
        if insights:
            b = getattr(insights, "briefing", None) or (insights.get("briefing") if isinstance(insights, dict) else None)
            if b:
                exec_summary = getattr(b, "executive_summary", None) or (b.get("executive_summary") if isinstance(b, dict) else None)
                key_findings = getattr(b, "key_findings", None) or (b.get("key_findings") if isinstance(b, dict) else [])
                if exec_summary:
                    findings_str = "\n".join(f"• {f}" for f in (key_findings or [])[:4])
                    briefing = f"{exec_summary}\n\n{findings_str}" if findings_str else exec_summary

        slides.append({
            "id": _uid(), "type": "insight",
            "title": "AI Analyst Narrative",
            "content": briefing or "Run full profiling with insights to generate an AI analyst briefing.",
        })

        # ── 9. Recommendations ───────────────────────────────────────
        rec_lines = []
        if insights:
            b = getattr(insights, "briefing", None) or (insights.get("briefing") if isinstance(insights, dict) else None)
            if b:
                actions = getattr(b, "recommended_actions", None) or (b.get("recommended_actions") if isinstance(b, dict) else [])
                rec_lines = [f"• {a}" for a in (actions or [])[:5]]
        if not rec_lines:
            if null_total > 0:
                rec_lines.append("• Impute or investigate high-null columns before modeling.")
            if dup_count > 0:
                rec_lines.append(f"• {dup_count:,} duplicate rows — consider deduplication.")
            if len(numeric_cols) >= 2:
                rec_lines.append("• Check multicollinearity among correlated features.")
            if not rec_lines:
                rec_lines.append("• Dataset appears clean. Proceed to feature engineering.")
        slides.append({
            "id": _uid(), "type": "insight",
            "title": "Recommendations",
            "content": "\n".join(rec_lines),
        })

        # ── 10. Next Steps ───────────────────────────────────────────
        next_steps = [
            "• Navigate to the Cleaning tab to apply recommended transformations.",
            "• Use the SQL Workbench for custom queries and exploration.",
            "• Export a full PDF/HTML report from the Reporting tab.",
        ]
        if quality_score and quality_score < 70:
            next_steps.insert(0, "• Priority: address data quality issues before analysis.")
        slides.append({
            "id": _uid(), "type": "insight",
            "title": "Suggested Next Steps",
            "content": "\n".join(next_steps),
        })

        return {"slides": slides, "slide_count": len(slides)}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Export ────────────────────────────────────────────────────────────

@router.post("/export")
async def export_story(request: ExportStoryRequest):
    """Export the data story as HTML, PDF, or PPTX."""
    try:
        from reporting.report_generator import ReportGenerator, ReportExporter
        from state import get_df, get_profile

        df = get_df(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        stored = get_profile(request.file_id)
        profile = _safe_profile(stored)
        insights_obj = _safe_insights(stored)

        profile_data = None
        if profile:
            profile_data = profile.model_dump() if hasattr(profile, "model_dump") else (profile.dict() if hasattr(profile, "dict") else profile)

        insights_data = None
        if insights_obj:
            insights_data = insights_obj.model_dump() if hasattr(insights_obj, "model_dump") else (insights_obj.dict() if hasattr(insights_obj, "dict") else insights_obj)

        fmt = request.format.lower()

        # ── PPTX export ──
        if fmt == "pptx":
            # Generate slides first, then build PPTX
            story_resp = await generate_story(StoryRequest(file_id=request.file_id))
            slides = story_resp.get("slides", [])
            pptx_bytes = _build_pptx(slides)
            return StreamingResponse(
                io.BytesIO(pptx_bytes),
                media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                headers={"Content-Disposition": f"attachment; filename=data_story_{request.file_id}.pptx"},
            )

        # ── HTML / PDF via ReportExporter ──
        report = ReportGenerator.generate(profile_data=profile_data, insights_data=insights_data)

        if fmt == "pdf":
            pdf_bytes = ReportExporter.to_pdf(report)
            return StreamingResponse(
                io.BytesIO(pdf_bytes),
                media_type="application/pdf",
                headers={"Content-Disposition": f"attachment; filename=data_story_{request.file_id}.pdf"},
            )

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


def _build_pptx(slides: list[dict]) -> bytes:
    """Build a PowerPoint presentation from slide dicts."""
    try:
        from pptx import Presentation
        from pptx.util import Inches, Pt
        from pptx.dml.color import RGBColor
    except ImportError:
        raise HTTPException(status_code=500, detail="python-pptx is not installed. Run: pip install python-pptx")

    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    for slide_data in slides:
        slide_type = slide_data.get("type", "insight")
        title = slide_data.get("title", "")
        content = slide_data.get("content", "")

        if slide_type == "title":
            layout = prs.slide_layouts[0]  # Title slide
        elif slide_type == "kpi":
            layout = prs.slide_layouts[5]  # Blank
        else:
            layout = prs.slide_layouts[1]  # Title + content

        slide = prs.slides.add_slide(layout)

        # Set title
        if slide.shapes.title:
            slide.shapes.title.text = title
            for para in slide.shapes.title.text_frame.paragraphs:
                for run in para.runs:
                    run.font.size = Pt(28)
                    run.font.color.rgb = RGBColor(0x1E, 0x29, 0x3B)

        # Set body
        if slide_type == "kpi":
            from pptx.util import Emu
            txBox = slide.shapes.add_textbox(Inches(1), Inches(1), Inches(11), Inches(1))
            txBox.text_frame.text = title
            for run in txBox.text_frame.paragraphs[0].runs:
                run.font.size = Pt(28)
                run.font.bold = True

            kpi_val = slide_data.get("kpiValue", "")
            kpi_box = slide.shapes.add_textbox(Inches(4), Inches(2.5), Inches(5), Inches(2))
            kpi_box.text_frame.text = kpi_val
            for run in kpi_box.text_frame.paragraphs[0].runs:
                run.font.size = Pt(60)
                run.font.bold = True
                run.font.color.rgb = RGBColor(0x63, 0x66, 0xF1)

            detail_box = slide.shapes.add_textbox(Inches(1), Inches(5), Inches(11), Inches(2))
            detail_box.text_frame.text = content
            for para in detail_box.text_frame.paragraphs:
                for run in para.runs:
                    run.font.size = Pt(14)
        else:
            # Use second placeholder if available
            body_placeholder = None
            for ph in slide.placeholders:
                if ph.placeholder_format.idx == 1:
                    body_placeholder = ph
                    break

            if body_placeholder:
                body_placeholder.text = content
                for para in body_placeholder.text_frame.paragraphs:
                    for run in para.runs:
                        run.font.size = Pt(16)
            else:
                from pptx.util import Emu
                txBox = slide.shapes.add_textbox(Inches(1), Inches(2), Inches(11), Inches(5))
                txBox.text_frame.word_wrap = True
                txBox.text_frame.text = content
                for para in txBox.text_frame.paragraphs:
                    for run in para.runs:
                        run.font.size = Pt(16)

    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()
