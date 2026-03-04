"""
Report Generator — Builds a comprehensive analysis report from profiling,
cleaning, and insights data. Exports to PDF, DOCX, HTML, and Jupyter Notebook.
"""

from __future__ import annotations

import io
import json
import time
from datetime import datetime
from typing import Any, Optional

import pandas as pd


class ReportSection:
    """One section of the full report."""

    def __init__(self, title: str, content: str, subsections: list[dict] = None,
                 tables: list[dict] = None, charts: list[dict] = None):
        self.title = title
        self.content = content
        self.subsections = subsections or []
        self.tables = tables or []
        self.charts = charts or []

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "content": self.content,
            "subsections": self.subsections,
            "tables": self.tables,
            "charts": self.charts,
        }


class FullReport:
    """Complete analysis report with all sections."""

    def __init__(self):
        self.title = "Data Intelligence Platform — Full Analysis Report"
        self.generated_at = datetime.utcnow().isoformat()
        self.sections: list[ReportSection] = []

    def add_section(self, section: ReportSection):
        self.sections.append(section)

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "generated_at": self.generated_at,
            "sections": [s.to_dict() for s in self.sections],
        }


class ReportGenerator:
    """
    Generates a comprehensive analysis report combining:
    - Executive summary
    - Dataset overview & profiling
    - Data quality scores
    - Preprocessing decision log
    - Before/after statistics
    - Feature importance
    - Recommendations
    """

    @staticmethod
    def generate(
        profile_data: Optional[dict] = None,
        cleaning_data: Optional[dict] = None,
        insights_data: Optional[dict] = None,
        audit_log: Optional[list[dict]] = None,
        before_after: Optional[dict] = None,
    ) -> FullReport:
        """Build the full report from all available data."""
        report = FullReport()

        # 1. Executive summary
        report.add_section(ReportGenerator._executive_summary(
            profile_data, insights_data, cleaning_data))

        # 2. Dataset overview
        if profile_data:
            report.add_section(ReportGenerator._dataset_overview(profile_data))

        # 3. Data quality assessment
        if insights_data and "quality_score" in insights_data:
            report.add_section(ReportGenerator._quality_assessment(
                insights_data["quality_score"]))

        # 4. Profiling findings
        if profile_data and "columns" in profile_data:
            report.add_section(ReportGenerator._profiling_findings(profile_data))

        # 5. Preprocessing decisions log
        if audit_log:
            report.add_section(ReportGenerator._preprocessing_log(audit_log))

        # 6. Before/after statistics
        if before_after:
            report.add_section(ReportGenerator._before_after_stats(before_after))

        # 7. Feature importance
        if insights_data and "feature_rankings" in insights_data:
            report.add_section(ReportGenerator._feature_importance(
                insights_data["feature_rankings"]))

        # 8. Anomaly warnings
        if insights_data and "anomalies" in insights_data:
            report.add_section(ReportGenerator._anomaly_summary(
                insights_data["anomalies"]))

        # 9. Recommendations
        report.add_section(ReportGenerator._recommendations(
            profile_data, insights_data, cleaning_data))

        return report

    # ── Section builders ─────────────────────────────────────────────

    @staticmethod
    def _executive_summary(profile: dict = None, insights: dict = None,
                           cleaning: dict = None) -> ReportSection:
        """Build executive summary."""
        parts = []
        if profile:
            rows = profile.get("total_rows", 0)
            cols = profile.get("total_columns", 0)
            domain = profile.get("estimated_domain", "Unknown")
            parts.append(
                f"This analysis covers a dataset of {rows:,} records across {cols} "
                f"attributes, classified as a **{domain}** domain dataset."
            )

        if insights and "quality_score" in insights:
            qs = insights["quality_score"]
            grade = qs.get("grade", "N/A")
            score = qs.get("overall_score", 0)
            parts.append(
                f"Overall data quality is rated **{grade}** ({score}/100)."
            )

        if insights and "analyst_briefing" in insights:
            brief = insights["analyst_briefing"]
            if isinstance(brief, dict) and "executive_summary" in brief:
                parts.append(brief["executive_summary"])

        if cleaning:
            total = cleaning.get("total_actions", 0)
            applied = cleaning.get("applied", 0)
            parts.append(
                f"{applied} of {total} recommended preprocessing steps were applied."
            )

        content = " ".join(parts) if parts else "No data available for summary."
        return ReportSection("Executive Summary", content)

    @staticmethod
    def _dataset_overview(profile: dict) -> ReportSection:
        """Dataset shape, memory, types breakdown."""
        rows = profile.get("total_rows", 0)
        cols = profile.get("total_columns", 0)
        mem = profile.get("memory_size_bytes", 0) / (1024 * 1024)
        domain = profile.get("estimated_domain", "Unknown")
        dup = profile.get("duplicate_row_count", 0)

        content = (
            f"The dataset contains **{rows:,}** rows and **{cols}** columns, "
            f"using approximately **{mem:.2f} MB** of memory. "
            f"Estimated domain: **{domain}**. "
            f"Duplicate rows detected: **{dup:,}**."
        )

        # Column type breakdown table
        columns = profile.get("columns", [])
        type_counts = {}
        for c in columns:
            t = c.get("semantic_type", c.get("ui_type", "unknown"))
            type_counts[t] = type_counts.get(t, 0) + 1

        tables = [{
            "title": "Column Type Distribution",
            "headers": ["Type", "Count"],
            "rows": [[t, str(c)] for t, c in sorted(type_counts.items())],
        }]

        return ReportSection("Dataset Overview", content, tables=tables)

    @staticmethod
    def _quality_assessment(quality: dict) -> ReportSection:
        """Data quality dimension scores."""
        content = (
            f"Overall quality grade: **{quality.get('grade', 'N/A')}** "
            f"(Score: {quality.get('overall_score', 0)}/100)"
        )

        dimensions = [
            ("Completeness", quality.get("completeness", 0)),
            ("Uniqueness", quality.get("uniqueness", 0)),
            ("Validity", quality.get("validity", 0)),
            ("Consistency", quality.get("consistency", 0)),
            ("Timeliness", quality.get("timeliness", 0)),
        ]

        tables = [{
            "title": "Quality Dimensions",
            "headers": ["Dimension", "Score (/100)"],
            "rows": [[d, str(s)] for d, s in dimensions if s],
        }]

        return ReportSection("Data Quality Assessment", content, tables=tables)

    @staticmethod
    def _profiling_findings(profile: dict) -> ReportSection:
        """Per-column profiling stats."""
        columns = profile.get("columns", [])

        rows_data = []
        for c in columns[:50]:  # Limit to 50 columns
            rows_data.append([
                c.get("name", ""),
                c.get("dtype", ""),
                f"{c.get('null_percentage', 0):.1f}%",
                str(c.get("unique_count", 0)),
                c.get("semantic_type", ""),
            ])

        tables = [{
            "title": "Column Statistics",
            "headers": ["Column", "Type", "Nulls %", "Unique", "Semantic Type"],
            "rows": rows_data,
        }]

        content = f"Profiling analysis for {len(columns)} columns."
        return ReportSection("Profiling Findings", content, tables=tables)

    @staticmethod
    def _preprocessing_log(audit_log: list[dict]) -> ReportSection:
        """Detailed log of every preprocessing decision."""
        rows_data = []
        for entry in audit_log:
            rows_data.append([
                entry.get("step_name", ""),
                entry.get("action_type", ""),
                entry.get("status", ""),
                entry.get("trigger_reason", ""),
                ", ".join(entry.get("columns_affected", [])[:3]),
                f"{entry.get('rows_before', 0)} → {entry.get('rows_after', 0)}",
            ])

        tables = [{
            "title": "Preprocessing Steps",
            "headers": ["Step", "Action", "Status", "Reason", "Columns", "Rows (Before→After)"],
            "rows": rows_data,
        }]

        applied = sum(1 for e in audit_log if e.get("status") == "applied")
        skipped = sum(1 for e in audit_log if e.get("status") == "skipped")

        content = (
            f"Total preprocessing steps: **{len(audit_log)}** "
            f"({applied} applied, {skipped} skipped)"
        )
        return ReportSection("Preprocessing Decisions Log", content, tables=tables)

    @staticmethod
    def _before_after_stats(comparison: dict) -> ReportSection:
        """Before/after statistics for transformed columns."""
        content = (
            f"Shape: {comparison.get('original_shape', '?')} → "
            f"{comparison.get('current_shape', '?')}"
        )

        col_changes = comparison.get("column_changes", {})
        rows_data = []
        for col, changes in col_changes.items():
            if isinstance(changes, dict):
                rows_data.append([
                    col,
                    str(changes.get("null_before", "")),
                    str(changes.get("null_after", "")),
                    str(changes.get("dtype_before", "")),
                    str(changes.get("dtype_after", "")),
                ])

        tables = []
        if rows_data:
            tables.append({
                "title": "Column Transformations",
                "headers": ["Column", "Nulls Before", "Nulls After", "Type Before", "Type After"],
                "rows": rows_data[:30],
            })

        return ReportSection("Before/After Statistics", content, tables=tables)

    @staticmethod
    def _feature_importance(rankings: list) -> ReportSection:
        """Feature importance table."""
        rows_data = []
        for r in rankings[:20]:
            if isinstance(r, dict):
                rows_data.append([
                    r.get("feature", ""),
                    f"{r.get('importance_score', 0):.1f}",
                    r.get("method", ""),
                    r.get("rationale", "")[:80],
                ])

        tables = [{
            "title": "Feature Rankings",
            "headers": ["Feature", "Score", "Method", "Rationale"],
            "rows": rows_data,
        }]

        content = f"Top {len(rows_data)} features by predictive importance."
        return ReportSection("Feature Importance", content, tables=tables)

    @staticmethod
    def _anomaly_summary(anomalies: list) -> ReportSection:
        """Anomaly warnings summary."""
        if not anomalies:
            return ReportSection("Anomaly Summary", "No anomalies detected.")

        rows_data = []
        for a in anomalies:
            if isinstance(a, dict):
                rows_data.append([
                    a.get("feature", ""),
                    a.get("severity", ""),
                    a.get("anomaly_type", ""),
                    a.get("description", "")[:100],
                ])

        tables = [{
            "title": "Anomaly Warnings",
            "headers": ["Feature", "Severity", "Type", "Description"],
            "rows": rows_data,
        }]

        critical = sum(1 for a in anomalies if isinstance(a, dict)
                       and a.get("severity", "").lower() in ("critical", "high"))
        content = (
            f"{len(anomalies)} anomalies detected ({critical} critical/high severity)."
        )
        return ReportSection("Anomaly Summary", content, tables=tables)

    @staticmethod
    def _recommendations(profile: dict = None, insights: dict = None,
                         cleaning: dict = None) -> ReportSection:
        """Recommended next steps."""
        recs = []

        if insights and "analyst_briefing" in insights:
            brief = insights["analyst_briefing"]
            if isinstance(brief, dict):
                for action in brief.get("recommended_actions", []):
                    recs.append(action)

        if profile:
            cols = profile.get("columns", [])
            high_null = [c["name"] for c in cols if c.get("null_percentage", 0) > 50]
            if high_null:
                recs.append(
                    f"Consider dropping columns with >50% nulls: {', '.join(high_null[:5])}"
                )

            cross = profile.get("cross_analysis", {})
            if isinstance(cross, dict):
                target = cross.get("target_analysis", {})
                if isinstance(target, dict) and target.get("target_column"):
                    recs.append(
                        f"Suggested modeling target: '{target['target_column']}' "
                        f"for {target.get('problem_type', 'classification')}"
                    )

        if not recs:
            recs.append("Dataset is ready for downstream analysis or modeling.")

        content = "\n".join(f"• {r}" for r in recs)
        return ReportSection("Recommended Next Steps", content)


# ── Export formatters ─────────────────────────────────────────────────

class ReportExporter:
    """Export FullReport to various formats."""

    @staticmethod
    def to_html(report: FullReport) -> str:
        """Generate an HTML report with embedded CSS charts."""
        lines = [
            "<!DOCTYPE html>",
            '<html lang="en"><head>',
            '<meta charset="utf-8"/>',
            f"<title>{report.title}</title>",
            "<style>",
            "body { font-family: 'Segoe UI', sans-serif; max-width: 900px; margin: 40px auto; padding: 20px; background: #0d1117; color: #c9d1d9; }",
            "h1 { color: #58a6ff; border-bottom: 2px solid #21262d; padding-bottom: 10px; }",
            "h2 { color: #79c0ff; margin-top: 30px; }",
            "h3 { color: #8b949e; margin-top: 16px; }",
            "table { border-collapse: collapse; width: 100%; margin: 12px 0; }",
            "th { background: #161b22; color: #58a6ff; padding: 8px 12px; text-align: left; border: 1px solid #30363d; font-size: 12px; }",
            "td { padding: 6px 12px; border: 1px solid #30363d; font-size: 12px; }",
            "tr:nth-child(even) { background: rgba(255,255,255,0.02); }",
            "p { line-height: 1.6; }",
            ".meta { color: #8b949e; font-size: 12px; }",
            "strong { color: #f0f6fc; }",
            # CSS chart styles
            ".chart-container { margin: 16px 0; padding: 16px; background: #161b22; border-radius: 8px; border: 1px solid #30363d; }",
            ".chart-title { font-size: 13px; font-weight: 600; color: #79c0ff; margin-bottom: 12px; }",
            ".bar-chart { display: flex; flex-direction: column; gap: 6px; }",
            ".bar-row { display: flex; align-items: center; gap: 8px; }",
            ".bar-label { min-width: 120px; font-size: 11px; text-align: right; color: #8b949e; }",
            ".bar-track { flex: 1; height: 20px; background: #21262d; border-radius: 4px; overflow: hidden; }",
            ".bar-fill { height: 100%; border-radius: 4px; transition: width 0.3s; display: flex; align-items: center; padding-left: 6px; font-size: 10px; color: white; font-weight: 600; }",
            ".bar-value { min-width: 60px; font-size: 11px; color: #c9d1d9; font-family: monospace; }",
            ".pie-chart { display: flex; align-items: center; gap: 24px; }",
            ".pie-circle { width: 120px; height: 120px; border-radius: 50%; position: relative; }",
            ".pie-legend { display: flex; flex-direction: column; gap: 4px; }",
            ".pie-legend-item { display: flex; align-items: center; gap: 6px; font-size: 11px; }",
            ".pie-legend-dot { width: 10px; height: 10px; border-radius: 2px; }",
            "</style>",
            "</head><body>",
            f"<h1>{report.title}</h1>",
            f'<p class="meta">Generated: {report.generated_at}</p>',
        ]

        chart_colors = ['#56B4E9', '#E69F00', '#009E73', '#CC79A7', '#0072B2', '#D55E00', '#F0E442', '#999']

        for section in report.sections:
            lines.append(f"<h2>{section.title}</h2>")
            # Convert markdown bold to HTML
            content = section.content.replace("**", "<strong>", 1)
            while "**" in content:
                content = content.replace("**", "</strong>", 1)
                if "**" in content:
                    content = content.replace("**", "<strong>", 1)
            lines.append(f"<p>{content}</p>")

            for table in section.tables:
                lines.append(f"<h3>{table.get('title', '')}</h3>")
                lines.append("<table>")
                lines.append("<tr>" + "".join(f"<th>{h}</th>" for h in table.get("headers", [])) + "</tr>")
                for row in table.get("rows", []):
                    lines.append("<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>")
                lines.append("</table>")

            # Render embedded charts
            for chart in section.charts:
                chart_type = chart.get("type", "bar")
                title = chart.get("title", "")
                data = chart.get("data", [])

                lines.append('<div class="chart-container">')
                lines.append(f'<div class="chart-title">{title}</div>')

                if chart_type == "bar" and data:
                    max_val = max((abs(d.get("value", 0)) for d in data), default=1) or 1
                    lines.append('<div class="bar-chart">')
                    for i, d in enumerate(data[:20]):
                        val = d.get("value", 0)
                        pct = min(abs(val) / max_val * 100, 100)
                        color = chart_colors[i % len(chart_colors)]
                        label = str(d.get("label", ""))[:30]
                        lines.append(f'<div class="bar-row">')
                        lines.append(f'  <span class="bar-label">{label}</span>')
                        lines.append(f'  <div class="bar-track"><div class="bar-fill" style="width:{pct:.0f}%;background:{color}">{val}</div></div>')
                        lines.append(f'</div>')
                    lines.append('</div>')

                elif chart_type == "pie" and data:
                    total = sum(d.get("value", 0) for d in data) or 1
                    # Build conic-gradient
                    gradients = []
                    cumulative = 0
                    for i, d in enumerate(data[:8]):
                        val = d.get("value", 0)
                        pct = val / total * 100
                        color = chart_colors[i % len(chart_colors)]
                        gradients.append(f"{color} {cumulative:.1f}% {cumulative + pct:.1f}%")
                        cumulative += pct
                    gradient = ", ".join(gradients)
                    lines.append('<div class="pie-chart">')
                    lines.append(f'  <div class="pie-circle" style="background:conic-gradient({gradient})"></div>')
                    lines.append('  <div class="pie-legend">')
                    for i, d in enumerate(data[:8]):
                        color = chart_colors[i % len(chart_colors)]
                        label = str(d.get("label", ""))[:30]
                        pct = d.get("value", 0) / total * 100
                        lines.append(f'    <div class="pie-legend-item"><div class="pie-legend-dot" style="background:{color}"></div>{label}: {pct:.1f}%</div>')
                    lines.append('  </div>')
                    lines.append('</div>')

                lines.append('</div>')

        lines.append("</body></html>")
        return "\n".join(lines)

    @staticmethod
    def to_pdf(report: FullReport) -> bytes:
        """Generate a PDF report."""
        try:
            from fpdf import FPDF
        except ImportError:
            return b""

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()

        # Title
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 12, report.title, ln=True, align="C")
        pdf.set_font("Arial", "", 10)
        pdf.cell(0, 6, f"Generated: {report.generated_at}", ln=True, align="C")
        pdf.ln(8)

        for section in report.sections:
            pdf.set_font("Arial", "B", 13)
            pdf.cell(0, 10, section.title, ln=True)
            pdf.set_font("Arial", "", 10)
            # Clean markdown bold markers
            text = section.content.replace("**", "")
            pdf.multi_cell(0, 5, text)
            pdf.ln(3)

            for table in section.tables:
                pdf.set_font("Arial", "B", 10)
                pdf.cell(0, 7, table.get("title", ""), ln=True)

                headers = table.get("headers", [])
                rows_data = table.get("rows", [])
                if not headers:
                    continue

                col_w = min(180 // len(headers), 50)
                pdf.set_font("Arial", "B", 8)
                for h in headers:
                    pdf.cell(col_w, 6, str(h)[:20], border=1)
                pdf.ln()

                pdf.set_font("Arial", "", 8)
                for row in rows_data[:30]:
                    for val in row:
                        pdf.cell(col_w, 5, str(val)[:20], border=1)
                    pdf.ln()
                pdf.ln(3)

        return bytes(pdf.output())

    @staticmethod
    def to_docx(report: FullReport) -> bytes:
        """Generate a DOCX report."""
        try:
            from docx import Document
        except ImportError:
            return b""

        doc = Document()
        doc.add_heading(report.title, 0)
        doc.add_paragraph(f"Generated: {report.generated_at}")

        for section in report.sections:
            doc.add_heading(section.title, level=1)
            doc.add_paragraph(section.content.replace("**", ""))

            for table in section.tables:
                doc.add_heading(table.get("title", ""), level=2)
                headers = table.get("headers", [])
                rows_data = table.get("rows", [])
                if not headers or not rows_data:
                    continue

                t = doc.add_table(rows=1, cols=len(headers))
                t.style = "Table Grid"
                for i, h in enumerate(headers):
                    t.rows[0].cells[i].text = str(h)
                for row in rows_data[:30]:
                    cells = t.add_row().cells
                    for i, val in enumerate(row):
                        if i < len(cells):
                            cells[i].text = str(val)

        bio = io.BytesIO()
        doc.save(bio)
        return bio.getvalue()

    @staticmethod
    def to_notebook(report: FullReport) -> str:
        """Generate a Jupyter Notebook (.ipynb) report."""
        cells = []

        # Title cell
        cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# {report.title}\n",
                f"*Generated: {report.generated_at}*\n",
            ],
        })

        for section in report.sections:
            # Section header
            cells.append({
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"## {section.title}\n\n{section.content}\n"],
            })

            # Tables as markdown
            for table in section.tables:
                headers = table.get("headers", [])
                rows_data = table.get("rows", [])
                if not headers:
                    continue

                lines = [f"### {table.get('title', '')}\n\n"]
                lines.append("| " + " | ".join(headers) + " |\n")
                lines.append("| " + " | ".join(["---"] * len(headers)) + " |\n")
                for row in rows_data[:30]:
                    lines.append("| " + " | ".join(str(v) for v in row) + " |\n")
                lines.append("\n")

                cells.append({
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": lines,
                })

        notebook = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
            },
            "cells": cells,
        }

        return json.dumps(notebook, indent=2)
