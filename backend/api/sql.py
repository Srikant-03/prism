"""
SQL API — FastAPI endpoints for the SQL Query Engine.
Provides table introspection, query execution, and export.
"""

from __future__ import annotations

import io
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from sql.sql_engine import SQLEngine
from sql.query_builder import QueryBuilder
from sql.nl_query import get_translator
from sql.template_generator import TemplateGenerator

router = APIRouter(prefix="/api/sql", tags=["sql"])

# ── Singleton engine instance ─────────────────────────────────────────
_engine: Optional[SQLEngine] = None


def get_engine() -> SQLEngine:
    """Get or create the global SQL engine instance."""
    global _engine
    if _engine is None:
        _engine = SQLEngine()
    return _engine


def register_table_from_upload(
    df, name: str, source: str = "raw", file_id: str = None,
) -> str:
    """Called by upload/cleaning modules to register tables."""
    engine = get_engine()
    return engine.register_dataframe(df, name, source, file_id)


# ── Request models ────────────────────────────────────────────────────

class ExecuteRequest(BaseModel):
    sql: Optional[str] = None
    query_spec: Optional[dict] = None


class ExportRequest(BaseModel):
    sql: Optional[str] = None
    query_spec: Optional[dict] = None
    format: str = "csv"  # csv, json, excel


class NLQueryRequest(BaseModel):
    question: str
    conversation_history: Optional[list] = None


class NLRefineRequest(BaseModel):
    original_question: str
    original_sql: str
    refinement: str


class TemplateExecuteRequest(BaseModel):
    sql: str
    params: Optional[dict] = None


class ViewRequest(BaseModel):
    name: str
    sql: str


class ExplainRequest(BaseModel):
    sql: str


# ── Endpoints ─────────────────────────────────────────────────────────

@router.get("/tables")
async def list_tables():
    """List all available tables with metadata."""
    engine = get_engine()
    return {"tables": engine.list_tables()}


@router.get("/columns/{table_name}")
async def get_columns(table_name: str):
    """Get column details for a table."""
    engine = get_engine()
    columns = engine.get_columns(table_name)
    if not columns:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return {"table": table_name, "columns": columns}


@router.get("/values/{table_name}/{column_name}")
async def get_column_values(table_name: str, column_name: str, limit: int = 50):
    """Get distinct values for autocomplete."""
    engine = get_engine()
    values = engine.get_column_values(table_name, column_name, limit)
    return {"table": table_name, "column": column_name, "values": values}


@router.get("/preview/{table_name}")
async def preview_table(table_name: str, limit: int = 10):
    """Get a preview of a table."""
    engine = get_engine()
    return engine.get_table_preview(table_name, limit)


@router.post("/execute")
async def execute_query(request: ExecuteRequest):
    """Execute a SQL query or query spec."""
    engine = get_engine()

    if request.query_spec:
        # Build SQL from spec
        errors = QueryBuilder.validate_spec(request.query_spec)
        if errors:
            return {"success": False, "errors": errors}
        sql = QueryBuilder.build(request.query_spec)
    elif request.sql:
        sql = request.sql
    else:
        raise HTTPException(status_code=400, detail="Provide 'sql' or 'query_spec'")

    result = engine.execute(sql)
    return result


@router.post("/preview-sql")
async def preview_sql(request: ExecuteRequest):
    """Generate SQL from a query spec without executing."""
    if not request.query_spec:
        raise HTTPException(status_code=400, detail="Provide 'query_spec'")

    errors = QueryBuilder.validate_spec(request.query_spec)
    if errors:
        return {"success": False, "errors": errors, "sql": ""}

    sql = QueryBuilder.build(request.query_spec)
    return {"success": True, "sql": sql}


@router.post("/export")
async def export_results(request: ExportRequest):
    """Execute query and export results."""
    from fastapi.responses import StreamingResponse

    engine = get_engine()

    if request.query_spec:
        sql = QueryBuilder.build(request.query_spec)
    elif request.sql:
        sql = request.sql
    else:
        raise HTTPException(status_code=400, detail="Provide 'sql' or 'query_spec'")

    try:
        df = engine.execute_to_dataframe(sql)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    fmt = request.format.lower()

    if fmt == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=query_results.csv"},
        )

    elif fmt == "json":
        return df.to_dict(orient="records")

    elif fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=query_results.xlsx"},
        )

    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {fmt}")


# ── Natural Language Query endpoints ──────────────────────────────────

@router.post("/nl-query")
async def nl_query(request: NLQueryRequest):
    """Translate natural language to SQL using Gemini."""
    engine = get_engine()
    translator = get_translator()
    result = await translator.translate(
        question=request.question,
        engine=engine,
        conversation_history=request.conversation_history,
    )
    return result


@router.post("/nl-refine")
async def nl_refine(request: NLRefineRequest):
    """Refine a previously generated SQL query."""
    engine = get_engine()
    translator = get_translator()
    result = await translator.refine(
        original_question=request.original_question,
        original_sql=request.original_sql,
        refinement=request.refinement,
        engine=engine,
    )
    return result


# ── Template Library endpoints ────────────────────────────────────────

@router.get("/templates/{table_name}")
async def get_templates(table_name: str):
    """Generate context-aware query templates for a table."""
    engine = get_engine()
    generator = TemplateGenerator(engine)
    templates = generator.generate_templates(table_name)

    # Group by category
    categories = {}
    for t in templates:
        cat = t["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(t)

    return {
        "table": table_name,
        "template_count": len(templates),
        "categories": categories,
    }


@router.post("/template-execute")
async def template_execute(request: TemplateExecuteRequest):
    """Execute a template with user-supplied parameters."""
    engine = get_engine()
    sql = request.sql

    # Replace template parameters {{param_name}} with values
    if request.params:
        for key, value in request.params.items():
            placeholder = "{{" + key + "}}"
            sql = sql.replace(placeholder, str(value))

    # Re-check for destructive keywords after parameter interpolation
    # to prevent injection via template params (e.g. {{col}} = "x; DROP TABLE y")
    stripped = sql.strip().upper()
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE"]
    for word in forbidden:
        if word in stripped.split():
            raise HTTPException(
                status_code=400,
                detail=f"Forbidden SQL keyword '{word}' detected after parameter interpolation.",
            )

    result = engine.execute(sql)
    return result


# ── Advanced SQL endpoints ────────────────────────────────────────────

@router.post("/explain")
async def explain_query(request: ExplainRequest):
    """Get the query execution plan."""
    engine = get_engine()
    return engine.explain_query(request.sql)


@router.post("/views")
async def create_view(request: ViewRequest):
    """Create a named virtual view."""
    engine = get_engine()
    return engine.create_view(request.name, request.sql)


@router.get("/views")
async def list_views():
    """List all views."""
    engine = get_engine()
    return {"views": engine.list_views()}


@router.delete("/views/{name}")
async def drop_view(name: str):
    """Drop a view."""
    engine = get_engine()
    return engine.drop_view(name)


@router.post("/execute-cached")
async def execute_cached(request: ExecuteRequest):
    """Execute with result caching."""
    engine = get_engine()
    sql = request.sql or ""
    if request.query_spec and not sql:
        sql = QueryBuilder.build(request.query_spec)
    return engine.execute_cached(sql)


@router.post("/clear-cache")
async def clear_cache():
    """Clear the query result cache."""
    engine = get_engine()
    count = engine.clear_cache()
    return {"cleared": count}


class FormatRequest(BaseModel):
    sql: str


@router.post("/format")
async def format_sql(request: FormatRequest):
    """Format/prettify a SQL query string."""
    try:
        import sqlparse
        formatted = sqlparse.format(
            request.sql,
            reindent=True,
            keyword_case="upper",
            identifier_case=None,
            strip_comments=False,
            indent_width=2,
        )
        return {"formatted": formatted}
    except ImportError:
        # Fallback: basic formatting without sqlparse
        import re
        sql = request.sql.strip()
        keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "ORDER BY",
            "GROUP BY", "HAVING", "LIMIT", "OFFSET", "JOIN",
            "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL OUTER JOIN",
            "ON", "AS", "CASE", "WHEN", "THEN", "ELSE", "END",
            "UNION", "UNION ALL", "INSERT", "UPDATE", "DELETE",
            "CREATE", "ALTER", "DROP",
        ]
        for kw in sorted(keywords, key=len, reverse=True):
            pattern = re.compile(rf'\b{re.escape(kw)}\b', re.IGNORECASE)
            sql = pattern.sub(kw, sql)
        return {"formatted": sql}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
