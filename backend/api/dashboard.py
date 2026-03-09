"""
Dashboard API — REST endpoints for the AI Dashboard Builder.

Endpoints:
  POST /api/dashboard/interpret  — NL prompt → ChartConfig
  POST /api/dashboard/query      — ChartConfig → SQL → data
  POST /api/dashboard/suggest    — AI follow-up suggestions
  POST /api/dashboards           — Save dashboard
  GET  /api/dashboards           — List dashboards
  GET  /api/dashboards/{id}      — Load dashboard
  DELETE /api/dashboards/{id}    — Delete dashboard
  GET  /api/dashboards/share/{id}— Read-only shared view
"""

from __future__ import annotations

import time
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from state import TTLStore, get_stored_dataframe, get_stored_data

from dashboard.chart_config_models import (
    ChartConfig,
    InterpretRequest,
    InterpretResponse,
    QueryRequest,
    SuggestPromptsRequest,
    DashboardModel,
    ClarificationRequest,
    FilterCondition,
)
from dashboard.prompt_interpreter import interpret_prompt, suggest_follow_ups
from dashboard.config_to_sql import config_to_sql

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dashboard"])

# Bounded store for saved dashboards (100 entries, 24hr TTL)
_dashboard_store: TTLStore = TTLStore(max_entries=100, ttl_seconds=86400)


def _get_schema_for_file(file_id: str) -> dict:
    """Build a schema dict from the stored dataframe for prompt interpretation."""
    df = get_stored_dataframe(file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    schema = {}
    for col in df.columns:
        col_info = {
            "dtype": str(df[col].dtype),
        }
        # Add sample values for context
        non_null = df[col].dropna()
        if len(non_null) > 0:
            samples = non_null.head(5).tolist()
            col_info["sample_values"] = [str(s)[:50] for s in samples]
        schema[col] = col_info
    return schema


def _get_table_name(file_id: str) -> str:
    """Get the DuckDB table name for a file_id."""
    stored = get_stored_data(file_id)
    if stored and "table_name" in stored:
        return stored["table_name"]
    # Fallback — use file_id as table name (DuckDB may register it this way)
    return f"upload_{file_id}"


# ──────────────────────────────────────────
# Prompt Interpretation
# ──────────────────────────────────────────

@router.post("/api/dashboard/interpret")
async def interpret(request: InterpretRequest):
    """Interpret a natural language prompt into a ChartConfig."""
    schema = _get_schema_for_file(request.file_id)

    result = await interpret_prompt(
        message=request.message,
        schema=schema,
        current_config=request.current_config,
        conversation_history=request.conversation_history,
    )

    if isinstance(result, ClarificationRequest):
        return InterpretResponse(
            success=True,
            clarification=result,
        )

    # Generate SQL and execute to get data
    config: ChartConfig = result
    try:
        table_name = _get_table_name(request.file_id)
        sql = config_to_sql(config, table_name)
        data = _execute_sql(request.file_id, sql)
        return InterpretResponse(
            success=True,
            config=config,
            sql=sql,
            data=data,
        )
    except Exception as e:
        logger.warning("SQL execution failed after interpretation: %s", e)
        return InterpretResponse(
            success=False,
            error=str(e),
            config=config,
            sql=None,
            data=None,
        )


@router.post("/api/dashboard/query")
async def query(request: QueryRequest):
    """Execute a ChartConfig query and return data rows."""
    try:
        table_name = _get_table_name(request.file_id)
        sql = config_to_sql(config=request.config, table_name=table_name, global_filters=request.global_filters)
        data = _execute_sql(request.file_id, sql)
        return {"sql": sql, "data": data, "row_count": len(data)}
    except Exception as e:
        logger.error("Query execution failed: %s", e)
        raise HTTPException(status_code=500, detail="Query execution failed. Please check your data and configuration.")


@router.post("/api/dashboard/suggest")
async def suggest(request: SuggestPromptsRequest):
    """Get AI-generated follow-up prompt suggestions."""
    schema = _get_schema_for_file(request.file_id)
    suggestions = await suggest_follow_ups(schema, request.current_config)
    return {"suggestions": suggestions}


def _execute_sql(file_id: str, sql: str) -> list[dict]:
    """Execute SQL against the DuckDB engine and return rows as dicts."""
    try:
        from sql.engine import execute_query
        result = execute_query(file_id, sql)
        if isinstance(result, dict):
            return result.get("rows", [])
        return []
    except ImportError:
        # Fallback: execute directly on the DataFrame
        import duckdb
        df = get_stored_dataframe(file_id)
        if df is None:
            return []
        try:
            table_name = _get_table_name(file_id)
            conn = duckdb.connect()
            conn.register(table_name, df)
            result_df = conn.execute(sql).df()
            return result_df.to_dict(orient="records")
        except Exception as e:
            logger.error("DuckDB execution failed: %s", e)
            return []


# ──────────────────────────────────────────
# Dashboard CRUD
# ──────────────────────────────────────────

@router.post("/api/dashboards")
async def save_dashboard(dashboard: DashboardModel):
    """Save a dashboard configuration."""
    existing = _dashboard_store.get(dashboard.id)
    if existing:
        if existing.get("file_id") != dashboard.file_id:
            raise HTTPException(status_code=403, detail="Cannot overwrite dashboard belonging to another file.")
        dashboard.created_at = existing.get("created_at", time.time())
    else:
        dashboard.created_at = time.time()
        
    dashboard.updated_at = time.time()
    _dashboard_store[dashboard.id] = dashboard.model_dump()
    return {"id": dashboard.id, "status": "saved"}


@router.get("/api/dashboards")
async def list_dashboards():
    """List all saved dashboards."""
    dashboards = []
    for dash_id in _dashboard_store:
        dash = _dashboard_store[dash_id]
        dashboards.append({
            "id": dash.get("id", dash_id),
            "title": dash.get("title", "Untitled"),
            "description": dash.get("description", ""),
            "widget_count": len(dash.get("widgets", [])),
            "updated_at": dash.get("updated_at", 0),
        })
    return {"dashboards": dashboards}


@router.get("/api/dashboards/share/{dashboard_id}")
async def get_shared_dashboard(dashboard_id: str):
    """Load a dashboard in read-only mode (shared view)."""
    if dashboard_id not in _dashboard_store:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    dash = _dashboard_store[dashboard_id]
    return {**dash, "readonly": True}


@router.get("/api/dashboards/{dashboard_id}")
async def get_dashboard(dashboard_id: str):
    """Load a dashboard by ID."""
    if dashboard_id not in _dashboard_store:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return _dashboard_store[dashboard_id]


@router.delete("/api/dashboards/{dashboard_id}")
async def delete_dashboard(dashboard_id: str):
    """Delete a dashboard."""
    if dashboard_id not in _dashboard_store:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    del _dashboard_store[dashboard_id]
    return {"deleted": True}
