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

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from state import TTLStore, get_stored_dataframe, get_stored_data

from dashboard.chart_config_models import (
    ChartConfig,
    ChartType,
    InterpretRequest,
    InterpretResponse,
    QueryRequest,
    SuggestPromptsRequest,
    DashboardModel,
    ClarificationRequest,
    FilterCondition,
    MeasureDefinition,
)
from dashboard.prompt_interpreter import interpret_prompt, suggest_follow_ups
from dashboard.config_to_sql import config_to_sql
from dashboard.auto_generator import generate_initial_dashboard
from dashboard.measure_engine import MeasureEngine
from dashboard.ai_analytics import (
    ForecastRequest,
    ForecastResponse,
    run_forecast,
    ExplainPointRequest,
    ExplainPointResponse,
    explain_point_delta,
    DecompositionTreeRequest,
    DecompositionTreeResponse,
    compute_decomposition_tree,
    KeyInfluencerRequest,
    KeyInfluencerResponse,
    compute_key_influencers,
    SmartNarrativeRequest,
    SmartNarrativeResponse,
    generate_smart_narrative,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dashboard"])

# Internal state store for dashboard results to avoid re-aggregating continuously
_dashboard_store: TTLStore = TTLStore("dashboard_local_cache", max_entries=100, ttl_seconds=86400)


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


def _get_business_context(file_id: str) -> str:
    from state import get_profile
    profile_dict = get_profile(file_id)
    if not profile_dict:
        return ""
    
    domain = profile_dict.get("estimated_domain", "")
    
    # Try to extract insights, handle both dict and pydantic models
    insights = profile_dict.get("insights", {})
    if hasattr(insights, 'model_dump'):
        insights = insights.model_dump()
        
    if isinstance(insights, dict):
        briefing = insights.get("analyst_briefing", {})
        if briefing:
            findings = "\n".join(briefing.get("key_findings", []))
            actions = "\n".join(briefing.get("recommended_actions", []))
            return f"Domain: {domain}\nKey Findings:\n{findings}\nRecommended Actions:\n{actions}"
            
    return f"Domain: {domain}"


def _get_table_name(file_id: str) -> str:
    """Get the DuckDB table name for a file_id by querying the SQL engine registry."""
    try:
        from api.sql import get_engine
        engine = get_engine()
        # Search the engine's table registry for a table with this file_id.
        # Guard with hasattr since _tables is a private attribute.
        tables = getattr(engine, "_tables", None) or getattr(engine, "tables", None)
        if tables:
            for tbl_name, meta in tables.items():
                if isinstance(meta, dict) and meta.get("file_id") == file_id:
                    return tbl_name
    except (AttributeError, Exception) as e:
        logger.warning("Could not query SQL engine for table name: %s", e)

    # Fallback: check state store
    stored = get_stored_data(file_id)
    if stored and isinstance(stored, dict) and "table_name" in stored:
        return stored["table_name"]

    # Last resort: use file_id as table name
    return f"upload_{file_id}"


# ──────────────────────────────────────────
# Prompt Interpretation
# ──────────────────────────────────────────

@router.post("/api/dashboard/interpret")
async def interpret(request: InterpretRequest):
    """Interpret a natural language prompt into a ChartConfig or MeasureDefinition."""
    schema = _get_schema_for_file(request.file_id)
    business_context = _get_business_context(request.file_id)

    result = await interpret_prompt(
        message=request.message,
        schema=schema,
        current_config=request.current_config,
        conversation_history=request.conversation_history,
        business_context=business_context,
        edit_mode=request.edit_mode,
    )

    if isinstance(result, MeasureDefinition):
        return InterpretResponse(
            success=True,
            intent="create_measure",
            measure=result,
        )

    if isinstance(result, ClarificationRequest):
        return InterpretResponse(
            success=True,
            intent="clarify",
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
            intent="modify_chart" if request.edit_mode else "create_chart",
            config=config,
            sql=sql,
            data=data,
        )
    except Exception as e:
        logger.warning("SQL execution failed after interpretation: %s", e)
        return InterpretResponse(
            success=False,
            intent="modify_chart" if request.edit_mode else "create_chart",
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


class AutoGenerateRequest(BaseModel):
    file_id: str

@router.post("/api/dashboard/auto")
async def auto_generate(request: AutoGenerateRequest):
    """Automatically generate an initial dashboard based on dataset context."""
    schema = _get_schema_for_file(request.file_id)
    business_context = _get_business_context(request.file_id)
    
    configs = await generate_initial_dashboard(schema, business_context)
    
    # Enforce temporal grain consistency on all auto-generated configs
    from dashboard.prompt_interpreter import _enforce_temporal_grain_consistency
    configs = [_enforce_temporal_grain_consistency(c, schema) for c in configs]
    
    results = []
    table_name = _get_table_name(request.file_id)
    for config in configs:
        if config.chart_type == ChartType.SMART_NARRATIVE:
            results.append({
                "config": config.model_dump(),
                "sql": None,
                "data": None,
                "success": True,
            })
            continue

        try:
            sql = config_to_sql(config, table_name)
            data = _execute_sql(request.file_id, sql)
            results.append({
                "config": config.model_dump(),
                "sql": sql,
                "data": data,
                "success": True,
            })
        except Exception as e:
            logger.warning("Auto generated config failed SQL execution: %s", e)
            results.append({
                "config": config.model_dump(),
                "success": False,
                "error": str(e),
            })
            
    return {"widgets": results}


@router.post("/api/dashboard/suggest")
async def suggest(request: SuggestPromptsRequest):
    """Get AI-generated follow-up prompt suggestions."""
    schema = _get_schema_for_file(request.file_id)
    suggestions = await suggest_follow_ups(schema, request.current_config)
    return {"suggestions": suggestions}


# ──────────────────────────────────────────
# AI Analytics & Super-Visual Endpoints
# ──────────────────────────────────────────

@router.post("/api/dashboard/forecast", response_model=ForecastResponse)
async def forecast_endpoint(request: ForecastRequest):
    """Run time-series forecasting with confidence intervals and anomaly detection."""
    df = get_stored_dataframe(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    try:
        return run_forecast(
            df=df,
            date_col=request.date_column,
            val_col=request.value_column,
            time_grain=request.time_grain,
            periods=request.periods,
            confidence=request.confidence,
            agg=request.aggregation,
        )
    except Exception as e:
        logger.error("Forecast failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/dashboard/explain-point", response_model=ExplainPointResponse)
async def explain_point_endpoint(request: ExplainPointRequest):
    """Explain root-cause dimensional drivers for a clicked data point."""
    df = get_stored_dataframe(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    try:
        return explain_point_delta(
            df=df,
            clicked_point=request.clicked_point,
            dimension_col=request.dimension_column,
            metric_col=request.metric_column,
            agg=request.aggregation,
            time_grain=request.time_grain,
        )
    except Exception as e:
        logger.error("Explain point failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/dashboard/decomposition-tree", response_model=DecompositionTreeResponse)
async def decomposition_tree_endpoint(request: DecompositionTreeRequest):
    """Compute interactive decomposition tree node splits."""
    df = get_stored_dataframe(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    try:
        return compute_decomposition_tree(
            df=df,
            metric_col=request.metric_column,
            agg=request.aggregation,
            current_path=request.current_path,
            split_dimension=request.split_dimension,
            split_strategy=request.split_strategy,
        )
    except Exception as e:
        logger.error("Decomposition tree failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/dashboard/key-influencers", response_model=KeyInfluencerResponse)
async def key_influencers_endpoint(request: KeyInfluencerRequest):
    """Compute feature drivers and lift multipliers for an outcome."""
    df = get_stored_dataframe(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    try:
        return compute_key_influencers(
            df=df,
            target_col=request.target_column,
            target_value=request.target_value,
        )
    except Exception as e:
        logger.error("Key influencers failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/dashboard/narrative", response_model=SmartNarrativeResponse)
async def smart_narrative_endpoint(request: SmartNarrativeRequest):
    """Generate dynamic, data-grounded narrative bullets."""
    df = get_stored_dataframe(request.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    try:
        return generate_smart_narrative(
            df=df,
            current_config=request.current_config,
        )
    except Exception as e:
        logger.error("Smart narrative failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


class MeasureCreateRequest(BaseModel):
    file_id: str
    name: str
    expression: str
    format: str = "number"
    description: Optional[str] = None


@router.post("/api/dashboard/measure/create")
async def create_measure_endpoint(request: MeasureCreateRequest):
    """Validate, test, and register a custom measure."""
    valid, err = MeasureEngine.validate_expression(request.expression)
    if not valid:
        raise HTTPException(status_code=400, detail=f"Invalid measure expression: {err}")

    # Test execution in DuckDB to ensure syntax is valid
    df = get_stored_dataframe(request.file_id)
    if df is not None:
        try:
            import duckdb
            compiled = MeasureEngine.compile_measure(request.expression)
            conn = duckdb.connect(":memory:")
            conn.register("t", df)
            conn.execute(f"SELECT {compiled} FROM t").fetchone()
        except Exception as exec_err:
            raise HTTPException(status_code=400, detail=f"Measure execution test failed: {exec_err}")

    return {
        "success": True,
        "measure": {
            "name": request.name,
            "expression": request.expression,
            "format": request.format,
            "description": request.description,
            "category": "custom",
        }
    }


class MeasureValidateRequest(BaseModel):
    expression: str


@router.post("/api/dashboard/measure/validate")
async def validate_measure_endpoint(request: MeasureValidateRequest):
    """Validate a DAX-style expression syntax."""
    valid, err = MeasureEngine.validate_expression(request.expression)
    return {"valid": valid, "error": err}


def _execute_sql(file_id: str, sql: str) -> list[dict]:
    """Execute SQL against the DuckDB engine and return rows as dicts.
    
    Raises RuntimeError with the SQL error message if execution fails,
    allowing callers to surface the error properly.
    """
    try:
        from api.sql import get_engine
        engine = get_engine()
        result = engine.execute(sql)
        if isinstance(result, dict):
            if result.get("success"):
                return result.get("rows", [])
            else:
                error_msg = result.get("error", "SQL execution failed")
                logger.warning("SQL execution error: %s", error_msg)
                raise RuntimeError(f"SQL error: {error_msg}")
        return []
    except RuntimeError:
        raise  # Re-raise our own errors
    except Exception as e:
        logger.error("Dashboard SQL execution failed: %s", e)
        # Fallback: try direct DuckDB with DataFrame registration
        try:
            import duckdb
            df = get_stored_dataframe(file_id)
            if df is None:
                raise RuntimeError("Dataset not found for SQL fallback execution")
            table_name = _get_table_name(file_id)
            conn = duckdb.connect()
            conn.register(table_name, df)
            result_df = conn.execute(sql).df()
            return result_df.to_dict(orient="records")
        except Exception as fallback_err:
            logger.error("Dashboard fallback SQL also failed: %s", fallback_err)
            raise RuntimeError(f"SQL execution failed: {fallback_err}")


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
            
    created_at = existing.get("created_at", time.time()) if existing else time.time()
    dashboard = dashboard.model_copy(update={"created_at": created_at, "updated_at": time.time()})
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
    return {"status": "deleted", "deleted": True}
