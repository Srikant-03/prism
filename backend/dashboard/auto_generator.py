"""
Dashboard Auto Generator — Creates an initial dashboard layout based on business context.
"""
from __future__ import annotations

import logging
from typing import List

from dashboard.chart_config_models import ChartConfig
from dashboard.prompt_interpreter import _extract_json, _build_schema_description
from llm.api_manager import with_llm_failover, get_active_client

try:
    from google.genai import types
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

logger = logging.getLogger(__name__)

AUTO_GENERATE_PROMPT = """You are an expert Data Analyst and BI Dashboard Designer. 
Your task is to generate a cohesive, initial dashboard of 4 to 6 charts for a dataset.

You will be provided with:
1. The Dataset Schema (columns and sample values)
2. The Business Context (domain, key findings, and recommended actions)

RULES:
1. Return ONLY a valid JSON array of chart configuration objects.
2. The objects MUST match this schema:
   - chart_type: "bar", "line", "area", "pie", "donut", "scatter", "heatmap", "treemap", "funnel", "kpi", "table"
   - title: descriptive chart title
   - x_axis: column name (optional)
   - y_axis: column name (optional)
   - group_by: column name (optional)
   - aggregation: "sum", "avg", "count", "min", "max", "median", "none"
   - kpi_value_column: for KPI charts
   - kpi_aggregation: for KPI charts
3. Ensure the charts are highly relevant to the provided Business Context. If the context says it's a financial dataset, create revenue KPIs, expense breakdowns, etc.
4. Try to include at least one KPI (high-level metric), one time-series (if a date exists), and one breakdown (e.g., pie or bar chart).
5. Only use columns that exist in the schema.
"""

@with_llm_failover(tier_rpm=5)
async def _execute_auto_generate_prompt(prompt: str) -> object:
    """Inner LLM call with failover — separated so generate_initial_dashboard can
    handle fallback logic without being wrapped by the failover decorator itself."""
    from config import AppConfig
    client = get_active_client()
    return await client.aio.models.generate_content(
        model=AppConfig.llm.MODEL_WORKHORSE,
        contents=[
            {"role": "user", "parts": [{"text": AUTO_GENERATE_PROMPT}]},
            {"role": "model", "parts": [{"text": "I understand. I will return only a JSON array of chart configurations."}]},
            {"role": "user", "parts": [{"text": prompt}]},
        ],
        config=types.GenerateContentConfig(
            temperature=0.3,
            response_mime_type="application/json",
        )
    )


async def generate_initial_dashboard(schema: dict, business_context: str) -> List[ChartConfig]:
    """
    Automatically generate an initial set of ChartConfigs for a dataset.

    NOTE: The LLM call is intentionally in the separate @with_llm_failover helper
    `_execute_auto_generate_prompt` to avoid wrapping the entire orchestration
    function (which includes fallback logic) inside the failover decorator.
    """
    if not HAS_GENAI:
        return _fallback_dashboard(schema)

    schema_desc = _build_schema_description(schema)
    prompt = f"Schema:\n{schema_desc}\n\nBusiness Context:\n{business_context}\n\nGenerate the initial dashboard JSON array:"

    try:
        response = await _execute_auto_generate_prompt(prompt)
        data = _extract_json(response.text)
        if isinstance(data, list):
            configs = []
            for item in data[:6]:
                try:
                    configs.append(ChartConfig(**item))
                except Exception as e:
                    logger.warning("Skipping invalid generated chart config: %s", e)
            if configs:
                return configs
    except Exception as e:
        logger.error("Auto dashboard generation failed: %s", e)

    return _fallback_dashboard(schema)


def _fallback_dashboard(schema: dict) -> List[ChartConfig]:
    # Fallback if LLM is unavailable
    columns = list(schema.keys())
    numeric_cols = [c for c in columns if schema[c].get("dtype", "") in ("int64", "float64", "int", "float", "number", "numeric")]
    cat_cols = [c for c in columns if schema[c].get("dtype", "") in ("object", "category", "string", "str")]
    date_cols = [c for c in columns if schema[c].get("dtype", "") in ("datetime64[ns]", "datetime", "date", "datetime64") or "date" in c.lower() or "time" in c.lower()]

    configs = []
    if numeric_cols:
        configs.append(ChartConfig(
            chart_type="kpi", title=f"Total {numeric_cols[0].replace('_', ' ').title()}", 
            y_axis=numeric_cols[0], aggregation="sum", kpi_value_column=numeric_cols[0], kpi_aggregation="sum"
        ))
    if numeric_cols and date_cols:
        from dashboard.chart_config_models import TimeGrain
        configs.append(ChartConfig(
            chart_type="line",
            title=f"Monthly {numeric_cols[0].replace('_', ' ').title()} Trend",
            y_axis=numeric_cols[0],
            x_axis=date_cols[0],
            aggregation="sum",
            x_axis_time_grain=TimeGrain.MONTH,
        ))
    if numeric_cols and cat_cols:
        configs.append(ChartConfig(
            chart_type="bar", title=f"{numeric_cols[0].replace('_', ' ').title()} by {cat_cols[0].replace('_', ' ').title()}",
            y_axis=numeric_cols[0], x_axis=cat_cols[0], aggregation="sum"
        ))
    return configs
