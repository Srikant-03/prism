"""
Dashboard Auto Generator — Creates an initial dashboard layout based on business context.
"""
from __future__ import annotations

import logging
import re
from typing import List

from dashboard.chart_config_models import ChartConfig, ChartType, AggregationType, TimeGrain, SortDirection, MetricSpec
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
4. Ensure visual diversity: Include at least one KPI (high-level metric), one time-series line chart (if a date exists), one categorical breakdown (bar or column), and one scatter chart (if multiple numerics exist). Avoid duplicate chart types.
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


def _is_surrogate_or_id(col: str) -> bool:
    c = col.lower().strip()
    if c in ("id", "index", "idx", "uuid", "guid", "row_id", "sequential_row_id"):
        return True
    if c.endswith("_id") or c.startswith("id_") or "_uuid" in c or "uuid_" in c:
        return True
    return False


def _score_metric_column(col: str) -> int:
    """Higher score for business-critical numeric metrics."""
    c = col.lower()
    score = 0
    if any(k in c for k in ("revenue", "sales", "amount", "spend", "cost", "profit")):
        score += 10
    if "net" in c:
        score += 7
    elif any(k in c for k in ("total", "gross")):
        score += 5
    if any(k in c for k in ("units", "quantity", "count", "volume", "score")):
        score += 3
    if any(k in c for k in ("rate", "price", "margin", "weight")):
        score += 2
    return score


def _score_categorical_column(col: str) -> int:
    """Higher score for meaningful business dimensions."""
    c = col.lower()
    score = 0
    if any(k in c for k in ("category", "product", "type", "department")):
        score += 10
    if any(k in c for k in ("region", "country", "city", "state", "territory", "location")):
        score += 8
    if any(k in c for k in ("tier", "segment", "status", "channel", "plan")):
        score += 6
    return score


async def generate_initial_dashboard(schema: dict, business_context: str) -> List[ChartConfig]:
    """
    Automatically generate an initial set of ChartConfigs for a dataset.
    Ensures at least 4 to 6 valid, cohesive widgets are returned.
    """
    fallback_configs = _fallback_dashboard(schema)

    if not HAS_GENAI:
        return fallback_configs

    schema_desc = _build_schema_description(schema)
    prompt = f"Schema:\n{schema_desc}\n\nBusiness Context:\n{business_context}\n\nGenerate the initial dashboard JSON array (4 to 6 charts):"

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
            # Ensure visual diversity: if scatter is missing and fallback has it, add or substitute
            has_scatter = any(c.chart_type == ChartType.SCATTER for c in configs)
            if not has_scatter:
                fallback_scatter = next((fc for fc in fallback_configs if fc.chart_type == ChartType.SCATTER), None)
                if fallback_scatter:
                    if len(configs) >= 5:
                        seen_types = set()
                        dup_idx = None
                        for idx, c in enumerate(configs):
                            if c.chart_type in seen_types:
                                dup_idx = idx
                                break
                            seen_types.add(c.chart_type)
                        if dup_idx is not None:
                            configs[dup_idx] = fallback_scatter
                        elif len(configs) < 6:
                            configs.append(fallback_scatter)
                    else:
                        configs.append(fallback_scatter)

            if len(configs) >= 4:
                return configs
            elif configs:
                # Augment with fallbacks to guarantee at least 4 widgets
                seen_titles = {c.title.lower() for c in configs}
                for fc in fallback_configs:
                    if fc.title.lower() not in seen_titles and len(configs) < 5:
                        configs.append(fc)
                return configs
    except Exception as e:
        logger.error("Auto dashboard generation failed: %s", e)

    return fallback_configs


def _fallback_dashboard(schema: dict) -> List[ChartConfig]:
    """
    Generates a rich, robust 4-6 chart dashboard fallback.
    Filters out surrogate keys and prioritizes key business metrics.
    """
    columns = list(schema.keys())
    all_numeric = [c for c in columns if schema[c].get("dtype", "") in ("int64", "float64", "int", "float", "number", "numeric")]
    all_cat = [c for c in columns if schema[c].get("dtype", "") in ("object", "category", "string", "str")]
    all_date = [c for c in columns if schema[c].get("dtype", "") in ("datetime64[ns]", "datetime", "date", "datetime64") or "date" in c.lower() or "time" in c.lower()]

    # Filter out surrogate IDs
    numeric_cols = [c for c in all_numeric if not _is_surrogate_or_id(c)]
    if not numeric_cols:
        numeric_cols = all_numeric

    cat_cols = [c for c in all_cat if not _is_surrogate_or_id(c)]
    if not cat_cols:
        cat_cols = all_cat

    date_cols = [c for c in all_date if not _is_surrogate_or_id(c)]
    if not date_cols:
        date_cols = all_date

    # Sort by domain importance
    numeric_cols.sort(key=_score_metric_column, reverse=True)
    cat_cols.sort(key=_score_categorical_column, reverse=True)
    # Prefer order_date or pure date over mixed/timestamp
    date_cols.sort(key=lambda c: (0 if "order_date" in c.lower() else (1 if "date" in c.lower() and "mixed" not in c.lower() else 2)))

    configs: List[ChartConfig] = []
    primary_metric = numeric_cols[0] if numeric_cols else None
    
    # Avoid selecting a clone/alternate currency or collinear variant as secondary metric
    secondary_metric = None
    if primary_metric and len(numeric_cols) > 1:
        p_base = re.sub(r'_(eur|usd|gbp|jpy|cad|aud|raw|clean)$', '', primary_metric.lower())
        for cand in numeric_cols[1:]:
            c_base = re.sub(r'_(eur|usd|gbp|jpy|cad|aud|raw|clean)$', '', cand.lower())
            if c_base == p_base:
                continue
            secondary_metric = cand
            break
        if not secondary_metric and len(numeric_cols) > 1:
            secondary_metric = numeric_cols[1]

    # For scatter plot, select a distinct volume, quantity, rate, or independent continuous metric
    scatter_x = None
    if primary_metric:
        p_base = re.sub(r'_(eur|usd|gbp|jpy|cad|aud|raw|clean)$', '', primary_metric.lower())
        for cand in numeric_cols:
            c_base = re.sub(r'_(eur|usd|gbp|jpy|cad|aud|raw|clean)$', '', cand.lower())
            if c_base == p_base:
                continue
            if any(k in cand.lower() for k in ("unit", "qty", "quantity", "discount", "rate", "weight", "price", "count")):
                scatter_x = cand
                break
        if not scatter_x:
            scatter_x = secondary_metric

    primary_cat = cat_cols[0] if cat_cols else None
    secondary_cat = cat_cols[1] if len(cat_cols) > 1 else None
    primary_date = date_cols[0] if date_cols else None

    # 1. Primary KPI
    if primary_metric:
        configs.append(ChartConfig(
            chart_type=ChartType.KPI,
            title=f"Total {primary_metric.replace('_', ' ').title()}",
            y_axis=primary_metric,
            aggregation=AggregationType.SUM,
            kpi_value_column=primary_metric,
            kpi_aggregation=AggregationType.SUM,
        ))

    # 2. Secondary KPI (volume / count or secondary metric)
    if secondary_metric:
        configs.append(ChartConfig(
            chart_type=ChartType.KPI,
            title=f"Total {secondary_metric.replace('_', ' ').title()}",
            y_axis=secondary_metric,
            aggregation=AggregationType.SUM,
            kpi_value_column=secondary_metric,
            kpi_aggregation=AggregationType.SUM,
        ))

    # 3. Time Series Monthly Trend
    if primary_metric and primary_date:
        configs.append(ChartConfig(
            chart_type=ChartType.LINE,
            title=f"Monthly {primary_metric.replace('_', ' ').title()} Trend",
            y_axis=primary_metric,
            x_axis=primary_date,
            aggregation=AggregationType.SUM,
            x_axis_time_grain=TimeGrain.MONTH,
        ))

    # 4. Primary Categorical Breakdown (Multi-Metric or Single Metric)
    if primary_metric and primary_cat:
        has_gross = any(c.lower() == "gross_revenue" for c in numeric_cols)
        has_net = any(c.lower() == "net_revenue" for c in numeric_cols)
        if has_gross and has_net:
            configs.append(ChartConfig(
                chart_type=ChartType.BAR,
                title=f"Gross vs Net Revenue by {primary_cat.replace('_', ' ').title()}",
                x_axis=primary_cat,
                metrics=[
                    MetricSpec(column="gross_revenue", aggregation=AggregationType.SUM, label="Gross Revenue"),
                    MetricSpec(column="net_revenue", aggregation=AggregationType.SUM, label="Net Revenue"),
                ],
                show_legend=True,
            ))
        else:
            configs.append(ChartConfig(
                chart_type=ChartType.BAR,
                title=f"{primary_metric.replace('_', ' ').title()} by {primary_cat.replace('_', ' ').title()}",
                y_axis=primary_metric,
                x_axis=primary_cat,
                aggregation=AggregationType.SUM,
                sort_by=primary_metric,
                sort_direction=SortDirection.DESC,
            ))

    # 5. Scatter Distribution
    if primary_metric and scatter_x:
        configs.append(ChartConfig(
            chart_type=ChartType.SCATTER,
            title=f"{scatter_x.replace('_', ' ').title()} vs {primary_metric.replace('_', ' ').title()}",
            x_axis=scatter_x,
            y_axis=primary_metric,
            group_by=primary_cat,
            aggregation=AggregationType.NONE,
        ))

    # 6. Live Smart Narrative Card
    configs.append(ChartConfig(
        chart_type=ChartType.SMART_NARRATIVE,
        title="Executive Smart Narrative",
        aggregation=AggregationType.NONE,
    ))

    return configs
