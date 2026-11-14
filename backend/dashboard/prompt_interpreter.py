"""
Dashboard Prompt Interpreter — Translates natural language into ChartConfig.

Core AI function: interpretPrompt(message, schema, currentConfig) → ChartConfig | ClarificationRequest.
Routes through llm/api_manager.py for Gemini key rotation and failover.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional
import asyncio

logger = logging.getLogger(__name__)

try:
    from google import genai
    from google.genai import types
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

from dashboard.chart_config_models import (
    ChartConfig,
    ChartType,
    AggregationType,
    SortDirection,
    FilterCondition,
    ClarificationRequest,
    TimeGrain,
    MetricSpec,
    MeasureDefinition,
)
from llm.api_manager import with_llm_failover, get_active_client


# ── System Prompt ──

SYSTEM_PROMPT = """You are a data visualization assistant. Given a user's natural language request and a dataset schema, produce a JSON object that configures a chart.

RULES:
1. Return ONLY valid JSON — no markdown, no code fences, no explanation.
2. If the user's request is truly ambiguous, return: {"clarification": "<one specific question>", "suggestions": ["option1", "option2"]}
   NOTE: Do NOT ask for clarification if the user asks for multiple metrics (e.g., 'total net_revenue and average discount_rate' or 'compare X vs Y'). Instead, return a multi-metric configuration with the 'metrics' list.
3. If the request is clear, return a chart configuration object with these fields:
   - chart_type: one of "bar", "line", "area", "pie", "donut", "scatter", "heatmap", "treemap", "funnel", "kpi", "table"
   - title: descriptive chart title
   - x_axis: column name for the x-axis (or category axis)
   - y_axis: column name for the y-axis (or value axis)
   - group_by: column to group/segment by (optional)
   - metrics: array of {column, aggregation, label} objects (optional, for multi-metric charts or KPI scorecards with multiple metrics)
   - aggregation: one of "sum", "avg", "count", "min", "max", "median", "none"
   - filters: array of {column, operator, value} objects (optional)
   - sort_by: column to sort by (optional)
   - sort_direction: "asc" or "desc"
   - limit: max rows to return (optional)
   - color_scheme: one of "default", "dark", "vibrant", "pastel", "monochrome", "ocean", "sunset", "forest"
   - show_legend: true/false
   - stacked: true/false (for bar/area)
   - trend_line: true/false
   - smooth: true/false (for line charts)
   - show_values: true/false (show data labels)
   - kpi_value_column: column for KPI value (for kpi type)
   - kpi_aggregation: aggregation for KPI
4. For follow-up edits: a previous config is provided. Only modify the fields the user asked to change; keep everything else the same.
5. Map column names EXACTLY as they appear in the schema — case-sensitive.
6. Choose the most appropriate chart type if the user doesn't specify one.
7. For filter operators, use: =, !=, >, <, >=, <=, in, not_in, between, like, is_null, is_not_null
8. DO NOT hallucinate dimensions. If comparing two metrics without a specified category (e.g. "compare A and B"):
   - LEAVE `x_axis` and `group_by` as strictly null if you want a one-dimensional aggregation.
   - Do NOT randomly pick a categorical column like "gender" or "id" to fill the x-axis.
   - If both the intended x-axis and y-axis are continuous numeric columns (e.g., "study hours" and "score"), you MUST use a "scatter" chart. Do not use a "bar" or "line" chart for two numeric coordinates.
9. TEMPORAL AGGREGATION RULE: If the chart title or user request mentions a temporal granularity ("daily", "weekly", "monthly", "quarterly", "yearly"):
   - Set `x_axis` to the date/time column.
   - Set `x_axis_time_grain` to the matching grain: "day", "week", "month", "quarter", or "year".
   - The aggregation level in the SQL MUST match the title exactly. A "Monthly Revenue Trend" must GROUP BY calendar month, NOT by raw date.
   - x_axis_time_grain: one of "day", "week", "month", "quarter", "year", or null (no temporal bucketing).
10. MULTI-METRIC RULE: If the user asks for multiple metrics (e.g., 'total net_revenue and average discount_rate', or 'compare gross_revenue vs net_revenue across category'):
   - Return a 'metrics' list with {column, aggregation, label} for each metric.
   - If a category dimension is specified, set x_axis to that column and chart_type to 'bar'.
   - If no dimension is specified, set chart_type to 'kpi' and leave x_axis null.
"""


def _build_schema_description(schema: dict) -> str:
    """Build a human-readable schema description for the system prompt."""
    lines = ["Dataset columns:"]
    for col_name, col_info in schema.items():
        dtype = col_info.get("dtype", "unknown")
        sample = col_info.get("sample_values", [])
        sample_str = f" (e.g. {', '.join(str(s) for s in sample[:3])})" if sample else ""
        lines.append(f"  - {col_name} ({dtype}){sample_str}")
    return "\n".join(lines)


def _extract_json(text: str) -> dict | None:
    """Extract JSON from LLM response, handling markdown code fences."""
    # Try direct parse
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting from code fences
    patterns = [
        r"```json\s*\n?(.*?)\n?\s*```",
        r"```\s*\n?(.*?)\n?\s*```",
        r"\{.*\}",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1) if match.lastindex else match.group(0))
            except (json.JSONDecodeError, IndexError):
                continue
    return None


def _is_followup_modifier(text: str) -> bool:
    """Check if the text is an incremental modifier rather than a new standalone query."""
    if not text:
        return False
    t = text.lower().strip()
    # Explicit standalone indicators
    if any(w in t for w in ("compare", "across", "breakdown", "monthly", "quarterly", "yearly", "weekly", "daily", " vs ", "scatter", "table", "total")):
        return False
    modifiers = (
        "add trend", "remove trend", "switch to", "change to", "make it",
        "show values", "hide values", "show labels", "hide labels",
        "sort by", "sort asc", "sort desc", "sort ascending", "sort descending",
        "filter to", "add filter", "remove filter",
        "stack", "unstack", "smooth", "curved",
    )
    return any(t.startswith(m) or f" {m} " in f" {t} " for m in modifiers)


def _parse_config_from_dict(data: dict, current_config: Optional[ChartConfig] = None, user_message: str = "") -> ChartConfig:
    """Parse a dict into a ChartConfig, merging with current_config only for follow-up modifiers."""
    if current_config and _is_followup_modifier(user_message):
        # Start from current config and overlay only provided fields
        base = current_config.model_dump()
        for key, value in data.items():
            if value is not None:
                base[key] = value
        return ChartConfig(**base)
    
    # ── LLM Hallucination Fix ──
    # If comparing two metrics without a specified dimension, the LLM loves to hallucinate one.
    # Detect columns that look like surrogate/identity keys (e.g., 'id', 'student_id', 'user_id',
    # 'gender') that were injected purely to make a bar chart work.
    x_axis = data.get("x_axis")
    y_axis = data.get("y_axis")
    y_sec = data.get("y_axis_secondary")

    def _is_likely_hallucinated_dimension(col: Optional[str]) -> bool:
        """Return True if this column looks like a surrogate/identity key the LLM hallucinated."""
        if not col:
            return False
        col_lower = col.lower().strip()
        # Exact low-cardinality identity patterns
        if col_lower in ("id", "gender", "sex"):
            return True
        # Ends with '_id' or starts with 'id_' (surrogate key patterns)
        if col_lower.endswith("_id") or col_lower.startswith("id_"):
            return True
        return False

    if _is_likely_hallucinated_dimension(x_axis):
        data["x_axis"] = None
        data["group_by"] = None
        if data.get("chart_type") == "bar" and (y_axis and y_sec):
            data["chart_type"] = "scatter"
        elif data.get("chart_type") == "bar":
            data["chart_type"] = "table"

    return ChartConfig(**{k: v for k, v in data.items() if v is not None})


# ── Temporal Grain Consistency ──

# Mapping from title keywords to expected TimeGrain values
_TITLE_GRAIN_KEYWORDS: dict[str, TimeGrain] = {
    "daily": TimeGrain.DAY,
    "day-by-day": TimeGrain.DAY,
    "per day": TimeGrain.DAY,
    "each day": TimeGrain.DAY,
    "weekly": TimeGrain.WEEK,
    "per week": TimeGrain.WEEK,
    "each week": TimeGrain.WEEK,
    "week-over-week": TimeGrain.WEEK,
    "monthly": TimeGrain.MONTH,
    "per month": TimeGrain.MONTH,
    "each month": TimeGrain.MONTH,
    "month-over-month": TimeGrain.MONTH,
    "quarterly": TimeGrain.QUARTER,
    "per quarter": TimeGrain.QUARTER,
    "each quarter": TimeGrain.QUARTER,
    "quarter-over-quarter": TimeGrain.QUARTER,
    "yearly": TimeGrain.YEAR,
    "annual": TimeGrain.YEAR,
    "per year": TimeGrain.YEAR,
    "each year": TimeGrain.YEAR,
    "year-over-year": TimeGrain.YEAR,
}


def _detect_grain_from_text(text: str) -> Optional[TimeGrain]:
    """Detect temporal granularity from title or prompt text."""
    text_lower = text.lower()
    # Check longer patterns first
    for keyword, grain in sorted(_TITLE_GRAIN_KEYWORDS.items(), key=lambda x: -len(x[0])):
        if keyword in text_lower:
            return grain
    return None


def _enforce_temporal_grain_consistency(
    config: ChartConfig,
    schema: Optional[dict] = None,
) -> ChartConfig:
    """Post-process: ensure the chart's x_axis_time_grain matches its title.

    If the title says 'Monthly Revenue Trend' but x_axis_time_grain is None,
    auto-set it to 'month'. Also ensures x_axis is a datetime column.
    """
    title_grain = _detect_grain_from_text(config.title or "")
    if not title_grain:
        return config

    # Title claims a temporal granularity — enforce it
    if config.x_axis_time_grain is None or config.x_axis_time_grain == TimeGrain.NONE:
        config = config.model_copy(update={"x_axis_time_grain": title_grain})

    # Verify x_axis is a datetime column (if schema available)
    if schema and config.x_axis:
        col_info = schema.get(config.x_axis, {})
        dtype = col_info.get("dtype", "").lower()
        is_datetime = (
            "date" in dtype or "time" in dtype
            or "date" in config.x_axis.lower() or "time" in config.x_axis.lower()
        )
        if not is_datetime:
            # x_axis is not datetime — try to find a datetime column in schema
            for col_name, info in schema.items():
                col_dtype = info.get("dtype", "").lower()
                if "date" in col_dtype or "time" in col_dtype or "date" in col_name.lower():
                    config = config.model_copy(update={"x_axis": col_name})
                    break

    # Ensure aggregation is set (temporal charts need aggregation)
    if config.aggregation is None or config.aggregation == AggregationType.NONE:
        config = config.model_copy(update={"aggregation": AggregationType.SUM})

    return config


@with_llm_failover(tier_rpm=5)
async def _execute_interpretation_prompt(full_prompt: str, system_prompt: str):
    from config import AppConfig
    client = get_active_client()
    return await client.aio.models.generate_content(
        model=AppConfig.llm.MODEL_WORKHORSE,
        contents=[
            {"role": "user", "parts": [{"text": system_prompt}]},
            {"role": "model", "parts": [{"text": "I understand. I will return only valid JSON chart configurations."}]},
            {"role": "user", "parts": [{"text": full_prompt}]},
        ],
        config=types.GenerateContentConfig(
            temperature=0.1,
            response_mime_type="application/json",
        )
    )

@with_llm_failover(tier_rpm=2)
async def _execute_followup_prompt(prompt: str):
    from config import AppConfig
    client = get_active_client()
    return await client.aio.models.generate_content(
        model=AppConfig.llm.MODEL_TRIAGE,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.7,
            response_mime_type="application/json",
        )
    )


async def interpret_prompt(
    message: str,
    schema: dict,
    current_config: Optional[ChartConfig] = None,
    conversation_history: Optional[list[str]] = None,
    business_context: Optional[str] = None,
    edit_mode: bool = False,
) -> ChartConfig | ClarificationRequest | MeasureDefinition:
    """
    Interpret a natural language prompt into a ChartConfig, ClarificationRequest, or MeasureDefinition.

    Args:
        message: User's natural language message.
        schema: Dataset schema as {col_name: {dtype, sample_values}}.
        current_config: Previous ChartConfig for follow-up edits.
        conversation_history: Prior prompts for context.
        business_context: The domain and key findings of the dataset.
        edit_mode: Whether explicit edit mode is active on a selected widget.

    Returns:
        ChartConfig, ClarificationRequest, or MeasureDefinition.
    """
    # 1. Measure Creation Intent (NL-to-DAX)
    m_match = re.match(r'^(?:create|define|add)\s+(?:a\s+)?measure\s+["\']?([^"\']+)["\']?\s+as\s+(.+)$', message.strip(), re.IGNORECASE)
    if m_match:
        name = m_match.group(1).strip()
        raw_expr = m_match.group(2).strip()
        expr = raw_expr
        if "divided by" in raw_expr.lower() or "/" in raw_expr:
            parts = re.split(r'\s+divided\s+by\s+|\s*/\s*', raw_expr, flags=re.IGNORECASE)
            if len(parts) == 2:
                c1, c2 = parts[0].strip(), parts[1].strip()
                expr = f"DIVIDE(SUM({c1}), SUM({c2}), 0)"
        elif re.match(r'^[a-zA-Z0-9_]+\s*\*\s*[0-9.]+$', raw_expr):
            col_part, mult_part = raw_expr.split("*")
            expr = f"SUM({col_part.strip()}) * {mult_part.strip()}"
        return MeasureDefinition(
            name=name,
            expression=expr,
            format="percent" if any(k in name.lower() for k in ("margin", "%", "rate", "pct")) else "number",
        )

    # 2. Check for truly ambiguous prompts (e.g. "show performance over time" without metric)
    msg_strip = message.lower().strip()
    ambiguous_patterns = ["show performance over time", "performance over time", "show trend over time", "performance trend"]
    if any(msg_strip == p for p in ambiguous_patterns):
        return ClarificationRequest(
            clarification="Which performance metric would you like to view over time?",
            suggestions=["Net Revenue", "Gross Revenue", "Units Sold"]
        )

    # If edit_mode is not enabled and the prompt is not an explicit modifier, do not inherit current_config
    effective_current = current_config if (edit_mode and _is_followup_modifier(message)) else None

    if not HAS_GENAI:
        return _fallback_interpret(message, schema, effective_current)

    schema_desc = _build_schema_description(schema)

    # Build the user message with context
    parts = [f"Schema:\n{schema_desc}\n"]
    if business_context:
        parts.append(f"Business Context:\n{business_context}\n")
    if effective_current:
        parts.append(f"Current chart config:\n{effective_current.model_dump_json(indent=2)}\n")
    if conversation_history:
        parts.append("Previous prompts:\n" + "\n".join(f"- {h}" for h in conversation_history[-5:]) + "\n")
    parts.append(f"User request: {message}")
    full_prompt = "\n".join(parts)

    try:
        response = await _execute_interpretation_prompt(full_prompt, SYSTEM_PROMPT)
        response_text = response.text
        data = _extract_json(response_text)

        if data is None:
            logger.warning("Failed to parse Gemini response: %s", response_text[:200])
            return _fallback_interpret(message, schema, effective_current)

        # Check if it's a clarification
        if "clarification" in data:
            msg_lower = message.lower()
            if any(w in msg_lower for w in ("and", "vs", "compare", "across", "where", "breakdown")):
                return _fallback_interpret(message, schema, effective_current)
            return ClarificationRequest(
                clarification=data["clarification"],
                suggestions=data.get("suggestions", []),
            )

        config = _parse_config_from_dict(data, effective_current, user_message=message)

        # Enforce temporal grain consistency:
        config = _enforce_temporal_grain_consistency(config, schema)
        prompt_grain = _detect_grain_from_text(message)
        if prompt_grain and (config.x_axis_time_grain is None or config.x_axis_time_grain == TimeGrain.NONE):
            config = config.model_copy(update={"x_axis_time_grain": prompt_grain})

        return config

    except Exception as e:
        logger.error("Gemini interpretation failed: %s", str(e))
        return _fallback_interpret(message, schema, effective_current)


def _fallback_interpret(
    message: str,
    schema: dict,
    current_config: Optional[ChartConfig] = None,
) -> ChartConfig:
    """
    Enhanced regex-based fallback when Gemini is unavailable.
    Accurately detects chart type, multi-metrics, dimensions, temporal grains,
    filters, and sorting without carrying over stale configuration.
    """
    msg_lower = message.lower().strip()

    # 1. Follow-up modifier handling
    if current_config and _is_followup_modifier(message):
        base = current_config.model_copy()
        if "line" in msg_lower:
            base.chart_type = ChartType.LINE
        elif "bar" in msg_lower:
            base.chart_type = ChartType.BAR
        elif "scatter" in msg_lower:
            base.chart_type = ChartType.SCATTER
        elif "pie" in msg_lower:
            base.chart_type = ChartType.PIE
        elif "kpi" in msg_lower:
            base.chart_type = ChartType.KPI

        if any(w in msg_lower for w in ("trend", "trendline")):
            base.trend_line = True
        if "stack" in msg_lower:
            base.stacked = True
        if any(w in msg_lower for w in ("smooth", "curved")):
            base.smooth = True
        if any(w in msg_lower for w in ("show values", "labels")):
            base.show_values = True
        if "descending" in msg_lower or "sort desc" in msg_lower:
            base.sort_direction = SortDirection.DESC
        elif "ascending" in msg_lower or "sort asc" in msg_lower:
            base.sort_direction = SortDirection.ASC
        return base

    # 2. Schema classification
    columns = list(schema.keys())

    numeric_cols = [c for c in columns if schema[c].get("dtype", "") in
                    ("int64", "float64", "int", "float", "number", "numeric", "int32", "float32")]
    categorical_cols = [c for c in columns if schema[c].get("dtype", "") in
                        ("object", "category", "string", "str", "bool")]
    datetime_cols = [c for c in columns if schema[c].get("dtype", "") in
                     ("datetime64[ns]", "datetime", "date", "datetime64") or
                     "date" in c.lower() or "time" in c.lower()]
    datetime_cols.sort(key=lambda c: 0 if "order_date" in c.lower() else (1 if "date" in c.lower() and "mixed" not in c.lower() else 2))

    # 3. Detect filters (e.g. "where churn_flag is 1", "for region = East")
    filters: list[FilterCondition] = []
    filter_cols_matched = set()
    filter_pattern = re.compile(
        r'(?:where|filter|for|when|if)?\s*([a-zA-Z_]\w*)\s*(=|!=|>=|<=|>|<|\bis\s+not\b|\bis\b|\bequals?\b|\bequal\s+to\b)\s*["\']?([^"\'\s,;)]+)["\']?',
        re.IGNORECASE
    )
    for m in filter_pattern.finditer(msg_lower):
        col_candidate, raw_op, raw_val = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        matched_col = None
        for col in columns:
            if col.lower() == col_candidate.lower() or col.lower().replace("_", " ") == col_candidate.lower():
                matched_col = col
                break
        if matched_col:
            op_lower = raw_op.lower()
            if op_lower in ("is", "equals", "equal to", "="):
                op = "="
            elif op_lower in ("is not", "!="):
                op = "!="
            else:
                op = raw_op

            try:
                parsed_val: Any = int(raw_val)
            except ValueError:
                try:
                    parsed_val = float(raw_val)
                except ValueError:
                    if raw_val.lower() == "true":
                        parsed_val = True
                    elif raw_val.lower() == "false":
                        parsed_val = False
                    else:
                        parsed_val = raw_val

            filters.append(FilterCondition(column=matched_col, operator=op, value=parsed_val))
            filter_cols_matched.add(matched_col.lower())

    text_for_metrics = msg_lower
    filter_where_idx = re.search(r'\b(?:where|filter|when|if)\b', msg_lower)
    if filter_where_idx:
        text_for_metrics = msg_lower[:filter_where_idx.start()].strip()

    # 4. Detect temporal grain & date x-axis
    prompt_grain = _detect_grain_from_text(msg_lower)
    x_axis_time_grain = None
    x_axis = None
    y_axis = None
    group_by = None

    if prompt_grain:
        x_axis_time_grain = prompt_grain
        if datetime_cols:
            x_axis = datetime_cols[0]

    # 5. Detect Dimension (across, by, per, for each)
    dim_match = re.search(
        r'(?:across|by|per|for\s+each|grouped\s+by|break\s*(?:down\s*)?by|split\s*by)\s+([a-zA-Z_]\w*(?:\s+[a-zA-Z_]\w*)?)',
        text_for_metrics
    )
    if dim_match:
        dim_candidate = dim_match.group(1).strip()
        if dim_candidate in ("quarter", "month", "year", "week", "day"):
            grain_map = {
                "quarter": TimeGrain.QUARTER,
                "month": TimeGrain.MONTH,
                "year": TimeGrain.YEAR,
                "week": TimeGrain.WEEK,
                "day": TimeGrain.DAY,
            }
            x_axis_time_grain = grain_map.get(dim_candidate, TimeGrain.MONTH)
            if datetime_cols:
                x_axis = datetime_cols[0]
        else:
            for col in columns:
                cl = col.lower()
                cr = cl.replace("_", " ")
                if cl == dim_candidate or cr == dim_candidate or dim_candidate.startswith(cl) or cl in dim_candidate:
                    if col.lower() not in filter_cols_matched:
                        x_axis = col
                        break

    # 6. Detect Multi-Metric & Aggregations
    found_metrics: list[tuple[str, AggregationType, int]] = []
    for col in numeric_cols:
        if col.lower() in filter_cols_matched:
            continue
        col_patterns = [r'\b' + re.escape(col.lower()) + r'\b', r'\b' + re.escape(col.lower().replace("_", " ")) + r'\b']
        for cp in col_patterns:
            m = re.search(cp, text_for_metrics)
            if m:
                pos = m.start()
                prefix = text_for_metrics[max(0, pos - 20):pos]
                agg = AggregationType.SUM
                if any(w in prefix for w in ("average", "avg", "mean")):
                    agg = AggregationType.AVG
                elif any(w in prefix for w in ("count", "number of", "how many")):
                    agg = AggregationType.COUNT
                elif any(w in prefix for w in ("min", "minimum", "lowest")):
                    agg = AggregationType.MIN
                elif any(w in prefix for w in ("max", "maximum", "highest")):
                    agg = AggregationType.MAX
                elif any(w in prefix for w in ("sum", "total")):
                    agg = AggregationType.SUM
                elif "rate" in col.lower() or "pct" in col.lower() or "percent" in col.lower():
                    agg = AggregationType.AVG
                found_metrics.append((col, agg, pos))
                break

    found_metrics.sort(key=lambda x: x[2])
    seen_metric_cols = set()
    unique_metrics = []
    for col, agg, pos in found_metrics:
        if col not in seen_metric_cols:
            seen_metric_cols.add(col)
            unique_metrics.append((col, agg))

    metrics: Optional[list[MetricSpec]] = None
    aggregation = AggregationType.SUM

    if len(unique_metrics) >= 2:
        metrics = [MetricSpec(column=c, aggregation=a) for c, a in unique_metrics]
    elif len(unique_metrics) == 1:
        y_axis = unique_metrics[0][0]
        aggregation = unique_metrics[0][1]

    # 7. Check if scatter plot
    is_scatter = any(w in msg_lower for w in ("scatter", "scatter plot")) or (" vs " in msg_lower and not x_axis_time_grain and not any(w in msg_lower for w in ("compare", "bar", "across")))
    if is_scatter:
        chart_type = ChartType.SCATTER
        metrics = None
        aggregation = AggregationType.NONE
        vs_match = re.search(r'([a-zA-Z_]\w*)\s+vs\s+([a-zA-Z_]\w*)', text_for_metrics)
        if vs_match:
            c1_name, c2_name = vs_match.group(1), vs_match.group(2)
            c1 = next((c for c in columns if c.lower() in (c1_name.lower(), c1_name.lower().replace(" ", "_"))), None)
            c2 = next((c for c in columns if c.lower() in (c2_name.lower(), c2_name.lower().replace(" ", "_"))), None)
            if c1 and c2:
                x_axis = c1
                y_axis = c2
        color_by = None
        color_match = re.search(r'(?:colored\s+by|color\s+by|by)\s+([a-zA-Z_]\w*)', msg_lower)
        if color_match:
            cat_name = color_match.group(1)
            for c in categorical_cols:
                if c.lower() == cat_name.lower() or c.lower().replace("_", " ") == cat_name.lower():
                    group_by = c
                    color_by = c
                    break
        title = f"{x_axis.replace('_', ' ').title() if x_axis else 'X'} vs {y_axis.replace('_', ' ').title() if y_axis else 'Y'}"
        if group_by:
            title += f" by {group_by.replace('_', ' ').title()}"
    elif metrics and len(metrics) >= 2:
        if x_axis:
            chart_type = ChartType.BAR
        else:
            chart_type = ChartType.KPI
    elif x_axis_time_grain or any(w in msg_lower for w in ("trend", "monthly", "weekly", "daily", "line")):
        chart_type = ChartType.LINE
    elif any(w in msg_lower for w in ("kpi", "total", "scorecard", "metric")) and not x_axis:
        chart_type = ChartType.KPI
    else:
        chart_type = ChartType.BAR

    # 8. Sort detection
    sort_direction = SortDirection.ASC
    sort_by = None
    has_sort = False
    if any(w in msg_lower for w in ("descending", "desc", "sorted desc", "sort desc", "highest", "top")):
        sort_direction = SortDirection.DESC
        has_sort = True
    elif any(w in msg_lower for w in ("ascending", "asc", "sorted asc", "sort asc", "lowest", "bottom")):
        sort_direction = SortDirection.ASC
        has_sort = True

    if has_sort:
        if metrics:
            sort_by = metrics[0].column
        elif y_axis:
            sort_by = y_axis
        elif x_axis:
            sort_by = x_axis

    # 9. Title generation
    if metrics:
        metric_labels = " & ".join(m.column.replace("_", " ").title() for m in metrics)
        if x_axis:
            title = f"{metric_labels} across {x_axis.replace('_', ' ').title()}"
        else:
            title = f"{metric_labels}"
    elif chart_type == ChartType.KPI:
        title = f"Total {(y_axis or 'Revenue').replace('_', ' ').title()}"
    elif chart_type == ChartType.LINE and x_axis_time_grain:
        grain_label = x_axis_time_grain.value.capitalize()
        title = f"{grain_label} {(y_axis or 'Revenue').replace('_', ' ').title()} Trend"
    elif x_axis and y_axis:
        title = f"{(y_axis).replace('_', ' ').title()} by {x_axis.replace('_', ' ').title()}"
    else:
        title = message[:80]

    config_kwargs = dict(
        chart_type=chart_type,
        title=title,
        x_axis=x_axis,
        y_axis=y_axis,
        group_by=group_by,
        color_by=color_by if is_scatter else None,
        aggregation=aggregation,
        x_axis_time_grain=x_axis_time_grain,
        metrics=metrics,
        trend_line=False,
        stacked=False,
        smooth=False,
        show_values=False,
        show_legend=True,
        show_grid=True,
        color_scheme="default",
    )
    if filters:
        config_kwargs["filters"] = filters
    if has_sort and sort_by:
        config_kwargs["sort_by"] = sort_by
        config_kwargs["sort_direction"] = sort_direction
    if chart_type == ChartType.KPI and y_axis and not metrics:
        config_kwargs["kpi_value_column"] = y_axis
        config_kwargs["kpi_aggregation"] = aggregation

    config = ChartConfig(**config_kwargs)
    config = _enforce_temporal_grain_consistency(config, schema)
    return config


async def suggest_follow_ups(
    schema: dict,
    current_config: Optional[ChartConfig] = None,
) -> list[str]:
    """Generate AI-powered follow-up prompt suggestions."""
    if not HAS_GENAI:
        return _fallback_suggestions(current_config, schema)

    try:
        schema_desc = _build_schema_description(schema)
        config_str = current_config.model_dump_json(indent=2) if current_config else "No chart yet."

        prompt = (
            f"Given this dataset schema:\n{schema_desc}\n\n"
            f"And current chart config:\n{config_str}\n\n"
            f"Suggest exactly 4 short follow-up prompts a user might type to enhance or modify the chart. "
            f"Return a JSON array of strings. Examples: \"Add a trend line\", \"Switch to pie chart\", "
            f"\"Filter to last 30 days\", \"Break down by category\". Return ONLY the JSON array."
        )

        response = await _execute_followup_prompt(prompt)
        text = response.text
        data = _extract_json(text)
        if isinstance(data, list):
            return [str(s) for s in data[:6]]
    except Exception as e:
        logger.warning("Suggestion generation failed: %s", e)

    return _fallback_suggestions(current_config, schema)


def _fallback_suggestions(config: Optional[ChartConfig] = None, schema: Optional[dict] = None) -> list[str]:
    """Context-aware follow-up suggestions when Gemini is unavailable."""
    suggestions = []
    columns = list(schema.keys()) if schema else []
    numeric_cols = [c for c in columns if schema and schema[c].get("dtype", "") in
                    ("int64", "float64", "int", "float", "number")] if schema else []
    cat_cols = [c for c in columns if schema and schema[c].get("dtype", "") in
                ("object", "category", "string")] if schema else []

    if config:
        # Chart type switches
        if config.chart_type == ChartType.BAR:
            suggestions.append("Switch to a line chart")
        elif config.chart_type == ChartType.LINE:
            suggestions.append("Switch to a bar chart")
        elif config.chart_type == ChartType.PIE:
            suggestions.append("Switch to a donut chart")
        else:
            suggestions.append("Switch to a bar chart")

        # Feature additions
        if not config.trend_line and config.chart_type in (ChartType.LINE, ChartType.BAR, ChartType.AREA):
            suggestions.append("Add a trend line")
        if not config.stacked and config.chart_type in (ChartType.BAR, ChartType.AREA):
            suggestions.append("Make it stacked")

        # Grouping suggestions using actual column names
        if not config.group_by and cat_cols:
            available = [c for c in cat_cols if c != config.x_axis]
            if available:
                suggestions.append(f"Break down by {available[0].replace('_', ' ')}")

        # Metric suggestions using actual column names
        if numeric_cols:
            other_metrics = [c for c in numeric_cols if c != config.y_axis]
            if other_metrics:
                suggestions.append(f"Show {other_metrics[0].replace('_', ' ')} instead")

        # Common actions
        suggestions.append("Show top 10")
        if not config.show_values:
            suggestions.append("Show data labels")
        suggestions.append("Show as KPI card")
    else:
        # No config yet — suggest initial charts using schema
        if numeric_cols and cat_cols:
            suggestions.append(f"Show {numeric_cols[0].replace('_', ' ')} by {cat_cols[0].replace('_', ' ')}")
        if numeric_cols:
            suggestions.append(f"Total {numeric_cols[0].replace('_', ' ')} as KPI")
        suggestions.append("Show all data as table")
        suggestions.append("Create a pie chart")

    return suggestions[:4]

