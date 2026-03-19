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
)
from llm.api_manager import with_llm_failover, get_active_client


# ── System Prompt ──

SYSTEM_PROMPT = """You are a data visualization assistant. Given a user's natural language request and a dataset schema, produce a JSON object that configures a chart.

RULES:
1. Return ONLY valid JSON — no markdown, no code fences, no explanation.
2. If the user's request is ambiguous, return: {"clarification": "<one specific question>", "suggestions": ["option1", "option2"]}
3. If the request is clear, return a chart configuration object with these fields:
   - chart_type: one of "bar", "line", "area", "pie", "donut", "scatter", "heatmap", "treemap", "funnel", "kpi", "table"
   - title: descriptive chart title
   - x_axis: column name for the x-axis (or category axis)
   - y_axis: column name for the y-axis (or value axis)
   - group_by: column to group/segment by (optional)
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
   - LEAVE `x_axis` and `group_by` as strictly null.
   - Do NOT randomly pick a categorical column like "gender" or "id" to fill the x-axis.
   - If the user explicitly asks for a "bar" chart but gives no category, change it to a "scatter" chart instead.
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


def _parse_config_from_dict(data: dict, current_config: Optional[ChartConfig] = None) -> ChartConfig:
    """Parse a dict into a ChartConfig, merging with current_config for follow-ups."""
    if current_config:
        # Start from current config and overlay only provided fields
        base = current_config.model_dump()
        for key, value in data.items():
            if value is not None:
                base[key] = value
        return ChartConfig(**base)
    
    # ── LLM Hallucination Fix ──
    # If comparing two metrics without a specified dimension, the LLM loves to hallucinate one.
    # For instance if x_axis is 'gender' but 'gender' isn't in the prompt.
    x_axis = data.get("x_axis")
    y_axis = data.get("y_axis")
    y_sec = data.get("y_axis_secondary")
    
    # We pass the schema keys in, but since we don't have the prompt here directly, 
    # we'll rely on the prompt's `ChartConfig` mapping. However we can check if 
    # the LLM picked a very arbitrary categorical column just to fill the X axis of a bar chart.
    if x_axis in ["gender", "id", "student_id", "user_id"]:
        # If the user asked for a metric, and the LLM just threw gender in to make a bar work:
        data["x_axis"] = None
        data["group_by"] = None
        if data.get("chart_type") == "bar" and (y_axis and y_sec):
            data["chart_type"] = "scatter"
        elif data.get("chart_type") == "bar":
            data["chart_type"] = "table"

    return ChartConfig(**{k: v for k, v in data.items() if v is not None})


@with_llm_failover(tier_rpm=5)
async def _execute_interpretation_prompt(full_prompt: str, system_prompt: str):
    client = get_active_client()
    return await client.aio.models.generate_content(
        model="gemini-2.0-flash",
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
    client = get_active_client()
    return await client.aio.models.generate_content(
        model="gemini-2.0-flash",
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
) -> ChartConfig | ClarificationRequest:
    """
    Interpret a natural language prompt into a ChartConfig.

    Args:
        message: User's natural language message.
        schema: Dataset schema as {col_name: {dtype, sample_values}}.
        current_config: Previous ChartConfig for follow-up edits.
        conversation_history: Prior prompts for context.

    Returns:
        ChartConfig or ClarificationRequest.
    """
    if not HAS_GENAI:
        return _fallback_interpret(message, schema, current_config)

    schema_desc = _build_schema_description(schema)

    # Build the user message with context
    parts = [f"Schema:\n{schema_desc}\n"]
    if current_config:
        parts.append(f"Current chart config:\n{current_config.model_dump_json(indent=2)}\n")
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
            return _fallback_interpret(message, schema, current_config)

        # Check if it's a clarification
        if "clarification" in data:
            return ClarificationRequest(
                clarification=data["clarification"],
                suggestions=data.get("suggestions", []),
            )

        return _parse_config_from_dict(data, current_config)

    except Exception as e:
        logger.error("Gemini interpretation failed: %s", str(e))
        return _fallback_interpret(message, schema, current_config)


def _fallback_interpret(
    message: str,
    schema: dict,
    current_config: Optional[ChartConfig] = None,
) -> ChartConfig:
    """
    Regex-based fallback when Gemini is unavailable.
    Detects chart type and basic column mapping from the prompt.
    """
    msg_lower = message.lower()

    # Detect chart type
    chart_type = ChartType.BAR  # default
    type_map = {
        "line": ChartType.LINE,
        "area": ChartType.AREA,
        "pie": ChartType.PIE,
        "donut": ChartType.DONUT,
        "scatter": ChartType.SCATTER,
        "heatmap": ChartType.HEATMAP,
        "heat map": ChartType.HEATMAP,
        "treemap": ChartType.TREEMAP,
        "tree map": ChartType.TREEMAP,
        "funnel": ChartType.FUNNEL,
        "kpi": ChartType.KPI,
        "metric": ChartType.KPI,
        "table": ChartType.TABLE,
        "bar": ChartType.BAR,
    }
    for keyword, ct in type_map.items():
        if keyword in msg_lower:
            chart_type = ct
            break

    # Detect columns mentioned in the prompt
    columns = list(schema.keys())
    mentioned_cols = [col for col in columns if col.lower() in msg_lower]

    # Detect aggregation
    aggregation = AggregationType.SUM
    agg_map = {
        "average": AggregationType.AVG,
        "avg": AggregationType.AVG,
        "mean": AggregationType.AVG,
        "count": AggregationType.COUNT,
        "minimum": AggregationType.MIN,
        "min": AggregationType.MIN,
        "maximum": AggregationType.MAX,
        "max": AggregationType.MAX,
        "median": AggregationType.MEDIAN,
    }
    for keyword, agg in agg_map.items():
        if keyword in msg_lower:
            aggregation = agg
            break

    # Detect modifiers
    trend_line = any(w in msg_lower for w in ["trend", "trendline", "trend line", "regression"])
    stacked = "stack" in msg_lower
    dark_mode = "dark" in msg_lower

    # Build config
    x_axis = None
    y_axis = None
    group_by = None

    if len(mentioned_cols) >= 2:
        # Heuristic: first mentioned is x, second is y
        x_axis = mentioned_cols[0]
        y_axis = mentioned_cols[1]
        if len(mentioned_cols) >= 3:
            group_by = mentioned_cols[2]
    elif len(mentioned_cols) == 1:
        if chart_type == ChartType.KPI:
            y_axis = mentioned_cols[0]
        else:
            y_axis = mentioned_cols[0]
            # Try to find a good x_axis
            for col in columns:
                dtype = schema[col].get("dtype", "")
                if col != mentioned_cols[0] and dtype in ("datetime", "date", "category", "object", "string"):
                    x_axis = col
                    break
    elif columns:
        # No columns mentioned — pick reasonable defaults
        numeric_cols = [c for c in columns if schema[c].get("dtype", "") in ("int64", "float64", "int", "float", "number", "numeric")]
        cat_cols = [c for c in columns if schema[c].get("dtype", "") in ("object", "category", "string", "datetime")]
        if cat_cols:
            x_axis = cat_cols[0]
        if numeric_cols:
            y_axis = numeric_cols[0]

    if current_config:
        base = current_config.model_copy()
        base.chart_type = chart_type
        if x_axis:
            base.x_axis = x_axis
        if y_axis:
            base.y_axis = y_axis
        if group_by:
            base.group_by = group_by
        base.aggregation = aggregation
        base.trend_line = base.trend_line or trend_line
        base.stacked = base.stacked or stacked
        if dark_mode:
            base.color_scheme = "dark"
        return base

    return ChartConfig(
        chart_type=chart_type,
        title=message[:80],
        x_axis=x_axis,
        y_axis=y_axis,
        group_by=group_by,
        aggregation=aggregation,
        trend_line=trend_line,
        stacked=stacked,
        color_scheme="dark" if dark_mode else "default",
    )


async def suggest_follow_ups(
    schema: dict,
    current_config: Optional[ChartConfig] = None,
) -> list[str]:
    """Generate AI-powered follow-up prompt suggestions."""
    if not HAS_GENAI:
        return _fallback_suggestions(current_config)

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

    return _fallback_suggestions(current_config)


def _fallback_suggestions(config: Optional[ChartConfig] = None) -> list[str]:
    """Static follow-up suggestions when Gemini is unavailable."""
    base = [
        "Switch to a line chart",
        "Add a trend line",
        "Show as dark mode",
        "Filter to top 10",
    ]
    if config:
        if not config.trend_line:
            base[1] = "Add a trend line"
        if config.chart_type == ChartType.BAR:
            base[0] = "Switch to a line chart"
        elif config.chart_type == ChartType.LINE:
            base[0] = "Switch to a bar chart"
        if not config.stacked:
            base.append("Make it stacked")
        if not config.group_by:
            base.append("Break down by category")
    return base[:4]
