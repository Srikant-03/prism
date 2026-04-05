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
    Enhanced regex-based fallback when Gemini is unavailable.
    Detects chart type, column mapping, aggregation, filters, sorting,
    grouping, and limit from the prompt using pattern matching.
    """
    msg_lower = message.lower().strip()

    # ── Detect chart type ──
    chart_type = ChartType.BAR  # default
    type_map = {
        "line chart": ChartType.LINE, "line graph": ChartType.LINE, "line": ChartType.LINE,
        "area chart": ChartType.AREA, "area": ChartType.AREA,
        "pie chart": ChartType.PIE, "pie": ChartType.PIE,
        "donut chart": ChartType.DONUT, "donut": ChartType.DONUT,
        "scatter plot": ChartType.SCATTER, "scatter chart": ChartType.SCATTER, "scatter": ChartType.SCATTER,
        "heatmap": ChartType.HEATMAP, "heat map": ChartType.HEATMAP,
        "treemap": ChartType.TREEMAP, "tree map": ChartType.TREEMAP,
        "funnel chart": ChartType.FUNNEL, "funnel": ChartType.FUNNEL,
        "kpi": ChartType.KPI, "metric": ChartType.KPI, "scorecard": ChartType.KPI,
        "total": ChartType.KPI, "overall": ChartType.KPI,
        "table": ChartType.TABLE, "data table": ChartType.TABLE, "grid": ChartType.TABLE,
        "bar chart": ChartType.BAR, "bar graph": ChartType.BAR, "bar": ChartType.BAR,
        "histogram": ChartType.BAR, "column chart": ChartType.BAR,
    }
    # Check longer patterns first for accuracy
    for keyword, ct in sorted(type_map.items(), key=lambda x: -len(x[0])):
        if keyword in msg_lower:
            chart_type = ct
            break

    # ── Classify schema columns ──
    columns = list(schema.keys())
    col_lower_map = {col.lower(): col for col in columns}

    numeric_cols = [c for c in columns if schema[c].get("dtype", "") in
                    ("int64", "float64", "int", "float", "number", "numeric", "int32", "float32")]
    categorical_cols = [c for c in columns if schema[c].get("dtype", "") in
                        ("object", "category", "string", "str", "bool")]
    datetime_cols = [c for c in columns if schema[c].get("dtype", "") in
                     ("datetime64[ns]", "datetime", "date", "datetime64") or
                     "date" in c.lower() or "time" in c.lower()]

    # ── Find columns mentioned in the prompt ──
    mentioned_cols = []
    for col in columns:
        # Match exact column name (case-insensitive, word-boundary)
        pattern = re.compile(r'\b' + re.escape(col.lower()) + r'\b')
        if pattern.search(msg_lower):
            mentioned_cols.append(col)
        else:
            # Also try matching with spaces instead of underscores
            col_readable = col.lower().replace("_", " ")
            if len(col_readable) > 2 and col_readable in msg_lower:
                mentioned_cols.append(col)

    # ── Detect "X by Y" pattern (e.g. "revenue by category") ──
    by_match = re.search(r'(\w[\w\s]*?)\s+by\s+(\w[\w\s]*?)(?:\s+and\s+|\s*$|\s+(?:as|in|for|with|where|show|using|group|top|bottom|limit))', msg_lower)
    by_y_col = None
    by_x_col = None
    by_group_col = None
    if by_match:
        y_phrase = by_match.group(1).strip()
        x_phrase = by_match.group(2).strip()
        # Match phrases to columns
        for col in columns:
            cl = col.lower().replace("_", " ")
            if cl in y_phrase or y_phrase in cl:
                by_y_col = col
            if cl in x_phrase or x_phrase in cl:
                by_x_col = col

    # ── Detect "group by" / "break down by" / "split by" ──
    group_match = re.search(r'(?:group\s*by|break\s*(?:down\s*)?by|split\s*by|per)\s+(\w[\w\s]*?)(?:\s*$|\s+(?:and|where|show|for|with|top|bottom))', msg_lower)
    if group_match:
        g_phrase = group_match.group(1).strip()
        for col in columns:
            cl = col.lower().replace("_", " ")
            if cl in g_phrase or g_phrase in cl:
                by_group_col = col
                break

    # ── Detect aggregation ──
    aggregation = AggregationType.SUM
    agg_map = {
        "average": AggregationType.AVG, "avg": AggregationType.AVG, "mean": AggregationType.AVG,
        "count": AggregationType.COUNT, "number of": AggregationType.COUNT, "how many": AggregationType.COUNT,
        "minimum": AggregationType.MIN, "min": AggregationType.MIN, "lowest": AggregationType.MIN,
        "maximum": AggregationType.MAX, "max": AggregationType.MAX, "highest": AggregationType.MAX,
        "median": AggregationType.MEDIAN,
        "sum": AggregationType.SUM, "total": AggregationType.SUM,
    }
    for keyword, agg in sorted(agg_map.items(), key=lambda x: -len(x[0])):
        if keyword in msg_lower:
            aggregation = agg
            break

    # ── Detect "top N" / "bottom N" ──
    limit = None
    sort_direction = None
    top_match = re.search(r'(?:top|best|highest|largest)\s+(\d+)', msg_lower)
    bottom_match = re.search(r'(?:bottom|worst|lowest|smallest)\s+(\d+)', msg_lower)
    if top_match:
        limit = int(top_match.group(1))
        sort_direction = SortDirection.DESC
    elif bottom_match:
        limit = int(bottom_match.group(1))
        sort_direction = SortDirection.ASC

    # ── Detect filters (e.g. "where age > 30", "for region = East") ──
    filters: list[FilterCondition] = []
    filter_patterns = [
        r'(?:where|filter|for|when|if)\s+(\w[\w\s]*?)\s*(=|!=|>|<|>=|<=)\s*["\']?(\w[\w\s]*?)["\']?\s*(?:$|and\b|or\b|,)',
        r'(\w+)\s*(?:equals?|is)\s+["\']?(\w[\w\s]*?)["\']?\s*(?:$|and\b|,)',
    ]
    for pat in filter_patterns:
        for m in re.finditer(pat, msg_lower):
            groups = m.groups()
            if len(groups) == 3:
                col_phrase, op, val = groups
            elif len(groups) == 2:
                col_phrase, val = groups
                op = "="
            else:
                continue
            # Match the column phrase to an actual column
            matched_col = None
            for col in columns:
                cl = col.lower().replace("_", " ")
                if cl in col_phrase.strip() or col_phrase.strip() in cl:
                    matched_col = col
                    break
            if matched_col:
                # Try to parse numeric values
                try:
                    parsed_val: any = int(val.strip())
                except ValueError:
                    try:
                        parsed_val = float(val.strip())
                    except ValueError:
                        parsed_val = val.strip()
                filters.append(FilterCondition(column=matched_col, operator=op.strip(), value=parsed_val))

    # ── Detect modifiers ──
    trend_line = any(w in msg_lower for w in ["trend", "trendline", "trend line", "regression"])
    stacked = "stack" in msg_lower
    smooth = any(w in msg_lower for w in ["smooth", "curved"])
    show_values = any(w in msg_lower for w in ["show values", "show numbers", "data labels", "label"])
    show_legend = "legend" not in msg_lower or "show legend" in msg_lower  # default True unless "no legend"
    show_grid = "no grid" not in msg_lower  # default True

    # ── Build axis assignments ──
    x_axis = by_x_col
    y_axis = by_y_col
    group_by = by_group_col

    if not x_axis and not y_axis:
        # Use explicitly mentioned columns
        if len(mentioned_cols) >= 2:
            # Heuristic: if first is categorical/datetime → x, if second is numeric → y
            if mentioned_cols[0] in categorical_cols or mentioned_cols[0] in datetime_cols:
                x_axis = mentioned_cols[0]
                y_axis = mentioned_cols[1]
            elif mentioned_cols[1] in categorical_cols or mentioned_cols[1] in datetime_cols:
                x_axis = mentioned_cols[1]
                y_axis = mentioned_cols[0]
            else:
                x_axis = mentioned_cols[0]
                y_axis = mentioned_cols[1]
            if len(mentioned_cols) >= 3:
                group_by = mentioned_cols[2]
        elif len(mentioned_cols) == 1:
            if mentioned_cols[0] in numeric_cols:
                y_axis = mentioned_cols[0]
                # Auto-pick a categorical x-axis
                for col in categorical_cols + datetime_cols:
                    if col != y_axis:
                        x_axis = col
                        break
            else:
                x_axis = mentioned_cols[0]
                # Auto-pick a numeric y-axis
                for col in numeric_cols:
                    x_axis_set = x_axis  # avoid shadowing
                    if col != x_axis_set:
                        y_axis = col
                        break

    # If still no axis assignment, pick smart defaults
    if not x_axis and not y_axis and columns:
        if chart_type == ChartType.KPI:
            y_axis = numeric_cols[0] if numeric_cols else columns[0]
        elif chart_type == ChartType.TABLE:
            pass  # Table doesn't need explicit axes
        else:
            if datetime_cols:
                x_axis = datetime_cols[0]
            elif categorical_cols:
                x_axis = categorical_cols[0]
            if numeric_cols:
                y_axis = numeric_cols[0]

    # ── Sort by ──
    sort_by = y_axis if sort_direction else None

    # ── Generate a clean title ──
    if chart_type == ChartType.KPI:
        agg_label = aggregation.value.capitalize() if aggregation != AggregationType.SUM else "Total"
        col_label = (y_axis or "Value").replace("_", " ").title()
        title = f"{agg_label} {col_label}"
    elif x_axis and y_axis:
        agg_label = aggregation.value.upper() if aggregation != AggregationType.SUM else ""
        y_label = y_axis.replace("_", " ").title()
        x_label = x_axis.replace("_", " ").title()
        prefix = f"{agg_label} " if agg_label else ""
        title = f"{prefix}{y_label} by {x_label}"
        if group_by:
            title += f" ({group_by.replace('_', ' ').title()})"
        if limit and sort_direction:
            dir_label = "Top" if sort_direction == SortDirection.DESC else "Bottom"
            title = f"{dir_label} {limit} — {title}"
    else:
        title = message[:80]

    # ── Build config ──
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
        base.smooth = base.smooth or smooth
        base.show_values = show_values if show_values else base.show_values
        if filters:
            base.filters = (base.filters or []) + filters
        if sort_by:
            base.sort_by = sort_by
            base.sort_direction = sort_direction
        if limit:
            base.limit = limit
        base.title = title
        return base

    config_kwargs = dict(
        chart_type=chart_type,
        title=title,
        x_axis=x_axis,
        y_axis=y_axis,
        group_by=group_by,
        aggregation=aggregation,
        trend_line=trend_line,
        stacked=stacked,
        smooth=smooth,
        show_values=show_values,
        show_legend=show_legend,
        show_grid=show_grid,
        color_scheme="default",
    )
    if filters:
        config_kwargs["filters"] = filters
    if sort_by:
        config_kwargs["sort_by"] = sort_by
        config_kwargs["sort_direction"] = sort_direction
    if limit:
        config_kwargs["limit"] = limit
    if chart_type == ChartType.KPI and y_axis:
        config_kwargs["kpi_value_column"] = y_axis
        config_kwargs["kpi_aggregation"] = aggregation.value

    return ChartConfig(**config_kwargs)


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

