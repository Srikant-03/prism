"""
Dashboard Config-to-SQL — Converts ChartConfig into executable DuckDB SQL.

Uses the existing sql/query_builder.py QueryBuilder to generate valid SQL
from the structured ChartConfig produced by the prompt interpreter.
"""

from __future__ import annotations

import logging
from typing import Optional

from dashboard.chart_config_models import (
    ChartConfig,
    ChartType,
    AggregationType,
    FilterCondition,
)
from sql.query_builder import QueryBuilder

logger = logging.getLogger(__name__)


def _agg_to_sql(agg: AggregationType) -> str:
    """Map AggregationType enum to SQL function name."""
    return {
        AggregationType.SUM: "SUM",
        AggregationType.AVG: "AVG",
        AggregationType.COUNT: "COUNT",
        AggregationType.MIN: "MIN",
        AggregationType.MAX: "MAX",
        AggregationType.MEDIAN: "MEDIAN",
        AggregationType.NONE: "",
    }.get(agg, "")


def _filter_to_condition(f: FilterCondition) -> dict:
    """Convert a FilterCondition to a QueryBuilder condition dict."""
    cond: dict = {
        "column": f.column,
        "operator": f.operator,
    }
    if f.operator in ("in", "not_in") and f.values:
        cond["value"] = f.values
    elif f.operator == "between" and f.values and len(f.values) >= 2:
        cond["value"] = f.values[:2]
    elif f.operator not in ("is_null", "is_not_null"):
        cond["value"] = f.value
    return cond


def config_to_sql(
    config: ChartConfig,
    table_name: str,
    global_filters: Optional[list[FilterCondition]] = None,
) -> str:
    """
    Convert a ChartConfig into a DuckDB SQL query string.

    Args:
        config: Structured chart configuration.
        table_name: Name of the DuckDB table to query.
        global_filters: Additional filters from the global filter bar.

    Returns:
        Valid SQL query string.
    """
    spec: dict = {
        "from": {"table": table_name},
    }

    columns: list[dict] = []
    group_by_cols: list[str] = []
    needs_aggregation = config.aggregation != AggregationType.NONE

    # ── KPI type: single aggregated value ──
    if config.chart_type == ChartType.KPI:
        value_col = config.kpi_value_column or config.y_axis
        if value_col:
            agg_fn = _agg_to_sql(config.kpi_aggregation or config.aggregation)
            if agg_fn:
                columns.append({"expression": f"{agg_fn}({value_col})", "alias": "value"})
            else:
                columns.append({"name": value_col, "alias": "value"})
        else:
            columns.append({"expression": "COUNT(*)", "alias": "value"})

        # Optional comparison
        if config.kpi_comparison_column:
            columns.append({"name": config.kpi_comparison_column, "alias": "comparison"})

    # ── Table type: all columns ──
    elif config.chart_type == ChartType.TABLE:
        if config.x_axis:
            columns.append({"name": config.x_axis})
        if config.y_axis:
            if needs_aggregation:
                agg_fn = _agg_to_sql(config.aggregation)
                columns.append({"expression": f"{agg_fn}({config.y_axis})", "alias": config.y_axis})
            else:
                columns.append({"name": config.y_axis})
        if config.group_by and config.group_by not in [config.x_axis]:
            columns.append({"name": config.group_by})

        if config.x_axis and needs_aggregation:
            group_by_cols.append(config.x_axis)
        if config.group_by and needs_aggregation:
            group_by_cols.append(config.group_by)

        if not columns:
            columns.append({"expression": "*"})

    # ── Standard chart types ──
    else:
        # X-axis (category/dimension)
        if config.x_axis:
            columns.append({"name": config.x_axis})
            if needs_aggregation:
                group_by_cols.append(config.x_axis)

        # Group-by (series/segment)
        if config.group_by:
            columns.append({"name": config.group_by})
            if needs_aggregation:
                group_by_cols.append(config.group_by)

        # Y-axis (measure)
        if config.y_axis:
            if needs_aggregation:
                agg_fn = _agg_to_sql(config.aggregation)
                columns.append({"expression": f"{agg_fn}({config.y_axis})", "alias": config.y_axis})
            else:
                columns.append({"name": config.y_axis})

        # Secondary Y-axis
        if config.y_axis_secondary:
            if needs_aggregation:
                agg_fn = _agg_to_sql(config.aggregation)
                columns.append({"expression": f"{agg_fn}({config.y_axis_secondary})", "alias": config.y_axis_secondary})
            else:
                columns.append({"name": config.y_axis_secondary})

        # Size-by for scatter/treemap
        if config.size_by and config.chart_type in (ChartType.SCATTER, ChartType.TREEMAP):
            if needs_aggregation:
                agg_fn = _agg_to_sql(config.aggregation)
                columns.append({"expression": f"{agg_fn}({config.size_by})", "alias": config.size_by})
            else:
                columns.append({"name": config.size_by})

    if columns:
        spec["columns"] = columns
    if group_by_cols:
        spec["group_by"] = group_by_cols

    # ── Filters ──
    all_filters = list(config.filters)
    if global_filters:
        all_filters.extend(global_filters)

    if all_filters:
        spec["where"] = [_filter_to_condition(f) for f in all_filters]

    # ── Sort ──
    if config.sort_by:
        spec["order_by"] = [{"column": config.sort_by, "direction": config.sort_direction.value}]
    elif config.x_axis and config.chart_type in (ChartType.LINE, ChartType.AREA):
        # Time-series charts should sort by x-axis
        spec["order_by"] = [{"column": config.x_axis, "direction": "asc"}]

    # ── Limit ──
    if config.limit:
        spec["limit"] = config.limit

    try:
        sql = QueryBuilder.build(spec)
        logger.info("Generated SQL: %s", sql)
        return sql
    except Exception as e:
        logger.error("QueryBuilder failed: %s — falling back to manual SQL", e)
        return _fallback_sql(config, table_name, all_filters)


def _fallback_sql(
    config: ChartConfig,
    table_name: str,
    filters: list[FilterCondition],
) -> str:
    """Generate SQL manually if QueryBuilder fails."""
    parts = ["SELECT"]

    # Columns
    select_parts = []
    agg_fn = _agg_to_sql(config.aggregation)
    needs_agg = config.aggregation != AggregationType.NONE

    if config.chart_type == ChartType.KPI:
        col = config.kpi_value_column or config.y_axis or "*"
        
        # Don't quote "*" 
        col_str = f'"{col}"' if col != "*" else "*"
            
        if agg_fn:
            select_parts.append(f"{agg_fn}({col_str}) AS value")
        else:
            select_parts.append(f"{col_str} AS value")
    else:
        if config.x_axis:
            select_parts.append(f'"{config.x_axis}"')
        if config.group_by:
            select_parts.append(f'"{config.group_by}"')
        if config.y_axis:
            if needs_agg and agg_fn:
                select_parts.append(f'{agg_fn}("{config.y_axis}") AS "{config.y_axis}"')
            else:
                select_parts.append(f'"{config.y_axis}"')

    if not select_parts:
        select_parts.append("*")

    parts.append(", ".join(select_parts))
    parts.append(f'FROM "{table_name}"')

    # WHERE
    if filters:
        conditions = []
        for f in filters:
            col = f.column.replace('"', '""') # Basic identifier escaping
            if f.operator == "is_null":
                conditions.append(f'"{col}" IS NULL')
            elif f.operator == "is_not_null":
                conditions.append(f'"{col}" IS NOT NULL')
            elif f.operator == "in" and f.values:
                # Basic escaping for string values
                vals = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in f.values)
                conditions.append(f'"{col}" IN ({vals})')
            elif f.operator == "not_in" and f.values:
                vals = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in f.values)
                conditions.append(f'"{col}" NOT IN ({vals})')
            elif f.operator == "between" and f.values and len(f.values) >= 2:
                v1 = str(f.values[0]).replace(chr(39), chr(39)*2)
                v2 = str(f.values[1]).replace(chr(39), chr(39)*2)
                conditions.append(f'"{col}" BETWEEN \'{v1}\' AND \'{v2}\'')
            else:
                val = str(f.value).replace(chr(39), chr(39)*2)
                conditions.append(f'"{col}" {f.operator} \'{val}\'')
        if conditions:
            parts.append("WHERE " + " AND ".join(conditions))

    # GROUP BY
    group_cols = []
    if config.x_axis and needs_agg:
        group_cols.append(f'"{config.x_axis}"')
    if config.group_by and needs_agg:
        group_cols.append(f'"{config.group_by}"')
    if group_cols:
        parts.append("GROUP BY " + ", ".join(group_cols))

    # ORDER BY
    if config.sort_by:
        parts.append(f'ORDER BY "{config.sort_by}" {config.sort_direction.value.upper()}')
    elif config.x_axis and config.chart_type in (ChartType.LINE, ChartType.AREA):
        parts.append(f'ORDER BY "{config.x_axis}" ASC')

    # LIMIT
    if config.limit:
        parts.append(f"LIMIT {config.limit}")

    return " ".join(parts)
