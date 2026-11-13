"""
Dashboard Config-to-SQL — Converts ChartConfig into executable DuckDB SQL.

Uses the existing sql/query_builder.py QueryBuilder to generate valid SQL
from the structured ChartConfig produced by the prompt interpreter.
Supports Power BI chart types, multi-metric aggregates, custom DAX measures,
advanced filter operators, boxplots, histograms, and Row-Level Security (RLS).
"""

from __future__ import annotations

import logging
from typing import Optional

from dashboard.chart_config_models import (
    ChartConfig,
    ChartType,
    AggregationType,
    FilterCondition,
    TimeGrain,
)
from dashboard.measure_engine import MeasureEngine
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


def _time_grain_sql_expr(col: str, grain: TimeGrain) -> tuple[str, str]:
    """Return (SELECT expression, alias) for temporal bucketing in DuckDB.

    Examples:
        month  -> strftime(DATE_TRUNC('month', TRY_CAST("date" AS DATE)), '%Y-%m')
        day    -> CAST(TRY_CAST("date" AS DATE) AS VARCHAR)
        week   -> strftime(DATE_TRUNC('week', TRY_CAST("date" AS DATE)), '%Y-W%W')
        quarter-> strftime(DATE_TRUNC('quarter', TRY_CAST("date" AS DATE)), '%Y') || '-Q' || QUARTER(TRY_CAST("date" AS DATE))
        year   -> strftime(TRY_CAST("date" AS DATE), '%Y')
    """
    cast_expr = f'TRY_CAST("{col}" AS DATE)'
    trunc_expr = f"DATE_TRUNC('{grain.value}', {cast_expr})"

    fmt_map = {
        TimeGrain.DAY:     f"CAST({cast_expr} AS VARCHAR)",
        TimeGrain.WEEK:    f"strftime({trunc_expr}, '%Y-W%W')",
        TimeGrain.MONTH:   f"strftime({trunc_expr}, '%Y-%m')",
        TimeGrain.QUARTER: f"strftime({trunc_expr}, '%Y') || '-Q' || QUARTER({cast_expr})",
        TimeGrain.YEAR:    f"strftime({cast_expr}, '%Y')",
    }
    return fmt_map.get(grain, f'"{col}"'), col


def _filter_to_condition(f: FilterCondition) -> dict:
    """Convert a FilterCondition to a QueryBuilder condition dict."""
    op = f.operator.lower().strip()
    cond: dict = {
        "column": f.column,
        "op": op,
    }

    if op in ("in", "not_in") and f.values:
        cond["values"] = f.values
    elif op == "between" and f.values and len(f.values) >= 2:
        cond["values"] = f.values[:2]
    elif op in ("contains", "like"):
        cond["op"] = "ilike"
        cond["value"] = f"%{f.value}%"
    elif op == "starts_with":
        cond["op"] = "ilike"
        cond["value"] = f"{f.value}%"
    elif op == "relative_date":
        # DuckDB relative date expressions
        val = str(f.value).lower().strip()
        if "30" in val:
            cond["op"] = ">="
            cond["expression"] = f'TRY_CAST("{f.column}" AS DATE) >= (CURRENT_DATE - INTERVAL \'30 days\')'
        elif "90" in val:
            cond["op"] = ">="
            cond["expression"] = f'TRY_CAST("{f.column}" AS DATE) >= (CURRENT_DATE - INTERVAL \'90 days\')'
        elif "year" in val:
            cond["op"] = ">="
            cond["expression"] = f'TRY_CAST("{f.column}" AS DATE) >= DATE_TRUNC(\'year\', CURRENT_DATE)'
    elif op not in ("is_null", "is_not_null"):
        cond["value"] = f.value

    return cond


def config_to_sql(
    config: ChartConfig,
    table_name: str,
    global_filters: Optional[list[FilterCondition]] = None,
    rls_predicate: Optional[str] = None,
) -> str:
    """
    Convert a ChartConfig into a DuckDB SQL query string.

    Args:
        config: Structured chart configuration.
        table_name: Name of the DuckDB table to query.
        global_filters: Additional filters from the global filter bar or slicers.
        rls_predicate: Optional Row-Level Security predicate to apply.

    Returns:
        Valid SQL query string.
    """
    spec: dict = {
        "from": {"table": table_name},
    }

    columns: list[dict] = []
    group_by_cols: list[str] = []
    needs_aggregation = config.aggregation != AggregationType.NONE

    # ── KPI & Multi-KPI Types ──
    if config.chart_type in (ChartType.KPI, ChartType.MULTI_KPI, ChartType.GAUGE, ChartType.BULLET):
        if config.metrics:
            for m in config.metrics:
                alias = m.alias or m.label or m.column
                if m.measure_expression:
                    compiled = MeasureEngine.compile_measure(m.measure_expression, primary_date_col=config.x_axis)
                    columns.append({"expression": compiled, "alias": alias})
                else:
                    agg_fn = _agg_to_sql(m.aggregation)
                    if agg_fn:
                        columns.append({"expression": f'{agg_fn}("{m.column}")', "alias": alias})
                    else:
                        columns.append({"column": m.column, "alias": alias})
        else:
            value_col = config.kpi_value_column or config.y_axis
            if value_col:
                agg_fn = _agg_to_sql(config.kpi_aggregation or config.aggregation)
                if agg_fn:
                    columns.append({"expression": f'{agg_fn}("{value_col}")', "alias": "value"})
                else:
                    columns.append({"column": value_col, "alias": "value"})
            else:
                columns.append({"expression": "COUNT(*)", "alias": "value"})

            if config.kpi_comparison_column:
                columns.append({"column": config.kpi_comparison_column, "alias": "comparison"})

    # ── Boxplot Type ──
    elif config.chart_type == ChartType.BOXPLOT:
        val_col = config.y_axis or (config.metrics[0].column if config.metrics else None)
        if not val_col:
            val_col = "value"
        if config.x_axis:
            columns.append({"column": config.x_axis})
            group_by_cols.append(config.x_axis)
        columns.extend([
            {"expression": f'MIN("{val_col}")', "alias": "min"},
            {"expression": f'QUANTILE_CONT("{val_col}", 0.25)', "alias": "q1"},
            {"expression": f'MEDIAN("{val_col}")', "alias": "median"},
            {"expression": f'QUANTILE_CONT("{val_col}", 0.75)', "alias": "q3"},
            {"expression": f'MAX("{val_col}")', "alias": "max"},
            {"expression": f'AVG("{val_col}")', "alias": "mean"},
        ])

    # ── Table & Matrix Types ──
    elif config.chart_type in (ChartType.TABLE, ChartType.MATRIX):
        if config.x_axis:
            columns.append({"column": config.x_axis})
            if needs_aggregation:
                group_by_cols.append(config.x_axis)

        if config.group_by and config.group_by != config.x_axis:
            columns.append({"column": config.group_by})
            if needs_aggregation:
                group_by_cols.append(config.group_by)

        if config.metrics:
            for m in config.metrics:
                alias = m.alias or m.label or m.column
                if m.measure_expression:
                    compiled = MeasureEngine.compile_measure(m.measure_expression, primary_date_col=config.x_axis)
                    columns.append({"expression": compiled, "alias": alias})
                else:
                    agg_fn = _agg_to_sql(m.aggregation)
                    if agg_fn and needs_aggregation:
                        columns.append({"expression": f'{agg_fn}("{m.column}")', "alias": alias})
                    else:
                        columns.append({"column": m.column, "alias": alias})
        elif config.y_axis:
            if needs_aggregation:
                agg_fn = _agg_to_sql(config.aggregation)
                columns.append({"expression": f'{agg_fn}("{config.y_axis}")', "alias": config.y_axis})
            else:
                columns.append({"column": config.y_axis})

        if not columns:
            columns.append({"expression": "*"})

    # ── Standard, Composition, and Statistical Charts ──
    else:
        # X-axis (category/dimension)
        if config.x_axis:
            if config.x_axis_time_grain and config.x_axis_time_grain != TimeGrain.NONE:
                grain_expr, alias = _time_grain_sql_expr(config.x_axis, config.x_axis_time_grain)
                columns.append({"expression": grain_expr, "alias": config.x_axis})
                if needs_aggregation:
                    group_by_cols.append(grain_expr)
            else:
                columns.append({"column": config.x_axis})
                if needs_aggregation:
                    group_by_cols.append(config.x_axis)

        # Group-by or Color-by
        grp = config.color_by or config.group_by
        if grp and grp != config.x_axis:
            columns.append({"column": grp})
            if needs_aggregation:
                group_by_cols.append(grp)

        # Multi-metric support
        if config.metrics:
            for m in config.metrics:
                alias = m.alias or m.label or m.column
                if m.measure_expression:
                    compiled = MeasureEngine.compile_measure(m.measure_expression, primary_date_col=config.x_axis)
                    columns.append({"expression": compiled, "alias": alias})
                else:
                    agg_fn = _agg_to_sql(m.aggregation)
                    if agg_fn:
                        columns.append({"expression": f'{agg_fn}("{m.column}")', "alias": alias})
                    else:
                        columns.append({"column": m.column, "alias": alias})
        else:
            if config.y_axis:
                if needs_aggregation:
                    agg_fn = _agg_to_sql(config.aggregation)
                    columns.append({"expression": f'{agg_fn}("{config.y_axis}")', "alias": config.y_axis})
                else:
                    columns.append({"column": config.y_axis})

            if config.y_axis_secondary:
                if needs_aggregation:
                    agg_fn = _agg_to_sql(config.aggregation)
                    columns.append({"expression": f'{agg_fn}("{config.y_axis_secondary}")', "alias": config.y_axis_secondary})
                else:
                    columns.append({"column": config.y_axis_secondary})

        # Size-by for scatter/bubble/treemap
        if config.size_by and config.chart_type in (ChartType.SCATTER, ChartType.BUBBLE, ChartType.TREEMAP):
            if needs_aggregation:
                agg_fn = _agg_to_sql(config.aggregation)
                columns.append({"expression": f'{agg_fn}("{config.size_by}")', "alias": config.size_by})
            else:
                columns.append({"column": config.size_by})

    if columns:
        spec["columns"] = columns
    if group_by_cols:
        spec["group_by"] = group_by_cols

    # ── Filters ──
    all_filters = list(config.filters)
    if global_filters:
        all_filters.extend(global_filters)

    # Auto-exclude nulls on x_axis for charts to prevent phantom null categories
    if config.x_axis and config.chart_type not in (ChartType.KPI, ChartType.MULTI_KPI, ChartType.GAUGE, ChartType.BULLET, ChartType.TABLE, ChartType.MATRIX):
        existing_cols = {f.column for f in all_filters}
        if config.x_axis not in existing_cols:
            all_filters.append(FilterCondition(column=config.x_axis, operator="is_not_null"))

    # For scatter and bubble charts, auto-exclude rows where y is NULL as well
    if config.chart_type in (ChartType.SCATTER, ChartType.BUBBLE) and config.y_axis:
        existing_cols = {f.column for f in all_filters}
        if config.y_axis not in existing_cols:
            all_filters.append(FilterCondition(column=config.y_axis, operator="is_not_null"))

    if all_filters:
        spec["where"] = [_filter_to_condition(f) for f in all_filters]

    # ── Sort ──
    if config.sort_by:
        if config.x_axis and config.sort_by == config.x_axis and config.x_axis_time_grain and config.x_axis_time_grain != TimeGrain.NONE:
            grain_expr, _ = _time_grain_sql_expr(config.x_axis, config.x_axis_time_grain)
            spec["order_by"] = [{"column": grain_expr, "direction": config.sort_direction.value, "is_expression": True}]
        else:
            spec["order_by"] = [{"column": config.sort_by, "direction": config.sort_direction.value}]
    elif config.x_axis:
        if config.x_axis_time_grain and config.x_axis_time_grain != TimeGrain.NONE:
            grain_expr, _ = _time_grain_sql_expr(config.x_axis, config.x_axis_time_grain)
            spec["order_by"] = [{"column": grain_expr, "direction": "asc", "is_expression": True}]
        elif config.chart_type in (ChartType.LINE, ChartType.AREA, ChartType.STACKED_AREA, ChartType.RIBBON):
            spec["order_by"] = [{"column": config.x_axis, "direction": "asc"}]

    # ── Limit ──
    if config.limit:
        spec["limit"] = config.limit

    try:
        sql = QueryBuilder.build(spec)
        if rls_predicate:
            sql = MeasureEngine.inject_rls(sql, rls_predicate)
        logger.info("Generated SQL: %s", sql)
        return sql
    except Exception as e:
        logger.error("QueryBuilder failed: %s — falling back to manual SQL", e)
        fallback = _fallback_sql(config, table_name, all_filters)
        if rls_predicate:
            fallback = MeasureEngine.inject_rls(fallback, rls_predicate)
        return fallback


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

    if config.chart_type in (ChartType.KPI, ChartType.MULTI_KPI, ChartType.GAUGE, ChartType.BULLET):
        if config.metrics:
            for m in config.metrics:
                alias = m.alias or m.label or m.column
                if m.measure_expression:
                    compiled = MeasureEngine.compile_measure(m.measure_expression, primary_date_col=config.x_axis)
                    select_parts.append(f'{compiled} AS "{alias}"')
                else:
                    m_agg = _agg_to_sql(m.aggregation)
                    if m_agg:
                        select_parts.append(f'{m_agg}("{m.column}") AS "{alias}"')
                    else:
                        select_parts.append(f'"{m.column}" AS "{alias}"')
        else:
            col = config.kpi_value_column or config.y_axis or "*"
            col_str = f'"{col}"' if col != "*" else "*"
            if agg_fn:
                select_parts.append(f"{agg_fn}({col_str}) AS value")
            else:
                select_parts.append(f"{col_str} AS value")
    else:
        if config.x_axis:
            if config.x_axis_time_grain and config.x_axis_time_grain != TimeGrain.NONE:
                grain_expr, alias = _time_grain_sql_expr(config.x_axis, config.x_axis_time_grain)
                select_parts.append(f'{grain_expr} AS "{config.x_axis}"')
            else:
                select_parts.append(f'"{config.x_axis}"')
        grp = config.color_by or config.group_by
        if grp and grp != config.x_axis:
            select_parts.append(f'"{grp}"')
        if config.metrics:
            for m in config.metrics:
                alias = m.alias or m.label or m.column
                if m.measure_expression:
                    compiled = MeasureEngine.compile_measure(m.measure_expression, primary_date_col=config.x_axis)
                    select_parts.append(f'{compiled} AS "{alias}"')
                else:
                    m_agg = _agg_to_sql(m.aggregation)
                    if m_agg:
                        select_parts.append(f'{m_agg}("{m.column}") AS "{alias}"')
                    else:
                        select_parts.append(f'"{m.column}" AS "{alias}"')
        else:
            if config.y_axis:
                if needs_agg and agg_fn:
                    select_parts.append(f'{agg_fn}("{config.y_axis}") AS "{config.y_axis}"')
                else:
                    select_parts.append(f'"{config.y_axis}"')
            if config.y_axis_secondary:
                if needs_agg and agg_fn:
                    select_parts.append(f'{agg_fn}("{config.y_axis_secondary}") AS "{config.y_axis_secondary}"')
                else:
                    select_parts.append(f'"{config.y_axis_secondary}"')

    if not select_parts:
        select_parts.append("*")

    parts.append(", ".join(select_parts))
    parts.append(f'FROM "{table_name}"')

    # WHERE
    if filters:
        conditions = []
        for f in filters:
            col = f.column.replace('"', '""')
            op = f.operator.lower().strip()
            if op == "is_null":
                conditions.append(f'"{col}" IS NULL')
            elif op == "is_not_null":
                conditions.append(f'"{col}" IS NOT NULL')
            elif op in ("contains", "like"):
                val = str(f.value).replace("'", "''")
                conditions.append(f'LOWER("{col}") LIKE LOWER(\'%{val}%\')')
            elif op == "starts_with":
                val = str(f.value).replace("'", "''")
                conditions.append(f'LOWER("{col}") LIKE LOWER(\'{val}%\')')
            elif op == "in" and f.values:
                vals = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in f.values)
                conditions.append(f'"{col}" IN ({vals})')
            elif op == "not_in" and f.values:
                vals = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in f.values)
                conditions.append(f'"{col}" NOT IN ({vals})')
            elif op == "between" and f.values and len(f.values) >= 2:
                v1 = str(f.values[0]).replace("'", "''")
                v2 = str(f.values[1]).replace("'", "''")
                conditions.append(f'"{col}" BETWEEN \'{v1}\' AND \'{v2}\'')
            else:
                val = str(f.value).replace("'", "''")
                valid_ops = {"=", "!=", ">", "<", ">=", "<=", "like", "ilike", "not like"}
                safe_op = op if op in valid_ops else "="
                conditions.append(f'"{col}" {safe_op} \'{val}\'')
        if conditions:
            parts.append("WHERE " + " AND ".join(conditions))

    # GROUP BY
    group_cols = []
    if config.x_axis and needs_agg:
        if config.x_axis_time_grain and config.x_axis_time_grain != TimeGrain.NONE:
            grain_expr, _ = _time_grain_sql_expr(config.x_axis, config.x_axis_time_grain)
            group_cols.append(grain_expr)
        else:
            group_cols.append(f'"{config.x_axis}"')
    grp = config.color_by or config.group_by
    if grp and grp != config.x_axis and needs_agg:
        group_cols.append(f'"{grp}"')
    if group_cols:
        parts.append("GROUP BY " + ", ".join(group_cols))

    # ORDER BY
    if config.sort_by:
        if config.x_axis and config.sort_by == config.x_axis and config.x_axis_time_grain and config.x_axis_time_grain != TimeGrain.NONE:
            grain_expr, _ = _time_grain_sql_expr(config.x_axis, config.x_axis_time_grain)
            parts.append(f'ORDER BY {grain_expr} {config.sort_direction.value.upper()}')
        else:
            parts.append(f'ORDER BY "{config.sort_by}" {config.sort_direction.value.upper()}')
    elif config.x_axis:
        if config.x_axis_time_grain and config.x_axis_time_grain != TimeGrain.NONE:
            grain_expr, _ = _time_grain_sql_expr(config.x_axis, config.x_axis_time_grain)
            parts.append(f'ORDER BY {grain_expr} ASC')
        elif config.chart_type in (ChartType.LINE, ChartType.AREA, ChartType.STACKED_AREA, ChartType.RIBBON):
            parts.append(f'ORDER BY "{config.x_axis}" ASC')

    # LIMIT
    if config.limit:
        parts.append(f"LIMIT {config.limit}")

    return " ".join(parts)
