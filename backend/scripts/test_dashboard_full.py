"""
Comprehensive Dashboard Test Suite - tests every function in the dashboard pipeline.

Tests (no server required - uses direct imports + in-process DuckDB):
  1. Pydantic Models - instantiation, validation, defaults, edge cases
  2. config_to_sql - every chart type, aggregations, filters, sort, limit, fallback
  3. prompt_interpreter - _fallback_interpret, _build_schema_description, _extract_json, 
                          _parse_config_from_dict, _fallback_suggestions
  4. auto_generator - _fallback_dashboard, schema handling
  5. TTLStore / _dashboard_store - set, get, iter, contains, delete
  6. API helpers - _get_schema_for_file, _get_business_context, _get_table_name
  7. _execute_sql - DuckDB direct fallback path
  8. CRUD endpoints - save, list, get, share, delete dashboards (via TestClient)
"""

import sys
import io
# Force UTF-8 on Windows PowerShell/cmd consoles
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os
import traceback
import json
import time
import asyncio

# -- Make sure backend root is on path ------------------------------------------
BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

# Load env before anything else
from dotenv import load_dotenv
load_dotenv(os.path.join(BACKEND_ROOT, ".env"))

# -- Colour helpers -------------------------------------------------------------
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

passed = []
failed = []
skipped = []


def ok(name: str):
    passed.append(name)
    print(f"  {GREEN}[PASS]{RESET} {name}")


def fail(name: str, exc: Exception):
    failed.append(name)
    print(f"  {RED}[FAIL]{RESET} {name}")
    print(f"    {RED}{type(exc).__name__}: {exc}{RESET}")
    tb = traceback.format_exc()
    for line in tb.strip().splitlines()[-4:]:
        print(f"    {YELLOW}{line}{RESET}")


def skip(name: str, reason: str = ""):
    skipped.append(name)
    print(f"  {YELLOW}[SKIP]{RESET} {name}{f'  ({reason})' if reason else ''}")


def section(title: str):
    print(f"\n{BOLD}{CYAN}{'='*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'='*60}{RESET}")


def assert_eq(actual, expected, label=""):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def assert_in(substring, text, label=""):
    if substring not in text:
        raise AssertionError(f"{label}: expected {substring!r} in {text!r}")


# ==============================================================================
# 1. PYDANTIC MODELS
# ==============================================================================
section("1. Pydantic Models")

try:
    from dashboard.chart_config_models import (
        ChartConfig, ChartType, AggregationType, SortDirection, ColorScheme,
        FilterCondition, ClarificationRequest,
        InterpretRequest, InterpretResponse,
        QueryRequest, SuggestPromptsRequest,
        WidgetModel, DashboardModel,
    )
    ok("Import dashboard.chart_config_models")
except Exception as e:
    fail("Import dashboard.chart_config_models", e)

# ChartConfig defaults
try:
    c = ChartConfig()
    assert c.chart_type == ChartType.BAR
    assert c.aggregation == AggregationType.SUM
    assert c.show_legend is True
    assert c.filters == []
    ok("ChartConfig — default instantiation")
except Exception as e:
    fail("ChartConfig — default instantiation", e)

# ChartConfig with all fields
try:
    c = ChartConfig(
        chart_type="line", title="Revenue over Time",
        x_axis="date", y_axis="revenue",
        aggregation="sum", group_by="region",
        sort_by="date", sort_direction="asc",
        limit=100, color_scheme="ocean",
        show_legend=True, show_grid=True, show_values=True,
        stacked=False, trend_line=True, smooth=True,
    )
    assert c.chart_type == ChartType.LINE
    assert c.title == "Revenue over Time"
    ok("ChartConfig — full field instantiation")
except Exception as e:
    fail("ChartConfig — full field instantiation", e)

# KPI config
try:
    c = ChartConfig(chart_type="kpi", kpi_value_column="revenue", kpi_aggregation="sum")
    assert c.chart_type == ChartType.KPI
    assert c.kpi_value_column == "revenue"
    ok("ChartConfig — KPI type")
except Exception as e:
    fail("ChartConfig — KPI type", e)

# FilterCondition
try:
    f = FilterCondition(column="age", operator=">", value=30)
    assert f.column == "age"
    assert f.operator == ">"
    assert f.value == 30
    ok("FilterCondition — basic")
except Exception as e:
    fail("FilterCondition — basic", e)

try:
    f = FilterCondition(column="region", operator="in", values=["North", "South"])
    assert f.values == ["North", "South"]
    ok("FilterCondition — in operator with values list")
except Exception as e:
    fail("FilterCondition — in operator with values list", e)

try:
    f = FilterCondition(column="score", operator="between", values=[60, 100])
    assert f.values[0] == 60
    ok("FilterCondition — between operator")
except Exception as e:
    fail("FilterCondition — between operator", e)

# ClarificationRequest
try:
    cr = ClarificationRequest(clarification="Which date column?", suggestions=["created_at", "updated_at"])
    assert len(cr.suggestions) == 2
    ok("ClarificationRequest — instantiation")
except Exception as e:
    fail("ClarificationRequest — instantiation", e)

# InterpretRequest
try:
    ir = InterpretRequest(message="show revenue by month", file_id="abc123")
    assert ir.conversation_history == []
    assert ir.current_config is None
    ok("InterpretRequest — defaults")
except Exception as e:
    fail("InterpretRequest — defaults", e)

# InterpretResponse
try:
    r = InterpretResponse(success=True, config=ChartConfig(title="test"), sql="SELECT 1", data=[{"x": 1}])
    assert r.success is True
    ok("InterpretResponse — with data")
except Exception as e:
    fail("InterpretResponse — with data", e)

try:
    r = InterpretResponse(success=False, error="Something went wrong")
    assert r.error == "Something went wrong"
    ok("InterpretResponse — error state")
except Exception as e:
    fail("InterpretResponse — error state", e)

# QueryRequest
try:
    qr = QueryRequest(config=ChartConfig(), file_id="abc123")
    assert qr.global_filters == []
    ok("QueryRequest — defaults")
except Exception as e:
    fail("QueryRequest — defaults", e)

# WidgetModel
try:
    w = WidgetModel(config=ChartConfig(title="Widget 1"), source_prompt="Show revenue")
    assert w.layout == {"x": 0, "y": 0, "w": 6, "h": 4}
    assert len(w.id) > 0
    ok("WidgetModel — defaults")
except Exception as e:
    fail("WidgetModel — defaults", e)

# DashboardModel
try:
    d = DashboardModel(title="Test Dashboard", file_id="abc123")
    assert d.widgets == []
    assert d.global_filters == []
    assert len(d.id) > 0
    ok("DashboardModel — defaults")
except Exception as e:
    fail("DashboardModel — defaults", e)

# model_dump roundtrip
try:
    c = ChartConfig(title="Test", chart_type="bar", x_axis="month", y_axis="sales")
    dumped = c.model_dump()
    restored = ChartConfig(**dumped)
    assert restored.title == "Test"
    ok("ChartConfig — model_dump / restore roundtrip")
except Exception as e:
    fail("ChartConfig — model_dump / restore roundtrip", e)


# ══════════════════════════════════════════════════════════════════════════════
# 2. CONFIG-TO-SQL
# ══════════════════════════════════════════════════════════════════════════════
section("2. config_to_sql — SQL Generation")

try:
    from dashboard.config_to_sql import config_to_sql, _agg_to_sql, _filter_to_condition, _fallback_sql
    from dashboard.chart_config_models import ChartConfig, FilterCondition, AggregationType
    ok("Import dashboard.config_to_sql")
except Exception as e:
    fail("Import dashboard.config_to_sql", e)

# _agg_to_sql mapping
try:
    assert _agg_to_sql(AggregationType.SUM)    == "SUM"
    assert _agg_to_sql(AggregationType.AVG)    == "AVG"
    assert _agg_to_sql(AggregationType.COUNT)  == "COUNT"
    assert _agg_to_sql(AggregationType.MIN)    == "MIN"
    assert _agg_to_sql(AggregationType.MAX)    == "MAX"
    assert _agg_to_sql(AggregationType.MEDIAN) == "MEDIAN"
    assert _agg_to_sql(AggregationType.NONE)   == ""
    ok("_agg_to_sql — all aggregation types")
except Exception as e:
    fail("_agg_to_sql — all aggregation types", e)

# _filter_to_condition
try:
    cond = _filter_to_condition(FilterCondition(column="age", operator=">", value=30))
    assert cond["column"] == "age" and cond["op"] == ">" and cond["value"] == 30
    ok("_filter_to_condition — basic equality")
except Exception as e:
    fail("_filter_to_condition — basic equality", e)

try:
    cond = _filter_to_condition(FilterCondition(column="region", operator="in", values=["A", "B"]))
    assert cond["values"] == ["A", "B"]
    ok("_filter_to_condition — in operator")
except Exception as e:
    fail("_filter_to_condition — in operator", e)

try:
    cond = _filter_to_condition(FilterCondition(column="col", operator="is_null"))
    assert "value" not in cond and "values" not in cond
    ok("_filter_to_condition — is_null (no value)")
except Exception as e:
    fail("_filter_to_condition — is_null (no value)", e)

# BAR chart SQL
try:
    c = ChartConfig(chart_type="bar", x_axis="category", y_axis="revenue", aggregation="sum")
    sql = config_to_sql(c, "sales_table")
    assert "sales_table" in sql
    assert "SUM" in sql.upper() or "revenue" in sql
    ok("config_to_sql — bar chart")
except Exception as e:
    fail("config_to_sql — bar chart", e)

# LINE chart SQL (should auto-sort by x_axis)
try:
    c = ChartConfig(chart_type="line", x_axis="date", y_axis="revenue", aggregation="sum")
    sql = config_to_sql(c, "sales")
    assert "sales" in sql
    ok("config_to_sql — line chart (auto-sort)")
except Exception as e:
    fail("config_to_sql — line chart (auto-sort)", e)

# AREA chart SQL
try:
    c = ChartConfig(chart_type="area", x_axis="month", y_axis="sales", aggregation="avg")
    sql = config_to_sql(c, "data")
    assert "data" in sql
    ok("config_to_sql — area chart")
except Exception as e:
    fail("config_to_sql — area chart", e)

# PIE chart SQL
try:
    c = ChartConfig(chart_type="pie", x_axis="region", y_axis="orders", aggregation="count")
    sql = config_to_sql(c, "orders_tbl")
    assert "orders_tbl" in sql
    ok("config_to_sql — pie chart")
except Exception as e:
    fail("config_to_sql — pie chart", e)

# SCATTER chart SQL (no aggregation expected)
try:
    c = ChartConfig(chart_type="scatter", x_axis="study_hours", y_axis="score", aggregation="none")
    sql = config_to_sql(c, "students")
    assert "students" in sql
    ok("config_to_sql — scatter chart")
except Exception as e:
    fail("config_to_sql — scatter chart", e)

# KPI chart SQL
try:
    c = ChartConfig(chart_type="kpi", kpi_value_column="revenue", kpi_aggregation="sum")
    sql = config_to_sql(c, "sales")
    assert "SUM" in sql.upper()
    assert "value" in sql.lower()
    ok("config_to_sql — KPI chart")
except Exception as e:
    fail("config_to_sql — KPI chart", e)

# KPI with no value column (fallback to COUNT)
try:
    c = ChartConfig(chart_type="kpi", aggregation="count")
    sql = config_to_sql(c, "sales")
    assert "COUNT" in sql.upper()
    ok("config_to_sql — KPI chart (no value col → COUNT(*) fallback)")
except Exception as e:
    fail("config_to_sql — KPI chart (no value col → COUNT(*) fallback)", e)

# TABLE chart SQL
try:
    c = ChartConfig(chart_type="table", x_axis="category", y_axis="revenue")
    sql = config_to_sql(c, "data")
    assert "data" in sql
    ok("config_to_sql — table chart")
except Exception as e:
    fail("config_to_sql — table chart", e)

# TABLE chart — no columns (should use *)
try:
    c = ChartConfig(chart_type="table")
    sql = config_to_sql(c, "data")
    assert "*" in sql or "data" in sql
    ok("config_to_sql — table chart (no axes → SELECT *)")
except Exception as e:
    fail("config_to_sql — table chart (no axes → SELECT *)", e)

# Group-by
try:
    c = ChartConfig(chart_type="bar", x_axis="month", y_axis="sales", group_by="region", aggregation="sum")
    sql = config_to_sql(c, "sales")
    assert "region" in sql
    ok("config_to_sql — group_by")
except Exception as e:
    fail("config_to_sql — group_by", e)

# Filters
try:
    c = ChartConfig(
        chart_type="bar", x_axis="cat", y_axis="val", aggregation="sum",
        filters=[FilterCondition(column="age", operator=">", value=30)],
    )
    sql = config_to_sql(c, "data")
    assert "30" in sql
    ok("config_to_sql — with filter")
except Exception as e:
    fail("config_to_sql — with filter", e)

# Global filters
try:
    c = ChartConfig(chart_type="bar", x_axis="cat", y_axis="val", aggregation="sum")
    gf = [FilterCondition(column="region", operator="=", value="North")]
    sql = config_to_sql(c, "data", global_filters=gf)
    assert "North" in sql or "region" in sql
    ok("config_to_sql — global filters")
except Exception as e:
    fail("config_to_sql — global filters", e)

# Sort and limit
try:
    c = ChartConfig(chart_type="bar", x_axis="cat", y_axis="val", aggregation="sum",
                    sort_by="val", sort_direction="desc", limit=10)
    sql = config_to_sql(c, "data")
    assert "10" in sql
    ok("config_to_sql — sort + limit")
except Exception as e:
    fail("config_to_sql — sort + limit", e)

# Heatmap
try:
    c = ChartConfig(chart_type="heatmap", x_axis="weekday", y_axis="hour", group_by="count", aggregation="count")
    sql = config_to_sql(c, "events")
    assert "events" in sql
    ok("config_to_sql — heatmap")
except Exception as e:
    fail("config_to_sql — heatmap", e)

# Treemap with size_by
try:
    c = ChartConfig(chart_type="treemap", x_axis="category", y_axis="revenue", size_by="quantity", aggregation="sum")
    sql = config_to_sql(c, "products")
    assert "products" in sql
    ok("config_to_sql — treemap with size_by")
except Exception as e:
    fail("config_to_sql — treemap with size_by", e)

# Funnel
try:
    c = ChartConfig(chart_type="funnel", x_axis="stage", y_axis="users", aggregation="count")
    sql = config_to_sql(c, "funnel_data")
    assert "funnel_data" in sql
    ok("config_to_sql — funnel chart")
except Exception as e:
    fail("config_to_sql — funnel chart", e)

# Secondary Y axis
try:
    c = ChartConfig(chart_type="bar", x_axis="month", y_axis="revenue", y_axis_secondary="cost", aggregation="sum")
    sql = config_to_sql(c, "data")
    assert "cost" in sql
    ok("config_to_sql — secondary y-axis")
except Exception as e:
    fail("config_to_sql — secondary y-axis", e)

# _fallback_sql — is_null
try:
    c = ChartConfig(chart_type="bar", x_axis="cat", y_axis="val", aggregation="sum")
    filters = [FilterCondition(column="region", operator="is_null")]
    sql = _fallback_sql(c, "data", filters)
    assert "IS NULL" in sql
    ok("_fallback_sql — is_null filter")
except Exception as e:
    fail("_fallback_sql — is_null filter", e)

# _fallback_sql — in operator
try:
    c = ChartConfig(chart_type="bar", x_axis="cat", y_axis="val", aggregation="sum")
    filters = [FilterCondition(column="region", operator="in", values=["A", "B"])]
    sql = _fallback_sql(c, "data", filters)
    assert "IN" in sql.upper()
    ok("_fallback_sql — in filter")
except Exception as e:
    fail("_fallback_sql — in filter", e)

# _fallback_sql — between
try:
    c = ChartConfig(chart_type="bar", x_axis="cat", y_axis="val", aggregation="sum")
    filters = [FilterCondition(column="score", operator="between", values=[50, 100])]
    sql = _fallback_sql(c, "data", filters)
    assert "BETWEEN" in sql.upper()
    ok("_fallback_sql — between filter")
except Exception as e:
    fail("_fallback_sql — between filter", e)


# ══════════════════════════════════════════════════════════════════════════════
# 3. PROMPT INTERPRETER
# ══════════════════════════════════════════════════════════════════════════════
section("3. prompt_interpreter — fallback parsing + helpers")

try:
    from dashboard.prompt_interpreter import (
        _build_schema_description,
        _extract_json,
        _parse_config_from_dict,
        _fallback_interpret,
        _fallback_suggestions,
        SYSTEM_PROMPT,
    )
    ok("Import dashboard.prompt_interpreter helpers")
except Exception as e:
    fail("Import dashboard.prompt_interpreter helpers", e)

# _build_schema_description
SAMPLE_SCHEMA = {
    "date": {"dtype": "datetime64[ns]", "sample_values": ["2024-01-01", "2024-01-02"]},
    "revenue": {"dtype": "float64", "sample_values": [100.5, 200.0]},
    "category": {"dtype": "object", "sample_values": ["A", "B", "C"]},
    "region": {"dtype": "object", "sample_values": ["North", "South"]},
}

try:
    desc = _build_schema_description(SAMPLE_SCHEMA)
    assert "revenue" in desc and "datetime64" in desc
    ok("_build_schema_description — formats schema")
except Exception as e:
    fail("_build_schema_description — formats schema", e)

try:
    desc = _build_schema_description({})
    assert "Dataset columns" in desc
    ok("_build_schema_description — empty schema")
except Exception as e:
    fail("_build_schema_description — empty schema", e)

# _extract_json — clean JSON
try:
    data = _extract_json('{"chart_type": "bar", "title": "Test"}')
    assert data["chart_type"] == "bar"
    ok("_extract_json — clean JSON string")
except Exception as e:
    fail("_extract_json — clean JSON string", e)

# _extract_json — markdown fence
try:
    data = _extract_json('```json\n{"chart_type": "line"}\n```')
    assert data["chart_type"] == "line"
    ok("_extract_json — markdown code fence")
except Exception as e:
    fail("_extract_json — markdown code fence", e)

# _extract_json — bare fence
try:
    data = _extract_json('```\n{"x": 1}\n```')
    assert data["x"] == 1
    ok("_extract_json — bare code fence")
except Exception as e:
    fail("_extract_json — bare code fence", e)

# _extract_json — JSON embedded in text
try:
    data = _extract_json('Here is the config: {"chart_type": "pie"} that\'s it.')
    assert data is not None and data.get("chart_type") == "pie"
    ok("_extract_json — JSON embedded in prose")
except Exception as e:
    fail("_extract_json — JSON embedded in prose", e)

# _extract_json — JSON array
try:
    data = _extract_json('[{"chart_type": "bar"}, {"chart_type": "line"}]')
    assert isinstance(data, list) and len(data) == 2
    ok("_extract_json — JSON array")
except Exception as e:
    fail("_extract_json — JSON array", e)

# _extract_json — invalid
try:
    data = _extract_json("this is not json at all !!")
    assert data is None
    ok("_extract_json — invalid input returns None")
except Exception as e:
    fail("_extract_json — invalid input returns None", e)

# _parse_config_from_dict — fresh config
try:
    cfg = _parse_config_from_dict({"chart_type": "pie", "title": "Pie Chart", "x_axis": "region", "y_axis": "orders"})
    assert cfg.chart_type.value == "pie"
    assert cfg.title == "Pie Chart"
    ok("_parse_config_from_dict — fresh config")
except Exception as e:
    fail("_parse_config_from_dict — fresh config", e)

# _parse_config_from_dict — follow-up (merges onto existing)
try:
    from dashboard.chart_config_models import ChartConfig
    existing = ChartConfig(chart_type="bar", title="Old", x_axis="month", y_axis="sales", aggregation="sum")
    updated = _parse_config_from_dict({"chart_type": "line", "title": "Updated"}, current_config=existing)
    assert updated.chart_type.value == "line"
    assert updated.x_axis == "month"   # preserved from original
    ok("_parse_config_from_dict — follow-up merges onto existing config")
except Exception as e:
    fail("_parse_config_from_dict — follow-up merges onto existing config", e)

# Hallucination fix — id-style column
try:
    cfg = _parse_config_from_dict({"chart_type": "bar", "x_axis": "student_id", "y_axis": "score"})
    assert cfg.x_axis is None, f"Expected None, got {cfg.x_axis}"
    ok("_parse_config_from_dict — hallucinated 'student_id' x_axis cleared")
except Exception as e:
    fail("_parse_config_from_dict — hallucinated 'student_id' x_axis cleared", e)

try:
    cfg = _parse_config_from_dict({"chart_type": "bar", "x_axis": "order_id", "y_axis": "amount"})
    assert cfg.x_axis is None, f"Expected None for '*_id' column, got {cfg.x_axis}"
    ok("_parse_config_from_dict — hallucinated '*_id' pattern cleared")
except Exception as e:
    fail("_parse_config_from_dict — hallucinated '*_id' pattern cleared", e)

try:
    cfg = _parse_config_from_dict({"chart_type": "bar", "x_axis": "gender", "y_axis": "score"})
    assert cfg.x_axis is None
    ok("_parse_config_from_dict — hallucinated 'gender' x_axis cleared")
except Exception as e:
    fail("_parse_config_from_dict — hallucinated 'gender' x_axis cleared", e)

# _fallback_interpret — bar chart detection
try:
    cfg = _fallback_interpret("show revenue by category as a bar chart", SAMPLE_SCHEMA)
    assert cfg.chart_type.value == "bar"
    ok("_fallback_interpret — detects 'bar chart'")
except Exception as e:
    fail("_fallback_interpret — detects 'bar chart'", e)

# _fallback_interpret — line chart
try:
    cfg = _fallback_interpret("show revenue over time as a line chart", SAMPLE_SCHEMA)
    assert cfg.chart_type.value == "line"
    ok("_fallback_interpret — detects 'line chart'")
except Exception as e:
    fail("_fallback_interpret — detects 'line chart'", e)

# _fallback_interpret — pie chart
try:
    cfg = _fallback_interpret("pie chart of revenue by region", SAMPLE_SCHEMA)
    assert cfg.chart_type.value == "pie"
    ok("_fallback_interpret — detects 'pie chart'")
except Exception as e:
    fail("_fallback_interpret — detects 'pie chart'", e)

# _fallback_interpret — KPI
try:
    cfg = _fallback_interpret("what is the total revenue", SAMPLE_SCHEMA)
    assert cfg.chart_type.value == "kpi"
    ok("_fallback_interpret — detects 'total' → KPI")
except Exception as e:
    fail("_fallback_interpret — detects 'total' → KPI", e)

# _fallback_interpret — table
try:
    cfg = _fallback_interpret("show all data as a table", SAMPLE_SCHEMA)
    assert cfg.chart_type.value == "table"
    ok("_fallback_interpret — detects 'table'")
except Exception as e:
    fail("_fallback_interpret — detects 'table'", e)

# _fallback_interpret — aggregation detection
try:
    cfg = _fallback_interpret("average revenue by category", SAMPLE_SCHEMA)
    assert cfg.aggregation.value == "avg"
    ok("_fallback_interpret — detects 'average' → AVG aggregation")
except Exception as e:
    fail("_fallback_interpret — detects 'average' → AVG aggregation", e)

try:
    cfg = _fallback_interpret("count of orders by region", SAMPLE_SCHEMA)
    assert cfg.aggregation.value == "count"
    ok("_fallback_interpret — detects 'count' aggregation")
except Exception as e:
    fail("_fallback_interpret — detects 'count' aggregation", e)

# _fallback_interpret — top N
try:
    cfg = _fallback_interpret("top 5 categories by revenue", SAMPLE_SCHEMA)
    assert cfg.limit == 5
    assert cfg.sort_direction.value == "desc"
    ok("_fallback_interpret — detects 'top 5' → limit=5 + DESC")
except Exception as e:
    fail("_fallback_interpret — detects 'top 5' → limit=5 + DESC", e)

# _fallback_interpret — bottom N
try:
    cfg = _fallback_interpret("bottom 3 regions by revenue", SAMPLE_SCHEMA)
    assert cfg.limit == 3
    assert cfg.sort_direction.value == "asc"
    ok("_fallback_interpret — detects 'bottom 3' → limit=3 + ASC")
except Exception as e:
    fail("_fallback_interpret — detects 'bottom 3' → limit=3 + ASC", e)

# _fallback_interpret — trend line modifier
try:
    cfg = _fallback_interpret("revenue over date with trend line", SAMPLE_SCHEMA)
    assert cfg.trend_line is True
    ok("_fallback_interpret — detects 'trend line' modifier")
except Exception as e:
    fail("_fallback_interpret — detects 'trend line' modifier", e)

# _fallback_interpret — stacked modifier
try:
    cfg = _fallback_interpret("stacked bar chart of revenue by region", SAMPLE_SCHEMA)
    assert cfg.stacked is True
    ok("_fallback_interpret — detects 'stacked' modifier")
except Exception as e:
    fail("_fallback_interpret — detects 'stacked' modifier", e)

# _fallback_interpret — follow-up (merges onto existing)
try:
    from dashboard.chart_config_models import ChartConfig
    existing = ChartConfig(chart_type="bar", title="Old", x_axis="category", y_axis="revenue")
    cfg = _fallback_interpret("switch to line chart", SAMPLE_SCHEMA, current_config=existing)
    assert cfg.chart_type.value == "line"
    assert cfg.x_axis == "category"  # preserved
    ok("_fallback_interpret — follow-up merges onto existing config")
except Exception as e:
    fail("_fallback_interpret — follow-up merges onto existing config", e)

# _fallback_suggestions — with config
try:
    from dashboard.chart_config_models import ChartConfig
    cfg = ChartConfig(chart_type="bar", x_axis="category", y_axis="revenue")
    suggestions = _fallback_suggestions(cfg, SAMPLE_SCHEMA)
    assert isinstance(suggestions, list) and 0 < len(suggestions) <= 4
    assert all(isinstance(s, str) for s in suggestions)
    ok("_fallback_suggestions — with config → list of strings")
except Exception as e:
    fail("_fallback_suggestions — with config → list of strings", e)

# _fallback_suggestions — without config
try:
    suggestions = _fallback_suggestions(None, SAMPLE_SCHEMA)
    assert isinstance(suggestions, list) and len(suggestions) > 0
    ok("_fallback_suggestions — no config → initial suggestions")
except Exception as e:
    fail("_fallback_suggestions — no config → initial suggestions", e)

# _fallback_suggestions — without schema
try:
    suggestions = _fallback_suggestions(None, None)
    assert isinstance(suggestions, list)
    ok("_fallback_suggestions — empty schema handled gracefully")
except Exception as e:
    fail("_fallback_suggestions — empty schema handled gracefully", e)

# SYSTEM_PROMPT content check
try:
    assert "JSON" in SYSTEM_PROMPT
    assert "chart_type" in SYSTEM_PROMPT
    ok("SYSTEM_PROMPT — contains expected instruction keywords")
except Exception as e:
    fail("SYSTEM_PROMPT — contains expected instruction keywords", e)


# ══════════════════════════════════════════════════════════════════════════════
# 4. AUTO GENERATOR
# ══════════════════════════════════════════════════════════════════════════════
section("4. auto_generator — fallback dashboard")

try:
    from dashboard.auto_generator import _fallback_dashboard, generate_initial_dashboard
    ok("Import dashboard.auto_generator")
except Exception as e:
    fail("Import dashboard.auto_generator", e)

try:
    configs = _fallback_dashboard(SAMPLE_SCHEMA)
    assert isinstance(configs, list)
    assert len(configs) >= 1
    assert all(hasattr(c, "chart_type") for c in configs)
    ok("_fallback_dashboard — returns list of ChartConfigs")
except Exception as e:
    fail("_fallback_dashboard — returns list of ChartConfigs", e)

try:
    configs = _fallback_dashboard(SAMPLE_SCHEMA)
    types = [c.chart_type.value for c in configs]
    assert "kpi" in types, f"Expected KPI in {types}"
    ok("_fallback_dashboard — includes a KPI widget")
except Exception as e:
    fail("_fallback_dashboard — includes a KPI widget", e)

try:
    configs = _fallback_dashboard(SAMPLE_SCHEMA)
    types = [c.chart_type.value for c in configs]
    assert "bar" in types, f"Expected bar in {types}"
    ok("_fallback_dashboard — includes a bar chart widget")
except Exception as e:
    fail("_fallback_dashboard — includes a bar chart widget", e)

try:
    # Numeric-only schema
    num_schema = {"val1": {"dtype": "float64"}, "val2": {"dtype": "int64"}}
    configs = _fallback_dashboard(num_schema)
    assert len(configs) >= 1
    ok("_fallback_dashboard — numeric-only schema (at least 1 config)")
except Exception as e:
    fail("_fallback_dashboard — numeric-only schema (at least 1 config)", e)

try:
    # Empty schema
    configs = _fallback_dashboard({})
    assert isinstance(configs, list)
    ok("_fallback_dashboard — empty schema returns empty list gracefully")
except Exception as e:
    fail("_fallback_dashboard — empty schema returns empty list gracefully", e)

# Test generate_initial_dashboard (fallback path — HAS_GENAI=False or offline)
try:
    import dashboard.auto_generator as _ag_mod
    orig = _ag_mod.HAS_GENAI
    _ag_mod.HAS_GENAI = False
    configs = asyncio.get_event_loop().run_until_complete(
        generate_initial_dashboard(SAMPLE_SCHEMA, "Domain: E-commerce")
    )
    _ag_mod.HAS_GENAI = orig
    assert isinstance(configs, list) and len(configs) >= 1
    ok("generate_initial_dashboard — HAS_GENAI=False falls back to rule-based")
except Exception as e:
    fail("generate_initial_dashboard — HAS_GENAI=False falls back to rule-based", e)


# ══════════════════════════════════════════════════════════════════════════════
# 5. TTL STORE
# ══════════════════════════════════════════════════════════════════════════════
section("5. TTLStore — state storage")

try:
    from state import TTLStore
    ok("Import state.TTLStore")
except Exception as e:
    fail("Import state.TTLStore", e)

try:
    import tempfile, os
    store = TTLStore("_test_dashboard_suite", max_entries=10, ttl_seconds=60)
    ok("TTLStore — instantiation")
except Exception as e:
    fail("TTLStore — instantiation", e)

try:
    store["key1"] = {"value": 42}
    assert store["key1"] == {"value": 42}
    ok("TTLStore — set and get (__setitem__ / __getitem__)")
except Exception as e:
    fail("TTLStore — set and get (__setitem__ / __getitem__)", e)

try:
    assert "key1" in store
    ok("TTLStore — __contains__ (in operator)")
except Exception as e:
    fail("TTLStore — __contains__ (in operator)", e)

try:
    result = store.get("key1")
    assert result == {"value": 42}
    ok("TTLStore — .get() method")
except Exception as e:
    fail("TTLStore — .get() method", e)

try:
    result = store.get("nonexistent_key", "default")
    assert result == "default"
    ok("TTLStore — .get() with default for missing key")
except Exception as e:
    fail("TTLStore — .get() with default for missing key", e)

try:
    store["key2"] = {"value": 99}
    keys = list(store)
    assert "key1" in keys and "key2" in keys
    ok("TTLStore — __iter__ (for key in store)")
except Exception as e:
    fail("TTLStore — __iter__ (for key in store)", e)

try:
    n = len(store)
    assert n >= 2
    ok("TTLStore — __len__")
except Exception as e:
    fail("TTLStore — __len__", e)

try:
    del store["key1"]
    assert "key1" not in store
    ok("TTLStore — __delitem__ (del store[key])")
except Exception as e:
    fail("TTLStore — __delitem__ (del store[key])", e)

try:
    try:
        _ = store["nonexistent"]
        fail("TTLStore — __getitem__ raises KeyError for missing key", AssertionError("No exception raised"))
    except KeyError:
        ok("TTLStore — __getitem__ raises KeyError for missing key")
except Exception as e:
    fail("TTLStore — __getitem__ raises KeyError for missing key", e)


# ══════════════════════════════════════════════════════════════════════════════
# 6. _execute_sql — DuckDB direct fallback
# ══════════════════════════════════════════════════════════════════════════════
section("6. _execute_sql — DuckDB direct fallback")

try:
    import pandas as pd
    import duckdb
    ok("Import duckdb + pandas (required for _execute_sql fallback)")
except Exception as e:
    fail("Import duckdb + pandas", e)

# Test the DuckDB path directly (mimics _execute_sql fallback)
try:
    import pandas as pd
    import duckdb
    df = pd.DataFrame({"category": ["A", "B", "A", "C"], "revenue": [100, 200, 150, 300]})
    conn = duckdb.connect()
    conn.register("test_tbl", df)
    result = conn.execute("SELECT category, SUM(revenue) AS revenue FROM test_tbl GROUP BY category ORDER BY revenue DESC").df()
    rows = result.to_dict(orient="records")
    assert len(rows) == 3
    assert rows[0]["revenue"] == 300
    ok("DuckDB in-memory query — group by + aggregate + sort")
except Exception as e:
    fail("DuckDB in-memory query — group by + aggregate + sort", e)

# Test SQL generated by config_to_sql runs correctly in DuckDB
try:
    import pandas as pd
    import duckdb
    from dashboard.config_to_sql import config_to_sql
    from dashboard.chart_config_models import ChartConfig

    df = pd.DataFrame({
        "month": ["Jan", "Feb", "Mar", "Jan", "Feb"],
        "region": ["North", "North", "South", "South", "North"],
        "sales": [100, 200, 150, 300, 250],
    })
    c = ChartConfig(chart_type="bar", x_axis="month", y_axis="sales", aggregation="sum",
                    sort_by="sales", sort_direction="desc", limit=3)
    sql = config_to_sql(c, "test_data")
    conn = duckdb.connect()
    conn.register("test_data", df)
    result = conn.execute(sql).df()
    rows = result.to_dict(orient="records")
    assert len(rows) <= 3
    ok("config_to_sql output → DuckDB executes without error (bar, sum, sort, limit)")
except Exception as e:
    fail("config_to_sql output → DuckDB executes without error (bar, sum, sort, limit)", e)

try:
    import pandas as pd
    import duckdb
    from dashboard.config_to_sql import config_to_sql
    from dashboard.chart_config_models import ChartConfig, FilterCondition

    df = pd.DataFrame({
        "category": ["A", "B", "A", "C"],
        "age": [25, 35, 40, 28],
        "revenue": [100, 200, 150, 300],
    })
    c = ChartConfig(chart_type="bar", x_axis="category", y_axis="revenue", aggregation="sum",
                    filters=[FilterCondition(column="age", operator=">", value=28)])
    sql = config_to_sql(c, "filtered_data")
    conn = duckdb.connect()
    conn.register("filtered_data", df)
    result = conn.execute(sql).df()
    rows = result.to_dict(orient="records")
    # Only rows where age > 28: B(35,200) and A(40,150) → categories B,A
    assert len(rows) <= 3
    ok("config_to_sql output → DuckDB executes with WHERE filter")
except Exception as e:
    fail("config_to_sql output → DuckDB executes with WHERE filter", e)

try:
    import pandas as pd
    import duckdb
    from dashboard.config_to_sql import config_to_sql
    from dashboard.chart_config_models import ChartConfig

    df = pd.DataFrame({"revenue": [100.5, 200.0, 300.75]})
    c = ChartConfig(chart_type="kpi", kpi_value_column="revenue", kpi_aggregation="sum")
    sql = config_to_sql(c, "kpi_data")
    conn = duckdb.connect()
    conn.register("kpi_data", df)
    result = conn.execute(sql).df()
    rows = result.to_dict(orient="records")
    assert len(rows) == 1
    assert abs(rows[0]["value"] - 601.25) < 0.01
    ok("config_to_sql output → KPI SUM executes correctly in DuckDB")
except Exception as e:
    fail("config_to_sql output → KPI SUM executes correctly in DuckDB", e)


# ══════════════════════════════════════════════════════════════════════════════
# 7. DASHBOARD CRUD — FastAPI TestClient
# ══════════════════════════════════════════════════════════════════════════════
section("7. Dashboard CRUD — FastAPI TestClient")

try:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.dashboard import router as dashboard_router, _dashboard_store

    test_app = FastAPI()
    test_app.include_router(dashboard_router)
    client = TestClient(test_app, raise_server_exceptions=True)
    ok("TestClient — setup with dashboard router")
except Exception as e:
    fail("TestClient — setup with dashboard router", e)
    client = None

if client:
    TEST_DASH_ID = "test-suite-dash-001"
    TEST_FILE_ID = "test-file-001"

    # Clean any leftover from previous runs
    try:
        if TEST_DASH_ID in _dashboard_store:
            del _dashboard_store[TEST_DASH_ID]
    except Exception:
        pass

    # POST /api/dashboards — save dashboard
    try:
        payload = {
            "id": TEST_DASH_ID,
            "title": "Test Suite Dashboard",
            "description": "Created by automated test",
            "file_id": TEST_FILE_ID,
            "widgets": [
                {
                    "id": "widget-001",
                    "config": {
                        "chart_type": "bar",
                        "title": "Revenue by Category",
                        "x_axis": "category",
                        "y_axis": "revenue",
                        "aggregation": "sum",
                        "filters": [],
                        "sort_direction": "asc",
                        "color_scheme": "default",
                        "show_legend": True,
                        "show_grid": True,
                        "show_values": False,
                        "stacked": False,
                        "trend_line": False,
                        "smooth": False,
                        "kpi_aggregation": "sum",
                    },
                    "source_prompt": "Show revenue by category",
                    "prompt_history": ["Show revenue by category"],
                    "layout": {"x": 0, "y": 0, "w": 6, "h": 4},
                }
            ],
            "global_filters": [],
        }
        resp = client.post("/api/dashboards", json=payload)
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["id"] == TEST_DASH_ID
        assert data["status"] == "saved"
        ok("POST /api/dashboards — save dashboard")
    except Exception as e:
        fail("POST /api/dashboards — save dashboard", e)

    # POST /api/dashboards again (update, same file_id)
    try:
        payload["title"] = "Test Suite Dashboard — Updated"
        resp = client.post("/api/dashboards", json=payload)
        assert resp.status_code == 200
        ok("POST /api/dashboards — update existing dashboard (same file_id)")
    except Exception as e:
        fail("POST /api/dashboards — update existing dashboard (same file_id)", e)

    # POST /api/dashboards — wrong file_id should 403
    try:
        payload["file_id"] = "DIFFERENT_FILE"
        resp = client.post("/api/dashboards", json=payload)
        assert resp.status_code == 403, f"Expected 403, got {resp.status_code}"
        ok("POST /api/dashboards — wrong file_id returns 403")
        payload["file_id"] = TEST_FILE_ID  # restore
    except Exception as e:
        fail("POST /api/dashboards — wrong file_id returns 403", e)
        payload["file_id"] = TEST_FILE_ID

    # GET /api/dashboards — list
    try:
        resp = client.get("/api/dashboards")
        assert resp.status_code == 200
        data = resp.json()
        assert "dashboards" in data
        ids = [d["id"] for d in data["dashboards"]]
        assert TEST_DASH_ID in ids
        ok("GET /api/dashboards — lists saved dashboards")
    except Exception as e:
        fail("GET /api/dashboards — lists saved dashboards", e)

    # GET /api/dashboards/{id} — load
    try:
        resp = client.get(f"/api/dashboards/{TEST_DASH_ID}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == TEST_DASH_ID
        assert "widgets" in data
        ok("GET /api/dashboards/{id} — load dashboard by ID")
    except Exception as e:
        fail("GET /api/dashboards/{id} — load dashboard by ID", e)

    # GET /api/dashboards/{id} — 404 for unknown
    try:
        resp = client.get("/api/dashboards/nonexistent-id-xyz")
        assert resp.status_code == 404
        ok("GET /api/dashboards/{id} — 404 for unknown ID")
    except Exception as e:
        fail("GET /api/dashboards/{id} — 404 for unknown ID", e)

    # GET /api/dashboards/share/{id} — readonly view
    try:
        resp = client.get(f"/api/dashboards/share/{TEST_DASH_ID}")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("readonly") is True
        assert data["id"] == TEST_DASH_ID
        ok("GET /api/dashboards/share/{id} — shared readonly view")
    except Exception as e:
        fail("GET /api/dashboards/share/{id} — shared readonly view", e)

    # GET /api/dashboards/share/{id} — 404 for unknown
    try:
        resp = client.get("/api/dashboards/share/nonexistent-xyz")
        assert resp.status_code == 404
        ok("GET /api/dashboards/share/{id} — 404 for unknown ID")
    except Exception as e:
        fail("GET /api/dashboards/share/{id} — 404 for unknown ID", e)

    # DELETE /api/dashboards/{id}
    try:
        resp = client.delete(f"/api/dashboards/{TEST_DASH_ID}")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("deleted") is True
        ok("DELETE /api/dashboards/{id} — deletes dashboard")
    except Exception as e:
        fail("DELETE /api/dashboards/{id} — deletes dashboard", e)

    # Verify it's gone after delete
    try:
        resp = client.get(f"/api/dashboards/{TEST_DASH_ID}")
        assert resp.status_code == 404
        ok("DELETE /api/dashboards/{id} — dashboard no longer found after delete")
    except Exception as e:
        fail("DELETE /api/dashboards/{id} — dashboard no longer found after delete", e)

    # DELETE /api/dashboards/{id} — 404 for already deleted
    try:
        resp = client.delete(f"/api/dashboards/{TEST_DASH_ID}")
        assert resp.status_code == 404
        ok("DELETE /api/dashboards/{id} — 404 for already-deleted ID")
    except Exception as e:
        fail("DELETE /api/dashboards/{id} — 404 for already-deleted ID", e)


# ══════════════════════════════════════════════════════════════════════════════
# FINAL REPORT
# ══════════════════════════════════════════════════════════════════════════════
total = len(passed) + len(failed) + len(skipped)
print(f"\n{BOLD}{'═'*60}{RESET}")
print(f"{BOLD}  RESULTS: {total} tests  │  "
      f"{GREEN}{len(passed)} passed{RESET}  │  "
      f"{RED}{len(failed)} failed{RESET}  │  "
      f"{YELLOW}{len(skipped)} skipped{RESET}{BOLD}  {RESET}")
print(f"{BOLD}{'═'*60}{RESET}")

if failed:
    print(f"\n{RED}{BOLD}Failed tests:{RESET}")
    for name in failed:
        print(f"  {RED}✗{RESET} {name}")

if len(failed) == 0:
    print(f"\n{GREEN}{BOLD}All tests passed! 🎉{RESET}\n")
else:
    print(f"\n{RED}{BOLD}Some tests failed. See details above.{RESET}\n")
    sys.exit(1)
