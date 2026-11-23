"""
tests/test_temporal_grain_sql.py
================================
Verifies that charts with temporal grain titles produce correctly aggregated SQL.
"""

import os
import sys
import duckdb
import pandas as pd
import pytest

from dashboard.chart_config_models import ChartConfig, ChartType, AggregationType, TimeGrain
from dashboard.config_to_sql import config_to_sql, _time_grain_sql_expr


# ─────────────────────────────────────────────────────────────────────────────
# 1. Unit tests for _time_grain_sql_expr
# ─────────────────────────────────────────────────────────────────────────────

def test_time_grain_month_expression():
    expr, alias = _time_grain_sql_expr("date", TimeGrain.MONTH)
    assert "%Y-%m" in expr
    assert "DATE_TRUNC" in expr
    assert alias == "date"


def test_time_grain_year_expression():
    expr, alias = _time_grain_sql_expr("date", TimeGrain.YEAR)
    assert "%Y" in expr
    assert alias == "date"


def test_time_grain_quarter_expression():
    expr, alias = _time_grain_sql_expr("date", TimeGrain.QUARTER)
    assert "QUARTER" in expr
    assert alias == "date"


def test_time_grain_day_expression():
    expr, alias = _time_grain_sql_expr("date", TimeGrain.DAY)
    assert "CAST" in expr
    assert alias == "date"


def test_time_grain_week_expression():
    expr, alias = _time_grain_sql_expr("date", TimeGrain.WEEK)
    assert "W" in expr
    assert "DATE_TRUNC" in expr
    assert alias == "date"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Integration: config_to_sql with time grain produces correct SQL
# ─────────────────────────────────────────────────────────────────────────────

def test_monthly_revenue_config_produces_month_aggregation():
    """A 'Monthly Revenue Trend' config with x_axis_time_grain='month' must GROUP BY YYYY-MM."""
    config = ChartConfig(
        chart_type=ChartType.LINE,
        title="Monthly Revenue Trend",
        x_axis="date",
        y_axis="revenue",
        aggregation=AggregationType.SUM,
        x_axis_time_grain=TimeGrain.MONTH,
    )

    sql = config_to_sql(config, "test_data")
    sql_upper = sql.upper()

    # Must contain DATE_TRUNC for month bucketing
    assert "DATE_TRUNC" in sql_upper or "STRFTIME" in sql_upper
    # Must contain SUM(revenue)
    assert "SUM" in sql_upper
    assert "REVENUE" in sql_upper
    # Must GROUP BY
    assert "GROUP BY" in sql_upper


def test_yearly_cost_config():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        title="Yearly Cost Overview",
        x_axis="date",
        y_axis="cost",
        aggregation=AggregationType.SUM,
        x_axis_time_grain=TimeGrain.YEAR,
    )

    sql = config_to_sql(config, "test_data")
    assert "%Y" in sql or "YEAR" in sql.upper()
    assert "SUM" in sql.upper()


def test_no_grain_config_uses_raw_column():
    """A config without x_axis_time_grain should use the raw column."""
    config = ChartConfig(
        chart_type=ChartType.LINE,
        title="Revenue by Date",
        x_axis="date",
        y_axis="revenue",
        aggregation=AggregationType.SUM,
    )

    sql = config_to_sql(config, "test_data")
    # Should NOT contain DATE_TRUNC (no grain set)
    assert "DATE_TRUNC" not in sql


# ─────────────────────────────────────────────────────────────────────────────
# 3. End-to-end: monthly SQL actually works with DuckDB
# ─────────────────────────────────────────────────────────────────────────────

def test_monthly_sql_executes_on_dashboard_data():
    """Execute monthly aggregation SQL on dashboard_test_data.csv via DuckDB."""
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")
    if not os.path.exists(csv_path):
        pytest.skip("dashboard_test_data.csv not found")

    df = pd.read_csv(csv_path)
    con = duckdb.connect()
    con.register("upload_test", df)

    config = ChartConfig(
        chart_type=ChartType.LINE,
        title="Monthly Revenue Trend",
        x_axis="date",
        y_axis="revenue",
        aggregation=AggregationType.SUM,
        x_axis_time_grain=TimeGrain.MONTH,
    )

    sql = config_to_sql(config, "upload_test")
    result = con.execute(sql).df()

    # dashboard_test_data.csv spans 2023-01 to 2024-06 = 18 months
    assert len(result) == 18, f"Expected 18 months, got {len(result)}"

    # Check that the date column has YYYY-MM format
    first_date = str(result.iloc[0]["date"])
    assert len(first_date) == 7, f"Expected YYYY-MM format, got '{first_date}'"
    assert "-" in first_date

    # Revenue should be aggregated (sum) for each month
    assert result["revenue"].sum() > 0


def test_quarterly_sql_executes_on_dashboard_data():
    """Execute quarterly aggregation SQL on dashboard_test_data.csv via DuckDB."""
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")
    if not os.path.exists(csv_path):
        pytest.skip("dashboard_test_data.csv not found")

    df = pd.read_csv(csv_path)
    con = duckdb.connect()
    con.register("upload_test", df)

    config = ChartConfig(
        chart_type=ChartType.BAR,
        title="Quarterly Revenue",
        x_axis="date",
        y_axis="revenue",
        aggregation=AggregationType.SUM,
        x_axis_time_grain=TimeGrain.QUARTER,
    )

    sql = config_to_sql(config, "upload_test")
    result = con.execute(sql).df()

    # 2023 Q1-Q4 + 2024 Q1-Q2 = 6 quarters
    assert len(result) == 6, f"Expected 6 quarters, got {len(result)}"

    # Check Q format
    first_q = str(result.iloc[0]["date"])
    assert "Q" in first_q, f"Expected quarter format with Q, got '{first_q}'"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Temporal grain consistency enforcement
# ─────────────────────────────────────────────────────────────────────────────

def test_enforce_temporal_grain_on_title_mismatch():
    """If title says 'Monthly' but grain is None, enforcement should set grain to MONTH."""
    from dashboard.prompt_interpreter import _enforce_temporal_grain_consistency

    config = ChartConfig(
        chart_type=ChartType.LINE,
        title="Monthly Revenue Trend",
        x_axis="date",
        y_axis="revenue",
        aggregation=AggregationType.SUM,
        # No x_axis_time_grain set — this is the bug
    )

    fixed = _enforce_temporal_grain_consistency(config)
    assert fixed.x_axis_time_grain == TimeGrain.MONTH


def test_enforce_temporal_grain_yearly():
    from dashboard.prompt_interpreter import _enforce_temporal_grain_consistency

    config = ChartConfig(
        chart_type=ChartType.BAR,
        title="Annual Revenue Summary",
        x_axis="date",
        y_axis="revenue",
        aggregation=AggregationType.SUM,
    )

    fixed = _enforce_temporal_grain_consistency(config)
    assert fixed.x_axis_time_grain == TimeGrain.YEAR


def test_no_enforcement_needed_for_non_temporal_title():
    from dashboard.prompt_interpreter import _enforce_temporal_grain_consistency

    config = ChartConfig(
        chart_type=ChartType.BAR,
        title="Revenue by Category",
        x_axis="category",
        y_axis="revenue",
        aggregation=AggregationType.SUM,
    )

    fixed = _enforce_temporal_grain_consistency(config)
    assert fixed.x_axis_time_grain is None


def test_grain_detection_from_prompt_text():
    from dashboard.prompt_interpreter import _detect_grain_from_text

    assert _detect_grain_from_text("Show monthly revenue") == TimeGrain.MONTH
    assert _detect_grain_from_text("weekly sales breakdown") == TimeGrain.WEEK
    assert _detect_grain_from_text("daily transaction count") == TimeGrain.DAY
    assert _detect_grain_from_text("quarterly cost analysis") == TimeGrain.QUARTER
    assert _detect_grain_from_text("year-over-year growth") == TimeGrain.YEAR
    assert _detect_grain_from_text("revenue by category") is None
