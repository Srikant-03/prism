"""
Unit and integration tests for Dashboard Semantic Model, Measure Engine, and Multi-Metric SQL Compiler.
Verifies Step 1 Power BI backend capabilities on prism_adversarial_e2e_test.csv.
"""

import os
import pytest
import duckdb
import pandas as pd

from dashboard.chart_config_models import (
    ChartConfig,
    ChartType,
    AggregationType,
    TimeGrain,
    FilterCondition,
    MetricSpec,
    DashboardModel,
    WidgetModel,
    MeasureDefinition,
)
from dashboard.config_to_sql import config_to_sql
from dashboard.measure_engine import MeasureEngine
from dashboard.semantic_model import build_semantic_model

DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "prism_adversarial_e2e_test.csv")


@pytest.fixture(scope="module")
def duck_conn():
    assert os.path.exists(DATASET_PATH), f"Dataset not found at {DATASET_PATH}"
    df = pd.read_csv(DATASET_PATH)
    conn = duckdb.connect(":memory:")
    conn.register("test_table", df)
    return conn, df


def test_monthly_trend_exact_ground_truth(duck_conn):
    """Monthly trend on order_date returns 18 buckets: 2024-01 -> 347358.56, 2025-04 -> 884634.68."""
    conn, df = duck_conn
    config = ChartConfig(
        chart_type=ChartType.LINE,
        x_axis="order_date",
        x_axis_time_grain=TimeGrain.MONTH,
        y_axis="net_revenue",
        aggregation=AggregationType.SUM,
    )
    sql = config_to_sql(config, "test_table")
    rows = conn.execute(sql).fetchall()

    assert len(rows) == 18, f"Expected 18 monthly buckets, got {len(rows)}"
    first_bucket, first_sum = rows[0]
    assert first_bucket == "2024-01"
    assert round(first_sum, 2) == 347358.56

    apr_row = [r for r in rows if r[0] == "2025-04"]
    assert len(apr_row) == 1
    assert round(apr_row[0][1], 2) == 884634.68


def test_quarterly_rollup_exact_ground_truth(duck_conn):
    """Quarterly rollup returns 6 buckets: 2024-Q1 -> 1199720.72 .. 2025-Q2 -> 1499384.03."""
    conn, df = duck_conn
    config = ChartConfig(
        chart_type=ChartType.COLUMN,
        x_axis="order_date",
        x_axis_time_grain=TimeGrain.QUARTER,
        y_axis="net_revenue",
        aggregation=AggregationType.SUM,
    )
    sql = config_to_sql(config, "test_table")
    rows = conn.execute(sql).fetchall()

    assert len(rows) == 6, f"Expected 6 quarterly buckets, got {len(rows)}"
    assert rows[0][0] == "2024-Q1"
    assert round(rows[0][1], 2) == 1199720.72

    assert rows[-1][0] == "2025-Q2"
    assert round(rows[-1][1], 2) == 1499384.03


def test_multi_metric_kpi_with_filter_exact_ground_truth(duck_conn):
    """Multi-metric filtered KPI on churn_flag = 1 returns 717251.37 and 0.1167."""
    conn, df = duck_conn
    config = ChartConfig(
        chart_type=ChartType.MULTI_KPI,
        metrics=[
            MetricSpec(column="net_revenue", aggregation=AggregationType.SUM, label="Total Net Revenue"),
            MetricSpec(column="discount_rate", aggregation=AggregationType.AVG, label="Avg Discount Rate"),
        ],
        filters=[
            FilterCondition(column="churn_flag", operator="=", value=1),
        ],
    )
    sql = config_to_sql(config, "test_table")
    row = conn.execute(sql).fetchone()

    assert row is not None
    total_net, avg_discount = row
    assert round(total_net, 2) == 717251.37
    assert round(avg_discount, 4) == 0.1167


def test_multi_metric_grouped_bar_exact_ground_truth(duck_conn):
    """Multi-metric grouped bar across product_category returns Enterprise Software: 3211284.88 & 2808625.61."""
    conn, df = duck_conn
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_axis="product_category",
        metrics=[
            MetricSpec(column="gross_revenue", aggregation=AggregationType.SUM, label="Gross Revenue"),
            MetricSpec(column="net_revenue", aggregation=AggregationType.SUM, label="Net Revenue"),
        ],
    )
    sql = config_to_sql(config, "test_table")
    rows = conn.execute(sql).fetchall()

    cat_map = {r[0]: (round(r[1], 2), round(r[2], 2)) for r in rows if r[0] is not None and r[1] is not None and r[2] is not None}
    assert "Enterprise Software" in cat_map
    assert cat_map["Enterprise Software"] == (3211284.88, 2808625.61)


def test_measure_engine_divide_and_calculate(duck_conn):
    """DIVIDE safe zero-division and CALCULATE filter overrides return mathematically exact outputs."""
    conn, df = duck_conn

    # 1. DIVIDE expression
    divide_expr = MeasureEngine.compile_measure('DIVIDE(SUM(net_revenue), SUM(gross_revenue), 0)')
    sql = f'SELECT ROUND({divide_expr}, 4) FROM "test_table"'
    val = conn.execute(sql).fetchone()[0]

    # Total net / total gross
    tot_net = df["net_revenue"].sum()
    tot_gross = df["gross_revenue"].sum()
    expected_ratio = round(tot_net / tot_gross, 4)
    assert val == expected_ratio

    # 2. CALCULATE expression
    calc_expr = MeasureEngine.compile_measure('CALCULATE(SUM(net_revenue), churn_flag = 1)')
    sql = f'SELECT ROUND({calc_expr}, 2) FROM "test_table"'
    calc_val = conn.execute(sql).fetchone()[0]
    assert calc_val == 717251.37


def test_measure_engine_window_measures(duck_conn):
    """SHAREOFTOTAL, RANK, and YTD return valid analytical results in DuckDB."""
    conn, df = duck_conn

    # 1. Share of total across categories
    share_expr = MeasureEngine.compile_measure('SHAREOFTOTAL(net_revenue)')
    sql = f'SELECT "product_category", ROUND({share_expr}, 4) FROM "test_table" WHERE "product_category" IS NOT NULL GROUP BY 1'
    rows = conn.execute(sql).fetchall()
    total_share = sum(r[1] for r in rows if r[1] is not None)
    assert abs(total_share - 1.0) < 0.01

    # 2. Rank
    rank_expr = MeasureEngine.compile_measure('RANK(net_revenue, DESC)')
    sql = f'SELECT "product_category", {rank_expr} FROM "test_table" GROUP BY 1 ORDER BY 2'
    ranked = conn.execute(sql).fetchall()
    assert ranked[0][1] == 1  # Top ranked is 1

    # 3. YTD
    ytd_expr = MeasureEngine.compile_measure('YTD(SUM(net_revenue), order_date)')
    sql = f'''
        SELECT "order_date", {ytd_expr} as ytd
        FROM "test_table"
        WHERE "order_date" IS NOT NULL
        ORDER BY "order_date"
        LIMIT 5
    '''
    ytd_rows = conn.execute(sql).fetchall()
    assert len(ytd_rows) == 5
    # YTD must be monotonically non-decreasing for positive revenue
    assert ytd_rows[1][1] >= ytd_rows[0][1]


def test_row_level_security_injection(duck_conn):
    """RLS predicate injection restricts query outputs without syntax errors."""
    conn, df = duck_conn
    config = ChartConfig(
        chart_type=ChartType.KPI,
        y_axis="net_revenue",
        aggregation=AggregationType.SUM,
    )
    # Without RLS
    sql_open = config_to_sql(config, "test_table")
    total_val = conn.execute(sql_open).fetchone()[0]

    # With RLS: country_code = 'US'
    sql_rls = config_to_sql(config, "test_table", rls_predicate="country_code = 'US'")
    us_val = conn.execute(sql_rls).fetchone()[0]

    assert us_val < total_val
    assert us_val > 0


def test_semantic_model_discovery(duck_conn):
    """Semantic model builder automatically discovers date, geographic, and category hierarchies."""
    conn, df = duck_conn
    model = build_semantic_model("test_e2e", df=df)

    assert len(model.hierarchies) >= 2
    h_types = [h.hierarchy_type for h in model.hierarchies]
    assert "date" in h_types or "geographic" in h_types

    # Measures generated
    assert len(model.measures) >= 5
    m_names = [m.name for m in model.measures]
    assert any("Revenue" in name for name in m_names)
    assert any("Margin" in name for name in m_names)


def test_dashboard_model_backward_compatibility():
    """Legacy single-page dashboard with widgets array automatically initializes pages[0]."""
    widget = WidgetModel(config=ChartConfig(title="Test Widget"))
    legacy_dash = DashboardModel(
        title="Legacy Dashboard",
        file_id="123",
        widgets=[widget],
    )
    assert len(legacy_dash.pages) == 1
    assert legacy_dash.pages[0].title == "Page 1"
    assert len(legacy_dash.pages[0].widgets) == 1
    assert legacy_dash.pages[0].widgets[0].config.title == "Test Widget"
