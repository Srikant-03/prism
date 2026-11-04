"""
Phase 2 Verification Gate — AI Dashboard Builder End-to-End Suite.
Asserts all 6 dashboard flows, multi-metric queries, temporal bucketing,
auto-generator robustness, and mutation-lock/churn-leakage prevention
against prism_adversarial_e2e_test.csv (245 rows x 27 cols).
"""

import os
import pytest
import pandas as pd
import duckdb

from dashboard.chart_config_models import (
    ChartConfig,
    ChartType,
    AggregationType,
    TimeGrain,
    SortDirection,
    FilterCondition,
    MetricSpec,
)
from dashboard.config_to_sql import config_to_sql
from dashboard.prompt_interpreter import _fallback_interpret, interpret_prompt
from dashboard.auto_generator import _fallback_dashboard

CSV_PATH = "prism_adversarial_e2e_test.csv"


@pytest.fixture(scope="module")
def dataset():
    assert os.path.exists(CSV_PATH), f"Dataset not found at {CSV_PATH}"
    df = pd.read_csv(CSV_PATH)
    assert df.shape == (245, 27)
    conn = duckdb.connect()
    conn.register("prism", df)
    schema = {col: {"dtype": str(df[col].dtype)} for col in df.columns}
    return df, conn, schema


def test_flow_1_auto_generate_widgets(dataset):
    """Flow 1: Auto-Generate produces >= 4 cohesive, executable widgets without surrogate keys."""
    df, conn, schema = dataset
    configs = _fallback_dashboard(schema)

    assert len(configs) >= 4, f"Expected at least 4 auto-generated widgets, got {len(configs)}"

    # Ensure surrogate keys were excluded
    for c in configs:
        assert c.x_axis not in ("sequential_row_id", "txn_uuid", "dup_txn_uuid")
        assert c.y_axis not in ("sequential_row_id", "txn_uuid", "dup_txn_uuid")

    # Widget 1: Primary KPI is Net Revenue
    kpi1 = configs[0]
    assert kpi1.chart_type == ChartType.KPI
    assert kpi1.y_axis == "net_revenue" or kpi1.kpi_value_column == "net_revenue"
    sql1 = config_to_sql(kpi1, "prism")
    res1 = conn.execute(sql1).df()
    assert len(res1) == 1
    val1 = float(res1.iloc[0, 0])
    assert abs(val1 - 6425384.44) < 1.0, f"Expected net revenue ~6.4M, got {val1}"

    # Verify all configs compile and execute cleanly
    for i, c in enumerate(configs):
        sql = config_to_sql(c, "prism")
        res = conn.execute(sql).df()
        assert len(res) > 0, f"Config {i} ({c.title}) returned 0 rows"


def test_flow_2_prompt_1_monthly_trend(dataset):
    """Flow 2: 'monthly net_revenue trend by order_date' -> 18 monthly points."""
    df, conn, schema = dataset
    cfg = _fallback_interpret("monthly net_revenue trend by order_date", schema)

    assert cfg.chart_type == ChartType.LINE
    assert cfg.x_axis == "order_date"
    assert cfg.y_axis == "net_revenue"
    assert cfg.x_axis_time_grain == TimeGrain.MONTH
    assert cfg.aggregation == AggregationType.SUM

    sql = config_to_sql(cfg, "prism")
    res = conn.execute(sql).df()
    assert len(res) == 18, f"Expected 18 monthly points, got {len(res)}"

    # Check first month (2024-01) and peak month (2025-04)
    jan_2024 = res[res["order_date"] == "2024-01"]
    assert len(jan_2024) == 1
    assert abs(float(jan_2024["net_revenue"].iloc[0]) - 347358.56) < 1.0

    apr_2025 = res[res["order_date"] == "2025-04"]
    assert len(apr_2025) == 1
    assert abs(float(apr_2025["net_revenue"].iloc[0]) - 884634.68) < 1.0


def test_flow_3_prompt_2_quarterly_breakdown(dataset):
    """Flow 3: 'breakdown net_revenue by quarter' -> 6 quarters (2024-Q1 to 2025-Q2)."""
    df, conn, schema = dataset
    cfg = _fallback_interpret("breakdown net_revenue by quarter", schema)

    assert cfg.x_axis == "order_date"
    assert cfg.y_axis == "net_revenue"
    assert cfg.x_axis_time_grain == TimeGrain.QUARTER
    assert cfg.aggregation == AggregationType.SUM

    sql = config_to_sql(cfg, "prism")
    res = conn.execute(sql).df()
    assert len(res) == 6, f"Expected 6 quarters, got {len(res)}"

    # Check 2024-Q1 and 2025-Q2
    q1_2024 = res[res["order_date"] == "2024-Q1"]
    assert len(q1_2024) == 1
    assert abs(float(q1_2024["net_revenue"].iloc[0]) - 1199720.72) < 1.0

    q2_2025 = res[res["order_date"] == "2025-Q2"]
    assert len(q2_2025) == 1
    assert abs(float(q2_2025["net_revenue"].iloc[0]) - 1499384.03) < 1.0


def test_flow_4_prompt_3_multi_metric_kpi_with_filter(dataset):
    """Flow 4: 'total net_revenue and average discount_rate where churn_flag is 1'."""
    df, conn, schema = dataset
    cfg = _fallback_interpret("total net_revenue and average discount_rate where churn_flag is 1", schema)

    assert cfg.chart_type == ChartType.KPI
    assert cfg.metrics is not None and len(cfg.metrics) == 2
    assert cfg.metrics[0].column == "net_revenue" and cfg.metrics[0].aggregation == AggregationType.SUM
    assert cfg.metrics[1].column == "discount_rate" and cfg.metrics[1].aggregation == AggregationType.AVG
    assert len(cfg.filters) == 1
    assert cfg.filters[0].column == "churn_flag" and cfg.filters[0].value == 1

    sql = config_to_sql(cfg, "prism")
    res = conn.execute(sql).df()
    assert len(res) == 1
    assert abs(float(res["net_revenue"].iloc[0]) - 717251.37) < 1.0
    assert abs(float(res["discount_rate"].iloc[0]) - 0.116667) < 0.001


def test_flow_5_prompt_4_multi_metric_comparison_no_leakage(dataset):
    """Flow 5: 'compare gross_revenue vs net_revenue across product_category sorted descending'."""
    df, conn, schema = dataset

    # Simulate passing prior KPI config with churn_flag = 1 as current_config
    prior_kpi_config = ChartConfig(
        chart_type=ChartType.KPI,
        title="Prior KPI",
        metrics=[MetricSpec(column="net_revenue", aggregation=AggregationType.SUM)],
        filters=[FilterCondition(column="churn_flag", operator="=", value=1)],
    )

    cfg = _fallback_interpret(
        "compare gross_revenue vs net_revenue across product_category sorted descending",
        schema,
        current_config=prior_kpi_config,
    )

    assert cfg.chart_type == ChartType.BAR
    assert cfg.x_axis == "product_category"
    assert cfg.metrics is not None and len(cfg.metrics) == 2
    assert cfg.metrics[0].column == "gross_revenue"
    assert cfg.metrics[1].column == "net_revenue"
    assert cfg.sort_by == "gross_revenue"
    assert cfg.sort_direction == SortDirection.DESC
    # CRITICAL: zero churn leakage from prior config!
    assert len(cfg.filters) == 0, f"Expected 0 filters, got {cfg.filters}"

    sql = config_to_sql(cfg, "prism")
    res = conn.execute(sql).df()
    top_row = res.iloc[0]
    assert top_row["product_category"] == "Enterprise Software"
    assert abs(float(top_row["gross_revenue"]) - 3211284.88) < 1.0
    assert abs(float(top_row["net_revenue"]) - 2808625.61) < 1.0


def test_flow_6_prompt_5_scatter_plot(dataset):
    """Flow 6: 'scatter units_sold vs net_revenue colored by product_category'."""
    df, conn, schema = dataset
    cfg = _fallback_interpret("scatter units_sold vs net_revenue colored by product_category", schema)

    assert cfg.chart_type == ChartType.SCATTER
    assert cfg.x_axis == "units_sold"
    assert cfg.y_axis == "net_revenue"
    assert cfg.group_by == "product_category"
    assert cfg.aggregation == AggregationType.NONE
    assert cfg.metrics is None

    sql = config_to_sql(cfg, "prism")
    res = conn.execute(sql).df()
    # 244 valid numeric coordinate points (1 null row excluded)
    assert len(res) == 244, f"Expected 244 scatter points, got {len(res)}"
    assert not res["units_sold"].isna().any()
    assert not res["net_revenue"].isna().any()
