"""
Unit and integration tests for Step 2: AI Copilot Brain, NL-to-Measure, Auto-Dashboard, Forecasting & Explain This Point.
Verifies on prism_adversarial_e2e_test.csv.
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
    ClarificationRequest,
    MeasureDefinition,
)
from dashboard.prompt_interpreter import interpret_prompt, _fallback_interpret
from dashboard.auto_generator import generate_initial_dashboard, _fallback_dashboard
from dashboard.ai_analytics import (
    run_forecast,
    explain_point_delta,
    compute_decomposition_tree,
    compute_key_influencers,
    generate_smart_narrative,
)
from dashboard.config_to_sql import config_to_sql

DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "prism_adversarial_e2e_test.csv")


@pytest.fixture(scope="module")
def setup_data():
    assert os.path.exists(DATASET_PATH), f"Dataset not found at {DATASET_PATH}"
    df = pd.read_csv(DATASET_PATH)
    schema = {col: {"dtype": str(df[col].dtype), "sample_values": df[col].dropna().head(3).tolist()} for col in df.columns}
    conn = duckdb.connect(":memory:")
    conn.register("test_table", df)
    return df, schema, conn


@pytest.mark.asyncio
async def test_auto_generate_six_widgets(setup_data):
    """Auto-generate dashboard returns >= 5 valid, non-redundant widgets."""
    df, schema, conn = setup_data
    configs = await generate_initial_dashboard(schema, "SaaS software sales and subscription business")

    assert len(configs) >= 5, f"Expected >= 5 auto-generated widgets, got {len(configs)}"
    types = [c.chart_type for c in configs]
    assert ChartType.KPI in types
    assert ChartType.LINE in types or ChartType.COLUMN in types
    assert ChartType.BAR in types
    assert ChartType.SCATTER in types

    # Validate that generated charts execute cleanly in DuckDB
    for c in configs:
        if c.chart_type == ChartType.SMART_NARRATIVE:
            continue
        sql = config_to_sql(c, "test_table")
        rows = conn.execute(sql).fetchall()
        assert len(rows) > 0, f"Query for {c.title} returned 0 rows"


@pytest.mark.asyncio
async def test_all_six_adversarial_nl_prompts(setup_data):
    """Verify all 6 adversarial prompts from evaluation benchmarks."""
    df, schema, conn = setup_data

    # Prompt 1: Monthly line trend
    p1 = _fallback_interpret("monthly net_revenue trend", schema)
    assert p1.chart_type == ChartType.LINE
    assert p1.x_axis_time_grain == TimeGrain.MONTH
    assert p1.x_axis == "order_date"
    assert p1.y_axis == "net_revenue"

    # Prompt 2: Quarterly breakdown + stacked follow-up
    p2 = _fallback_interpret("quarterly gross_revenue breakdown", schema)
    assert p2.x_axis_time_grain == TimeGrain.QUARTER
    assert p2.y_axis == "gross_revenue"
    # Edit modifier follow-up
    p2_mod = _fallback_interpret("switch to stacked bar", schema, current_config=p2)
    assert p2_mod.chart_type == ChartType.BAR
    assert p2_mod.stacked is True
    assert p2_mod.x_axis_time_grain == TimeGrain.QUARTER

    # Prompt 3: Filtered multi-metric KPI
    p3 = _fallback_interpret("total net_revenue and average discount_rate where churn_flag is 1", schema)
    assert p3.chart_type == ChartType.KPI or p3.metrics is not None
    assert len(p3.metrics) == 2
    assert p3.metrics[0].column == "net_revenue"
    assert p3.metrics[0].aggregation == AggregationType.SUM
    assert p3.metrics[1].column == "discount_rate"
    assert p3.metrics[1].aggregation == AggregationType.AVG
    assert len(p3.filters) == 1
    assert p3.filters[0].column == "churn_flag"
    assert p3.filters[0].value == 1

    sql_p3 = config_to_sql(p3, "test_table")
    row_p3 = conn.execute(sql_p3).fetchone()
    assert round(row_p3[0], 2) == 717251.37
    assert round(row_p3[1], 4) == 0.1167

    # Prompt 4: Multi-metric comparison
    p4 = _fallback_interpret("gross_revenue vs net_revenue across product_category", schema)
    assert p4.chart_type == ChartType.BAR
    assert p4.x_axis == "product_category"
    assert len(p4.metrics) == 2
    assert p4.metrics[0].column == "gross_revenue"
    assert p4.metrics[1].column == "net_revenue"

    sql_p4 = config_to_sql(p4, "test_table")
    rows_p4 = conn.execute(sql_p4).fetchall()
    ent_row = [r for r in rows_p4 if r[0] == "Enterprise Software"][0]
    assert round(ent_row[1], 2) == 3211284.88
    assert round(ent_row[2], 2) == 2808625.61

    # Prompt 5: Scatter plot with colored category
    p5 = _fallback_interpret("scatter units_sold vs net_revenue colored by product_category", schema)
    assert p5.chart_type == ChartType.SCATTER
    assert p5.x_axis == "units_sold"
    assert p5.y_axis == "net_revenue"
    assert p5.color_by == "product_category"

    # Prompt 6: Ambiguous prompt returns clarification
    p6 = await interpret_prompt("show performance over time", schema)
    assert isinstance(p6, ClarificationRequest)
    assert len(p6.suggestions) > 0


@pytest.mark.asyncio
async def test_nl_to_measure_creation(setup_data):
    """NL-to-Measure compiles natural language formula to MeasureDefinition."""
    df, schema, conn = setup_data
    res = await interpret_prompt("Create a measure Net Margin % as net_revenue divided by gross_revenue", schema)
    assert isinstance(res, MeasureDefinition)
    assert res.name == "Net Margin %"
    assert "DIVIDE" in res.expression
    assert res.format == "percent"


def test_forecasting_with_confidence_and_anomalies(setup_data):
    """Forecasting returns future periods and detected anomalies."""
    df, schema, conn = setup_data
    forecast_res = run_forecast(
        df=df,
        date_col="order_date",
        val_col="net_revenue",
        time_grain="month",
        periods=6,
        confidence=0.95,
    )

    assert len(forecast_res.points) == 18 + 6  # 18 historical + 6 forecast
    # Check historical points
    assert forecast_res.points[0].actual == 347358.56
    # Check forecast points
    future_pts = [p for p in forecast_res.points if p.actual is None]
    assert len(future_pts) == 6
    assert future_pts[0].forecast > 0
    assert future_pts[0].upper_95 > future_pts[0].forecast
    assert future_pts[0].lower_95 <= future_pts[0].forecast


def test_explain_point_2025_04_spike(setup_data):
    """Explain This Point on 2025-04 identifies Electronics and top outlier transaction."""
    df, schema, conn = setup_data
    explain_res = explain_point_delta(
        df=df,
        clicked_point="2025-04",
        dimension_col="order_date",
        metric_col="net_revenue",
        time_grain="month",
    )

    assert explain_res.clicked_point == "2025-04"
    assert explain_res.current_value == 884634.68
    assert explain_res.previous_point == "2025-03"
    assert explain_res.previous_value == 331993.61
    assert explain_res.top_driver_category == "Electronics"
    assert len(explain_res.waterfall) >= 3

    # Outlier transaction
    assert len(explain_res.top_outlier_transactions) > 0
    top_txn = explain_res.top_outlier_transactions[0]
    assert top_txn.get("txn_uuid") == "TXN-10015"
    assert top_txn.get("product_category") == "Electronics"
    assert "Electronics" in explain_res.narrative


def test_decomposition_tree_node_splits(setup_data):
    """Decomposition tree computes hierarchical breakdown accurately."""
    df, schema, conn = setup_data
    decomp_res = compute_decomposition_tree(
        df=df,
        metric_col="net_revenue",
        split_dimension="product_category",
    )

    assert decomp_res.dimension_split == "product_category"
    assert len(decomp_res.nodes) >= 4
    # Enterprise Software is largest category
    assert decomp_res.nodes[0].name == "Enterprise Software"
    assert round(decomp_res.nodes[0].value, 2) == 2808625.61


def test_key_influencers_and_leakage_protection(setup_data):
    """Key influencers discovers drivers while protecting against data leakage."""
    df, schema, conn = setup_data
    ki_res = compute_key_influencers(
        df=df,
        target_col="churn_flag",
        target_value=1,
    )

    assert ki_res.target_column == "churn_flag"
    assert len(ki_res.drivers) > 0
    # post_churn_refund must be guarded as leakage
    assert "post_churn_refund" in ki_res.guarded_leakage_columns


def test_smart_narrative_grounded_in_data(setup_data):
    """Smart narrative generates bullet points without hallucinated finance boilerplate."""
    df, schema, conn = setup_data
    narrative_res = generate_smart_narrative(df=df)

    assert len(narrative_res.bullets) >= 3
    full_text = " ".join(narrative_res.bullets)

    # Must contain ground-truth data
    assert "Net Revenue" in full_text
    assert "Enterprise Software" in full_text

    # Must NOT contain hallucinated boilerplate
    assert "NZSIOC" not in full_text
    assert "ANZSIC" not in full_text
    assert "salary" not in full_text
    assert "ROE" not in full_text
