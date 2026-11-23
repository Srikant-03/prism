import os
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from profiling.temporal_analyzer import TemporalAnalyzer
from profiling.cross_column_models import TemporalAnalysis, TemporalDiagnostic, EventTrendComponent


def test_dashboard_test_data_temporal_analysis():
    """
    Verify dashboard_test_data.csv:
    - 143 rows
    - 111 unique dates, 32 duplicate date rows
    - Non-monotonic order
    - Irregular spacing with >75% gaps in 538 days span
    - Should correctly diagnose 'event_transactions', skip classical decomposition,
      and produce event_trends.
    """
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")
    if not os.path.exists(csv_path):
        pytest.skip("dashboard_test_data.csv not found")

    df = pd.read_csv(csv_path)

    # Mock type map
    types_map = {
        col: "numeric_continuous" for col in df.select_dtypes(include=[np.number]).columns
    }
    types_map["date"] = "datetime"
    types_map["category"] = "categorical_nominal"
    types_map["region"] = "categorical_nominal"
    types_map["channel"] = "categorical_nominal"

    result = TemporalAnalyzer().analyze(df, types_map)

    # Verify primary time column detected
    assert result.has_temporal_patterns is True
    assert result.primary_time_col == "date"

    # Verify diagnostic
    diag = result.diagnostic
    assert diag is not None
    assert diag.total_records == 143
    assert diag.unique_timestamps == 111
    assert diag.duplicate_count == 32
    assert diag.has_duplicates is True
    assert diag.is_regular is False
    assert diag.is_monotonically_sorted is False
    assert diag.series_type == "event_transactions"

    # Verify decomposition guardrail
    assert diag.decomposition_eligible is False
    assert "missing" in diag.decomposition_ineligibility_reason.lower() or "gap" in diag.decomposition_ineligibility_reason.lower()
    assert result.analysis_mode == "event_trend"

    # Verify event trends produced
    assert len(result.event_trends) > 0
    assert "revenue" in result.event_trends
    rev_trend = result.event_trends["revenue"]
    assert isinstance(rev_trend, EventTrendComponent)
    assert len(rev_trend.timestamps) > 0
    assert len(rev_trend.values) == len(rev_trend.timestamps)
    assert len(rev_trend.rolling_mean) == len(rev_trend.timestamps)
    assert len(rev_trend.rolling_std) == len(rev_trend.timestamps)
    assert "rolling" in rev_trend.explanation.lower() or "trend" in rev_trend.explanation.lower()

    # Classical decompositions should be empty because decomposition is not eligible
    assert len(result.decompositions) == 0


def test_regular_time_series_decomposition():
    """
    Verify clean, regular daily series without gaps:
    - 60 consecutive daily points
    - Should be detected as regular_time_series
    - decomposition_eligible should be True
    - Classical STL decomposition should be produced
    """
    dates = pd.date_range("2023-01-01", periods=60, freq="D")
    # Linear trend + weekly sine wave + noise
    trend = np.linspace(10, 50, 60)
    seasonality = 5 * np.sin(2 * np.pi * np.arange(60) / 7)
    noise = np.random.RandomState(42).normal(0, 1, 60)
    sales = trend + seasonality + noise

    df = pd.DataFrame({
        "timestamp": dates,
        "sales": sales
    })

    types_map = {
        "timestamp": "datetime",
        "sales": "numeric_continuous"
    }

    result = TemporalAnalyzer().analyze(df, types_map)

    assert result.has_temporal_patterns is True
    assert result.primary_time_col == "timestamp"

    diag = result.diagnostic
    assert diag is not None
    assert diag.is_regular is True
    assert diag.has_duplicates is False
    assert diag.duplicate_count == 0
    assert diag.series_type == "regular_time_series"
    assert diag.decomposition_eligible is True
    assert result.analysis_mode == "classical_decomposition"

    # Verify decomposition outputs
    assert "sales" in result.decompositions
    decomp = result.decompositions["sales"]
    assert len(decomp.trend) == 60
    assert len(decomp.seasonal) == 60
    assert len(decomp.residual) == 60
    assert len(decomp.timestamps) == 60


def test_metric_aware_aggregation_rules():
    """
    Verify volume vs rate aggregation rules:
    - revenue, cost, units_sold, prospect_count -> sum
    - avg_order_value, customer_satisfaction, discount_pct -> mean
    """
    assert TemporalAnalyzer._get_metric_agg_function("revenue") == "sum"
    assert TemporalAnalyzer._get_metric_agg_function("cost") == "sum"
    assert TemporalAnalyzer._get_metric_agg_function("units_sold") == "sum"
    assert TemporalAnalyzer._get_metric_agg_function("prospect_count") == "sum"

    assert TemporalAnalyzer._get_metric_agg_function("avg_order_value") == "mean"
    assert TemporalAnalyzer._get_metric_agg_function("customer_satisfaction") == "mean"
    assert TemporalAnalyzer._get_metric_agg_function("return_rate") == "mean"
    assert TemporalAnalyzer._get_metric_agg_function("discount_pct") == "mean"


def test_repeated_timestamps_aggregation():
    """
    Test a series with repeated dates where frequency is regular daily after aggregation.
    """
    dates = [
        "2023-01-01", "2023-01-01",
        "2023-01-02",
        "2023-01-03", "2023-01-03",
        "2023-01-04",
        "2023-01-05", "2023-01-05",
        "2023-01-06",
        "2023-01-07"
    ]
    df = pd.DataFrame({
        "event_date": pd.to_datetime(dates),
        "revenue": [100.0, 150.0, 200.0, 120.0, 130.0, 250.0, 110.0, 140.0, 300.0, 280.0]
    })
    types_map = {"event_date": "datetime", "revenue": "numeric_continuous"}

    result = TemporalAnalyzer().analyze(df, types_map)

    assert result.has_temporal_patterns is True
    diag = result.diagnostic
    assert diag is not None
    assert diag.has_duplicates is True
    assert diag.duplicate_count == 3
    assert diag.unique_timestamps == 7
    # Since 7 points < 14 (need 2 * period for STL), decomposition_eligible will be False with clear reason
    assert diag.decomposition_eligible is False
    assert "at least" in diag.decomposition_ineligibility_reason.lower()
