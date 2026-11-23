"""
tests/test_outlier_interpretation.py
====================================
Tests for the corrected outlier interpretation logic:
1. Statistical outlier: IQR/Z-score/Modified Z-score must NOT be labeled as data-entry errors.
   - Output must say: "Statistically unusual observation; manual review recommended."
   - likely_errors must be 0 when no business constraints are violated.
2. Contextual / group outlier: temporal envelope or subgroup deviation.
3. Potential data-quality issue: density anomalies / multivariate deviations.
4. Confirmed invalid value / data-entry error: only when concrete domain constraints are violated.
5. End-to-end verification on dashboard_test_data.csv:
   - revenue, cost, and prospect_count report 0 likely data-entry errors.
6. Anomaly registry and cleaning recommendations consistency.
"""

import os
import numpy as np
import pandas as pd
import pytest

from cleaning.outlier_handler import OutlierHandler
from cleaning.cleaning_models import (
    OutlierClassification,
    OutlierMethod,
    ActionType,
    ActionCategory,
)
from insights.anomaly_detector import AnomalyDetector
from insights.insight_models import AnomalySeverity
from profiling.profiling_models import DatasetProfile, ColumnProfile, SemanticType, NumericProfile


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: Statistical outlier (IQR / Modified Z-score)
# ─────────────────────────────────────────────────────────────────────────────
def test_1_statistical_outlier_not_labeled_as_error():
    """Statistical outliers under distributional rules must NOT be called data-entry errors."""
    np.random.seed(42)
    # Lognormal / skewed distribution with natural heavy tail
    data = np.random.lognormal(mean=3.0, sigma=1.2, size=150)
    df = pd.DataFrame({"metric": data})

    handler = OutlierHandler(df, "test-file")
    actions, report = handler.analyze()

    assert len(report.column_summaries) == 1
    summary = report.column_summaries[0]
    assert summary.total_outliers > 0
    # Crucial assertion: distributional algorithms must NOT invent likely errors!
    assert summary.likely_errors == 0
    assert summary.confirmed_invalid_count == 0
    assert summary.statistical_outliers_count == summary.total_outliers

    # Sample outlier details
    for od in summary.sample_outliers:
        assert od.classification == OutlierClassification.STATISTICAL_OUTLIER
        assert od.is_likely_error is False
        assert od.error_likelihood == 0.0
        assert od.error_reasoning == "Statistically unusual observation; manual review recommended."

    # Cleaning action evidence & description
    act = actions[0]
    assert "Statistically unusual observation; manual review recommended." in act.evidence
    assert "data entry error" not in act.evidence.lower()
    assert "data-entry error" not in act.evidence.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Contextual / group outlier (Time-series envelope)
# ─────────────────────────────────────────────────────────────────────────────
def test_2_contextual_outlier_classification():
    """Outlier flagged in a temporal envelope is classified as CONTEXTUAL_OUTLIER."""
    from cleaning.cleaning_models import OutlierDetail
    stats = {"min": 0, "max": 100, "median": 50}

    handler = OutlierHandler(pd.DataFrame({"x": [10, 20]}), "test")
    od = OutlierDetail(
        column="hourly_traffic",
        row_index=5,
        value=85.0,
        detection_methods=[OutlierMethod.TIMESERIES_ENVELOPE],
    )
    handler._classify_outlier(od, stats)

    assert od.classification == OutlierClassification.CONTEXTUAL_OUTLIER
    assert od.is_likely_error is False
    assert "Contextual outlier" in od.classification_reason
    assert "manual review recommended" in od.classification_reason


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: Potential data-quality issue (DBSCAN / LOF)
# ─────────────────────────────────────────────────────────────────────────────
def test_3_potential_data_quality_classification():
    """Density anomalies are classified as POTENTIAL_DATA_QUALITY."""
    from cleaning.cleaning_models import OutlierDetail
    stats = {"min": 0, "max": 100, "median": 50}

    handler = OutlierHandler(pd.DataFrame({"x": [10, 20]}), "test")
    od = OutlierDetail(
        column="signal_strength",
        row_index=12,
        value=3.2,
        detection_methods=[OutlierMethod.DBSCAN, OutlierMethod.LOF],
    )
    handler._classify_outlier(od, stats)

    assert od.classification == OutlierClassification.POTENTIAL_DATA_QUALITY
    assert od.is_likely_error is False
    assert "Potential data-quality issue" in od.classification_reason


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Confirmed invalid value / data-entry error (Concrete evidence)
# ─────────────────────────────────────────────────────────────────────────────
def test_4_confirmed_invalid_value():
    """Only values violating explicit domain constraints are classified as CONFIRMED_INVALID / data-entry errors."""
    df = pd.DataFrame({
        "age": [25, 30, 250, -5, 40, 22, 28, 31, 45, 50, 38, 27, 33, 42, 29, 36, 41, 26, 34, 39],
        "price": [10, 20, -500, 30, 40, 50, 15, 25, 35, 45, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140],
    })
    handler = OutlierHandler(df, "test")
    actions, report = handler.analyze()

    age_summary = next(s for s in report.column_summaries if s.column == "age")
    assert age_summary.likely_errors > 0  # Concrete evidence: age 250 and -5
    assert age_summary.confirmed_invalid_count > 0

    # Confirmed invalid values must be marked as errors
    invalid_outliers = [o for o in age_summary.sample_outliers if o.classification == OutlierClassification.CONFIRMED_INVALID]
    assert len(invalid_outliers) > 0
    for o in invalid_outliers:
        assert o.is_likely_error is True
        assert "Violates business constraint" in o.error_reasoning

    # Action recommendation should replace boundary for confirmed invalid values
    age_action = next(a for a in actions if "age" in a.target_columns)
    assert age_action.action_type == ActionType.REPLACE_BOUNDARY
    assert "confirmed invalid" in age_action.evidence


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: End-to-end on dashboard_test_data.csv
# ─────────────────────────────────────────────────────────────────────────────
def test_5_dashboard_test_data_no_invented_errors():
    """On dashboard_test_data.csv, revenue, cost, and prospect_count must NOT report invented data-entry errors."""
    csv_path = "../dashboard_test_data.csv"
    if not os.path.exists(csv_path):
        csv_path = "dashboard_test_data.csv"
    assert os.path.exists(csv_path), "dashboard_test_data.csv not found"

    df = pd.read_csv(csv_path)
    handler = OutlierHandler(df, "dashboard-test")
    actions, report = handler.analyze()

    columns_to_check = ["revenue", "cost", "prospect_count", "units_sold", "avg_order_value"]
    for col in columns_to_check:
        summary = next((s for s in report.column_summaries if s.column == col), None)
        assert summary is not None, f"Expected outlier summary for {col}"
        assert summary.total_outliers > 0, f"Expected statistical outliers for {col}"
        # ZERO data-entry errors should be claimed from statistical algorithms alone!
        assert summary.likely_errors == 0, f"Unexpected data-entry errors reported on {col}: {summary.likely_errors}"
        assert summary.confirmed_invalid_count == 0

        # Check the generated action
        action = next(a for a in actions if col in a.target_columns)
        assert "Statistically unusual observation; manual review recommended." in action.evidence
        assert "data entry error" not in action.evidence.lower()
        assert "data-entry error" not in action.evidence.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: Anomaly Registry formatting
# ─────────────────────────────────────────────────────────────────────────────
def test_6_anomaly_registry_outlier_terminology():
    """AnomalyDetector must use 'Statistically unusual observation; manual review recommended.'"""
    prof = DatasetProfile(
        file_id="test_anom",
        total_rows=100,
        total_columns=1,
        columns=[
            ColumnProfile(
                name="revenue",
                position=0,
                inferred_dtype="float",
                semantic_type=SemanticType.NUMERIC_CONTINUOUS,
                distinct_count=90,
                null_count=0,
                null_percentage=0.0,
                numeric=NumericProfile(
                    min=10.0,
                    max=5000.0,
                    mean=250.0,
                    std_dev=300.0,
                    box_outliers=[1500.0, 2200.0, 3100.0, 4500.0, 4800.0, 5000.0],  # 6% outliers
                )
            )
        ]
    )

    warnings = AnomalyDetector.detect(prof)
    dist_warnings = [w for w in warnings if w.category == "Distribution" and w.feature == "revenue"]
    assert len(dist_warnings) == 1

    w = dist_warnings[0]
    assert w.severity == AnomalySeverity.INFO
    assert "Statistically unusual observation; manual review recommended." in w.description
    assert "data entry error" not in w.description.lower()
    assert "data-entry error" not in w.description.lower()
    assert "Review extreme values against business domain context" in w.recommendation
