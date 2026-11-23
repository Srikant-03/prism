"""
Tests for missing-value recommendation on discount_pct and temporal interpolation safety.

Verifies:
1. 'discount_pct' on dashboard_test_data.csv receives the semantic recommendation:
   "'discount_pct' has 25 missing values. Imputation strategy requires semantic analysis; interpolation is not automatically justified."
2. Interpolation is never recommended simply because a date column exists.
3. Chronologically unsorted datasets (date.is_monotonic_increasing = False) are detected.
4. Repeated dates are detected and prevent naive interpolation.
5. Interpolation preserves original row ordering after execution.
6. Interpolation is group-aware and does not interpolate across unrelated categorical groups.
7. Promotional/business metrics are protected from automatic interpolation.
"""

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from cleaning.missing_handler import MissingHandler
from cleaning.decision_engine import DecisionEngine
from cleaning.cleaning_models import CleaningAction, ActionType, ActionCategory, ActionConfidence


@pytest.fixture
def test_csv_path() -> Path:
    candidates = [
        Path("dashboard_test_data.csv"),
        Path("../dashboard_test_data.csv"),
        Path("c:/abbu/dashboard_test_data.csv"),
    ]
    for p in candidates:
        if p.is_file():
            return p
    pytest.skip("dashboard_test_data.csv not found")


def test_discount_pct_on_supplied_csv(test_csv_path):
    """
    On dashboard_test_data.csv:
    - discount_pct has 25 missing values.
    - date is not monotonic and has repeated dates.
    - System must NOT recommend interpolation.
    - System must produce semantic recommendation.
    """
    df = pd.read_csv(test_csv_path)
    df["date"] = pd.to_datetime(df["date"])

    handler = MissingHandler(df, str(test_csv_path))
    actions, report = handler.analyze()

    discount_actions = [a for a in actions if "discount_pct" in a.target_columns]
    assert len(discount_actions) == 1, f"Expected 1 action for discount_pct, found {len(discount_actions)}"

    action = discount_actions[0]

    # Must NOT recommend linear interpolation
    assert action.action_type != ActionType.INTERPOLATE, (
        "Linear interpolation must NOT be recommended for promotional discount_pct"
    )

    # Must produce the expected semantic recommendation
    expected_rec = (
        "'discount_pct' has 25 missing values. Imputation strategy requires semantic analysis; "
        "interpolation is not automatically justified."
    )
    assert action.recommendation == expected_rec, (
        f"Recommendation mismatch.\nExpected: {expected_rec}\nGot: {action.recommendation}"
    )

    # Reasoning must mention unsorted / repeated dates and promotional nature
    assert "date.is_monotonic_increasing = False" in action.reasoning
    assert "repeated dates" in action.reasoning
    assert "Promotional features are discrete business decisions" in action.reasoning

    # Options must provide domain-appropriate choices
    opt_keys = [o.key for o in action.options]
    assert "impute_constant" in opt_keys
    assert "impute_median" in opt_keys
    assert "add_indicator" in opt_keys


def test_unsorted_dataset_detection():
    """Datasets that are not chronologically ordered must not use interpolation."""
    df_unsorted = pd.DataFrame({
        "date": ["2023-01-05", "2023-01-01", "2023-01-10", "2023-01-08"],
        "sensor_reading": [np.nan, 12.0, 24.0, 18.0],
    })
    handler = MissingHandler(df_unsorted, "test_unsorted")
    meta = handler._get_temporal_metadata()

    assert meta["has_date_col"] is True
    assert meta["is_chronologically_ordered"] is False

    is_eligible, reason = handler._check_interpolation_eligibility("sensor_reading", "numeric")
    assert is_eligible is False
    assert "not chronologically ordered" in reason


def test_repeated_dates_detection():
    """Datasets with repeated dates must reject naive 1D interpolation."""
    df_repeated = pd.DataFrame({
        "date": ["2023-01-01", "2023-01-01", "2023-01-02", "2023-01-02"],
        "sensor_reading": [10.0, np.nan, 20.0, 30.0],
    })
    handler = MissingHandler(df_repeated, "test_repeated")
    meta = handler._get_temporal_metadata()

    assert meta["has_repeated_dates"] is True
    assert meta["repeated_dates_count"] > 0

    is_eligible, reason = handler._check_interpolation_eligibility("sensor_reading", "numeric")
    assert is_eligible is False
    assert "repeated dates" in reason


def test_continuous_vs_promotional_semantics():
    """Only continuous physical/sensor measurements in ordered datasets are eligible for interpolation."""
    # Chronologically sorted, unique dates
    df_sorted = pd.DataFrame({
        "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
        "ambient_temperature": [20.0, np.nan, 24.0, 26.0, 28.0],
        "discount_pct": [5.0, np.nan, 10.0, 10.0, 15.0],
        "revenue": [100.0, np.nan, 300.0, 400.0, 500.0],
    })
    handler = MissingHandler(df_sorted, "test_sorted")

    # ambient_temperature IS eligible
    is_eligible, _ = handler._check_interpolation_eligibility("ambient_temperature", "numeric")
    assert is_eligible is True

    # discount_pct is promotional, NOT eligible
    is_eligible_disc, reason_disc = handler._check_interpolation_eligibility("discount_pct", "numeric")
    assert is_eligible_disc is False
    assert "promotional" in reason_disc

    # revenue is business metric, NOT eligible
    is_eligible_rev, reason_rev = handler._check_interpolation_eligibility("revenue", "numeric")
    assert is_eligible_rev is False


def test_order_preservation_and_time_sorting_in_interpolation():
    """
    When interpolation is executed on an unsorted dataset:
    - Calculation must be done in chronological order.
    - Original row ordering must be preserved afterwards.
    """
    df_unsorted = pd.DataFrame({
        "date": ["2023-01-05", "2023-01-01", "2023-01-10"],
        "sensor_reading": [np.nan, 10.0, 20.0],
        "row_id": [0, 1, 2],
    })
    action = CleaningAction(
        category=ActionCategory.MISSING_VALUES,
        action_type=ActionType.INTERPOLATE,
        confidence=ActionConfidence.DEFINITIVE,
        evidence="test",
        recommendation="test",
        reasoning="test",
        target_columns=["sensor_reading"],
    )
    engine = DecisionEngine(df_unsorted, "test_engine")
    res = engine._cmd_interpolate(df_unsorted.copy(), action, None)

    # Row 0 (date 2023-01-05) is halfway between 2023-01-01 (10) and 2023-01-10 (20)
    assert res.loc[0, "sensor_reading"] == pytest.approx(15.0)

    # Original row order preserved
    assert list(res["row_id"]) == [0, 1, 2]


def test_group_aware_interpolation():
    """Interpolation must not leak values across distinct categorical groups."""
    df_grouped = pd.DataFrame({
        "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-01", "2023-01-02", "2023-01-03"],
        "category": ["Food", "Food", "Food", "Electronics", "Electronics", "Electronics"],
        "sensor_temp": [100.0, np.nan, 300.0, 1.0, np.nan, 3.0],
        "row_id": [0, 1, 2, 3, 4, 5],
    })
    action = CleaningAction(
        category=ActionCategory.MISSING_VALUES,
        action_type=ActionType.INTERPOLATE,
        confidence=ActionConfidence.DEFINITIVE,
        evidence="test",
        recommendation="test",
        reasoning="test",
        target_columns=["sensor_temp"],
    )
    engine = DecisionEngine(df_grouped, "test_engine")
    res = engine._cmd_interpolate(df_grouped.copy(), action, None)

    # Food group: halfway between 100 and 300 = 200
    assert res.loc[1, "sensor_temp"] == pytest.approx(200.0)

    # Electronics group: halfway between 1 and 3 = 2
    assert res.loc[4, "sensor_temp"] == pytest.approx(2.0)

    # Row ordering preserved
    assert list(res["row_id"]) == [0, 1, 2, 3, 4, 5]


def test_promotional_variable_protected_in_cmd_interpolate():
    """Even if _cmd_interpolate is invoked on discount_pct, it must not interpolate."""
    df_promo = pd.DataFrame({
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "discount_pct": [10.0, np.nan, 50.0],
    })
    action = CleaningAction(
        category=ActionCategory.MISSING_VALUES,
        action_type=ActionType.INTERPOLATE,
        confidence=ActionConfidence.DEFINITIVE,
        evidence="test",
        recommendation="test",
        reasoning="test",
        target_columns=["discount_pct"],
    )
    engine = DecisionEngine(df_promo, "test_engine")
    res = engine._cmd_interpolate(df_promo.copy(), action, None)

    # Must NOT interpolate to 30.0; filled with constant 0.0 for discount
    assert res.loc[1, "discount_pct"] == 0.0
