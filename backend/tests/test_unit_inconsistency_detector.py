import os
import pytest
import pandas as pd
import numpy as np

from cleaning.standardization_handler import StandardizationHandler
from cleaning.cleaning_models import ActionType


@pytest.fixture
def dashboard_df():
    csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../dashboard_test_data.csv"))
    if not os.path.exists(csv_path):
        csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../dashboard_test_data.csv"))
    return pd.read_csv(csv_path)


def test_1_customer_satisfaction_never_suspected_inches_cm(dashboard_df):
    """
    customer_satisfaction (range 1.0–5.0) has natural bimodal customer sentiment.
    Even with peak ratio ~2.54, it must NEVER be flagged as suspected inches/cm.
    """
    handler = StandardizationHandler(dashboard_df, "test")
    res = handler._check_unit_inconsistency("customer_satisfaction")
    assert res is None, f"Expected None for customer_satisfaction, got: {res}"


def test_2_response_time_days_never_suspected_celsius_fahrenheit(dashboard_df):
    """
    response_time_days is a time duration in days (range 0.6–8.0).
    It must NEVER be flagged as suspected celsius/fahrenheit.
    """
    handler = StandardizationHandler(dashboard_df, "test")
    res = handler._check_unit_inconsistency("response_time_days")
    assert res is None, f"Expected None for response_time_days, got: {res}"


def test_3_percentages_and_rates_never_flagged(dashboard_df):
    """
    Percentages, rates, and ratios (discount_pct, return_rate) must never
    be flagged as unit inconsistencies.
    """
    handler = StandardizationHandler(dashboard_df, "test")
    for col in ["discount_pct", "return_rate"]:
        res = handler._check_unit_inconsistency(col)
        assert res is None, f"Expected None for {col}, got: {res}"


def test_4_counts_and_currency_never_flagged(dashboard_df):
    """
    Counts (units_sold, prospect_count) and currency (revenue, cost, profit, avg_order_value)
    must never be flagged as physical unit inconsistencies.
    """
    handler = StandardizationHandler(dashboard_df, "test")
    for col in ["revenue", "cost", "profit", "units_sold", "avg_order_value", "prospect_count"]:
        res = handler._check_unit_inconsistency(col)
        assert res is None, f"Expected None for {col}, got: {res}"


def test_5_dashboard_test_data_zero_false_unit_issues(dashboard_df):
    """
    On the complete dashboard_test_data.csv, there must be ZERO false unit inconsistency
    alerts and ZERO FIX_UNIT_INCONSISTENCY actions generated.
    """
    handler = StandardizationHandler(dashboard_df, "test")
    actions, report = handler.analyze()

    unit_issues = report.get("unit_issues", [])
    assert len(unit_issues) == 0, f"Expected 0 unit issues, found: {unit_issues}"

    unit_actions = [a for a in actions if a.action_type == ActionType.FIX_UNIT_INCONSISTENCY]
    assert len(unit_actions) == 0, f"Expected 0 unit actions, found: {unit_actions}"


def test_6_true_physical_weight_detection():
    """
    When semantic evidence (weight) AND statistical evidence (ratio ~2.2046) both exist,
    the detector correctly infers kg/lbs.
    """
    np.random.seed(42)
    # Mixture of 70 kg (mode 1) and 154.3 lbs (mode 2)
    kg_vals = np.random.normal(70, 3, size=50)
    lbs_vals = np.random.normal(154.3, 5, size=50)
    df = pd.DataFrame({"package_weight": np.concatenate([kg_vals, lbs_vals])})

    handler = StandardizationHandler(df, "test")
    res = handler._check_unit_inconsistency("package_weight")
    assert res is not None
    assert res["suspected_units"] == "kg/lbs"

    action = handler._build_unit_action("package_weight", res)
    assert action.action_type == ActionType.FIX_UNIT_INCONSISTENCY
    assert "kg/lbs" in action.evidence


def test_7_true_physical_dimension_detection():
    """
    When semantic evidence (height) AND statistical evidence (ratio ~2.54) both exist,
    the detector correctly infers inches/cm.
    """
    np.random.seed(42)
    in_vals = np.random.normal(10, 0.5, size=50)
    cm_vals = np.random.normal(25.4, 1.0, size=50)
    df = pd.DataFrame({"item_height": np.concatenate([in_vals, cm_vals])})

    handler = StandardizationHandler(df, "test")
    res = handler._check_unit_inconsistency("item_height")
    assert res is not None
    assert res["suspected_units"] == "inches/cm"


def test_8_unknown_measurement_column_multimodal_explanation():
    """
    For a column with generic measurement name and multiple modes not matching
    a known physical unit transformation, the detector:
    1. Does NOT guess specific units (suspected_units is None)
    2. Reports the exact wording:
       'Multiple modes detected. Investigate whether this reflects subgroup structure or mixed measurement conventions.'
    """
    np.random.seed(42)
    # Modes at 10 and 18 (ratio 1.8, but not temperature semantics)
    mode1 = np.random.normal(10, 1.0, size=50)
    mode2 = np.random.normal(18, 1.0, size=50)
    df = pd.DataFrame({"sensor_reading": np.concatenate([mode1, mode2])})

    handler = StandardizationHandler(df, "test")
    res = handler._check_unit_inconsistency("sensor_reading")
    assert res is not None
    assert res["suspected_units"] is None

    action = handler._build_unit_action("sensor_reading", res)
    expected_msg = "Multiple modes detected. Investigate whether this reflects subgroup structure or mixed measurement conventions."
    assert expected_msg in action.evidence
    assert expected_msg in action.reasoning
