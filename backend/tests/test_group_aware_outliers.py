import os
import pytest
import pandas as pd
import numpy as np

from cleaning.outlier_handler import OutlierHandler
from cleaning.cleaning_models import (
    OutlierClassification,
    ActionType,
)


@pytest.fixture
def dashboard_df():
    csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../dashboard_test_data.csv"))
    if not os.path.exists(csv_path):
        csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../dashboard_test_data.csv"))
    return pd.read_csv(csv_path)


def test_1_food_fresh_produce_units_sold_recognized_as_subgroup(dashboard_df):
    """
    Verify Food / Fresh Produce high-unit observations in units_sold
    are recognized as legitimate subgroup behavior rather than errors.
    """
    handler = OutlierHandler(dashboard_df, "test_file")
    actions, report = handler.analyze()

    summ = next(s for s in report.column_summaries if s.column == "units_sold")
    assert summ.grouping_dimension in ("category + subcategory", "subcategory")
    assert summ.subgroup_normal_count > 0
    assert summ.likely_errors == 0  # Not declared as errors

    # Check sample outliers for Food / Fresh Produce
    food_samples = [o for o in summ.sample_outliers if "Fresh Produce" in (o.group_name or "")]
    assert len(food_samples) > 0

    # Normal within subgroup
    for o in food_samples:
        if o.value < 550:
            assert o.is_group_outlier is False
            assert o.contextual_explanation == "Globally unusual but normal for this product group"
            assert o.classification == OutlierClassification.CONTEXTUAL_OUTLIER

    # Extreme observation within subgroup (value 638)
    all_iqr = handler._iqr_detect("units_sold", dashboard_df["units_sold"].dropna(), handler._compute_stats(dashboard_df["units_sold"].dropna()))
    handler._evaluate_group_outliers("units_sold", dashboard_df["units_sold"].dropna(), all_iqr, "category + subcategory", ["category", "subcategory"])
    row_638 = next(o for o in all_iqr if o.value == 638.0)
    assert row_638.is_group_outlier is True
    assert row_638.contextual_explanation == "Unusual even within its peer group."


def test_2_electronics_smartphones_avg_order_value_recognized(dashboard_df):
    """
    Verify Electronics / Smartphones high avg_order_value observations
    are recognized as normal for their product group.
    """
    handler = OutlierHandler(dashboard_df, "test_file")
    actions, report = handler.analyze()

    summ = next(s for s in report.column_summaries if s.column == "avg_order_value")
    assert summ.subgroup_normal_count > 0
    assert summ.likely_errors == 0

    smartphone_samples = [o for o in summ.sample_outliers if "Smartphones" in (o.group_name or "")]
    assert len(smartphone_samples) > 0
    for o in smartphone_samples:
        if o.value > 3000:
            assert o.is_group_outlier is False
            assert o.contextual_explanation == "Globally unusual but normal for this product group"
            assert o.classification == OutlierClassification.CONTEXTUAL_OUTLIER


def test_3_no_automatic_winsorization_for_subgroup_scale_differences(dashboard_df):
    """
    Verify the engine does not automatically winsorize columns whose outliers
    are driven by different group scales (units_sold, avg_order_value, revenue, cost).
    """
    handler = OutlierHandler(dashboard_df, "test_file")
    actions, report = handler.analyze()

    for col in ["units_sold", "avg_order_value", "revenue", "cost"]:
        summ = next(s for s in report.column_summaries if s.column == col)
        # Treatment must be FLAG_OUTLIER, never WINSORIZE
        assert summ.recommended_treatment == ActionType.FLAG_OUTLIER
        assert "Do not automatically winsorize" in summ.treatment_reasoning


def test_4_cleaning_action_options_and_evidence(dashboard_df):
    """
    Verify CleaningActions generated for subgroup columns preserve scale options
    and describe subgroup counts clearly.
    """
    handler = OutlierHandler(dashboard_df, "test_file")
    actions, report = handler.analyze()

    units_action = next(a for a in actions if "units_sold" in a.target_columns)
    assert units_action.action_type == ActionType.FLAG_OUTLIER
    assert "globally unusual but normal for this product group" in units_action.evidence
    assert "Unusual even within their peer group" in units_action.evidence or "unusual even within" in units_action.evidence

    # Check user options
    option_keys = [opt.key for opt in units_action.options]
    assert "flag" in option_keys
    assert "stratified_scale" in option_keys
    # Default is non-destructive flag
    default_opt = next(opt for opt in units_action.options if opt.is_default)
    assert default_opt.key == "flag"


def test_5_fallback_when_no_meaningful_subgroups():
    """
    Verify that when there are no meaningful grouping dimensions (eta_sq < 0.10),
    the handler falls back gracefully to global statistical outlier detection.
    """
    np.random.seed(42)
    n = 100
    df = pd.DataFrame({
        "group": np.random.choice(["GroupA", "GroupB"], size=n),
        "val": np.random.normal(50, 5, size=n),
    })
    # Add an extreme global outlier
    df.loc[0, "val"] = 500.0

    handler = OutlierHandler(df, "test_file")
    actions, report = handler.analyze()

    summ = next(s for s in report.column_summaries if s.column == "val")
    # Groups have nearly identical means/variance, eta_sq is tiny
    # Should fall back gracefully
    assert summ.total_outliers >= 1
    assert summ.likely_errors == 0
