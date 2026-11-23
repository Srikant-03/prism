"""
Tests for Feature Scaling & Transformation Recommender.
Verifies that continuous variables are not destructively binarized due to bimodality alone,
that UI descriptions are consistent, and that explicit domain thresholds/business rules
correctly control binarization.
"""

import os
import numpy as np
import pandas as pd
import pytest

from cleaning.scaling_handler import ScalingHandler
from cleaning.cleaning_models import ActionType


CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")


def test_no_forced_binarization_on_dashboard_csv():
    """
    Continuous variables in dashboard_test_data.csv must not be recommended
    for BINARIZE merely due to bimodal/multimodal distributions or subgroup structures.
    """
    df = pd.read_csv(CSV_PATH)
    handler = ScalingHandler(df, "test_file_id")
    actions, report = handler.analyze()

    recommendations = {a.target_columns[0]: a for a in actions}

    forbidden_binarize_cols = [
        "revenue",
        "cost",
        "profit",
        "units_sold",
        "avg_order_value",
        "customer_satisfaction",
        "response_time_days",
        "discount_pct",
        "prospect_count",
    ]

    for col in forbidden_binarize_cols:
        assert col in recommendations, f"Expected scaling recommendation for {col}"
        action = recommendations[col]
        assert action.action_type != ActionType.BINARIZE, (
            f"Column '{col}' was improperly recommended for BINARIZE! "
            f"Strategy: {action.action_type.value}, reasoning: {action.reasoning}"
        )
        # Check that binarize option is present in user options but NOT default
        binarize_opt = next((opt for opt in action.options if opt.key == "binarize"), None)
        assert binarize_opt is not None, f"'binarize' option should exist for {col}"
        assert not binarize_opt.is_default, f"'binarize' should NOT be default for {col}"


def test_appropriate_non_destructive_scalers_assigned():
    """Verify each column receives an appropriate continuous/ordinal scaler."""
    df = pd.read_csv(CSV_PATH)
    handler = ScalingHandler(df, "test_file_id")
    actions, _ = handler.analyze()
    rec_types = {a.target_columns[0]: a.action_type for a in actions}

    # Financial positive right-skewed columns: Box-Cox or Log1p
    assert rec_types["revenue"] in (ActionType.BOXCOX_TRANSFORM, ActionType.LOG1P_TRANSFORM)
    assert rec_types["cost"] in (ActionType.BOXCOX_TRANSFORM, ActionType.LOG1P_TRANSFORM)
    assert rec_types["profit"] in (ActionType.BOXCOX_TRANSFORM, ActionType.LOG1P_TRANSFORM)
    assert rec_types["avg_order_value"] in (ActionType.BOXCOX_TRANSFORM, ActionType.LOG1P_TRANSFORM)

    # Ordinal rating: MinMaxScaler [0, 1] preserving ordinal spacing
    assert rec_types["customer_satisfaction"] == ActionType.MINMAX_SCALE

    # Symmetric continuous duration: StandardScaler
    assert rec_types["response_time_days"] == ActionType.STANDARD_SCALE

    # Count with extreme tail / outliers: RobustScaler or Log1p
    assert rec_types["prospect_count"] in (ActionType.ROBUST_SCALE, ActionType.LOG1P_TRANSFORM)
    assert rec_types["units_sold"] in (ActionType.ROBUST_SCALE, ActionType.LOG1P_TRANSFORM)


def test_bimodal_distribution_alone_does_not_force_binarize():
    """A synthetic bimodal distribution must not automatically recommend BINARIZE."""
    np.random.seed(42)
    # Mixture of two Gaussians: clearly bimodal
    mode1 = np.random.normal(10, 1, 100)
    mode2 = np.random.normal(25, 1, 100)
    bimodal_data = np.concatenate([mode1, mode2])

    df = pd.DataFrame({"sensor_reading": bimodal_data})
    handler = ScalingHandler(df, "test_file_id")
    actions, _ = handler.analyze()

    assert len(actions) == 1
    action = actions[0]
    assert action.action_type != ActionType.BINARIZE
    assert "binarize" in [opt.key for opt in action.options]
    binarize_opt = next(opt for opt in action.options if opt.key == "binarize")
    assert not binarize_opt.is_default


def test_ui_consistency_description_and_recommendation():
    """
    Fixes the UI inconsistency where operation was BINARIZE but description said 'Scales all values'.
    When BINARIZE is explicitly configured, impact.description must say 'Binarizes ... into 0/1 indicator'.
    When scaling/transforming, it should accurately say 'Scales' or 'Transforms'.
    """
    df = pd.DataFrame({
        "continuous_val": [10.0, 12.0, 15.0, 18.0, 20.0],
        "target_flag": [1.0, 2.0, 3.0, 8.0, 9.0],
    })

    # Case 1: Explicit binarize via domain threshold
    handler_bin = ScalingHandler(
        df, "test_file_id", metadata={"domain_thresholds": {"target_flag": 5.0}}
    )
    actions_bin, _ = handler_bin.analyze()
    act_bin = next(a for a in actions_bin if a.target_columns[0] == "target_flag")

    assert act_bin.action_type == ActionType.BINARIZE
    assert "Binarizes" in act_bin.impact.description
    assert "Scales all" not in act_bin.impact.description
    assert "5.00" in act_bin.recommendation

    # Case 2: Scaling
    act_scale = next(a for a in actions_bin if a.target_columns[0] == "continuous_val")
    assert "Scales all" in act_scale.impact.description or "Transforms all" in act_scale.impact.description


def test_explicit_domain_threshold_and_business_rules():
    """Explicit domain thresholds and business rules correctly trigger binarization."""
    df = pd.DataFrame({
        "customer_satisfaction": [1.0, 2.0, 3.0, 4.0, 5.0, 4.5, 3.5],
        "turnaround_days": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
    })

    # Explicit threshold
    handler1 = ScalingHandler(
        df, "test_file_id",
        metadata={"domain_thresholds": {"customer_satisfaction": 4.0}}
    )
    actions1, _ = handler1.analyze()
    cs_act = next(a for a in actions1 if a.target_columns[0] == "customer_satisfaction")
    assert cs_act.action_type == ActionType.BINARIZE
    assert cs_act.metadata["threshold"] == 4.0

    # Documented business rule
    handler2 = ScalingHandler(
        df, "test_file_id",
        business_rules=["turnaround_days: binarize into SLA breach indicator if threshold > 3"]
    )
    actions2, _ = handler2.analyze()
    ta_act = next(a for a in actions2 if a.target_columns[0] == "turnaround_days")
    assert ta_act.action_type == ActionType.BINARIZE


def test_binary_classification_target_cutoff():
    """Binary classification task with explicit target cutoff triggers binarize for target."""
    df = pd.DataFrame({
        "score": [10.0, 20.0, 30.0, 40.0, 50.0],
        "feature_x": [1.0, 2.0, 3.0, 4.0, 5.0],
    })
    handler = ScalingHandler(
        df, "test_file_id",
        target_column="score",
        task_type="binary_classification",
        metadata={"binary_target_threshold": 25.0}
    )
    actions, _ = handler.analyze()
    score_act = next(a for a in actions if a.target_columns[0] == "score")
    assert score_act.action_type == ActionType.BINARIZE
    assert score_act.metadata["threshold"] == 25.0

    feat_act = next(a for a in actions if a.target_columns[0] == "feature_x")
    assert feat_act.action_type != ActionType.BINARIZE


def test_apply_scaling_binarize_with_threshold():
    """Verify execution of apply_scaling with threshold parameter."""
    df = pd.DataFrame({"val": [1.0, 2.0, 3.0, 4.0, 5.0]})
    transformed = ScalingHandler.apply_scaling(df.copy(), "val", "binarize", threshold=3.5)
    assert transformed["val"].tolist() == [0, 0, 0, 1, 1]

    # Without threshold -> defaults to median (3.0)
    transformed_med = ScalingHandler.apply_scaling(df.copy(), "val", "binarize")
    assert transformed_med["val"].tolist() == [0, 0, 1, 1, 1]
