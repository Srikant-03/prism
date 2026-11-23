"""
Tests for Target Contamination Prevention in Feature Engineering.
Enforces strict X/y separation across PCA, feature clustering, VIF analysis,
correlation-based feature reduction, zero-variance filters, and multivariate outlier detection.
"""

import os
import numpy as np
import pandas as pd
import pytest

from cleaning.feature_selection_handler import FeatureSelectionHandler
from cleaning.outlier_handler import OutlierHandler
from cleaning.decision_engine import DecisionEngine
from cleaning.cleaning_models import ActionType


CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")


def test_pca_excludes_target_and_preserves_target():
    """Verify target variable is excluded from PCA analysis and preserved after apply_pca."""
    df = pd.read_csv(CSV_PATH)
    target = "prospect_count"
    all_numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    assert target in all_numeric

    handler = FeatureSelectionHandler(df, "test_file_id", target_column=target)
    actions, report = handler.analyze()

    # 1. Check report and actions
    pca_info = report.get("pca_suggestion")
    if pca_info:
        # Total components must be len(all_numeric) - 1
        assert pca_info["total_components"] == len(all_numeric) - 1

    pca_actions = [a for a in actions if a.action_type == ActionType.SUGGEST_PCA]
    for act in pca_actions:
        assert target not in act.target_columns
        assert f"{len(all_numeric) - 1} numeric predictor features" in act.evidence

    # 2. apply_pca execution must preserve target
    predictor_cols = [c for c in all_numeric if c != target]
    transformed = FeatureSelectionHandler.apply_pca(
        df.copy(), predictor_cols, n_components=3, target_column=target
    )
    assert target in transformed.columns, f"Target '{target}' was deleted by apply_pca!"
    assert "PC1" in transformed.columns
    assert "PC2" in transformed.columns
    assert "PC3" in transformed.columns

    # 3. Assertion test: passing target into _pca_analysis must raise AssertionError
    with pytest.raises(AssertionError, match=f"Target '{target}' must not enter predictor PCA"):
        handler._pca_analysis(all_numeric)

    # 4. Assertion test: forcing target into apply_pca without target_column must fail
    with pytest.raises(AssertionError, match="must not enter predictor PCA"):
        FeatureSelectionHandler.apply_pca(df.copy(), all_numeric, n_components=3, target_column=target)


def test_vif_analysis_excludes_target():
    """Verify target variable is strictly excluded from predictor VIF analysis."""
    df = pd.read_csv(CSV_PATH)
    target = "prospect_count"
    all_numeric = df.select_dtypes(include=[np.number]).columns.tolist()

    handler = FeatureSelectionHandler(df, "test_file_id", target_column=target)
    actions, report = handler.analyze()

    # VIF report must not contain target
    high_vif = report.get("high_vif_columns", [])
    for item in high_vif:
        assert item["column"] != target, f"Target '{target}' found in VIF report!"

    vif_actions = [a for a in actions if a.action_type == ActionType.DROP_HIGH_VIF]
    for act in vif_actions:
        assert target not in act.target_columns

    # Assertion test: passing target into _high_vif must raise AssertionError
    with pytest.raises(AssertionError, match=f"Target '{target}' must not enter predictor VIF"):
        handler._high_vif(all_numeric)


def test_feature_clustering_excludes_target():
    """Verify target variable is strictly excluded from predictor feature clusters."""
    df = pd.read_csv(CSV_PATH)
    target = "prospect_count"
    all_numeric = df.select_dtypes(include=[np.number]).columns.tolist()

    handler = FeatureSelectionHandler(df, "test_file_id", target_column=target)
    actions, report = handler.analyze()

    clusters = report.get("feature_clusters", [])
    for cluster in clusters:
        assert target not in cluster["members"], f"Target '{target}' clustered with features!"
        assert cluster["representative"] != target
        assert target not in cluster["members_to_drop"]

    cluster_actions = [a for a in actions if a.action_type == ActionType.CLUSTER_FEATURES]
    for act in cluster_actions:
        assert target not in act.target_columns

    # Assertion test: passing target into _feature_clustering must raise AssertionError
    with pytest.raises(AssertionError, match=f"Target '{target}' must not enter feature clustering"):
        handler._feature_clustering(all_numeric)


def test_high_correlation_does_not_drop_target_or_predictors():
    """
    Supervised correlation between a feature and target is valid for feature ranking,
    not for unsupervised dropping. High correlation with y must not recommend dropping y or x.
    """
    np.random.seed(42)
    y = np.random.randn(100)
    # x1 is highly correlated with y (r = 0.99)
    x1 = y + np.random.normal(0, 0.05, 100)
    x2 = np.random.randn(100)
    x3 = np.random.randn(100)

    df = pd.DataFrame({"feat_1": x1, "feat_2": x2, "feat_3": x3, "target_y": y})

    handler = FeatureSelectionHandler(
        df, "test_file_id", target_column="target_y", correlation_threshold=0.90
    )
    actions, report = handler.analyze()

    # Verify no action recommends dropping target_y or feat_1 because of correlation with target_y
    for act in actions:
        if act.action_type == ActionType.DROP_HIGH_CORRELATION:
            assert "target_y" not in act.target_columns, "Target was marked for dropping by correlation!"
            assert "feat_1" not in act.target_columns, "Predictor correlating with target was dropped as redundant!"

    # Assertion test: passing target into _high_correlation must raise AssertionError
    with pytest.raises(AssertionError, match="must not enter predictor correlation analysis"):
        handler._high_correlation(["feat_1", "feat_2", "feat_3", "target_y"])


def test_multivariate_outliers_excludes_target():
    """Multivariate outlier detection for predictor selection must run strictly on X, not X + y."""
    df = pd.read_csv(CSV_PATH)
    target = "prospect_count"

    handler = OutlierHandler(df, "test_file_id", target_column=target)
    assert target not in handler.predictor_numeric_cols

    actions, report = handler.analyze()

    mv_actions = [a for a in actions if a.action_type == ActionType.FLAG_OUTLIER and "multivariate" in a.evidence.lower()]
    for act in mv_actions:
        assert target not in act.target_columns, f"Target '{target}' entered multivariate predictor columns!"
        assert f"{len(handler.predictor_numeric_cols)} numeric predictor columns" in act.recommendation

    # Assertion test: if target is forced into predictor_numeric_cols, _multivariate_detect must raise AssertionError
    handler.predictor_numeric_cols.append(target)
    with pytest.raises(AssertionError, match=f"Target '{target}' must not enter predictor multivariate"):
        handler._multivariate_detect()


def test_zero_and_near_zero_variance_guards_target():
    """Feature selection must never recommend dropping the target, even if target has low variance."""
    df = pd.DataFrame({
        "constant_feature": [1, 1, 1, 1, 1],
        "near_constant_feature": [1, 1, 1, 1, 2],
        "target_col": [0, 0, 0, 0, 0],  # Constant target
    })

    handler = FeatureSelectionHandler(df, "test_file_id", target_column="target_col")
    actions, report = handler.analyze()

    # Constant feature should be in report
    assert "constant_feature" in report["zero_variance"]
    # Target MUST NOT be in zero_variance drop report
    assert "target_col" not in report["zero_variance"]

    for act in actions:
        assert "target_col" not in act.target_columns, f"Target 'target_col' was marked for drop in {act.action_type}"


def test_decision_engine_end_to_end_x_y_separation():
    """End-to-end test on dashboard_test_data.csv with prospect_count as target."""
    df = pd.read_csv(CSV_PATH)
    target = "prospect_count"

    engine = DecisionEngine(df, "test_file_id", target_column=target)
    plan = engine.analyze()

    # Zero actions must recommend dropping the target
    for act in plan.actions:
        if act.action_type in (
            ActionType.DROP_ZERO_VARIANCE,
            ActionType.DROP_NEAR_ZERO_VARIANCE,
            ActionType.DROP_HIGH_CORRELATION,
            ActionType.DROP_HIGH_VIF,
            ActionType.DROP_LOW_MI,
            ActionType.CLUSTER_FEATURES,
        ):
            assert target not in act.target_columns, (
                f"Target '{target}' was targeted for deletion in action {act.action_type}!"
            )

    # Verify PCA command execution via decision engine preserves target
    pca_action = next((a for a in plan.actions if a.action_type == ActionType.SUGGEST_PCA), None)
    if pca_action:
        df_pca = engine._cmd_suggest_pca(df.copy(), pca_action, "pca_95")
        assert target in df_pca.columns, f"Target '{target}' was lost during PCA execution!"
        assert "PC1" in df_pca.columns
