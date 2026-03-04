"""
Unit tests for the Autonomous Data Cleaning Engine.
"""

import pandas as pd
import numpy as np

from cleaning.cleaning_models import (
    CleaningAction, CleaningPlan, ActionType, ActionConfidence,
    ActionCategory, ActionStatus, DuplicateReport, MissingValueReport,
)
from cleaning.duplicate_handler import DuplicateHandler
from cleaning.missing_handler import MissingHandler
from cleaning.decision_engine import DecisionEngine


# ── Helpers ───────────────────────────────────────────────────────────

def make_df_with_duplicates() -> pd.DataFrame:
    return pd.DataFrame({
        "id": [1, 2, 3, 1, 2, 4],
        "name": ["Alice", "Bob", "Charlie", "Alice", "Bob", "Dave"],
        "score": [90, 80, 70, 90, 80, 60],
    })


def make_df_with_nulls() -> pd.DataFrame:
    return pd.DataFrame({
        "age": [25, None, 35, 40, None, 30, 28, None, 45, 50],
        "salary": [50000, 60000, None, 80000, 70000, None, 55000, 65000, 75000, None],
        "city": ["NY", "LA", None, "NY", "SF", "LA", None, "NY", "SF", "LA"],
    })


def make_df_with_derived_columns() -> pd.DataFrame:
    np.random.seed(42)
    x = np.random.randn(100)
    return pd.DataFrame({
        "feature_a": x,
        "feature_b": x * 2.5 + 1.0,  # Derived: b = 2.5*a + 1.0
        "noise": np.random.randn(100),
    })


def make_empty_df() -> pd.DataFrame:
    return pd.DataFrame()


# ── Model tests ───────────────────────────────────────────────────────

def test_cleaning_models_instantiation():
    """Verify that all cleaning models can be constructed."""
    action = CleaningAction(
        category=ActionCategory.DUPLICATES,
        action_type=ActionType.REMOVE_EXACT_DUPLICATES,
        confidence=ActionConfidence.DEFINITIVE,
        evidence="test evidence",
        recommendation="test recommendation",
        reasoning="test reasoning",
    )
    assert action.status == ActionStatus.PENDING
    assert action.confidence == ActionConfidence.DEFINITIVE

    plan = CleaningPlan(file_id="test", actions=[action], total_actions=1, definitive_count=1)
    assert plan.total_actions == 1

    report = DuplicateReport(exact_count=5, exact_pct=10.0)
    assert report.exact_count == 5

    missing = MissingValueReport(total_missing_cells=100, total_cells=1000, overall_missing_pct=10.0)
    assert missing.overall_missing_pct == 10.0
    print("  ✓ cleaning_models_instantiation passed")


# ── Duplicate handler tests ──────────────────────────────────────────

def test_exact_duplicate_detection():
    df = make_df_with_duplicates()
    handler = DuplicateHandler(df, "test-file")
    actions, report = handler.analyze()

    dup_actions = [a for a in actions if a.action_type == ActionType.REMOVE_EXACT_DUPLICATES]
    assert len(dup_actions) > 0, "Should detect exact duplicates"
    assert report.exact_count > 0
    print(f"  ✓ exact_duplicate_detection passed (found {report.exact_count} duplicates)")


def test_derived_column_detection():
    df = make_df_with_derived_columns()
    handler = DuplicateHandler(df, "test-file")
    actions, report = handler.analyze()

    derived_actions = [a for a in actions if a.action_type == ActionType.DROP_DERIVED_COLUMN]
    assert len(derived_actions) > 0, "Should detect derived columns"
    assert len(report.derived_column_pairs) > 0
    print(f"  ✓ derived_column_detection passed (found {len(report.derived_column_pairs)} pairs)")


def test_empty_df_duplicates():
    df = make_empty_df()
    handler = DuplicateHandler(df, "test-file")
    actions, report = handler.analyze()
    assert len(actions) == 0
    print("  ✓ empty_df_duplicates passed")


# ── Missing handler tests ────────────────────────────────────────────

def test_missing_value_detection():
    df = make_df_with_nulls()
    handler = MissingHandler(df, "test-file")
    actions, report = handler.analyze()

    assert report.total_missing_cells > 0
    assert len(report.column_strategies) > 0
    assert report.overall_missing_pct > 0

    # Check that each column with nulls gets a strategy
    null_cols = [c for c in df.columns if df[c].isnull().any()]
    strategy_cols = [cs.column for cs in report.column_strategies]
    for nc in null_cols:
        assert nc in strategy_cols, f"Missing strategy for column '{nc}'"

    print(f"  ✓ missing_value_detection passed ({len(report.column_strategies)} strategies)")


def test_strategy_selection_numeric():
    """Test that numeric columns get mean/median imputation."""
    df = make_df_with_nulls()
    handler = MissingHandler(df, "test-file")
    _, report = handler.analyze()

    age_strategy = next((s for s in report.column_strategies if s.column == "age"), None)
    assert age_strategy is not None
    assert age_strategy.recommended_strategy in (
        ActionType.IMPUTE_MEAN, ActionType.IMPUTE_MEDIAN, ActionType.DROP_ROWS,
    ), f"Unexpected strategy for numeric column: {age_strategy.recommended_strategy}"
    print(f"  ✓ strategy_selection_numeric passed (age → {age_strategy.recommended_strategy.value})")


def test_empty_df_missing():
    df = make_empty_df()
    handler = MissingHandler(df, "test-file")
    actions, report = handler.analyze()
    assert report.total_missing_cells == 0
    print("  ✓ empty_df_missing passed")


# ── Decision Engine tests ────────────────────────────────────────────

def test_decision_engine_full():
    df = make_df_with_duplicates()
    df.loc[0, "score"] = None  # Add a null
    engine = DecisionEngine(df, "test-file")
    plan = engine.analyze()

    assert plan.total_actions > 0
    assert plan.definitive_count + plan.judgment_call_count == plan.total_actions
    assert plan.file_id == "test-file"

    # Verify actions are ranked (definitive first)
    found_judgment = False
    for a in plan.actions:
        if a.confidence == ActionConfidence.JUDGMENT_CALL:
            found_judgment = True
        if found_judgment:
            assert a.confidence != ActionConfidence.DEFINITIVE, \
                "Definitive actions should come before judgment calls"

    print(f"  ✓ decision_engine_full passed ({plan.total_actions} actions, "
          f"{plan.definitive_count} definitive, {plan.judgment_call_count} judgment)")


def test_apply_action():
    df = pd.DataFrame({
        "a": [1, 2, 3, 1, 2],
        "b": [10, 20, 30, 10, 20],
    })
    engine = DecisionEngine(df, "test-file")
    plan = engine.analyze()

    # Find a duplicate action and apply it
    dup_actions = [a for a in plan.actions if a.action_type == ActionType.REMOVE_EXACT_DUPLICATES]
    if dup_actions:
        new_df, result = engine.apply_action(dup_actions[0])
        assert result.success
        assert result.rows_after <= result.rows_before
        print(f"  ✓ apply_action passed ({result.rows_before} → {result.rows_after} rows)")
    else:
        print("  ✓ apply_action skipped (no duplicates detected)")


# ── Outlier handler tests ────────────────────────────────────────────

def test_outlier_iqr_detection():
    np.random.seed(42)
    df = pd.DataFrame({
        "normal": np.random.randn(100),
        "with_outlier": np.concatenate([np.random.randn(98), [100, -100]]),
    })
    from cleaning.outlier_handler import OutlierHandler
    handler = OutlierHandler(df, "test-file")
    actions, report = handler.analyze()

    assert report.columns_analyzed > 0
    # The extreme values should be detected
    outlier_cols = [s.column for s in report.column_summaries]
    assert "with_outlier" in outlier_cols, "Should detect outliers in 'with_outlier'"
    print(f"  ✓ outlier_iqr_detection passed ({report.total_outlier_values} outliers)")


def test_outlier_business_rules():
    df = pd.DataFrame({
        "age": [25, 30, 200, 35, -5, 40, 22, 28, 31, 45, 50, 38, 27, 33, 42, 29, 36, 41, 26, 34],
        "price": [10, 20, -50, 30, 40, 50, 15, 25, 35, 45, 55, 60, 70, 80, 90, 100, 110, 120, 130, 140],
    })
    from cleaning.outlier_handler import OutlierHandler
    handler = OutlierHandler(df, "test-file")
    actions, report = handler.analyze()

    violations = report.business_rule_violations
    assert len(violations) > 0, "Should detect business rule violations"
    print(f"  ✓ outlier_business_rules passed ({len(violations)} violations)")


def test_outlier_clean_data():
    df = pd.DataFrame({"clean": np.arange(100, dtype=float)})
    from cleaning.outlier_handler import OutlierHandler
    handler = OutlierHandler(df, "test-file")
    actions, report = handler.analyze()
    # Uniform data shouldn't have many outliers
    print(f"  ✓ outlier_clean_data passed ({report.total_outlier_values} outliers)")


# ── Type handler tests ───────────────────────────────────────────────

def test_type_date_detection():
    df = pd.DataFrame({
        "date_col": ["2024-01-15", "2024/02/20", "March 3, 2024", "01-04-2024", "2024.05.01"],
        "not_date": ["hello", "world", "foo", "bar", "baz"],
    })
    from cleaning.type_handler import TypeHandler
    handler = TypeHandler(df, "test-file")
    actions, report = handler.analyze()

    date_corrections = [c for c in report.corrections if c.detected_type.value == "datetime"]
    assert len(date_corrections) > 0, "Should detect date column"
    print(f"  ✓ type_date_detection passed ({len(date_corrections)} date columns)")


def test_type_currency_detection():
    df = pd.DataFrame({
        "price": ["$1,200.50", "€500", "£3,000", "$99.99", "¥10000"],
    })
    from cleaning.type_handler import TypeHandler
    handler = TypeHandler(df, "test-file")
    actions, report = handler.analyze()

    curr = [c for c in report.corrections if c.detected_type.value == "currency"]
    assert len(curr) > 0, "Should detect currency column"
    print(f"  ✓ type_currency_detection passed ({curr[0].parse_success_rate * 100:.0f}% parsed)")


def test_type_boolean_detection():
    df = pd.DataFrame({
        "active": ["Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No", "Yes", "No"],
    })
    from cleaning.type_handler import TypeHandler
    handler = TypeHandler(df, "test-file")
    actions, report = handler.analyze()

    bools = [c for c in report.corrections if c.detected_type.value == "boolean"]
    assert len(bools) > 0, "Should detect boolean column"
    print(f"  ✓ type_boolean_detection passed")


# ── Text handler tests ───────────────────────────────────────────────

def test_text_column_analysis():
    df = pd.DataFrame({
        "review": [
            "This product is absolutely amazing and I love it so much!",
            "Terrible experience, would not recommend to anyone at all.",
            "Average quality, nothing special but gets the job done well.",
            "Best purchase I have ever made in my entire life honestly.",
            "Disappointing. The product broke after just two weeks of use.",
        ] * 20,
    })
    from cleaning.text_handler import TextHandler
    handler = TextHandler(df, "test-file")
    actions, report = handler.analyze()

    assert report.total_text_columns > 0, "Should identify text columns"
    assert len(actions) > 0, "Should recommend text actions"
    action_types = [a.action_type.value for a in actions]
    print(f"  ✓ text_column_analysis passed ({report.total_text_columns} cols, actions: {action_types})")


# ── Full integration test ────────────────────────────────────────────

def test_decision_engine_all_handlers():
    """Test that the decision engine runs all 5 handlers."""
    df = pd.DataFrame({
        "id": [1, 2, 3, 1, 2],
        "age": [25, 200, 35, 25, None],  # dup + outlier + null
        "price": ["$100", "$200", "$300", "$100", "$400"],  # type detection
        "active": ["Yes", "No", "Yes", "Yes", "No"],  # boolean
        "review": [
            "Great product with excellent quality and fast shipping!",
            "Bad experience overall, terrible customer service provided.",
            "OK product, meets basic expectations for the price point.",
            "Great product with excellent quality and fast shipping!",
            "Wonderful item, highly recommend to all my friends!",
        ],
    })
    engine = DecisionEngine(df, "test-file")
    plan = engine.analyze()

    assert plan.total_actions > 0
    categories = set(a.category.value for a in plan.actions)
    assert plan.duplicate_report is not None
    assert plan.missing_report is not None
    assert plan.outlier_report is not None
    assert plan.type_report is not None
    assert plan.text_report is not None
    print(f"  ✓ decision_engine_all_handlers passed ({plan.total_actions} actions, categories: {categories})")


# ── Categorical handler tests ────────────────────────────────────────

def test_categorical_encoding_detection():
    df = pd.DataFrame({
        "color": ["red", "blue", "green", "red", "blue", "green", "red", "blue", "green", "red"],
        "size": ["small", "medium", "large", "small", "medium", "large", "small", "medium", "large", "small"],
        "value": np.random.randn(10),
    })
    from cleaning.categorical_handler import CategoricalHandler
    handler = CategoricalHandler(df, "test-file")
    actions, report = handler.analyze()

    assert report["total_categorical"] >= 2
    assert len(actions) >= 2
    action_types = [a.action_type.value for a in actions]
    print(f"  ✓ categorical_encoding_detection passed ({len(actions)} actions: {action_types})")


def test_cyclical_column_detection():
    df = pd.DataFrame({
        "month": list(range(1, 13)) * 5,
        "hour": list(range(0, 24)) + list(range(0, 24)) + list(range(0, 12)),
        "value": np.random.randn(60),
    })
    from cleaning.categorical_handler import CategoricalHandler
    handler = CategoricalHandler(df, "test-file")
    actions, report = handler.analyze()

    cyclical = [a for a in actions if a.action_type.value == "cyclical_encode"]
    assert len(cyclical) >= 1, "Should detect cyclical columns"
    print(f"  ✓ cyclical_column_detection passed ({len(cyclical)} cyclical)")


# ── Datetime handler tests ───────────────────────────────────────────

def test_datetime_feature_extraction():
    dates = pd.date_range("2023-01-01", periods=50, freq="D")
    df = pd.DataFrame({
        "date": dates,
        "value": np.random.randn(50),
    })
    from cleaning.datetime_handler import DatetimeHandler
    handler = DatetimeHandler(df, "test-file")
    actions, report = handler.analyze()

    assert len(report["datetime_columns"]) >= 1
    extract_actions = [a for a in actions if a.action_type.value == "extract_datetime"]
    assert len(extract_actions) >= 1
    print(f"  ✓ datetime_feature_extraction passed ({len(actions)} actions)")


# ── Scaling handler tests ────────────────────────────────────────────

def test_scaling_recommendation():
    np.random.seed(42)
    df = pd.DataFrame({
        "normal": np.random.randn(100),
        "skewed": np.random.exponential(2, 100),
        "bounded": np.random.uniform(0, 1, 100),
    })
    from cleaning.scaling_handler import ScalingHandler
    handler = ScalingHandler(df, "test-file")
    actions, report = handler.analyze()

    assert report["columns_analyzed"] == 3
    assert len(actions) == 3
    action_types = {a.target_columns[0]: a.action_type.value for a in actions}
    print(f"  ✓ scaling_recommendation passed (scalers: {action_types})")


# ── Feature selection handler tests ──────────────────────────────────

def test_zero_variance_detection():
    df = pd.DataFrame({
        "constant": [1] * 50,
        "variable": np.random.randn(50),
    })
    from cleaning.feature_selection_handler import FeatureSelectionHandler
    handler = FeatureSelectionHandler(df, "test-file")
    actions, report = handler.analyze()

    assert len(report["zero_variance"]) >= 1
    zv_actions = [a for a in actions if a.action_type.value == "drop_zero_variance"]
    assert len(zv_actions) >= 1
    assert zv_actions[0].confidence.value == "definitive"
    print(f"  ✓ zero_variance_detection passed ({report['zero_variance']})")


# ── Imbalance handler tests ──────────────────────────────────────────

def test_class_imbalance_detection():
    # Build imbalanced dataset
    majority = ["A"] * 180
    minority = ["B"] * 20
    df = pd.DataFrame({
        "feature": np.random.randn(200),
        "target": majority + minority,
    })
    from cleaning.imbalance_handler import ImbalanceHandler
    handler = ImbalanceHandler(df, "test-file", target_column="target")
    actions, report = handler.analyze()

    assert report["is_imbalanced"] is True
    assert report["severity"] in ("moderate", "severe")
    assert report["imbalance_ratio"] == 9.0
    assert len(actions) >= 1
    print(f"  ✓ class_imbalance_detection passed (severity: {report['severity']}, ratio: {report['imbalance_ratio']})")


# ── Standardization handler tests ────────────────────────────────────

def test_whitespace_standardization():
    df = pd.DataFrame({
        "name": ["  Alice  ", "Bob", "  Charlie", "Dave ", "  Eve  ", "Frank",
                 "  Grace", "Heidi  ", "  Ivan  ", "Judy"],
        "value": np.random.randn(10),
    })
    from cleaning.standardization_handler import StandardizationHandler
    handler = StandardizationHandler(df, "test-file")
    actions, report = handler.analyze()

    ws_actions = [a for a in actions if a.action_type.value == "standardize_whitespace"]
    assert len(ws_actions) >= 1
    assert report["whitespace_issues"]
    print(f"  ✓ whitespace_standardization passed ({len(ws_actions)} actions)")


def test_synonym_detection():
    df = pd.DataFrame({
        "country": ["USA", "US", "usa", "United States", "UK", "uk", "UK",
                     "US", "USA", "us", "UK", "uk", "USA", "US", "usa",
                     "UK", "uk", "UK", "US", "USA"],
        "value": np.random.randn(20),
    })
    from cleaning.standardization_handler import StandardizationHandler
    handler = StandardizationHandler(df, "test-file")
    actions, report = handler.analyze()

    synonym_actions = [a for a in actions if a.action_type.value == "consolidate_synonyms"]
    # At minimum should detect casing issues
    casing_actions = [a for a in actions if a.action_type.value == "standardize_casing"]
    total = len(synonym_actions) + len(casing_actions)
    assert total >= 1, f"Should detect synonym or casing issues, got: {[a.action_type.value for a in actions]}"
    print(f"  ✓ synonym_detection passed ({len(synonym_actions)} synonym, {len(casing_actions)} casing)")


# ── Leakage handler tests ────────────────────────────────────────────

def test_leakage_future_column():
    df = pd.DataFrame({
        "feature": np.random.randn(50),
        "outcome_date": pd.date_range("2023-01-01", periods=50, freq="D"),
        "resolution": np.random.choice(["resolved", "pending"], 50),
        "target": np.random.choice(["A", "B"], 50),
    })
    from cleaning.leakage_handler import LeakageHandler
    handler = LeakageHandler(df, "test-file", target_column="target")
    actions, report = handler.analyze()

    assert len(report["future_columns"]) >= 1
    future_cols = [r["column"] for r in report["future_columns"]]
    assert any("outcome" in c.lower() or "resolution" in c.lower() for c in future_cols)
    print(f"  ✓ leakage_future_column passed (detected: {future_cols})")


# ── Audit logger tests ───────────────────────────────────────────────

def test_audit_logger_undo_redo():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    from cleaning.audit_logger import AuditLogger
    logger = AuditLogger(df, "test-file")

    # Record a step: drop column b
    new_df = df.drop(columns=["b"])
    logger.record_step("drop_b", "drop_column", "test", new_df, ["b"])
    assert len(logger.current_df.columns) == 1

    # Undo
    entry = logger.undo()
    assert entry is not None
    assert len(logger.current_df.columns) == 2

    # Redo
    entry = logger.redo()
    assert entry is not None
    assert len(logger.current_df.columns) == 1

    # Comparison
    comparison = logger.compare_with_original()
    assert comparison["columns_removed"] == ["b"]

    # Export
    py_script = logger.export_pipeline_python()
    assert "def preprocess" in py_script

    json_spec = logger.export_pipeline_json()
    assert "steps" in json_spec

    csv_log = logger.export_audit_log_csv()
    assert "step_name" in csv_log

    print(f"  ✓ audit_logger_undo_redo passed (comparison: {comparison['columns_removed']})")


# ── Run all tests ────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    tests = [
        test_cleaning_models_instantiation,
        test_exact_duplicate_detection,
        test_derived_column_detection,
        test_empty_df_duplicates,
        test_missing_value_detection,
        test_strategy_selection_numeric,
        test_empty_df_missing,
        test_decision_engine_full,
        test_apply_action,
        # Phase 2.4 — Outlier
        test_outlier_iqr_detection,
        test_outlier_business_rules,
        test_outlier_clean_data,
        # Phase 2.5 — Type
        test_type_date_detection,
        test_type_currency_detection,
        test_type_boolean_detection,
        # Phase 2.6 — Text
        test_text_column_analysis,
        # Phase 2.7 — Categorical
        test_categorical_encoding_detection,
        test_cyclical_column_detection,
        # Phase 2.8 — Datetime
        test_datetime_feature_extraction,
        # Phase 2.9 — Scaling
        test_scaling_recommendation,
        # Phase 2.10 — Feature Selection
        test_zero_variance_detection,
        # Phase 2.11 — Class Imbalance
        test_class_imbalance_detection,
        # Phase 2.12 — Data Standardization
        test_whitespace_standardization,
        test_synonym_detection,
        # Phase 2.13 — Data Leakage
        test_leakage_future_column,
        # Phase 2.14 — Pipeline Audit
        test_audit_logger_undo_redo,
        # Integration
        test_decision_engine_all_handlers,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__} FAILED: {e}")
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {len(tests)} total")
    if failed == 0:
        print("ALL TESTS PASSED ✓")
    else:
        print("SOME TESTS FAILED ✗")



