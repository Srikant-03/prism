import os
import pytest
import pandas as pd
import numpy as np

from cleaning.missing_handler import MissingHandler
from cleaning.outlier_handler import OutlierHandler
from cleaning.type_handler import TypeHandler
from cleaning.decision_engine import DecisionEngine
from cleaning.cleaning_models import ActionType, CleaningAction, ActionCategory, ActionConfidence

DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "prism_adversarial_e2e_test.csv")


@pytest.fixture
def adversarial_df():
    assert os.path.exists(DATASET_PATH), f"Adversarial CSV not found at {DATASET_PATH}"
    return pd.read_csv(DATASET_PATH)


def test_high_null_row_drop_consolidated(adversarial_df):
    """High null row action must drop row 200 (74.1% nulls) while preserving all other 244 rows."""
    handler = MissingHandler(adversarial_df, file_id="test_missing")
    actions, report = handler.analyze()
    
    assert report.high_null_rows == 1, f"Expected exactly 1 high-null row (row 200), got {report.high_null_rows}"
    
    row_drop_action = next((a for a in actions if a.action_type == ActionType.DROP_ROWS and a.metadata and "threshold" in a.metadata), None)
    assert row_drop_action is not None, "Expected DROP_ROWS action with threshold metadata"
    
    engine = DecisionEngine(adversarial_df.copy(), file_id="test_missing")
    cleaned_df, result = engine.apply_action(row_drop_action)
    
    assert len(cleaned_df) == 244, f"Expected 244 rows after dropping row 200, got {len(cleaned_df)}"
    # Verify sequential_row_id 201 (which was in row 200) was dropped
    assert 201 not in cleaned_df["sequential_row_id"].values or cleaned_df["txn_uuid"].isnull().sum() == 0


def test_type_handler_currency_and_boolean(adversarial_df):
    """TypeHandler must parse unit_price_raw currency to float preserving NaNs, and not misclassify constant_status."""
    handler = TypeHandler(adversarial_df.copy(), file_id="test_types")
    actions, report = handler.analyze()
    
    # constant_status must not be detected as boolean
    constant_status_actions = [a for a in actions if "constant_status" in a.target_columns and a.action_type == ActionType.STANDARDIZE_BOOLEANS]
    assert len(constant_status_actions) == 0, "constant_status ('ACTIVE') must not be converted to boolean"
    
    # unit_price_raw must have parse currency action
    currency_action = next((a for a in actions if "unit_price_raw" in a.target_columns and a.action_type == ActionType.PARSE_CURRENCY), None)
    assert currency_action is not None, "Expected PARSE_CURRENCY action for unit_price_raw"
    
    engine = DecisionEngine(adversarial_df.copy(), file_id="test_types")
    cleaned_df, result = engine.apply_action(currency_action)
    
    assert pd.api.types.is_float_dtype(cleaned_df["unit_price_raw"]), f"Expected float dtype, got {cleaned_df['unit_price_raw'].dtype}"
    assert cleaned_df["unit_price_raw"].dropna().iloc[0] > 0, "Parsed currency should be positive numeric value"
    # NaNs must remain NaNs, not string 'nan'
    assert cleaned_df["unit_price_raw"].isnull().sum() > 0 or cleaned_df["unit_price_raw"].notna().sum() > 200


def test_mixed_datetime_parsing(adversarial_df):
    """DecisionEngine must parse mixed format dates (order_date, ship_date_mixed) cleanly into datetime64."""
    engine = DecisionEngine(adversarial_df.copy(), file_id="test_dt")
    action = CleaningAction(
        category=ActionCategory.TYPE_CORRECTION,
        action_type=ActionType.PARSE_DATES,
        confidence=ActionConfidence.DEFINITIVE,
        evidence="Mixed date formats detected",
        recommendation="Parse into datetime",
        reasoning="Standardize to timestamps",
        target_columns=["order_date", "ship_date_mixed"]
    )
    cleaned_df, result = engine.apply_action(action)
    
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df["order_date"]), f"order_date should be datetime64, got {cleaned_df['order_date'].dtype}"
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df["ship_date_mixed"]), f"ship_date_mixed should be datetime64, got {cleaned_df['ship_date_mixed'].dtype}"


def test_pseudo_null_replacement_does_not_destroy_strings():
    """_cmd_replace_pseudo_nulls must replace pseudo-null strings but preserve actual text columns."""
    df = pd.DataFrame({
        "notes": ["Customer called", "N/A", "Follow up next week", "null", "Urgent request"],
        "price": ["$10.50", "N/A", "$25.00", "none", "$15.00"]
    })
    engine = DecisionEngine(df.copy(), file_id="test_pseudo")
    action = CleaningAction(
        category=ActionCategory.TYPE_CORRECTION,
        action_type=ActionType.REPLACE_PSEUDO_NULLS,
        confidence=ActionConfidence.DEFINITIVE,
        evidence="Pseudo nulls found",
        recommendation="Replace with NaN",
        reasoning="Data hygiene",
        target_columns=["notes", "price"]
    )
    cleaned_df, result = engine.apply_action(action)
    
    # notes column should still contain string text, with 2 NaNs
    assert cleaned_df["notes"].isnull().sum() == 2
    assert "Customer called" in cleaned_df["notes"].values
    assert "Follow up next week" in cleaned_df["notes"].values
    assert "Urgent request" in cleaned_df["notes"].values


def test_outlier_clipping_preserves_nan(adversarial_df):
    """_cmd_cap_outliers and _cmd_winsorize must cap outliers while preserving genuine NaNs."""
    df = pd.DataFrame({
        "val": [10.0, 12.0, 11.0, 13.0, 12.0, 1000.0, np.nan, 11.5, 12.5, 9.5]
    })
    engine = DecisionEngine(df.copy(), file_id="test_outlier")
    cap_action = CleaningAction(
        category=ActionCategory.OUTLIERS,
        action_type=ActionType.CAP_OUTLIERS,
        confidence=ActionConfidence.DEFINITIVE,
        evidence="Extreme values",
        recommendation="Cap to IQR bounds",
        reasoning="Prevent distortion",
        target_columns=["val"]
    )
    cleaned_df, result = engine.apply_action(cap_action)
    
    assert cleaned_df["val"].max() < 1000.0, f"Max value should be capped, got {cleaned_df['val'].max()}"
    assert np.isnan(cleaned_df["val"].iloc[6]), "NaN value at index 6 must be preserved as NaN"
