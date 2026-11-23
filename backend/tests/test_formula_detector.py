"""
tests/test_formula_detector.py
==============================
Unit tests for deterministic formula and mathematical dependency detection.
Verifies:
1. profit ≈ revenue - cost is detected in dashboard_test_data.csv.
2. Canonical reporting: "`profit` is deterministically derived from `revenue` and `cost`."
3. Target interpretation:
   - target != profit: classified primarily as redundancy / mathematical dependency (never data leakage).
   - target == profit: classified as direct target leakage.
4. Detection of simple relationships:
   - A = B + C
   - A = B - C
   - A = B * C
   - A = B / C
   - Percentage formulas (A = B * C / 100, A = B * (1 - C / 100))
5. Integration with LeakageHandler, FeatureSelectionHandler, and RelationshipClassifier.
"""

import os
import numpy as np
import pandas as pd
import pytest

from profiling.formula_detector import (
    detect_formula_dependencies,
    detect_three_variable_formulas,
    DeterministicFormula,
)
from insights.relationship_classifier import (
    RelationshipClassifier,
    RelationshipType,
)
from cleaning.leakage_handler import LeakageHandler
from cleaning.feature_selection_handler import FeatureSelectionHandler


# ─────────────────────────────────────────────────────────────────────────────
# 1. Tests on dashboard_test_data.csv
# ─────────────────────────────────────────────────────────────────────────────

def test_profit_revenue_cost_detection_in_dashboard_data():
    """Detect profit = revenue - cost on dashboard_test_data.csv."""
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")
    df = pd.read_csv(csv_path)

    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1

    profit_formula = next((f for f in formulas if f.dependent == "profit"), None)
    assert profit_formula is not None
    assert sorted(profit_formula.operands) == ["cost", "revenue"]
    assert profit_formula.operation == "subtraction"
    assert profit_formula.match_ratio >= 0.99
    # Exact canonical description requested by user
    assert profit_formula.description == "`profit` is deterministically derived from `revenue` and `cost`."


def test_target_not_profit_classified_as_mathematical_dependency():
    """When target != profit (e.g. target == prospect_count), classify as redundancy / mathematical dependency."""
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")
    df = pd.read_csv(csv_path)

    formulas = detect_formula_dependencies(df, target_column="prospect_count")
    profit_formula = next((f for f in formulas if f.dependent == "profit"), None)
    assert profit_formula is not None

    # Classify formula with target != profit
    rel = RelationshipClassifier.classify_formula(profit_formula, target_column="prospect_count")
    assert rel.type == RelationshipType.MATHEMATICAL_DEPENDENCY
    assert rel.category == "Deterministic Redundancy"
    assert "data leakage" not in rel.description.lower()
    assert rel.description == "`profit` is deterministically derived from `revenue` and `cost`."

    # LeakageHandler MUST NOT flag profit, revenue, cost as target leakage
    lh = LeakageHandler(df, file_id="f1", target_column="prospect_count")
    actions, report = lh.analyze()
    formula_leaks = report.get("formula_target_leakage", [])
    assert len(formula_leaks) == 0

    # FeatureSelectionHandler flags it as redundancy
    fsh = FeatureSelectionHandler(df, file_id="f1", target_column="prospect_count")
    fs_actions, fs_report = fsh.analyze()
    math_deps = fs_report.get("mathematical_dependencies", [])
    assert any(m["dependent"] == "profit" for m in math_deps)


def test_target_equals_profit_classified_as_direct_target_leakage():
    """When target == profit, using revenue and cost to predict profit is direct target leakage."""
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard_test_data.csv")
    df = pd.read_csv(csv_path)

    formulas = detect_formula_dependencies(df, target_column="profit")
    profit_formula = next((f for f in formulas if f.dependent == "profit"), None)
    assert profit_formula is not None

    # RelationshipClassifier check
    rel = RelationshipClassifier.classify_formula(profit_formula, target_column="profit")
    assert rel.type == RelationshipType.TARGET_LEAKAGE
    assert rel.category == "Data Leakage"
    assert rel.severity == "Critical"
    assert "direct target leakage" in rel.description.lower()

    # LeakageHandler check
    lh = LeakageHandler(df, file_id="f1", target_column="profit")
    actions, report = lh.analyze()
    formula_leaks = report.get("formula_target_leakage", [])
    assert len(formula_leaks) >= 1
    fl = formula_leaks[0]
    assert fl["dependent"] == "profit"
    assert sorted(fl["operands"]) == ["cost", "revenue"]
    assert "direct target leakage" in fl["reasoning"].lower()


# ─────────────────────────────────────────────────────────────────────────────
# 2. Simple relationship detection tests
# ─────────────────────────────────────────────────────────────────────────────

def test_detect_addition_formula():
    """Test A = B + C."""
    np.random.seed(42)
    b = np.random.uniform(10, 100, size=50)
    c = np.random.uniform(5, 50, size=50)
    a = b + c + np.random.normal(0, 1e-5, size=50)  # floating point noise

    df = pd.DataFrame({"total": a, "subtotal": b, "tax": c})
    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1
    f = formulas[0]
    assert f.dependent == "total"
    assert sorted(f.operands) == ["subtotal", "tax"]
    assert f.operation == "addition"
    assert f.match_ratio == 1.0


def test_detect_subtraction_formula():
    """Test A = B - C."""
    np.random.seed(42)
    revenue = np.random.uniform(100, 500, size=40)
    cost = np.random.uniform(20, 80, size=40)
    profit = revenue - cost

    df = pd.DataFrame({"revenue": revenue, "cost": cost, "profit": profit})
    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1
    f = formulas[0]
    assert f.dependent == "profit"
    assert f.operands == ["revenue", "cost"]
    assert f.operation == "subtraction"
    assert f.description == "`profit` is deterministically derived from `revenue` and `cost`."


def test_detect_multiplication_formula():
    """Test A = B * C."""
    np.random.seed(42)
    units = np.random.randint(1, 20, size=40).astype(float)
    unit_price = np.random.uniform(10.0, 50.0, size=40).round(2)
    total_sales = (units * unit_price).round(2)

    df = pd.DataFrame({"units": units, "unit_price": unit_price, "total_sales": total_sales})
    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1
    f = formulas[0]
    assert f.dependent == "total_sales"
    assert sorted(f.operands) == ["unit_price", "units"]
    assert f.operation == "multiplication"


def test_detect_division_formula():
    """Test A = B / C."""
    np.random.seed(42)
    distance = np.random.uniform(50, 500, size=30)
    hours = np.random.uniform(1, 10, size=30)
    speed = distance / hours

    df = pd.DataFrame({"distance": distance, "hours": hours, "speed": speed})
    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1
    f = formulas[0]
    assert f.dependent == "speed"
    assert f.operands == ["distance", "hours"]
    assert f.operation == "division"


def test_detect_percentage_formula():
    """Test percentage discount: discount_amount = price * discount_pct / 100."""
    np.random.seed(42)
    price = np.random.uniform(50, 200, size=40).round(2)
    discount_pct = np.random.choice([5.0, 10.0, 15.0, 20.0, 25.0], size=40)
    discount_amount = (price * (discount_pct / 100.0)).round(2)

    df = pd.DataFrame({"price": price, "discount_pct": discount_pct, "discount_amount": discount_amount})
    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1
    f = next((f for f in formulas if f.dependent == "discount_amount"), None)
    assert f is not None
    assert f.operation == "percentage"
    assert sorted(f.operands) == ["discount_pct", "price"]


def test_detect_discounted_price_formula():
    """Test discounted price: net_price = price * (1 - discount_pct / 100)."""
    np.random.seed(42)
    price = np.random.uniform(100, 500, size=30).round(2)
    discount_pct = np.random.choice([10.0, 20.0, 30.0], size=30)
    net_price = (price * (1.0 - discount_pct / 100.0)).round(2)

    df = pd.DataFrame({"price": price, "discount_pct": discount_pct, "net_price": net_price})
    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1
    f = next((f for f in formulas if f.dependent == "net_price"), None)
    assert f is not None
    assert f.operation == "percentage"


def test_no_spurious_formula_on_random_data():
    """Independent random variables should not produce deterministic formulas."""
    np.random.seed(99)
    df = pd.DataFrame({
        "x1": np.random.uniform(1, 100, size=50),
        "x2": np.random.uniform(1, 100, size=50),
        "x3": np.random.uniform(1, 100, size=50),
    })
    formulas = detect_formula_dependencies(df)
    assert len(formulas) == 0


def test_handles_nans_gracefully():
    """Rows with NaNs should be skipped without breaking detection."""
    b = [10.0, 20.0, np.nan, 40.0, 50.0, 60.0, 70.0, 80.0]
    c = [1.0, 2.0, 3.0, np.nan, 5.0, 6.0, 7.0, 8.0]
    a = [11.0, 22.0, np.nan, np.nan, 55.0, 66.0, 77.0, 88.0]

    df = pd.DataFrame({"total": a, "subtotal": b, "fee": c})
    formulas = detect_formula_dependencies(df)
    assert len(formulas) >= 1
    assert formulas[0].dependent == "total"
    assert formulas[0].match_ratio == 1.0
