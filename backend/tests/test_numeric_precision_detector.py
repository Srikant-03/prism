"""
Tests for the numeric precision detector.
Verifies:
1. Source text precision analysis (avoiding IEEE 754 float representation noise).
2. Never reporting 20 decimal places or recommending rounding to 20 decimals.
3. Accurate dominant and exceptional precision reporting on dashboard_test_data.csv.
4. No false-positive rounding recommendations on dashboard_test_data.csv.
5. Action generation only when genuine excess decimal precision is present.
"""

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from cleaning.standardization_handler import StandardizationHandler


@pytest.fixture
def test_csv_path() -> Path:
    # Check current directory and parent directory
    candidates = [
        Path("dashboard_test_data.csv"),
        Path("../dashboard_test_data.csv"),
        Path("c:/abbu/dashboard_test_data.csv"),
    ]
    for p in candidates:
        if p.is_file():
            return p
    pytest.skip("dashboard_test_data.csv not found")


def test_supplied_csv_dominant_precisions(test_csv_path):
    """Verify that dominant and exceptional decimal precisions on the supplied CSV match source reality."""
    df = pd.read_csv(test_csv_path)
    handler = StandardizationHandler(df, str(test_csv_path))
    _, report = handler.analyze()

    analysis = report.get("precision_analysis", {})

    # Expected dominant precisions
    expected_dominant = {
        "revenue": 2,
        "cost": 2,
        "profit": 2,
        "avg_order_value": 2,
        "customer_satisfaction": 1,
        "response_time_days": 1,
        "return_rate": 1,
        "discount_pct": 1,
    }

    for col, exp_dom in expected_dominant.items():
        assert col in analysis, f"Column '{col}' should be analyzed for precision"
        info = analysis[col]
        assert info["dominant_precision"] == exp_dom, (
            f"Expected dominant precision {exp_dom} for '{col}', got {info['dominant_precision']}"
        )
        assert info["dominant_precision"] != 20, f"Precision must not be 20 decimals for '{col}'"

    # Verify exceptional precision reporting on revenue, cost, profit
    rev_info = analysis["revenue"]
    assert rev_info["decimal_counts"][2] > 100  # mostly 2 decimals
    assert 1 in rev_info["exceptional_precisions"] or 0 in rev_info["exceptional_precisions"]
    assert rev_info["excess_precision_count"] == 0


def test_supplied_csv_no_false_rounding_actions(test_csv_path):
    """Verify that dashboard_test_data.csv does NOT trigger false-positive rounding recommendations."""
    df = pd.read_csv(test_csv_path)
    handler = StandardizationHandler(df, str(test_csv_path))
    actions, report = handler.analyze()

    precision_actions = [a for a in actions if a.action_type.value == "standardize_precision"]
    assert len(precision_actions) == 0, (
        f"Supplied CSV should have zero precision actions, but got {len(precision_actions)}: "
        f"{[a.recommendation for a in precision_actions]}"
    )

    assert len(report["precision_issues"]) == 0, (
        f"Supplied CSV should have zero precision issues flagged, got {len(report['precision_issues'])}"
    )


def test_never_recommends_20_decimals():
    """Ensure the system never reports 20 decimal places or recommends rounding to 20 decimals."""
    # Create floating-point numbers that have binary representation expansion
    floats = [0.1 + 0.2, 1.0 / 3.0, 7.0 / 9.0, 12.34, 15.678] * 20
    df = pd.DataFrame({"calc_val": floats})

    handler = StandardizationHandler(df, "calc_file")
    actions, report = handler.analyze()

    analysis = report.get("precision_analysis", {})
    if "calc_val" in analysis:
        assert analysis["calc_val"]["dominant_precision"] <= 6
        assert analysis["calc_val"]["dominant_precision"] != 20

    for a in actions:
        if a.action_type.value == "standardize_precision":
            assert "20" not in a.recommendation
            for opt in a.options:
                assert opt.key != "20"


def test_omitted_trailing_zeros_not_flagged():
    """
    Values with fewer decimals than dominant precision (e.g. 10.5 or 10 when dominant is 2)
    represent standard omitted trailing zeros, not an excess precision problem.
    """
    # 80 values with 2 decimals, 15 with 1 decimal, 5 with 0 decimals
    vals = ["12.45"] * 80 + ["12.4"] * 15 + ["12"] * 5
    raw_df = pd.DataFrame({"amount": vals})
    num_df = pd.DataFrame({"amount": [float(x) for x in vals]})

    handler = StandardizationHandler(num_df, "test_file", raw_df=raw_df)
    actions, report = handler.analyze()

    prec_actions = [a for a in actions if a.action_type.value == "standardize_precision"]
    assert len(prec_actions) == 0

    info = handler._check_precision("amount")
    assert info is not None
    assert info["dominant_precision"] == 2
    assert info["exceptional_precisions"] == {0: 5, 1: 15}
    assert info["excess_precision_count"] == 0
    assert info["has_meaningful_inconsistency"] is False


def test_meaningful_excess_precision_detected():
    """When a column contains genuine excess precision (e.g. 6 decimals mixed into 2), it is flagged."""
    # 90 values with 2 decimals, 10 values with 6 decimals
    vals = ["19.99"] * 90 + ["19.991234"] * 10
    raw_df = pd.DataFrame({"price": vals})
    num_df = pd.DataFrame({"price": [float(x) for x in vals]})

    handler = StandardizationHandler(num_df, "test_file", raw_df=raw_df)
    actions, report = handler.analyze()

    prec_actions = [a for a in actions if a.action_type.value == "standardize_precision"]
    assert len(prec_actions) == 1
    action = prec_actions[0]

    assert action.recommendation == "Round 'price' to 2 decimal places."
    assert "excess decimal precision" in action.evidence
    assert action.options[0].key == "2"
    assert action.options[0].is_default is True


def test_raw_text_source_preservation():
    """String source text with explicit '.0' is preserved accurately."""
    vals = ["1.0", "2.0", "3.0", "4.0", "5.0"] * 20
    raw_df = pd.DataFrame({"rating": vals})
    num_df = pd.DataFrame({"rating": [float(x) for x in vals]})

    handler = StandardizationHandler(num_df, "rating_file", raw_df=raw_df)
    info = handler._check_precision("rating")

    assert info is not None
    assert info["dominant_precision"] == 1
    assert info["decimal_counts"] == {1: 100}
    assert info["has_meaningful_inconsistency"] is False
