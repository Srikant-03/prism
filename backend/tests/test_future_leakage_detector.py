"""
Tests for prediction-time aware future leakage detection and response_time_days.

Verifies:
1. On dashboard_test_data.csv, 'response_time_days' is treated as a conditional warning,
   NOT as a confirmed factual leakage claim:
   "'response_time_days' may cause target/future leakage if this value is only known after the prediction timestamp. Confirm feature availability at inference time."
2. Semantic suspicion alone does not convert to confirmed future leakage.
3. Only classifies as confirmed future leakage when analysis has:
   - prediction timestamp vs feature timestamp
   - target timestamp vs feature timestamp
   - explicit metadata
   - or a documented business rule
4. RelationshipClassifier generates 'Conditional Future Leakage Warning' (Medium severity)
   for semantic suspicions, and 'Confirmed Future Leakage' (High severity) only with evidence.
"""

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from cleaning.leakage_handler import LeakageHandler
from cleaning.cleaning_models import ActionType, ActionConfidence
from insights.relationship_classifier import RelationshipClassifier, RelationshipType


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


def test_response_time_days_on_supplied_csv_is_conditional_warning(test_csv_path):
    """
    On dashboard_test_data.csv:
    - response_time_days has no prediction timestamp or metadata.
    - System must NOT claim factually that it is future-looking.
    - Must produce the exact conditional warning recommendation.
    """
    df = pd.read_csv(test_csv_path)
    handler = LeakageHandler(df, str(test_csv_path), target_column="prospect_count")
    actions, report = handler.analyze()

    resp_actions = [a for a in actions if "response_time_days" in a.target_columns]
    assert len(resp_actions) == 1, f"Expected 1 future leakage action, found {len(resp_actions)}"

    action = resp_actions[0]
    assert action.action_type == ActionType.FLAG_LEAKAGE_FUTURE

    # Confidence must be JUDGMENT_CALL, not DEFINITIVE
    assert action.confidence == ActionConfidence.JUDGMENT_CALL

    # Must produce the conditional warning recommendation
    expected_rec = (
        "'response_time_days' may cause target/future leakage if this value is only known "
        "after the prediction timestamp. Confirm feature availability at inference time."
    )
    assert action.recommendation == expected_rec, (
        f"Recommendation mismatch.\nExpected: {expected_rec}\nGot: {action.recommendation}"
    )

    # Must NOT make a factual claim that it refers to a future event
    assert "semantically refers to a future event and is therefore future-looking" not in action.evidence
    assert "semantically refers to a future event and is therefore future-looking" not in action.reasoning

    # Default option must allow confirming availability at inference
    opt_keys = [o.key for o in action.options]
    assert "confirm_available" in opt_keys
    assert action.options[0].key == "confirm_available"
    assert action.options[0].is_default is True


def test_relationship_classifier_conditional_warning():
    """RelationshipClassifier produces Conditional Future Leakage Warning without timestamps."""
    clf = RelationshipClassifier()
    res = clf.classify_pair("response_time_days", "prospect_count", score=0.88)

    assert res.category == "Conditional Future Leakage Warning"
    assert res.severity == "Medium"
    expected_msg = (
        "'response_time_days' may cause target/future leakage if this value is only known "
        "after the prediction timestamp. Confirm feature availability at inference time."
    )
    assert res.recommendation == expected_msg
    assert res.description == expected_msg


def test_confirmed_future_leakage_via_timestamps():
    """Confirmed future leakage is triggered when feature timestamp is after prediction timestamp."""
    df = pd.DataFrame({"response_time_days": [1.0, 2.0], "val": [10, 20]})
    handler = LeakageHandler(
        df,
        "test_ts",
        prediction_timestamp="2023-01-01 10:00:00",
        feature_timestamps={"response_time_days": "2023-01-01 15:00:00"},
    )
    actions, report = handler.analyze()

    future_actions = [a for a in actions if a.action_type == ActionType.FLAG_LEAKAGE_FUTURE]
    assert len(future_actions) == 1
    action = future_actions[0]

    assert action.confidence == ActionConfidence.DEFINITIVE
    assert "CRITICAL" in action.recommendation
    assert "confirmed post-prediction future leakage" in action.recommendation
    assert "feature timestamp" in action.evidence
    assert action.options[0].key == "remove"


def test_confirmed_future_leakage_via_metadata():
    """Confirmed future leakage is triggered when metadata explicitly declares column as post-prediction."""
    df = pd.DataFrame({"response_time_days": [1.0, 2.0], "val": [10, 20]})
    handler = LeakageHandler(
        df,
        "test_meta",
        metadata={"response_time_days": {"is_post_prediction": True}},
    )
    actions, report = handler.analyze()

    future_actions = [a for a in actions if a.action_type == ActionType.FLAG_LEAKAGE_FUTURE]
    assert len(future_actions) == 1
    action = future_actions[0]

    assert action.confidence == ActionConfidence.DEFINITIVE
    assert "CRITICAL" in action.recommendation
    assert "explicit metadata" in action.evidence


def test_confirmed_future_leakage_via_business_rule():
    """Confirmed future leakage is triggered when a documented business rule applies."""
    df = pd.DataFrame({"response_time_days": [1.0, 2.0], "val": [10, 20]})
    handler = LeakageHandler(
        df,
        "test_rules",
        business_rules=["response_time_days is a post-prediction metric measured at ticket closure"],
    )
    actions, report = handler.analyze()

    future_actions = [a for a in actions if a.action_type == ActionType.FLAG_LEAKAGE_FUTURE]
    assert len(future_actions) == 1
    action = future_actions[0]

    assert action.confidence == ActionConfidence.DEFINITIVE
    assert "CRITICAL" in action.recommendation
    assert "documented business rule" in action.evidence


def test_relationship_classifier_confirmed_with_rules():
    """RelationshipClassifier flags confirmed leakage with documented business rule."""
    clf = RelationshipClassifier()
    res = clf.classify_pair(
        "response_time_days",
        "target",
        business_rules=["response_time_days is post-prediction outcome"],
    )

    assert res.category == "Confirmed Future Leakage"
    assert res.severity == "High"
    assert "Confirmed post-prediction leakage" in res.recommendation
