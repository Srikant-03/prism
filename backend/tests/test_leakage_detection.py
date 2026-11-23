"""
tests/test_leakage_detection.py
===============================
Regression and unit tests for the relationship classifier and leakage-detection logic fix:
- 5 distinct relationship types (Derived, Duplicate, Target Leakage, Future Information, Ordinary Association)
- AnomalyDetector severity and category mapping for derived date features
- LeakageHandler skipping date-derived columns in predictor detection
- BriefingGenerator filtering out feature redundancy from CRITICAL findings
- Story slide 7 rendering of feature redundancy and severity badges
"""

import pandas as pd
import pytest

from insights.relationship_classifier import RelationshipClassifier, RelationshipType
from insights.anomaly_detector import AnomalyDetector
from insights.insight_models import AnomalyWarning, AnomalySeverity, DataQualityScore
from insights.briefing_generator import BriefingGenerator
from profiling.profiling_models import DatasetProfile, ColumnProfile
from cleaning.leakage_handler import LeakageHandler, _is_date_derived_column


def test_1_relationship_classifier_date_and_month():
    """1. RelationshipClassifier.classify_pair(date_col, month_name_col) -> DERIVED_FEATURE, not TARGET_LEAKAGE."""
    res = RelationshipClassifier.classify_pair(
        col1="date",
        col2="month_name",
        col1_semantic="datetime",
        col2_semantic="categorical_nominal",
        score=1.0,
    )
    assert res.type == RelationshipType.DERIVED_FEATURE
    assert res.severity == "Low"
    assert res.category == "Feature Redundancy"
    assert "derived" in res.description.lower()
    assert res.type != RelationshipType.TARGET_LEAKAGE


def test_2_relationship_classifier_date_and_year():
    """2. RelationshipClassifier.classify_pair(date_col, year_col) -> DERIVED_FEATURE."""
    res = RelationshipClassifier.classify_pair(
        col1="date",
        col2="year",
        col1_semantic="datetime",
        col2_semantic="numeric_discrete",
        score=1.0,
    )
    assert res.type == RelationshipType.DERIVED_FEATURE
    assert res.severity == "Low"
    assert res.category == "Feature Redundancy"


def test_3_relationship_classifier_ordinary_association():
    """3. RelationshipClassifier.classify_pair(revenue, cost) -> ORDINARY_ASSOCIATION."""
    df = pd.DataFrame({
        "revenue": [100.0, 200.0, 300.0, 400.0],
        "cost": [50.0, 105.0, 145.0, 210.0],
    })
    res = RelationshipClassifier.classify_pair(
        col1="revenue",
        col2="cost",
        df=df,
        score=0.992,
        target_column=None,
    )
    assert res.type == RelationshipType.ORDINARY_ASSOCIATION
    assert res.severity == "Informational"
    assert res.category == "Strong Association"


def test_4_relationship_classifier_target_leakage():
    """4. RelationshipClassifier.classify_pair(target, target_derived) -> TARGET_LEAKAGE."""
    res = RelationshipClassifier.classify_pair(
        col1="churn_prob_score",
        col2="churn",
        score=0.99,
        target_column="churn",
    )
    assert res.type == RelationshipType.TARGET_LEAKAGE
    assert res.severity == "Critical"
    assert res.category == "Data Leakage"


def test_5_and_6_anomaly_detector_derived_pair():
    """5 & 6. AnomalyDetector.detect() with date <-> month_name pair -> NOT CRITICAL, category is 'Feature Redundancy', severity LOW."""
    col_date = ColumnProfile(
        name="date",
        position=0,
        original_dtype="object",
        inferred_dtype="datetime",
        semantic_type="datetime",
        total_count=100,
        null_count=0,
        null_percentage=0.0,
        distinct_count=100,
        distinct_percentage=100.0,
    )
    col_month = ColumnProfile(
        name="month_name",
        position=1,
        original_dtype="object",
        inferred_dtype="string",
        semantic_type="categorical_nominal",
        total_count=100,
        null_count=0,
        null_percentage=0.0,
        distinct_count=12,
        distinct_percentage=12.0,
    )

    profile = DatasetProfile(
        dataset_id="test_ds",
        filename="test.csv",
        total_rows=100,
        total_columns=2,
        columns=[col_date, col_month],
        memory_usage_bytes=1024,
        duplicate_rows_count=0,
        duplicate_rows_percentage=0.0,
        cross_analysis={
            "correlations": {
                "strongest_pairs": [
                    {"col1": "date", "col2": "month_name", "score": 1.0}
                ]
            },
            "target": {"target_column": None},
        },
    )

    warnings = AnomalyDetector.detect(profile)
    # Find the warning for the date <-> month_name pair
    pair_warnings = [w for w in warnings if w.feature == "date" and "month_name" in w.description]
    assert len(pair_warnings) == 1
    w = pair_warnings[0]
    assert w.severity == AnomalySeverity.LOW
    assert w.category == "Feature Redundancy"
    assert w.severity != AnomalySeverity.CRITICAL


def test_7_leakage_handler_skips_date_derived():
    """7. LeakageHandler._detect_perfect_predictors() does not flag month_name against year."""
    assert _is_date_derived_column("month_name", "year") is True
    assert _is_date_derived_column("date", "quarter") is True
    assert _is_date_derived_column("customer_age", "revenue") is False

    df = pd.DataFrame({
        "year": [2024] * 12,
        "month_name": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    })
    handler = LeakageHandler(df=df, file_id="file1", target_column="year")
    predictors = handler._detect_perfect_predictors(target="year")
    # month_name should be skipped and NOT included in predictors
    pred_cols = [p["column"] for p in predictors]
    assert "month_name" not in pred_cols


def test_8_briefing_generator_filters_derived_redundancy():
    """8. BriefingGenerator.generate() does not surface derived-feature findings as 'CRITICAL RISK'."""
    col = ColumnProfile(
        name="month_name",
        position=0,
        original_dtype="object",
        inferred_dtype="string",
        semantic_type="categorical_nominal",
        total_count=100,
        null_count=0,
        null_percentage=0.0,
        distinct_count=12,
        distinct_percentage=12.0,
    )
    profile = DatasetProfile(
        dataset_id="test_ds",
        filename="test.csv",
        total_rows=100,
        total_columns=1,
        columns=[col],
        memory_usage_bytes=1024,
        memory_size_bytes=1024,
        estimated_domain="General",
        duplicate_rows_count=0,
        duplicate_rows_percentage=0.0,
    )
    quality = DataQualityScore(
        completeness=100.0,
        uniqueness=100.0,
        validity=100.0,
        consistency=100.0,
        timeliness=None,
        overall_score=100.0,
        grade="A",
    )
    # Anomaly with category Feature Redundancy (even if someone marked it critical)
    anomalies = [
        AnomalyWarning(
            feature="month_name",
            severity=AnomalySeverity.CRITICAL,
            category="Feature Redundancy",
            description="month_name is deterministically derived from date.",
            recommendation="Keep or drop based on model preference.",
        )
    ]

    briefing = BriefingGenerator.generate(profile=profile, quality=quality, anomalies=anomalies, rankings=[])
    findings = briefing.key_findings
    for f in findings:
        assert "CRITICAL RISK" not in f or "month_name" not in f


@pytest.mark.asyncio
async def test_9_story_slide_7_rendering(monkeypatch):
    """9. Story slide 7 anomaly list -> 'Feature Redundancy' items rendered with ℹ️ Redundancy: prefix."""
    from api.story import generate_story, StoryRequest
    import state

    dummy_df = pd.DataFrame({"a": [1, 2, 3]})
    dummy_stored = {
        "profile": None,
        "insights": {
            "anomalies": [
                {
                    "feature": "month_name",
                    "severity": "Low",
                    "category": "Feature Redundancy",
                    "description": "month_name is deterministically derived from date.",
                }
            ]
        }
    }
    monkeypatch.setattr(state, "get_df", lambda fid: dummy_df)
    monkeypatch.setattr(state, "get_profile", lambda fid: dummy_stored)

    req = StoryRequest(file_id="dummy_file")
    result = await generate_story(request=req)

    # Find slide 7 (Anomaly & Risk Registry)
    slides = result["slides"]
    risk_slide = next(s for s in slides if s.get("title") == "Anomaly & Risk Registry")
    bullets = risk_slide.get("bullets", [])
    assert any("ℹ️ Redundancy:" in b for b in bullets)
