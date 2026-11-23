"""
tests/test_functional_dependency.py
====================================
Tests for functional dependency detection and its separation from data leakage:
1. One-to-one categorical mapping (e.g. region <-> country)
2. Many-to-one categorical mapping (e.g. city -> country, subcategory -> category)
3. True target leakage (feature encoding or derived from the specified target)
4. Ordinary statistical association / correlation (non-deterministic high association)
5. Anomaly registry integration (severity LOW, category Deterministic Redundancy, no "leakage")
6. Feature selection engine integration (proposes dropping dependent feature to reduce redundancy)
7. Executive report integration (briefing & story classify as redundancy, zero critical leakage risks)
8. End-to-end on dashboard_test_data.csv (exact expected output string, zero leakage false positives)
"""

import pandas as pd
import pytest

from profiling.functional_dependency import (
    detect_functional_dependency,
    find_all_functional_dependencies,
    FunctionalDependencyResult,
)
from insights.relationship_classifier import RelationshipClassifier, RelationshipType
from insights.anomaly_detector import AnomalyDetector
from insights.insight_models import AnomalySeverity, AnomalyWarning
from profiling.profiling_models import DatasetProfile, ColumnProfile, SemanticType
from profiling.correlation_analyzer import CorrelationAnalyzer
from cleaning.feature_selection_handler import FeatureSelectionHandler
from cleaning.cleaning_models import ActionCategory, ActionType
from insights.briefing_generator import BriefingGenerator


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: One-to-one categorical mapping (region <-> country)
# ─────────────────────────────────────────────────────────────────────────────
def test_1_one_to_one_categorical_mapping():
    """Exact 1-to-1 bijective mapping between region and country."""
    df = pd.DataFrame({
        "region":  ["North", "South", "East", "West"] * 25,
        "country": ["USA", "Mexico", "Canada", "UK"] * 25,
    })

    # 1. Detector check
    fd = detect_functional_dependency(df, "region", "country")
    assert fd is not None
    assert fd.is_bijective is True
    assert fd.determinant in ("region", "country")
    assert fd.dependent in ("region", "country")
    expected_msg = "`country` is deterministically determined by `region` in this dataset. Keeping both may introduce redundant information."
    assert fd.description == expected_msg

    # 2. RelationshipClassifier check
    res = RelationshipClassifier.classify_pair(
        col1="region",
        col2="country",
        df=df,
        score=1.0,
    )
    assert res.type == RelationshipType.FUNCTIONAL_DEPENDENCY
    assert res.severity == "Low"
    assert res.category == "Deterministic Redundancy"
    assert res.description == expected_msg
    # Ensure the word "leakage" is nowhere in the result
    assert "leakage" not in res.description.lower()
    assert "leakage" not in res.category.lower()
    assert "leakage" not in res.recommendation.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Many-to-one categorical mapping (city -> country)
# ─────────────────────────────────────────────────────────────────────────────
def test_2_many_to_one_mapping():
    """Directional mapping: multiple cities belong to the same country, but city uniquely determines country."""
    df = pd.DataFrame({
        "city":    ["Seattle", "New York", "Miami", "Toronto", "Montreal", "London", "Manchester"] * 10,
        "country": ["USA",     "USA",      "USA",   "Canada",  "Canada",   "UK",     "UK"] * 10,
    })

    # Detector check: city -> country holds, but country -> city does NOT
    fd = detect_functional_dependency(df, "city", "country")
    assert fd is not None
    assert fd.is_bijective is False
    assert fd.determinant == "city"
    assert fd.dependent == "country"
    expected_msg = "`country` is deterministically determined by `city` in this dataset. Keeping both may introduce redundant information."
    assert fd.description == expected_msg

    # Classifier check (city, country)
    res1 = RelationshipClassifier.classify_pair(col1="city", col2="country", df=df, score=1.0)
    assert res1.type == RelationshipType.FUNCTIONAL_DEPENDENCY
    assert res1.severity == "Low"
    assert res1.description == expected_msg
    assert "leakage" not in res1.description.lower()

    # Classifier check reverse order (country, city): must still identify city -> country
    res2 = RelationshipClassifier.classify_pair(col1="country", col2="city", df=df, score=1.0)
    assert res2.type == RelationshipType.FUNCTIONAL_DEPENDENCY
    assert res2.description == expected_msg
    assert "leakage" not in res2.description.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: True target leakage
# ─────────────────────────────────────────────────────────────────────────────
def test_3_true_target_leakage():
    """A feature that directly encodes or is derived from the specified target must be flagged as TARGET_LEAKAGE."""
    df = pd.DataFrame({
        "churn": [0, 1, 0, 1, 0, 1] * 20,
        "churn_indicator": [0, 1, 0, 1, 0, 1] * 20,
        "age": [25, 45, 32, 54, 21, 62] * 20,
    })

    res = RelationshipClassifier.classify_pair(
        col1="churn_indicator",
        col2="churn",
        df=df,
        target_column="churn",
        score=1.0,
    )
    assert res.type == RelationshipType.TARGET_LEAKAGE
    assert res.severity == "Critical"
    assert res.category == "Data Leakage"
    assert "target leakage" in res.description.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Ordinary correlation
# ─────────────────────────────────────────────────────────────────────────────
def test_4_ordinary_correlation():
    """Strong statistical association that is neither deterministic nor target leakage."""
    # Contingency without 100% purity (some overlap)
    df = pd.DataFrame({
        "channel":    ["Direct", "Direct", "Search", "Search", "Social", "Social", "Social"] * 20,
        "category":   ["Hardware", "Software", "Hardware", "Software", "Hardware", "Software", "Cloud"] * 20,
    })

    # Functional dependency should NOT hold
    fd = detect_functional_dependency(df, "channel", "category")
    assert fd is None

    res = RelationshipClassifier.classify_pair(
        col1="channel",
        col2="category",
        df=df,
        score=0.65,
    )
    assert res.type == RelationshipType.ORDINARY_ASSOCIATION
    assert res.category == "Strong Association"
    assert res.severity == "Informational"
    assert "leakage" not in res.description.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: Anomaly Registry Integration
# ─────────────────────────────────────────────────────────────────────────────
def test_5_anomaly_registry_integration():
    """AnomalyDetector must register functional dependency as LOW severity Deterministic Redundancy."""
    import numpy as np
    np.random.seed(42)
    df = pd.DataFrame({
        "region":  ["North", "South", "East", "West"] * 25,
        "country": ["USA", "Mexico", "Canada", "UK"] * 25,
        "sales":   np.random.normal(100, 15, 100),
    })

    prof = DatasetProfile(
        file_id="test_reg",
        total_rows=len(df),
        total_columns=len(df.columns),
        columns=[
            ColumnProfile(name=c, position=i, inferred_dtype="string", semantic_type=SemanticType.CATEGORICAL_NOMINAL, distinct_count=df[c].nunique(), null_count=0, null_percentage=0.0)
            for i, c in enumerate(df.columns)
        ]
    )
    corr = CorrelationAnalyzer().analyze(df, prof)
    prof.cross_analysis = {"correlations": corr.model_dump()}

    warnings = AnomalyDetector.detect(prof)

    # Filter for warnings concerning region or country
    geo_warnings = [w for w in warnings if w.feature in ("region", "country") or "region" in str(w.description) or "country" in str(w.description)]
    assert len(geo_warnings) > 0

    for w in geo_warnings:
        assert w.severity == AnomalySeverity.LOW
        assert w.category == "Deterministic Redundancy"
        assert "`country` is deterministically determined by `region` in this dataset. Keeping both may introduce redundant information." in w.description
        assert "leakage" not in w.description.lower()
        assert "leakage" not in w.category.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: Feature Selection Engine Integration
# ─────────────────────────────────────────────────────────────────────────────
def test_6_feature_selection_engine_integration():
    """FeatureSelectionHandler proposes dropping the determined feature to reduce redundancy."""
    df = pd.DataFrame({
        "region":  ["North", "South", "East", "West"] * 25,
        "country": ["USA", "Mexico", "Canada", "UK"] * 25,
        "sales":   [100, 200, 150, 300] * 25,
    })

    handler = FeatureSelectionHandler(df, file_id="test_fs")
    actions, report = handler.analyze()

    fd_actions = [a for a in actions if a.action_type == ActionType.FLAG_DERIVED_REDUNDANCY]
    assert len(fd_actions) > 0

    act = fd_actions[0]
    assert act.category == ActionCategory.FEATURE_SELECTION
    assert "country" in act.target_columns
    assert "`country` is deterministically determined by `region` in this dataset. Keeping both may introduce redundant information." in act.evidence
    assert "leakage" not in act.evidence.lower()
    assert "leakage" not in act.reasoning.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: Executive Report (Briefing) Integration
# ─────────────────────────────────────────────────────────────────────────────
def test_7_executive_report_integration():
    """BriefingGenerator must never report region/country as CRITICAL RISK or leakage."""
    from insights.insight_models import DataQualityScore
    df = pd.DataFrame({
        "region":  ["North", "South", "East", "West"] * 25,
        "country": ["USA", "Mexico", "Canada", "UK"] * 25,
        "sales":   [100, 200, 150, 300] * 25,
    })

    prof = DatasetProfile(
        file_id="test_brief",
        total_rows=len(df),
        total_columns=len(df.columns),
        columns=[
            ColumnProfile(name=c, position=i, inferred_dtype="string", semantic_type=SemanticType.CATEGORICAL_NOMINAL, distinct_count=df[c].nunique(), null_count=0, null_percentage=0.0)
            for i, c in enumerate(df.columns)
        ]
    )
    corr = CorrelationAnalyzer().analyze(df, prof)
    prof.cross_analysis = {"correlations": corr.model_dump()}
    anomalies = AnomalyDetector.detect(prof)
    quality = DataQualityScore(completeness=100.0, uniqueness=100.0, validity=100.0, consistency=100.0, overall_score=100.0, grade="A")

    briefing = BriefingGenerator.generate(prof, quality, anomalies=anomalies, rankings=[], df=df)

    # Check key findings
    for finding in briefing.key_findings:
        if "region" in finding or "country" in finding:
            assert "CRITICAL RISK" not in finding
            assert "leakage" not in finding.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: End-to-end on dashboard_test_data.csv
# ─────────────────────────────────────────────────────────────────────────────
def test_8_dashboard_test_data_csv():
    """Full validation on the actual dashboard_test_data.csv file."""
    import os
    csv_path = "../dashboard_test_data.csv"
    if not os.path.exists(csv_path):
        csv_path = "dashboard_test_data.csv"
    assert os.path.exists(csv_path), "dashboard_test_data.csv not found"

    df = pd.read_csv(csv_path)

    # 1. Direct detector test
    fd = detect_functional_dependency(df, "region", "country")
    assert fd is not None
    assert fd.description == "`country` is deterministically determined by `region` in this dataset. Keeping both may introduce redundant information."

    # 2. Classifier test
    cls = RelationshipClassifier.classify_pair("region", "country", df=df, score=1.0)
    assert cls.type == RelationshipType.FUNCTIONAL_DEPENDENCY
    assert cls.category == "Deterministic Redundancy"
    assert cls.severity == "Low"
    assert "leakage" not in cls.description.lower()
    assert cls.description == "`country` is deterministically determined by `region` in this dataset. Keeping both may introduce redundant information."

    # 3. Anomaly detector check
    prof = DatasetProfile(
        file_id="test_csv",
        total_rows=len(df),
        total_columns=len(df.columns),
        columns=[
            ColumnProfile(name=c, position=i, inferred_dtype="string", semantic_type=SemanticType.CATEGORICAL_NOMINAL, distinct_count=df[c].nunique(), null_count=0, null_percentage=0.0)
            for i, c in enumerate(df.columns)
        ]
    )
    corr = CorrelationAnalyzer().analyze(df, prof)
    prof.cross_analysis = {"correlations": corr.model_dump()}
    warnings = AnomalyDetector.detect(prof)

    for w in warnings:
        if w.feature in ("region", "country") or "region" in str(w.description) or "country" in str(w.description):
            assert w.category != "Data Leakage", f"Unexpected Data Leakage on {w}"
            assert w.severity != AnomalySeverity.CRITICAL, f"Unexpected Critical severity on {w}"
            assert "leakage" not in w.description.lower(), f"Leakage word used in {w.description}"
