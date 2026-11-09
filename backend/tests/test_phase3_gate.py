import os
import pytest
import pandas as pd
import numpy as np

from profiling.engine import DataProfiler
from profiling.target_detector import TargetDetector
from profiling.type_detector import SemanticTypeDetector
from profiling.profiling_models import SemanticType
from profiling.formula_detector import detect_formula_dependencies, check_formula_match
from insights.briefing_generator import BriefingGenerator
from insights.anomaly_detector import AnomalyDetector
from insights.insight_models import DataQualityScore, FeatureImportance

DATASET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "prism_adversarial_e2e_test.csv")


@pytest.fixture(scope="module")
def adversarial_df():
    assert os.path.exists(DATASET_PATH), f"Adversarial test CSV not found at {DATASET_PATH}"
    return pd.read_csv(DATASET_PATH)


@pytest.fixture(scope="module")
def adversarial_profile(adversarial_df):
    result = DataProfiler.profile(adversarial_df, file_id="test_adversarial")
    return result.profile


def test_target_detector_churn_flag(adversarial_df, adversarial_profile):
    """TargetDetector must detect churn_flag with binary_classification, not post_churn_refund."""
    detector = TargetDetector()
    correlations = adversarial_profile.cross_analysis.get("correlations") if adversarial_profile.cross_analysis else None
    
    # Analyze
    target_analysis = detector.analyze(adversarial_df, adversarial_profile, correlations)
    
    assert target_analysis.is_target_detected is True, "Target should be detected"
    assert target_analysis.target_column == "churn_flag", f"Expected 'churn_flag' as target, got '{target_analysis.target_column}'"
    assert target_analysis.problem_type == "binary_classification", f"Expected 'binary_classification', got '{target_analysis.problem_type}'"
    assert target_analysis.confidence >= 0.80, f"Expected high confidence >= 0.80, got {target_analysis.confidence}"
    assert target_analysis.class_distribution is not None, "Class distribution should be populated"


def test_constant_status_not_boolean(adversarial_df):
    """A constant column with single value like 'ACTIVE' must NOT be classified as BOOLEAN."""
    col_series = adversarial_df["constant_status"]
    sem_type, conf = SemanticTypeDetector.detect(col_series, "constant_status")
    
    assert sem_type != SemanticType.BOOLEAN, f"constant_status must not be BOOLEAN, got {sem_type}"
    assert sem_type == SemanticType.CATEGORICAL_NOMINAL, f"Expected CATEGORICAL_NOMINAL, got {sem_type}"


def test_formula_detection_discount_rate(adversarial_df):
    """Formula detector must find net_revenue = gross_revenue * (1 - discount_rate) and no false positives on rare_audit_flag."""
    formulas = detect_formula_dependencies(adversarial_df)
    
    # Check for net_revenue formula
    net_rev_formulas = [f for f in formulas if f.dependent == "net_revenue"]
    assert len(net_rev_formulas) > 0, "Expected at least one formula for net_revenue"
    
    matching_formula = next((f for f in net_rev_formulas if "gross_revenue" in f.operands and "discount_rate" in f.operands), None)
    assert matching_formula is not None, f"Expected formula linking net_revenue with gross_revenue and discount_rate, got: {[f.formula_expr for f in net_rev_formulas]}"
    assert matching_formula.match_ratio >= 0.98, f"Formula match ratio should be >= 0.98, got {matching_formula.match_ratio}"
    
    # Check NO false positive formulas on rare_audit_flag (zero-dominated column)
    rare_audit_formulas = [f for f in formulas if f.dependent == "rare_audit_flag"]
    assert len(rare_audit_formulas) == 0, f"rare_audit_flag must not have false-positive formulas, found: {[f.formula_expr for f in rare_audit_formulas]}"


def test_check_formula_match_zero_dominated_rejection():
    """check_formula_match must reject spurious formulas on zero-dominated series."""
    # 99% zeros, but non-zero values do NOT match
    s_a = pd.Series([0.0] * 99 + [50.0])
    expected_a = pd.Series([0.0] * 99 + [12.0])
    
    ok, ratio, count = check_formula_match(s_a, expected_a, min_match_ratio=0.98)
    assert ok is False, "Zero-dominated series with mismatching non-zeros must be rejected"


def test_briefing_generator_no_hallucinations(adversarial_profile, adversarial_df):
    """BriefingGenerator must not emit fabricated NZSIOC/ROE/salary text."""
    quality = DataQualityScore(
        overall_score=85,
        grade="B",
        completeness=90,
        uniqueness=95,
        validity=85,
        consistency=80
    )
    anomalies = AnomalyDetector.detect(adversarial_profile)
    rankings = [FeatureImportance(feature="churn_flag", importance_score=95, variance_score=80.0, reasoning="Target variable")]
    
    briefing = BriefingGenerator.generate(adversarial_profile, quality, anomalies, rankings, df=adversarial_df)
    
    all_text = " ".join(briefing.key_findings + briefing.recommended_actions).lower()
    
    forbidden_tokens = ["nzsioc", "anzsic", "operating expenditures", "salary growth", "pre-tax surplus", "return on equity"]
    for token in forbidden_tokens:
        assert token not in all_text, f"Fabricated token '{token}' found in briefing!"


def test_anomaly_detector_deduplication(adversarial_profile):
    """AnomalyDetector must deduplicate warnings."""
    warnings = AnomalyDetector.detect(adversarial_profile)
    signatures = [(w.feature, str(w.severity), str(w.category), str(w.description)) for w in warnings]
    assert len(signatures) == len(set(signatures)), "Anomaly warnings should be unique"
