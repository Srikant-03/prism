from pydantic import ValidationError
from insights.insight_models import (
    DataQualityScore,
    AnomalyWarning,
    AnomalySeverity,
    FeatureImportance,
    AnalystBriefing,
    DatasetInsights
)
from insights.quality_scorer import QualityScorer
from insights.anomaly_detector import AnomalyDetector
from insights.feature_ranker import FeatureRanker
from insights.briefing_generator import BriefingGenerator
from insights.export_service import ExportService
from profiling.profiling_models import DatasetProfile, ColumnProfile, SemanticType

def test_insight_models_instantiation():
    """Test that all models can be instantiated correctly manually."""
    
    q_score = DataQualityScore(
        completeness=90.5,
        uniqueness=99.0,
        validity=100.0,
        consistency=100.0,
        timeliness=None,
        overall_score=95.0,
        grade="A"
    )
    assert q_score.grade == "A"
    
    warning = AnomalyWarning(
        feature="Age",
        severity=AnomalySeverity.HIGH,
        category="Missingness",
        description="Too many nulls",
        recommendation="Drop"
    )
    assert warning.severity == "High"
    
    ranking = FeatureImportance(
        feature="Age",
        importance_score=85,
        variance_score=0.9,
        correlation_to_target=None,
        mutual_information=None,
        reasoning="Good variance"
    )
    assert ranking.feature == "Age"
    
    briefing = AnalystBriefing(
        executive_summary="X",
        dataset_characteristics="Y",
        quality_assessment="Z",
        key_findings=["1"],
        recommended_actions=["2"]
    )
    
    insights = DatasetInsights(
        quality_score=q_score,
        anomalies=[warning],
        feature_ranking=[ranking],
        analyst_briefing=briefing
    )
    assert len(insights.anomalies) == 1

def test_generators_with_empty_profile():
    """Ensure generators don't crash on an empty profile."""
    empty_profile = DatasetProfile(
        file_name="empty.csv",
        total_rows=0,
        total_columns=0,
        columns=[]
    )
    
    quality = QualityScorer.calculate_scores(empty_profile)
    assert quality.grade == "F"
    assert quality.overall_score == 0
    
    anomalies = AnomalyDetector.detect(empty_profile)
    assert len(anomalies) > 0
    assert anomalies[0].severity == AnomalySeverity.CRITICAL
    
    rankings = FeatureRanker.rank_features(empty_profile)
    assert len(rankings) == 0
    
    briefing = BriefingGenerator.generate(empty_profile, quality, anomalies, rankings)
    assert "0" in briefing.dataset_characteristics

    insights = DatasetInsights(
        quality_score=quality,
        anomalies=anomalies,
        feature_ranking=rankings,
        analyst_briefing=briefing
    )
    
    pdf = ExportService.generate_pdf(insights)
    assert isinstance(pdf, bytes)
    assert len(pdf) > 0
    
    docx = ExportService.generate_docx(insights)
    assert isinstance(docx, bytes)
    assert len(docx) > 0
