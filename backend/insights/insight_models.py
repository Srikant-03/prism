from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

# ── Data Quality Score ──

class DataQualityScore(BaseModel):
    completeness: float = Field(..., description="Score 0-100 based on inverse of missing values")
    uniqueness: float = Field(..., description="Score 0-100 based on distinct counts relative to row count")
    validity: float = Field(..., description="Score 0-100 based on type adherence and formatting")
    consistency: float = Field(..., description="Score 0-100 based on mixed types, casing, etc.")
    timeliness: Optional[float] = Field(None, description="Score 0-100 based on recentness and temporal gaps (if applicable)")
    overall_score: float = Field(..., description="Weighted average of the quality dimensions")
    grade: str = Field(..., description="A, B, C, D, or F based on the overall score")

# ── Anomaly Registry ──

class AnomalySeverity(str):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    INFO = "Informational"

class AnomalyWarning(BaseModel):
    feature: Optional[str] = Field(None, description="The column this anomaly pertains to (if applicable)")
    severity: str = Field(..., description="Severity level from Critical to Informational")
    category: str = Field(..., description="e.g., 'Missingness', 'Distribution', 'Correlation', 'PII'")
    description: str = Field(..., description="A plain-English description of the finding")
    recommendation: Optional[str] = Field(None, description="Suggested action to take (e.g., 'Drop column', 'Impute values')")

# ── Feature Importance ──

class FeatureImportance(BaseModel):
    feature: str = Field(..., description="Column name")
    importance_score: float = Field(..., description="0-100 score indicating overall usefulness")
    variance_score: float = Field(..., description="Normalized variance or entropy measure")
    correlation_to_target: Optional[float] = Field(None, description="Absolute correlation to the detected target variable")
    mutual_information: Optional[float] = Field(None, description="Aggregated MI score with other features")
    reasoning: str = Field(..., description="Brief explanation of why this feature ranks where it does")

# ── Analyst Briefing ──

class AnalystBriefing(BaseModel):
    executive_summary: str = Field(..., description="High-level paragraph summarizing the dataset's purpose and state")
    dataset_characteristics: str = Field(..., description="Text block detailing rows, columns, memory size, and inferred domain")
    quality_assessment: str = Field(..., description="Narrative summary of the DataQualityScore dimensions")
    key_findings: List[str] = Field(default_factory=list, description="Top 3-5 most interesting statistical discoveries")
    recommended_actions: List[str] = Field(default_factory=list, description="Immediate next steps (e.g., handling missing data, dropping useless columns)")

# ── Combined Insights Export ──

class DatasetInsights(BaseModel):
    quality_score: DataQualityScore
    anomalies: List[AnomalyWarning] = Field(default_factory=list)
    feature_ranking: List[FeatureImportance] = Field(default_factory=list)
    analyst_briefing: AnalystBriefing
