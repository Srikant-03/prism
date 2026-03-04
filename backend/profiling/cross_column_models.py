from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Union

# -------------------------------------------------------------------
# Correlation & Association
# -------------------------------------------------------------------

class CorrelationPair(BaseModel):
    col1: str = Field(..., description="First column name")
    col2: str = Field(..., description="Second column name")
    score: float = Field(..., description="Correlation score [-1.0 to 1.0] or association strength [0.0 to 1.0]")
    p_value: Optional[float] = Field(None, description="P-value for significance")
    metric: str = Field(..., description="Metric used (Pearson, Spearman, Cramér's V, Eta-squared, etc.)")
    is_significant: bool = Field(False, description="Whether the correlation is statistically significant (p < 0.05)")

class MulticollinearityReport(BaseModel):
    has_multicollinearity: bool = Field(..., description="Whether strong multicollinearity was detected")
    vif_scores: Dict[str, float] = Field(..., description="Variance Inflation Factors for numeric columns")
    warnings: List[str] = Field(default_factory=list, description="Warnings about highly correlated sets")

class CorrelationAnalysis(BaseModel):
    correlation_matrix: Dict[str, Dict[str, float]] = Field(..., description="Square matrix of default correlations for heatmap rendering")
    strongest_pairs: List[CorrelationPair] = Field(..., description="Top strongest correlated pairs across all metrics")
    multicollinearity: MulticollinearityReport = Field(..., description="Multicollinearity detection via VIF")
    mutual_information: Dict[str, Dict[str, float]] = Field(..., description="Pair-wise Mutual Information scores")

# -------------------------------------------------------------------
# Target Detection
# -------------------------------------------------------------------

class FeatureImportance(BaseModel):
    feature: str = Field(..., description="Feature column name")
    importance_score: float = Field(..., description="Correlation/Mutual info to target (0.0 to 1.0)")

class TargetAnalysis(BaseModel):
    is_target_detected: bool = Field(..., description="Whether a likely target was found")
    target_column: Optional[str] = Field(None, description="The inferred target column")
    confidence: float = Field(0.0, description="Confidence in this target choice")
    justification: Optional[str] = Field(None, description="Reasoning for picking this target")
    problem_type: Optional[str] = Field(None, description="'binary_classification', 'multiclass_classification', 'regression'")
    class_distribution: Optional[Dict[str, float]] = Field(None, description="If classification, distribution of classes")
    imbalance_ratio: Optional[float] = Field(None, description="Ratio of majority to minority class")
    top_predictors: List[FeatureImportance] = Field(default_factory=list, description="Top features associated with target")

# -------------------------------------------------------------------
# Temporal & Spatial Patterns
# -------------------------------------------------------------------

class TimeSeriesComponent(BaseModel):
    trend: List[float] = Field(..., description="Trend component")
    seasonal: List[float] = Field(..., description="Seasonality component")
    residual: List[float] = Field(..., description="Residual component")
    timestamps: List[str] = Field(..., description="Time index (ISO format strings) matching component arrays")

class TemporalAnalysis(BaseModel):
    has_temporal_patterns: bool = Field(..., description="True if useful time-series patterns found")
    primary_time_col: Optional[str] = Field(None, description="The datetime column used for indexing")
    decompositions: Dict[str, TimeSeriesComponent] = Field(default_factory=dict, description="STL decompositions for top numeric columns")
    detected_periodicities: List[str] = Field(default_factory=list, description="e.g. 'Weekly dips on weekends', 'End of month spikes'")

class GeoAnalysis(BaseModel):
    has_geo_patterns: bool = Field(..., description="True if useful geographic entities found")
    geo_columns: List[str] = Field(default_factory=list, description="Columns classified as geo_coordinate or location names")
    bounding_box: Optional[Dict[str, float]] = Field(None, description="{'min_lat', 'max_lat', 'min_lon', 'max_lon'}")
    geo_distribution: Dict[str, int] = Field(default_factory=dict, description="Aggregation by region/country for map rendering")

# -------------------------------------------------------------------
# Unified Result
# -------------------------------------------------------------------

class CrossColumnProfile(BaseModel):
    correlations: CorrelationAnalysis = Field(..., description="Correlation and Multicollinearity")
    target: TargetAnalysis = Field(..., description="Target detection and problem framing")
    temporal: TemporalAnalysis = Field(..., description="Time-series specific structural patterns")
    geo: GeoAnalysis = Field(..., description="Geospatial distribution patterns")
