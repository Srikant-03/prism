"""
Pydantic models for all profiling results.
Covers dataset-level, per-column universal, and type-specific profiling.
"""

from __future__ import annotations

import enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ──────────────────────────────────────────
# Semantic Types (12+)
# ──────────────────────────────────────────

class SemanticType(str, enum.Enum):
    NUMERIC_CONTINUOUS = "numeric_continuous"
    NUMERIC_DISCRETE = "numeric_discrete"
    CATEGORICAL_NOMINAL = "categorical_nominal"
    CATEGORICAL_ORDINAL = "categorical_ordinal"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    DURATION = "duration"
    FREE_TEXT = "free_text"
    URL = "url"
    EMAIL = "email"
    PHONE = "phone"
    GEO_COORDINATE = "geo_coordinate"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    ID_KEY = "id_key"
    BINARY_ENCODED = "binary_encoded"
    HASHED = "hashed"
    IP_ADDRESS = "ip_address"
    UNKNOWN = "unknown"


class CardinalityClass(str, enum.Enum):
    BINARY = "binary"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"
    NEAR_UNIQUE = "near_unique"
    UNIQUE = "unique"


class SkewnessInterpretation(str, enum.Enum):
    SYMMETRIC = "symmetric"
    MODERATELY_SKEWED = "moderately_skewed"
    HIGHLY_SKEWED = "highly_skewed"


class KurtosisInterpretation(str, enum.Enum):
    MESOKURTIC = "mesokurtic"
    LEPTOKURTIC = "leptokurtic"
    PLATYKURTIC = "platykurtic"


class FrequencyPattern(str, enum.Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    HOURLY = "hourly"
    IRREGULAR = "irregular"


# ──────────────────────────────────────────
# Value Frequency
# ──────────────────────────────────────────

class ValueFrequency(BaseModel):
    value: Any
    count: int
    percentage: float


# ──────────────────────────────────────────
# Numeric Profile
# ──────────────────────────────────────────

class NumericProfile(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None
    range: Optional[float] = None
    sum: Optional[float] = None

    mean: Optional[float] = None
    trimmed_mean_5: Optional[float] = None
    geometric_mean: Optional[float] = None
    harmonic_mean: Optional[float] = None

    median: Optional[float] = None
    modes: list[float] = Field(default_factory=list)

    std_dev: Optional[float] = None
    variance: Optional[float] = None
    coefficient_of_variation: Optional[float] = None

    skewness: Optional[float] = None
    skewness_interpretation: Optional[SkewnessInterpretation] = None
    kurtosis: Optional[float] = None
    kurtosis_interpretation: Optional[KurtosisInterpretation] = None

    percentiles: dict[str, float] = Field(default_factory=dict)
    iqr: Optional[float] = None

    zero_count: int = 0
    negative_count: int = 0
    positive_count: int = 0
    integer_valued_count: int = 0

    # Histogram data
    histogram_bins: list[float] = Field(default_factory=list)
    histogram_counts: list[int] = Field(default_factory=list)
    histogram_method: str = "freedman_diaconis"

    # KDE data
    kde_x: list[float] = Field(default_factory=list)
    kde_y: list[float] = Field(default_factory=list)

    # Box plot data
    box_q1: Optional[float] = None
    box_q2: Optional[float] = None
    box_q3: Optional[float] = None
    box_whisker_low: Optional[float] = None
    box_whisker_high: Optional[float] = None
    box_outliers: list[float] = Field(default_factory=list)

    # Q-Q plot data
    qq_theoretical: list[float] = Field(default_factory=list)
    qq_sample: list[float] = Field(default_factory=list)

    # Formatting issues
    formatting_issues: list[str] = Field(default_factory=list)


# ──────────────────────────────────────────
# Categorical Profile
# ──────────────────────────────────────────

class CategoricalProfile(BaseModel):
    cardinality: int = 0
    cardinality_class: Optional[CardinalityClass] = None

    top_values: list[ValueFrequency] = Field(default_factory=list)
    bottom_values: list[ValueFrequency] = Field(default_factory=list)

    # Pie chart data (for low cardinality)
    pie_data: list[dict[str, Any]] = Field(default_factory=list)

    # Treemap data (for medium cardinality)
    treemap_data: list[dict[str, Any]] = Field(default_factory=list)

    # Word cloud data (for free-text / high cardinality)
    word_cloud_data: list[dict[str, Any]] = Field(default_factory=list)

    # Quality checks
    case_inconsistencies: list[dict[str, Any]] = Field(default_factory=list)
    whitespace_issues: list[dict[str, Any]] = Field(default_factory=list)
    special_char_contamination: list[str] = Field(default_factory=list)
    suspected_ordinal: bool = False
    ordinal_order: list[str] = Field(default_factory=list)


# ──────────────────────────────────────────
# Datetime Profile
# ──────────────────────────────────────────

class DatetimeProfile(BaseModel):
    detected_formats: list[str] = Field(default_factory=list)
    mixed_formats: bool = False

    earliest: Optional[str] = None
    latest: Optional[str] = None
    time_span_days: Optional[float] = None
    coverage_density: Optional[float] = None

    frequency: Optional[FrequencyPattern] = None
    frequency_justification: str = ""

    gaps: list[dict[str, Any]] = Field(default_factory=list)
    gap_count: int = 0

    timezone_info: str = "unknown"
    mixed_timezones: bool = False

    future_dates_count: int = 0
    implausible_dates_count: int = 0

    seasonality_indicator: Optional[str] = None


# ──────────────────────────────────────────
# Boolean Profile
# ──────────────────────────────────────────

class BooleanProfile(BaseModel):
    true_count: int = 0
    false_count: int = 0
    true_ratio: float = 0.0
    false_ratio: float = 0.0
    is_disguised: bool = False
    disguised_mapping: dict[str, str] = Field(default_factory=dict)


# ──────────────────────────────────────────
# Text Profile
# ──────────────────────────────────────────

class TextProfile(BaseModel):
    avg_length: float = 0.0
    min_length: int = 0
    max_length: int = 0
    avg_token_count: float = 0.0

    detected_language: Optional[str] = None
    language_confidence: float = 0.0

    entity_types_found: list[str] = Field(default_factory=list)
    html_contamination: bool = False
    markdown_contamination: bool = False

    pii_risks: list[dict[str, Any]] = Field(default_factory=list)
    has_pii_risk: bool = False


# ──────────────────────────────────────────
# Per-Column Profile
# ──────────────────────────────────────────

class ColumnProfile(BaseModel):
    name: str
    position: int

    # Universal
    inferred_dtype: str
    semantic_type: SemanticType
    semantic_type_confidence: float = Field(0.0, ge=0.0, le=1.0)

    null_count: int = 0
    null_percentage: float = 0.0
    non_null_count: int = 0
    distinct_count: int = 0
    distinct_percentage: float = 0.0

    most_frequent: Optional[ValueFrequency] = None
    least_frequent: Optional[ValueFrequency] = None
    sample_values: list[Any] = Field(default_factory=list)

    quality_score: float = Field(0.0, ge=0.0, le=100.0)
    quality_justification: str = ""

    # Type-specific profiles (only one populated per column)
    numeric: Optional[NumericProfile] = None
    categorical: Optional[CategoricalProfile] = None
    datetime: Optional[DatetimeProfile] = None
    boolean: Optional[BooleanProfile] = None
    text: Optional[TextProfile] = None


# ──────────────────────────────────────────
# Key Detection
# ──────────────────────────────────────────

class KeyCandidate(BaseModel):
    columns: list[str]
    uniqueness: float
    justification: str


# ──────────────────────────────────────────
# Dataset-Level Profile
# ──────────────────────────────────────────

class DatasetProfile(BaseModel):
    # Metadata
    total_rows: int = 0
    total_columns: int = 0
    memory_size_bytes: int = 0
    memory_size_readable: str = ""
    disk_size_bytes: int = 0
    disk_size_readable: str = ""

    # Schema
    inferred_schema: dict[str, str] = Field(default_factory=dict)
    schema_confidence: dict[str, float] = Field(default_factory=dict)

    # Domain
    estimated_domain: str = ""
    domain_confidence: float = 0.0
    domain_justification: str = ""

    # Scores
    structural_completeness: float = 0.0
    schema_consistency: float = 0.0
    schema_consistency_details: dict[str, str] = Field(default_factory=dict)

    # Temporal
    temporal_columns: list[str] = Field(default_factory=list)
    temporal_coverage: Optional[dict[str, Any]] = None

    # Key detection
    primary_key_candidates: list[KeyCandidate] = Field(default_factory=list)
    foreign_key_candidates: list[KeyCandidate] = Field(default_factory=list)
    id_columns: list[str] = Field(default_factory=list)

    # Per-column profiles
    columns: list[ColumnProfile] = Field(default_factory=list)

    # Profiling time
    profiling_time_seconds: float = 0.0

    # Cross-Column analysis
    cross_analysis: Optional[dict[str, Any]] = None

    # Generated analytical insights
    insights: Optional[Any] = None


# ──────────────────────────────────────────
# API Response
# ──────────────────────────────────────────

class ProfilingResult(BaseModel):
    success: bool
    file_id: str
    profile: DatasetProfile
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
