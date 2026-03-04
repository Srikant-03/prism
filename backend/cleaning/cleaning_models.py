"""
Pydantic models for the Autonomous Data Cleaning & Preprocessing Engine.
Every cleaning action carries evidence, reasoning, a before/after preview,
and a quantitative impact estimate.
"""

from __future__ import annotations

import enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ──────────────────────────────────────────
# Enums
# ──────────────────────────────────────────

class ActionType(str, enum.Enum):
    """All possible cleaning operations."""
    # Duplicate handling
    REMOVE_EXACT_DUPLICATES = "remove_exact_duplicates"
    REMOVE_SUBSET_DUPLICATES = "remove_subset_duplicates"
    MERGE_NEAR_DUPLICATES = "merge_near_duplicates"
    DROP_DUPLICATE_COLUMN = "drop_duplicate_column"
    DROP_DERIVED_COLUMN = "drop_derived_column"

    # Missing value treatment
    DROP_COLUMN = "drop_column"
    DROP_ROWS = "drop_rows"
    IMPUTE_MEAN = "impute_mean"
    IMPUTE_MEDIAN = "impute_median"
    IMPUTE_MODE = "impute_mode"
    IMPUTE_CONSTANT = "impute_constant"
    IMPUTE_KNN = "impute_knn"
    IMPUTE_ITERATIVE = "impute_iterative"
    FFILL = "ffill"
    BFILL = "bfill"
    INTERPOLATE = "interpolate"
    ADD_INDICATOR = "add_indicator"
    FLAG_ONLY = "flag_only"

    # Outlier handling
    CAP_OUTLIERS = "cap_outliers"
    WINSORIZE = "winsorize"
    LOG_TRANSFORM = "log_transform"
    REPLACE_BOUNDARY = "replace_boundary"
    REMOVE_OUTLIER_ROWS = "remove_outlier_rows"
    FLAG_OUTLIER = "flag_outlier"

    # Data type correction
    CONVERT_TYPE = "convert_type"
    PARSE_DATES = "parse_dates"
    PARSE_CURRENCY = "parse_currency"
    STANDARDIZE_BOOLEANS = "standardize_booleans"
    REPLACE_PSEUDO_NULLS = "replace_pseudo_nulls"
    FLAG_INTEGER_CATEGORIES = "flag_integer_categories"
    EXPAND_JSON = "expand_json"
    VALIDATE_STRUCTURED = "validate_structured"

    # Text preprocessing
    NORMALIZE_TEXT = "normalize_text"
    EXTRACT_TEXT_FEATURES = "extract_text_features"
    TFIDF_VECTORIZE = "tfidf_vectorize"
    DROP_RAW_TEXT = "drop_raw_text"
    STEM_LEMMATIZE = "stem_lemmatize"
    REMOVE_STOPWORDS = "remove_stopwords"
    CORRECT_SPELLING = "correct_spelling"

    # Categorical encoding
    LABEL_ENCODE = "label_encode"
    ONE_HOT_ENCODE = "one_hot_encode"
    ORDINAL_ENCODE = "ordinal_encode"
    FREQUENCY_ENCODE = "frequency_encode"
    TARGET_ENCODE = "target_encode"
    BINARY_ENCODE = "binary_encode"
    HASH_ENCODE = "hash_encode"
    SUGGEST_EMBEDDING = "suggest_embedding"
    CYCLICAL_ENCODE = "cyclical_encode"

    # Datetime feature engineering
    EXTRACT_DATETIME = "extract_datetime"
    DERIVE_DATETIME_FLAGS = "derive_datetime_flags"
    COMPUTE_ELAPSED_TIME = "compute_elapsed_time"
    COMPUTE_TIME_DELTAS = "compute_time_deltas"
    TIME_SERIES_FEATURES = "time_series_features"

    # Feature scaling & normalization
    STANDARD_SCALE = "standard_scale"
    MINMAX_SCALE = "minmax_scale"
    MAXABS_SCALE = "maxabs_scale"
    ROBUST_SCALE = "robust_scale"
    LOG1P_TRANSFORM = "log1p_transform"
    BOXCOX_TRANSFORM = "boxcox_transform"
    YEOJOHNSON_TRANSFORM = "yeojohnson_transform"
    QUANTILE_UNIFORM = "quantile_uniform"
    QUANTILE_NORMAL = "quantile_normal"
    BINARIZE = "binarize"

    # Feature selection & dimensionality
    DROP_ZERO_VARIANCE = "drop_zero_variance"
    DROP_NEAR_ZERO_VARIANCE = "drop_near_zero_variance"
    DROP_HIGH_CORRELATION = "drop_high_correlation"
    DROP_HIGH_VIF = "drop_high_vif"
    DROP_LOW_MI = "drop_low_mutual_info"
    SUGGEST_PCA = "suggest_pca"
    CLUSTER_FEATURES = "cluster_features"

    # Class imbalance handling
    RANDOM_OVERSAMPLE = "random_oversample"
    SMOTE = "smote"
    SMOTE_NC = "smote_nc"
    ADASYN = "adasyn"
    RANDOM_UNDERSAMPLE = "random_undersample"
    TOMEK_LINKS = "tomek_links"
    EDITED_NEAREST_NEIGHBORS = "edited_nearest_neighbors"
    NEAR_MISS = "near_miss"
    SMOTEENN = "smoteenn"
    SMOTETOMEK = "smotetomek"
    CLASS_WEIGHTS = "class_weights"
    ANOMALY_FRAMING = "anomaly_framing"

    # Data standardization & consistency
    STANDARDIZE_CASING = "standardize_casing"
    STANDARDIZE_WHITESPACE = "standardize_whitespace"
    NORMALIZE_UNICODE = "normalize_unicode"
    CONSOLIDATE_SYNONYMS = "consolidate_synonyms"
    FIX_UNIT_INCONSISTENCY = "fix_unit_inconsistency"
    STANDARDIZE_PRECISION = "standardize_precision"
    STANDARDIZE_PHONE = "standardize_phone"
    STANDARDIZE_EMAIL = "standardize_email"
    STANDARDIZE_URL = "standardize_url"
    STANDARDIZE_CURRENCY_FORMAT = "standardize_currency_format"

    # Data leakage detection
    FLAG_LEAKAGE_TEMPORAL = "flag_leakage_temporal"
    FLAG_LEAKAGE_PREDICTOR = "flag_leakage_predictor"
    FLAG_LEAKAGE_ID = "flag_leakage_id"
    FLAG_LEAKAGE_FUTURE = "flag_leakage_future"


class ActionConfidence(str, enum.Enum):
    """Whether the action is safe to auto-apply or requires user judgment."""
    DEFINITIVE = "definitive"
    JUDGMENT_CALL = "judgment_call"


class ActionCategory(str, enum.Enum):
    """Top-level grouping of actions for the UI."""
    DUPLICATES = "duplicates"
    MISSING_VALUES = "missing_values"
    OUTLIERS = "outliers"
    TYPE_CORRECTION = "type_correction"
    TEXT_PREPROCESSING = "text_preprocessing"
    CATEGORICAL_ENCODING = "categorical_encoding"
    DATETIME_ENGINEERING = "datetime_engineering"
    FEATURE_SCALING = "feature_scaling"
    FEATURE_SELECTION = "feature_selection"
    CLASS_IMBALANCE = "class_imbalance"
    DATA_STANDARDIZATION = "data_standardization"
    DATA_LEAKAGE = "data_leakage"
    STRUCTURAL = "structural"


class ActionStatus(str, enum.Enum):
    """State of a proposed action."""
    PENDING = "pending"
    APPLIED = "applied"
    SKIPPED = "skipped"


class MissingPattern(str, enum.Enum):
    """Missing data mechanism classification."""
    MCAR = "mcar"
    MAR = "mar"
    MNAR = "mnar"
    UNKNOWN = "unknown"


class DuplicateType(str, enum.Enum):
    """Type of duplicate detected."""
    EXACT_ROW = "exact_row"
    SUBSET_ROW = "subset_row"
    NEAR_DUPLICATE = "near_duplicate"
    DUPLICATE_COLUMN = "duplicate_column"
    DERIVED_COLUMN = "derived_column"


class OutlierMethod(str, enum.Enum):
    """Detection method that flagged an outlier."""
    IQR_MILD = "iqr_mild"       # 1.5× IQR
    IQR_EXTREME = "iqr_extreme"  # 3× IQR
    ZSCORE = "zscore"
    MODIFIED_ZSCORE = "modified_zscore"
    ISOLATION_FOREST = "isolation_forest"
    DBSCAN = "dbscan"
    LOF = "lof"
    TIMESERIES_ENVELOPE = "timeseries_envelope"
    BUSINESS_RULE = "business_rule"


class DetectedType(str, enum.Enum):
    """Types the type-detection engine can identify."""
    DATETIME = "datetime"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    FLOAT = "float"
    CATEGORICAL_INT = "categorical_int"
    JSON_BLOB = "json_blob"
    LIST = "list"
    URL = "url"
    EMAIL = "email"
    IP_ADDRESS = "ip_address"
    UUID = "uuid"
    FREE_TEXT = "free_text"
    UNKNOWN = "unknown"


# ──────────────────────────────────────────
# Sub-models
# ──────────────────────────────────────────

class PreviewSample(BaseModel):
    """Before/after preview for a cleaning action."""
    before: list[dict[str, Any]] = Field(default_factory=list, description="Sample rows before the action")
    after: list[dict[str, Any]] = Field(default_factory=list, description="Sample rows after the action")
    columns_before: list[str] = Field(default_factory=list)
    columns_after: list[str] = Field(default_factory=list)


class ImpactEstimate(BaseModel):
    """Quantitative impact of applying an action."""
    rows_before: int = 0
    rows_after: int = 0
    rows_affected: int = 0
    rows_affected_pct: float = 0.0
    columns_before: int = 0
    columns_after: int = 0
    columns_affected: int = 0
    memory_delta_bytes: int = 0
    description: str = ""


class UserOption(BaseModel):
    """A selectable option presented to the user for judgment calls."""
    key: str
    label: str
    description: str = ""
    is_default: bool = False


# ──────────────────────────────────────────
# Duplicate-specific models
# ──────────────────────────────────────────

class DuplicateGroup(BaseModel):
    """A group of duplicate rows / columns for review."""
    duplicate_type: DuplicateType
    group_id: int
    size: int
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    columns_involved: list[str] = Field(default_factory=list)
    similarity_score: Optional[float] = None
    description: str = ""


class DuplicateReport(BaseModel):
    """Aggregate report for all duplicate types."""
    exact_count: int = 0
    exact_pct: float = 0.0
    subset_groups: list[DuplicateGroup] = Field(default_factory=list)
    near_duplicate_clusters: list[DuplicateGroup] = Field(default_factory=list)
    duplicate_column_pairs: list[dict[str, Any]] = Field(default_factory=list)
    derived_column_pairs: list[dict[str, Any]] = Field(default_factory=list)


# ──────────────────────────────────────────
# Missing-value-specific models
# ──────────────────────────────────────────

class ColumnMissingStrategy(BaseModel):
    """Recommended missing-value strategy for a single column."""
    column: str
    null_count: int = 0
    null_pct: float = 0.0
    recommended_strategy: ActionType
    reasoning: str = ""
    alternative_strategies: list[ActionType] = Field(default_factory=list)
    feature_importance: Optional[float] = None
    missing_pattern: MissingPattern = MissingPattern.UNKNOWN


class MissingValueReport(BaseModel):
    """Aggregate report for missing value analysis."""
    total_missing_cells: int = 0
    total_cells: int = 0
    overall_missing_pct: float = 0.0
    column_strategies: list[ColumnMissingStrategy] = Field(default_factory=list)
    high_null_rows: int = 0
    high_null_row_threshold: float = 0.0
    pattern_analysis: dict[str, Any] = Field(default_factory=dict)
    nullity_matrix: Optional[dict[str, Any]] = None


# ──────────────────────────────────────────
# Outlier-specific models
# ──────────────────────────────────────────

class OutlierDetail(BaseModel):
    """A single detected outlier with full context."""
    column: str
    row_index: int
    value: float
    detection_methods: list[OutlierMethod] = Field(default_factory=list)
    z_score: Optional[float] = None
    modified_z_score: Optional[float] = None
    iqr_multiple: Optional[float] = None
    is_likely_error: bool = False
    error_likelihood: float = 0.0  # 0–1 probability
    error_reasoning: str = ""
    context_values: dict[str, Any] = Field(default_factory=dict)


class ColumnOutlierSummary(BaseModel):
    """Outlier summary for a single column."""
    column: str
    total_outliers: int = 0
    outlier_pct: float = 0.0
    methods_used: list[OutlierMethod] = Field(default_factory=list)
    likely_errors: int = 0
    sample_outliers: list[OutlierDetail] = Field(default_factory=list)
    distribution_stats: dict[str, Any] = Field(default_factory=dict)
    recommended_treatment: ActionType = ActionType.FLAG_OUTLIER
    treatment_reasoning: str = ""


class OutlierReport(BaseModel):
    """Aggregate outlier report."""
    total_outlier_rows: int = 0
    total_outlier_values: int = 0
    columns_analyzed: int = 0
    column_summaries: list[ColumnOutlierSummary] = Field(default_factory=list)
    multivariate_outliers: list[dict[str, Any]] = Field(default_factory=list)
    business_rule_violations: list[dict[str, Any]] = Field(default_factory=list)


# ──────────────────────────────────────────
# Type-correction-specific models
# ──────────────────────────────────────────

class TypeCorrectionDetail(BaseModel):
    """Detected type mismatch for a column."""
    column: str
    current_dtype: str = ""
    detected_type: DetectedType = DetectedType.UNKNOWN
    confidence: float = 0.0
    parse_success_rate: float = 0.0
    sample_before: list[str] = Field(default_factory=list)
    sample_after: list[str] = Field(default_factory=list)
    unparseable_count: int = 0
    unparseable_samples: list[str] = Field(default_factory=list)


class TypeCorrectionReport(BaseModel):
    """Type correction analysis report."""
    corrections: list[TypeCorrectionDetail] = Field(default_factory=list)
    mixed_type_columns: list[str] = Field(default_factory=list)
    structured_data_columns: list[dict[str, Any]] = Field(default_factory=list)
    total_columns_analyzed: int = 0
    total_corrections_found: int = 0


# ──────────────────────────────────────────
# Text-preprocessing-specific models
# ──────────────────────────────────────────

class TextColumnAnalysis(BaseModel):
    """Analysis of a single text column."""
    column: str
    avg_token_count: float = 0.0
    avg_char_length: float = 0.0
    detected_language: str = "en"
    language_confidence: float = 0.0
    has_html: bool = False
    has_markdown: bool = False
    has_special_chars: bool = False
    has_emojis: bool = False
    has_urls: bool = False
    unique_ratio: float = 0.0
    recommended_operations: list[ActionType] = Field(default_factory=list)
    extracted_features_preview: dict[str, Any] = Field(default_factory=dict)


class TextFeatureReport(BaseModel):
    """Text preprocessing analysis report."""
    text_columns: list[TextColumnAnalysis] = Field(default_factory=list)
    total_text_columns: int = 0
    total_features_extractable: int = 0


# ──────────────────────────────────────────
# Core Action & Plan models
# ──────────────────────────────────────────

class CleaningAction(BaseModel):
    """A single proposed cleaning operation with full evidence chain."""
    index: int = 0
    category: ActionCategory
    action_type: ActionType
    confidence: ActionConfidence
    status: ActionStatus = ActionStatus.PENDING

    # Evidence chain
    evidence: str = Field(..., description="What was found in the data")
    recommendation: str = Field(..., description="What action is recommended")
    reasoning: str = Field(..., description="Why this action is appropriate")

    # Targets
    target_columns: list[str] = Field(default_factory=list)
    target_rows: Optional[list[int]] = None

    # Preview & impact
    preview: Optional[PreviewSample] = None
    impact: ImpactEstimate = Field(default_factory=ImpactEstimate)

    # User options (for judgment calls)
    options: list[UserOption] = Field(default_factory=list)
    selected_option: Optional[str] = None

    # Extra metadata
    metadata: dict[str, Any] = Field(default_factory=dict)


class CleaningPlan(BaseModel):
    """Complete cleaning plan generated by the decision engine."""
    file_id: str
    total_actions: int = 0
    definitive_count: int = 0
    judgment_call_count: int = 0
    actions: list[CleaningAction] = Field(default_factory=list)

    # Sub-reports
    duplicate_report: Optional[DuplicateReport] = None
    missing_report: Optional[MissingValueReport] = None
    outlier_report: Optional[OutlierReport] = None
    type_report: Optional[TypeCorrectionReport] = None
    text_report: Optional[TextFeatureReport] = None

    # Summary
    estimated_rows_affected: int = 0
    estimated_rows_affected_pct: float = 0.0
    estimated_columns_affected: int = 0


class ActionResult(BaseModel):
    """Outcome after applying a single cleaning action."""
    success: bool
    action_index: int
    action_type: ActionType
    rows_before: int
    rows_after: int
    columns_before: int
    columns_after: int
    description: str = ""
    preview_after: list[dict[str, Any]] = Field(default_factory=list)
