/**
 * TypeScript types for the profiling engine results.
 * Mirrors the backend profiling_models.py Pydantic models.
 */

// Semantic Types
export type SemanticType =
    | 'numeric_continuous' | 'numeric_discrete' | 'categorical_nominal'
    | 'categorical_ordinal' | 'boolean' | 'datetime' | 'duration'
    | 'free_text' | 'url' | 'email' | 'phone' | 'geo_coordinate'
    | 'currency' | 'percentage' | 'id_key' | 'binary_encoded'
    | 'hashed' | 'ip_address' | 'unknown';

export type CardinalityClass = 'binary' | 'low' | 'medium' | 'high' | 'very_high' | 'near_unique' | 'unique';
export type SkewnessInterpretation = 'symmetric' | 'moderately_skewed' | 'highly_skewed';
export type KurtosisInterpretation = 'mesokurtic' | 'leptokurtic' | 'platykurtic';
export type FrequencyPattern = 'daily' | 'weekly' | 'monthly' | 'yearly' | 'hourly' | 'irregular';

export interface ValueFrequency {
    value: unknown;
    count: number;
    percentage: number;
}

export interface NumericProfile {
    min: number | null; max: number | null; range: number | null; sum: number | null;
    mean: number | null; trimmed_mean_5: number | null;
    geometric_mean: number | null; harmonic_mean: number | null;
    median: number | null; modes: number[];
    std_dev: number | null; variance: number | null; coefficient_of_variation: number | null;
    skewness: number | null; skewness_interpretation: SkewnessInterpretation | null;
    kurtosis: number | null; kurtosis_interpretation: KurtosisInterpretation | null;
    percentiles: Record<string, number>;
    iqr: number | null;
    zero_count: number; negative_count: number; positive_count: number; integer_valued_count: number;
    histogram_bins: number[]; histogram_counts: number[]; histogram_method: string;
    kde_x: number[]; kde_y: number[];
    box_q1: number | null; box_q2: number | null; box_q3: number | null;
    box_whisker_low: number | null; box_whisker_high: number | null; box_outliers: number[];
    qq_theoretical: number[]; qq_sample: number[];
    formatting_issues: string[];
}

export interface CategoricalProfile {
    cardinality: number; cardinality_class: CardinalityClass | null;
    top_values: ValueFrequency[]; bottom_values: ValueFrequency[];
    pie_data: { name: string; value: number }[];
    treemap_data: { name: string; value: number }[];
    word_cloud_data: { text: string; value: number }[];
    case_inconsistencies: { normalized: string; variants: string[]; variant_count: number }[];
    whitespace_issues: { type: string; count: number; examples: string[] }[];
    special_char_contamination: string[];
    suspected_ordinal: boolean;
    ordinal_order: string[];
}

export interface DatetimeProfile {
    detected_formats: string[]; mixed_formats: boolean;
    earliest: string | null; latest: string | null;
    time_span_days: number | null; coverage_density: number | null;
    frequency: FrequencyPattern | null; frequency_justification: string;
    gaps: { start: string; end: string; duration_days: number }[];
    gap_count: number;
    timezone_info: string; mixed_timezones: boolean;
    future_dates_count: number; implausible_dates_count: number;
    seasonality_indicator: string | null;
}

export interface BooleanProfile {
    true_count: number; false_count: number;
    true_ratio: number; false_ratio: number;
    is_disguised: boolean;
    disguised_mapping: Record<string, string>;
}

export interface TextProfile {
    avg_length: number; min_length: number; max_length: number;
    avg_token_count: number;
    detected_language: string | null; language_confidence: number;
    entity_types_found: string[];
    html_contamination: boolean; markdown_contamination: boolean;
    pii_risks: { type: string; count?: number; percentage?: number; confidence: string; recommendation: string }[];
    has_pii_risk: boolean;
}

export interface ColumnProfile {
    name: string; position: number;
    inferred_dtype: string; semantic_type: SemanticType;
    semantic_type_confidence: number;
    null_count: number; null_percentage: number; non_null_count: number;
    distinct_count: number; distinct_percentage: number;
    most_frequent: ValueFrequency | null;
    least_frequent: ValueFrequency | null;
    sample_values: unknown[];
    quality_score: number; quality_justification: string;
    numeric: NumericProfile | null;
    categorical: CategoricalProfile | null;
    datetime: DatetimeProfile | null;
    boolean: BooleanProfile | null;
    text: TextProfile | null;
}

export interface KeyCandidate {
    columns: string[];
    uniqueness: number;
    justification: string;
}

export interface DatasetProfile {
    total_rows: number; total_columns: number;
    memory_size_bytes: number; memory_size_readable: string;
    disk_size_bytes: number; disk_size_readable: string;
    inferred_schema: Record<string, string>;
    schema_confidence: Record<string, number>;
    estimated_domain: string; domain_confidence: number; domain_justification: string;
    structural_completeness: number; schema_consistency: number;
    schema_consistency_details: Record<string, string>;
    temporal_columns: string[];
    temporal_coverage: { column: string; earliest: string; latest: string; span_days: number } | null;
    primary_key_candidates: KeyCandidate[];
    foreign_key_candidates: KeyCandidate[];
    id_columns: string[];
    columns: ColumnProfile[];
    profiling_time_seconds: number;
    cross_analysis?: any;
    insights?: import('./insight').DatasetInsights;
}

export interface ProfilingResult {
    success: boolean;
    file_id: string;
    profile: DatasetProfile;
    warnings: string[];
    errors: string[];
}

// ──────────────────────────────────────────
// Cross-Column Analysis
// ──────────────────────────────────────────

export interface CorrelationPair {
    col1: string;
    col2: string;
    score: number;
    p_value?: number | null;
    metric: string;
    is_significant: boolean;
}

export interface MulticollinearityReport {
    has_multicollinearity: boolean;
    vif_scores: Record<string, number>;
    warnings: string[];
}

export interface CorrelationAnalysis {
    correlation_matrix: Record<string, Record<string, number>>;
    strongest_pairs: CorrelationPair[];
    multicollinearity: MulticollinearityReport;
    mutual_information: Record<string, Record<string, number>>;
}

export interface FeatureImportance {
    feature: string;
    importance_score: number;
}

export interface TargetAnalysis {
    is_target_detected: boolean;
    target_column?: string | null;
    confidence: number;
    justification?: string | null;
    problem_type?: string | null;
    class_distribution?: Record<string, number> | null;
    imbalance_ratio?: number | null;
    top_predictors: FeatureImportance[];
}

export interface TimeSeriesComponent {
    trend: number[];
    seasonal: number[];
    residual: number[];
    timestamps: string[];
}

export interface TemporalAnalysis {
    has_temporal_patterns: boolean;
    primary_time_col?: string | null;
    decompositions: Record<string, TimeSeriesComponent>;
    detected_periodicities: string[];
}

export interface GeoAnalysis {
    has_geo_patterns: boolean;
    geo_columns: string[];
    bounding_box?: {
        min_lat: number;
        max_lat: number;
        min_lon: number;
        max_lon: number;
    } | null;
    geo_distribution: Record<string, number>;
}

export interface CrossColumnProfile {
    correlations: CorrelationAnalysis;
    target: TargetAnalysis;
    temporal: TemporalAnalysis;
    geo: GeoAnalysis;
}
