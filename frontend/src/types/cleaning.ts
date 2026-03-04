/**
 * TypeScript types for the Data Cleaning Pipeline.
 * Mirrors backend cleaning_models.py.
 */

export type ActionType =
    // Duplicates
    | 'remove_exact_duplicates' | 'remove_subset_duplicates' | 'merge_near_duplicates'
    | 'drop_duplicate_column' | 'drop_derived_column'
    // Missing values
    | 'drop_column' | 'drop_rows'
    | 'impute_mean' | 'impute_median' | 'impute_mode' | 'impute_constant'
    | 'impute_knn' | 'impute_iterative'
    | 'ffill' | 'bfill' | 'interpolate'
    | 'add_indicator' | 'flag_only'
    // Outliers
    | 'cap_outliers' | 'winsorize' | 'log_transform' | 'replace_boundary'
    | 'remove_outlier_rows' | 'flag_outlier'
    // Type correction
    | 'convert_type' | 'parse_dates' | 'parse_currency' | 'standardize_booleans'
    | 'replace_pseudo_nulls' | 'flag_integer_categories' | 'expand_json' | 'validate_structured'
    // Text preprocessing
    | 'normalize_text' | 'extract_text_features' | 'tfidf_vectorize' | 'drop_raw_text'
    | 'stem_lemmatize' | 'remove_stopwords' | 'correct_spelling'
    // Categorical encoding
    | 'label_encode' | 'one_hot_encode' | 'ordinal_encode' | 'frequency_encode'
    | 'target_encode' | 'binary_encode' | 'hash_encode' | 'suggest_embedding' | 'cyclical_encode'
    // Datetime engineering
    | 'extract_datetime' | 'derive_datetime_flags' | 'compute_elapsed_time'
    | 'compute_time_deltas' | 'time_series_features'
    // Feature scaling
    | 'standard_scale' | 'minmax_scale' | 'maxabs_scale' | 'robust_scale'
    | 'log1p_transform' | 'boxcox_transform' | 'yeojohnson_transform'
    | 'quantile_uniform' | 'quantile_normal' | 'binarize'
    // Feature selection
    | 'drop_zero_variance' | 'drop_near_zero_variance' | 'drop_high_correlation'
    | 'drop_high_vif' | 'drop_low_mutual_info' | 'suggest_pca' | 'cluster_features'
    // Class imbalance
    | 'random_oversample' | 'smote' | 'smote_nc' | 'adasyn'
    | 'random_undersample' | 'tomek_links' | 'edited_nearest_neighbors' | 'near_miss'
    | 'smoteenn' | 'smotetomek' | 'class_weights' | 'anomaly_framing'
    // Data standardization
    | 'standardize_casing' | 'standardize_whitespace' | 'normalize_unicode'
    | 'consolidate_synonyms' | 'fix_unit_inconsistency' | 'standardize_precision'
    | 'standardize_phone' | 'standardize_email' | 'standardize_url' | 'standardize_currency_format'
    // Data leakage
    | 'flag_leakage_temporal' | 'flag_leakage_predictor' | 'flag_leakage_id' | 'flag_leakage_future';

export type ActionConfidence = 'definitive' | 'judgment_call';
export type ActionCategory = 'duplicates' | 'missing_values' | 'outliers' | 'type_correction'
    | 'text_preprocessing' | 'categorical_encoding' | 'datetime_engineering'
    | 'feature_scaling' | 'feature_selection'
    | 'class_imbalance' | 'data_standardization' | 'data_leakage' | 'structural';
export type ActionStatus = 'pending' | 'applied' | 'skipped';
export type MissingPattern = 'mcar' | 'mar' | 'mnar' | 'unknown';
export type DuplicateType = 'exact_row' | 'subset_row' | 'near_duplicate' | 'duplicate_column' | 'derived_column';
export type OutlierMethod = 'iqr_mild' | 'iqr_extreme' | 'zscore' | 'modified_zscore' | 'isolation_forest' | 'dbscan' | 'lof' | 'timeseries_envelope' | 'business_rule';
export type DetectedType = 'datetime' | 'currency' | 'percentage' | 'boolean' | 'integer' | 'float' | 'categorical_int' | 'json_blob' | 'list' | 'url' | 'email' | 'ip_address' | 'uuid' | 'free_text' | 'unknown';

export interface PreviewSample {
    before: Record<string, unknown>[];
    after: Record<string, unknown>[];
    columns_before: string[];
    columns_after: string[];
}

export interface ImpactEstimate {
    rows_before: number;
    rows_after: number;
    rows_affected: number;
    rows_affected_pct: number;
    columns_before: number;
    columns_after: number;
    columns_affected: number;
    memory_delta_bytes: number;
    description: string;
}

export interface UserOption {
    key: string;
    label: string;
    description: string;
    is_default: boolean;
}

export interface DuplicateGroup {
    duplicate_type: DuplicateType;
    group_id: number;
    size: number;
    sample_rows: Record<string, unknown>[];
    columns_involved: string[];
    similarity_score: number | null;
    description: string;
}

export interface DuplicateReport {
    exact_count: number;
    exact_pct: number;
    subset_groups: DuplicateGroup[];
    near_duplicate_clusters: DuplicateGroup[];
    duplicate_column_pairs: Record<string, unknown>[];
    derived_column_pairs: Record<string, unknown>[];
}

export interface ColumnMissingStrategy {
    column: string;
    null_count: number;
    null_pct: number;
    recommended_strategy: ActionType;
    reasoning: string;
    alternative_strategies: ActionType[];
    feature_importance: number | null;
    missing_pattern: MissingPattern;
}

export interface MissingValueReport {
    total_missing_cells: number;
    total_cells: number;
    overall_missing_pct: number;
    column_strategies: ColumnMissingStrategy[];
    high_null_rows: number;
    high_null_row_threshold: number;
    pattern_analysis: Record<string, unknown>;
    nullity_matrix: {
        columns: string[];
        data: number[][];
        sample_size: number;
    } | null;
}

// ── Outlier types ────────────────────────────────────────────────────

export interface OutlierDetail {
    column: string;
    row_index: number;
    value: number;
    detection_methods: OutlierMethod[];
    z_score: number | null;
    modified_z_score: number | null;
    iqr_multiple: number | null;
    is_likely_error: boolean;
    error_likelihood: number;
    error_reasoning: string;
    context_values: Record<string, unknown>;
}

export interface ColumnOutlierSummary {
    column: string;
    total_outliers: number;
    outlier_pct: number;
    methods_used: OutlierMethod[];
    likely_errors: number;
    sample_outliers: OutlierDetail[];
    distribution_stats: Record<string, number>;
    recommended_treatment: ActionType;
    treatment_reasoning: string;
}

export interface OutlierReport {
    total_outlier_rows: number;
    total_outlier_values: number;
    columns_analyzed: number;
    column_summaries: ColumnOutlierSummary[];
    multivariate_outliers: Record<string, unknown>[];
    business_rule_violations: Record<string, unknown>[];
}

// ── Type correction types ────────────────────────────────────────────

export interface TypeCorrectionDetail {
    column: string;
    current_dtype: string;
    detected_type: DetectedType;
    confidence: number;
    parse_success_rate: number;
    sample_before: string[];
    sample_after: string[];
    unparseable_count: number;
    unparseable_samples: string[];
}

export interface TypeCorrectionReport {
    corrections: TypeCorrectionDetail[];
    mixed_type_columns: string[];
    structured_data_columns: Record<string, unknown>[];
    total_columns_analyzed: number;
    total_corrections_found: number;
}

// ── Text preprocessing types ─────────────────────────────────────────

export interface TextColumnAnalysis {
    column: string;
    avg_token_count: number;
    avg_char_length: number;
    detected_language: string;
    language_confidence: number;
    has_html: boolean;
    has_markdown: boolean;
    has_special_chars: boolean;
    has_emojis: boolean;
    has_urls: boolean;
    unique_ratio: number;
    recommended_operations: ActionType[];
    extracted_features_preview: Record<string, unknown>;
}

export interface TextFeatureReport {
    text_columns: TextColumnAnalysis[];
    total_text_columns: number;
    total_features_extractable: number;
}

// ── Core models ──────────────────────────────────────────────────────

export interface CleaningAction {
    index: number;
    category: ActionCategory;
    action_type: ActionType;
    confidence: ActionConfidence;
    status: ActionStatus;
    evidence: string;
    recommendation: string;
    reasoning: string;
    target_columns: string[];
    target_rows: number[] | null;
    preview: PreviewSample | null;
    impact: ImpactEstimate;
    options: UserOption[];
    selected_option: string | null;
    metadata: Record<string, unknown>;
}

export interface CleaningPlan {
    file_id: string;
    total_actions: number;
    definitive_count: number;
    judgment_call_count: number;
    actions: CleaningAction[];
    duplicate_report: DuplicateReport | null;
    missing_report: MissingValueReport | null;
    outlier_report: OutlierReport | null;
    type_report: TypeCorrectionReport | null;
    text_report: TextFeatureReport | null;
    estimated_rows_affected: number;
    estimated_rows_affected_pct: number;
    estimated_columns_affected: number;
}

export interface ActionResult {
    success: boolean;
    action_index: number;
    action_type: ActionType;
    rows_before: number;
    rows_after: number;
    columns_before: number;
    columns_after: number;
    description: string;
    preview_after: Record<string, unknown>[];
}
