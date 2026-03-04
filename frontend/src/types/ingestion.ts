/**
 * TypeScript type definitions mirroring the backend Pydantic models.
 * These are the contracts between frontend and backend.
 */

// ── Enums ──

export type FileFormat = 'csv' | 'excel' | 'json' | 'parquet' | 'feather' | 'xml' | 'sql' | 'compressed' | 'unknown';

export type IngestionStage =
  | 'uploading'
  | 'detecting_format'
  | 'detecting_encoding'
  | 'decompressing'
  | 'parsing'
  | 'validating'
  | 'checking_malformed'
  | 'complete'
  | 'error';

export type SchemaRelationship = 'same_schema' | 'different_schema' | 'mixed';

export type MalformedSeverity = 'warning' | 'error';

// ── Sub-types ──

export interface EncodingInfo {
  encoding: string;
  confidence: number;
  language?: string | null;
}

export interface DelimiterInfo {
  delimiter: string;
  confidence: number;
  alternatives: string[];
}

export interface SheetInfo {
  name: string;
  index: number;
  row_count: number;
  col_count: number;
  has_merged_cells: boolean;
  preview: unknown[][];
}

export interface ColumnInfo {
  name: string;
  dtype: string;
  non_null_count: number;
  null_count: number;
  sample_values: unknown[];
  unique_count?: number | null;
}

export interface MalformedRow {
  row_number: number;
  raw_content: string;
  issue: string;
  severity: MalformedSeverity;
  affected_columns: string[];
  suggested_fix?: string | null;
}

export interface MalformedReport {
  has_issues: boolean;
  total_issues: number;
  issues: MalformedRow[];
  summary: string;
  best_effort_rows_parsed: number;
  best_effort_rows_dropped: number;
}

export interface SchemaComparisonEntry {
  filename: string;
  columns: string[];
  dtypes: Record<string, string>;
  row_count: number;
}

export interface SchemaComparison {
  relationship: SchemaRelationship;
  confidence: number;
  justification: string;
  files: SchemaComparisonEntry[];
  common_columns: string[];
  differing_columns: Record<string, string[]>;
}

// ── Progress ──

export interface ProgressUpdate {
  file_id: string;
  stage: IngestionStage;
  progress_pct: number;
  bytes_read: number;
  total_bytes: number;
  eta_seconds?: number | null;
  memory_usage_mb: number;
  message: string;
}

// ── File Metadata ──

export interface FileMetadata {
  file_id: string;
  original_filename: string;
  file_size_bytes: number;
  format: FileFormat;
  encoding?: EncodingInfo | null;
  delimiter?: DelimiterInfo | null;
  sheets?: SheetInfo[] | null;
  row_count: number;
  col_count: number;
  columns: ColumnInfo[];
  ingestion_time_seconds: number;
}

// ── Ingestion Result ──

export interface IngestionResult {
  success: boolean;
  file_id: string;
  metadata: FileMetadata;
  preview_data: Record<string, unknown>[];
  malformed_report?: MalformedReport | null;
  warnings: string[];
  errors: string[];
  justification: string;
  requires_sheet_selection: boolean;
  requires_schema_decision: boolean;
  schema_comparison?: SchemaComparison | null;
  profile?: import('./profiling').DatasetProfile;
}

// ── Multi-file Result ──

export interface MultiFileResult {
  results: Record<string, IngestionResult>;
  schema_comparison?: SchemaComparison | null;
  requires_schema_decision: boolean;
}

// ── Malformed Comparison (side-by-side viewer) ──

export interface MalformedComparison {
  type: 'row_issue' | 'column_issue';
  row_number?: number;
  raw?: string;
  parsed?: Record<string, string>;
  issue: string;
  severity: string;
  suggested_fix?: string;
  affected_columns?: string[];
}

// ── Upload State ──

export type UploadStatus = 'idle' | 'uploading' | 'processing' | 'awaiting_sheet_selection' | 'awaiting_malformed_review' | 'awaiting_schema_decision' | 'complete' | 'error';

export interface UploadState {
  status: UploadStatus;
  progress: ProgressUpdate | null;
  result: IngestionResult | null;
  multiResult: MultiFileResult | null;
  error: string | null;
}
