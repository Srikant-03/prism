"""
Pydantic models for all API request/response schemas.
These are the structured contracts between backend and frontend.
"""

from __future__ import annotations

import enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ──────────────────────────────────────────
# Enums
# ──────────────────────────────────────────

class FileFormat(str, enum.Enum):
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    PARQUET = "parquet"
    FEATHER = "feather"
    XML = "xml"
    SQL = "sql"
    COMPRESSED = "compressed"
    UNKNOWN = "unknown"


class IngestionStage(str, enum.Enum):
    UPLOADING = "uploading"
    DETECTING_FORMAT = "detecting_format"
    DETECTING_ENCODING = "detecting_encoding"
    DECOMPRESSING = "decompressing"
    PARSING = "parsing"
    VALIDATING = "validating"
    CHECKING_MALFORMED = "checking_malformed"
    COMPLETE = "complete"
    ERROR = "error"


class SchemaRelationship(str, enum.Enum):
    SAME_SCHEMA = "same_schema"        # Files can be merged
    DIFFERENT_SCHEMA = "different_schema"  # Files are separate tables
    MIXED = "mixed"                     # Some match, some don't — user decides


class MalformedSeverity(str, enum.Enum):
    WARNING = "warning"    # Data can still be parsed, but something is off
    ERROR = "error"        # Data loss or corruption detected


# ──────────────────────────────────────────
# Sub-models
# ──────────────────────────────────────────

class EncodingInfo(BaseModel):
    """Detected encoding information with confidence."""
    encoding: str = Field(..., description="Detected encoding name (e.g., 'utf-8', 'latin-1')")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Detection confidence 0-1")
    language: Optional[str] = Field(None, description="Detected language if available")


class DelimiterInfo(BaseModel):
    """Detected delimiter information."""
    delimiter: str = Field(..., description="Detected delimiter character")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Detection confidence 0-1")
    alternatives: list[str] = Field(default_factory=list, description="Other possible delimiters")


class SheetInfo(BaseModel):
    """Information about a single Excel sheet."""
    name: str
    index: int
    row_count: int
    col_count: int
    has_merged_cells: bool = False
    preview: list[list[Any]] = Field(default_factory=list, description="First few rows as nested lists")


class ColumnInfo(BaseModel):
    """Metadata about a single column."""
    name: str
    dtype: str
    non_null_count: int
    null_count: int
    sample_values: list[Any] = Field(default_factory=list)
    unique_count: Optional[int] = None


class MalformedRow(BaseModel):
    """A single malformed row report."""
    row_number: int
    raw_content: str
    issue: str
    severity: MalformedSeverity
    affected_columns: list[str] = Field(default_factory=list)
    suggested_fix: Optional[str] = None


class MalformedReport(BaseModel):
    """Complete report of malformed data in a file."""
    has_issues: bool = False
    total_issues: int = 0
    issues: list[MalformedRow] = Field(default_factory=list)
    summary: str = ""
    best_effort_rows_parsed: int = 0
    best_effort_rows_dropped: int = 0


class SchemaComparisonEntry(BaseModel):
    """Schema comparison for a single file in a multi-file upload."""
    filename: str
    columns: list[str]
    dtypes: dict[str, str]
    row_count: int


class SchemaComparison(BaseModel):
    """Result of comparing schemas across multiple files."""
    relationship: SchemaRelationship
    confidence: float = Field(..., ge=0.0, le=1.0)
    justification: str
    files: list[SchemaComparisonEntry] = Field(default_factory=list)
    common_columns: list[str] = Field(default_factory=list)
    differing_columns: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Map of filename → columns unique to that file"
    )


# ──────────────────────────────────────────
# Progress
# ──────────────────────────────────────────

class ProgressUpdate(BaseModel):
    """Real-time progress update sent via WebSocket."""
    file_id: str
    stage: IngestionStage
    progress_pct: float = Field(0.0, ge=0.0, le=100.0)
    bytes_read: int = 0
    total_bytes: int = 0
    eta_seconds: Optional[float] = None
    memory_usage_mb: float = 0.0
    message: str = ""


# ──────────────────────────────────────────
# File Metadata
# ──────────────────────────────────────────

class FileMetadata(BaseModel):
    """Complete metadata about an ingested file."""
    file_id: str
    original_filename: str
    file_size_bytes: int
    format: FileFormat
    encoding: Optional[EncodingInfo] = None
    delimiter: Optional[DelimiterInfo] = None
    sheets: Optional[list[SheetInfo]] = None
    row_count: int = 0
    col_count: int = 0
    columns: list[ColumnInfo] = Field(default_factory=list)
    ingestion_time_seconds: float = 0.0


# ──────────────────────────────────────────
# Ingestion Result
# ──────────────────────────────────────────

class IngestionResult(BaseModel):
    """The complete result of ingesting a file."""
    success: bool
    file_id: str
    metadata: FileMetadata
    preview_data: list[dict[str, Any]] = Field(
        default_factory=list,
        description="First N rows as list of dicts for table display"
    )
    malformed_report: Optional[MalformedReport] = None
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    justification: str = Field(
        "",
        description="Plain-English explanation of what was detected and how it was handled"
    )

    # For Excel files requiring sheet selection
    requires_sheet_selection: bool = False

    # For multi-file uploads requiring schema decision
    requires_schema_decision: bool = False
    schema_comparison: Optional[SchemaComparison] = None


# ──────────────────────────────────────────
# Request models
# ──────────────────────────────────────────

class SheetSelectionRequest(BaseModel):
    """User's sheet selection for Excel files."""
    file_id: str
    selected_sheets: list[int] = Field(..., description="Indices of selected sheets")


class MalformedConfirmRequest(BaseModel):
    """User's decision on malformed data."""
    file_id: str
    accept_best_effort: bool
    drop_malformed_rows: bool = False


class MultiFileResolveRequest(BaseModel):
    """User's decision on multi-file schema conflict."""
    file_ids: list[str]
    action: str = Field(..., description="'merge', 'separate', or 'exclude'")
    merge_file_ids: list[str] = Field(default_factory=list, description="File IDs to merge if action=merge")
