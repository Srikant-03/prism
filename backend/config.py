"""
Application configuration — all thresholds and settings are centralized here.
Nothing is hardcoded elsewhere in the application.
"""

import os
from pathlib import Path


class IngestionConfig:
    """Configuration for the file ingestion engine."""

    # --- File Size Thresholds ---
    # Files above this size (bytes) trigger chunked reading mode
    LARGE_FILE_THRESHOLD: int = int(os.getenv("LARGE_FILE_THRESHOLD", 50 * 1024 * 1024))  # 50 MB

    # Chunk size for reading large files
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", 5 * 1024 * 1024))  # 5 MB per chunk

    # Maximum file size allowed for upload (bytes); default 500 MB
    MAX_FILE_SIZE: int = int(os.getenv("MAX_FILE_SIZE", 500 * 1024 * 1024))

    # --- Preview Settings ---
    # Number of rows to include in data preview
    PREVIEW_ROWS: int = int(os.getenv("PREVIEW_ROWS", 100))

    # Number of rows sampled for type/delimiter detection
    DETECTION_SAMPLE_ROWS: int = int(os.getenv("DETECTION_SAMPLE_ROWS", 10000))

    # --- Encoding Detection ---
    # Minimum confidence (0-1) to accept an encoding detection result
    ENCODING_CONFIDENCE_THRESHOLD: float = float(os.getenv("ENCODING_CONFIDENCE_THRESHOLD", 0.5))

    # Bytes to sample for encoding detection
    ENCODING_SAMPLE_BYTES: int = int(os.getenv("ENCODING_SAMPLE_BYTES", 100000))

    # --- Malformed Data ---
    # Maximum number of malformed rows to report in detail
    MAX_MALFORMED_REPORT_ROWS: int = int(os.getenv("MAX_MALFORMED_REPORT_ROWS", 500))

    # --- Supported Formats ---
    # Map of file extensions to parser identifiers
    FORMAT_REGISTRY: dict = {
        ".csv": "csv",
        ".tsv": "csv",
        ".txt": "csv",
        ".xlsx": "excel",
        ".xls": "excel",
        ".xlsm": "excel",
        ".json": "json",
        ".parquet": "parquet",
        ".feather": "parquet",
        ".xml": "xml",
        ".sql": "sql",
        ".zip": "compressed",
        ".gz": "compressed",
        ".gzip": "compressed",
    }

    # --- Allowed MIME Types for Upload ---
    ALLOWED_MIME_TYPES: set = {
        "text/csv", "text/plain", "text/tab-separated-values",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
        "application/json",
        "application/xml", "text/xml",
        "application/octet-stream",  # fallback for parquet/feather/sql/compressed
        "application/x-parquet",
        "application/sql",
        "application/zip", "application/gzip", "application/x-gzip",
    }

    # --- Allowed File Extensions ---
    ALLOWED_EXTENSIONS: set = set(FORMAT_REGISTRY.keys())

    # --- Upload ---
    UPLOAD_DIR: Path = Path(os.getenv("UPLOAD_DIR", "./uploads"))
    TEMP_DIR: Path = Path(os.getenv("TEMP_DIR", "./temp"))

    # --- Schema Comparison ---
    # Minimum column name overlap ratio to classify files as same-schema
    SCHEMA_MATCH_THRESHOLD: float = float(os.getenv("SCHEMA_MATCH_THRESHOLD", 0.8))

    @classmethod
    def ensure_dirs(cls):
        """Create required directories if they don't exist."""
        cls.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        cls.TEMP_DIR.mkdir(parents=True, exist_ok=True)


class CleaningConfig:
    """Configuration for the autonomous data cleaning engine."""

    # --- Missing Values ---
    NULL_DROP_COLUMN_THRESHOLD_BASE: float = float(os.getenv("NULL_DROP_COL_THRESHOLD", 0.7))
    NULL_DROP_ROW_THRESHOLD_BASE: float = float(os.getenv("NULL_DROP_ROW_THRESHOLD", 0.6))

    # --- Duplicates ---
    FUZZY_MATCH_THRESHOLD: float = float(os.getenv("FUZZY_MATCH_THRESHOLD", 0.85))
    DUPLICATE_COLUMN_SIMILARITY: float = float(os.getenv("DUP_COL_SIMILARITY", 0.98))
    DERIVED_COLUMN_R2_THRESHOLD: float = float(os.getenv("DERIVED_R2_THRESHOLD", 0.99))
    NEAR_DUPLICATE_SAMPLE_SIZE: int = int(os.getenv("NEAR_DUP_SAMPLE", 10000))


class LLMConfig:
    """Configuration for the LLM-powered features (NL Query)."""

    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    
    # Parse a comma-separated list of keys, falling back to the single key if not found
    _keys_env = os.getenv("GEMINI_API_KEYS", "")
    GEMINI_API_KEYS: list[str] = [k.strip() for k in _keys_env.split(",")] if _keys_env else ([GEMINI_API_KEY] if GEMINI_API_KEY else [])
    MODEL_HEAVY: str = os.getenv("LLM_MODEL_HEAVY", "gemini-2.5-flash")
    MODEL_WORKHORSE: str = os.getenv("LLM_MODEL_WORKHORSE", "gemini-2.5-flash")
    MODEL_TRIAGE: str = os.getenv("LLM_MODEL_TRIAGE", "gemini-2.5-flash-lite")
    TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", 0.1))
    MAX_TOKENS: int = int(os.getenv("LLM_MAX_TOKENS", 4096))


class AppConfig:
    """Top-level application configuration."""

    VERSION: str = os.getenv("APP_VERSION", "2.0.0")
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", 8000))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000").split(",")
    API_KEY: str = os.getenv("DATA_INTEL_API_KEY", "dev-secret-key-123")
    RATE_LIMIT: str = os.getenv("RATE_LIMIT", "60/minute")            # General API rate limit
    RATE_LIMIT_UPLOAD: str = os.getenv("RATE_LIMIT_UPLOAD", "10/minute")  # Stricter limit for uploads

    ingestion = IngestionConfig()
    cleaning = CleaningConfig()
    llm = LLMConfig()
