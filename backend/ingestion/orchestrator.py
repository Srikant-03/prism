"""
Ingestion Orchestrator — the central brain of the file ingestion pipeline.

Receives an uploaded file and:
1. Detects format (MIME + extension + magic bytes)
2. Decompresses if needed
3. Auto-detects encoding
4. Routes to the correct parser
5. Handles large files via chunked reading
6. Runs post-parse malformed data analysis
7. For multi-file uploads: compares schemas
8. Returns a structured IngestionResult with data, metadata, and justifications
"""

from __future__ import annotations

import os
import time
import uuid
import shutil
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from config import AppConfig, IngestionConfig
from models.schemas import (
    ColumnInfo,
    FileFormat,
    FileMetadata,
    IngestionResult,
    IngestionStage,
    MalformedReport,
    ProgressUpdate,
    SchemaComparison,
)
from ingestion.detector import FormatDetector, EncodingDetector, DelimiterDetector
from ingestion.parsers.base import BaseParser, ParseResult
from ingestion.parsers.csv_parser import CSVParser
from ingestion.parsers.excel_parser import ExcelParser
from ingestion.parsers.json_parser import JSONParser
from ingestion.parsers.parquet_parser import ParquetParser
from ingestion.parsers.xml_parser import XMLParser
from ingestion.parsers.sql_parser import SQLParser
from ingestion.parsers.compressed import CompressedParser
from ingestion.chunked_reader import ChunkedReader
from ingestion.malformed_handler import MalformedHandler
from ingestion.schema_comparator import SchemaComparator


# In-memory store for ingestion results (in production, use Redis/DB)
_ingestion_store: dict[str, dict] = {}


class IngestionOrchestrator:
    """
    Orchestrates the complete file ingestion pipeline.
    Every decision is evidence-based and justified.
    """

    def __init__(
        self,
        progress_callback: Optional[Callable[[ProgressUpdate], None]] = None,
    ):
        """
        Args:
            progress_callback: Called with ProgressUpdate at each stage change.
                              Used to stream progress via WebSocket.
        """
        self.config = IngestionConfig()
        self.progress_callback = progress_callback

    def _emit_progress(
        self, file_id: str, stage: IngestionStage, pct: float = 0, message: str = ""
    ):
        """Send a progress update if a callback is registered."""
        if self.progress_callback:
            import psutil
            try:
                mem = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
            except Exception:
                mem = 0
            self.progress_callback(ProgressUpdate(
                file_id=file_id,
                stage=stage,
                progress_pct=pct,
                memory_usage_mb=round(mem, 1),
                message=message,
            ))

    async def ingest_file(
        self,
        file_path: Path,
        original_filename: str,
        file_id: Optional[str] = None,
    ) -> IngestionResult:
        """
        Main entry point: ingest a single file through the full pipeline.
        
        Returns a complete IngestionResult with data preview, metadata,
        malformed report, and justification narrative.
        """
        file_id = file_id or str(uuid.uuid4())
        start_time = time.time()
        justification_parts = []
        warnings = []

        try:
            # ─── Stage 1: Format Detection ───
            self._emit_progress(file_id, IngestionStage.DETECTING_FORMAT, 5,
                                "Detecting file format...")
            file_format = FormatDetector.detect(file_path)
            justification_parts.append(f"Detected format: {file_format.value}.")

            # ─── Stage 2: Decompression (if needed) ───
            if file_format == FileFormat.COMPRESSED:
                self._emit_progress(file_id, IngestionStage.DECOMPRESSING, 10,
                                    "Decompressing archive...")
                decompress_result = self._handle_compressed(file_path, file_id)
                if decompress_result is not None:
                    return decompress_result

            # ─── Stage 3: Encoding Detection ───
            self._emit_progress(file_id, IngestionStage.DETECTING_ENCODING, 15,
                                "Detecting file encoding...")

            # Binary formats don't need encoding detection
            if file_format in (FileFormat.PARQUET, FileFormat.FEATHER):
                encoding_info = None
                encoding = "utf-8"
                justification_parts.append("Binary columnar format — encoding detection skipped.")
            else:
                encoding_info = EncodingDetector.detect(file_path)
                encoding = encoding_info.encoding
                justification_parts.append(
                    f"Detected encoding: {encoding} (confidence: {encoding_info.confidence:.0%})."
                )
                if encoding_info.confidence < self.config.ENCODING_CONFIDENCE_THRESHOLD:
                    warnings.append(
                        f"Low confidence encoding detection ({encoding_info.confidence:.0%}). "
                        f"Proceeding with '{encoding}' but data may have encoding issues."
                    )

            # ─── Stage 4: Create Parser ───
            parser = self._create_parser(file_format, file_path, encoding)
            if parser is None:
                return self._error_result(
                    file_id, file_path, original_filename, start_time,
                    f"No parser available for format: {file_format.value}"
                )

            # ─── Stage 5: Validation ───
            is_valid, validation_msg = parser.validate()
            if not is_valid:
                return self._error_result(
                    file_id, file_path, original_filename, start_time,
                    f"File validation failed: {validation_msg}"
                )

            # ─── Stage 6: Parsing ───
            self._emit_progress(file_id, IngestionStage.PARSING, 25, "Parsing file data...")

            file_size = file_path.stat().st_size
            chunked_reader = ChunkedReader(file_path, file_id)

            if chunked_reader.is_large_file:
                # Large file → chunked parsing
                justification_parts.append(
                    f"File size ({chunked_reader._format_bytes(file_size)}) exceeds threshold "
                    f"({chunked_reader._format_bytes(self.config.LARGE_FILE_THRESHOLD)}). "
                    f"Using chunked reading mode."
                )
                chunked_reader.start()
                ws_callback = self.progress_callback

                def progress_fn(pct, br, tb):
                    chunked_reader.update(br)
                    p = chunked_reader.get_progress()
                    p.progress_pct = pct
                    p.stage = IngestionStage.PARSING
                    if ws_callback:
                        ws_callback(p)

                parse_result = parser.parse_chunked(
                    chunk_size=chunked_reader.get_optimal_chunk_size(),
                    progress_callback=progress_fn,
                )
            else:
                parse_result = parser.parse()

            justification_parts.append(parse_result.justification)
            warnings.extend(parse_result.warnings)

            # Handle Excel sheet selection requirement
            if parse_result.metadata.get("requires_sheet_selection"):
                sheets = parse_result.metadata.get("sheets", [])
                from models.schemas import SheetInfo as SI
                sheet_infos = []
                for s in sheets:
                    if isinstance(s, dict):
                        sheet_infos.append(SI(**s))
                    else:
                        sheet_infos.append(s)

                metadata = FileMetadata(
                    file_id=file_id,
                    original_filename=original_filename,
                    file_size_bytes=file_size,
                    format=file_format,
                    encoding=encoding_info,
                    sheets=sheet_infos,
                    ingestion_time_seconds=round(time.time() - start_time, 3),
                )

                result = IngestionResult(
                    success=True,
                    file_id=file_id,
                    metadata=metadata,
                    requires_sheet_selection=True,
                    justification=" ".join(justification_parts),
                    warnings=warnings,
                )

                _ingestion_store[file_id] = {
                    "file_path": str(file_path),
                    "parser": parser,
                    "encoding": encoding,
                    "encoding_info": encoding_info,
                    "format": file_format,
                    "original_filename": original_filename,
                    "start_time": start_time,
                }

                return result

            df = parse_result.dataframe

            # ─── Stage 7: Post-parse Malformed Analysis ───
            self._emit_progress(file_id, IngestionStage.CHECKING_MALFORMED, 80,
                                "Checking data quality...")
            malformed_report = MalformedHandler.analyze(df, parse_result.malformed)

            # ─── Stage 8: Build Result ───
            self._emit_progress(file_id, IngestionStage.COMPLETE, 100, "Ingestion complete!")

            preview_rows = self.config.PREVIEW_ROWS
            preview_data = (
                df.head(preview_rows).fillna("").to_dict(orient="records")
                if not df.empty else []
            )

            # Build column info
            columns = []
            for col in df.columns:
                series = df[col]
                columns.append(ColumnInfo(
                    name=str(col),
                    dtype=str(series.dtype),
                    non_null_count=int(series.notna().sum()),
                    null_count=int(series.isna().sum()),
                    sample_values=series.dropna().head(5).tolist(),
                    unique_count=int(series.nunique()) if len(df) < 100000 else None,
                ))

            metadata = FileMetadata(
                file_id=file_id,
                original_filename=original_filename,
                file_size_bytes=file_size,
                format=file_format,
                encoding=encoding_info,
                delimiter=None,
                row_count=len(df),
                col_count=len(df.columns),
                columns=columns,
                ingestion_time_seconds=round(time.time() - start_time, 3),
            )

            # Add delimiter info for CSV
            if file_format == FileFormat.CSV and "delimiter" in parse_result.metadata:
                from models.schemas import DelimiterInfo
                metadata.delimiter = DelimiterInfo(
                    delimiter=parse_result.metadata["delimiter"],
                    confidence=parse_result.metadata.get("delimiter_confidence", 0.0),
                    alternatives=parse_result.metadata.get("delimiter_alternatives", []),
                )

            # Store the DataFrame for later use
            _ingestion_store[file_id] = {
                "dataframe": df,
                "file_path": str(file_path),
                "metadata": metadata,
                "malformed_report": malformed_report,
            }

            return IngestionResult(
                success=True,
                file_id=file_id,
                metadata=metadata,
                preview_data=preview_data,
                malformed_report=malformed_report,
                warnings=warnings,
                justification=" ".join(justification_parts),
            )

        except Exception as e:
            self._emit_progress(file_id, IngestionStage.ERROR, 0, f"Error: {str(e)}")
            return self._error_result(
                file_id, file_path, original_filename, start_time, str(e)
            )

    async def ingest_multiple_files(
        self,
        file_paths: list[tuple[Path, str]],  # (path, original_filename) pairs
    ) -> dict[str, Any]:
        """
        Ingest multiple files, compare their schemas, and classify relationships.
        
        Returns individual results plus schema comparison.
        """
        results: dict[str, IngestionResult] = {}
        dataframes: dict[str, pd.DataFrame] = {}

        for file_path, original_filename in file_paths:
            result = await self.ingest_file(file_path, original_filename)
            results[result.file_id] = result

            if result.success and result.file_id in _ingestion_store:
                stored = _ingestion_store[result.file_id]
                if "dataframe" in stored:
                    dataframes[original_filename] = stored["dataframe"]

        # Compare schemas if multiple files were successfully parsed
        schema_comparison = None
        if len(dataframes) >= 2:
            schema_comparison = SchemaComparator.compare(dataframes)

        return {
            "results": results,
            "schema_comparison": schema_comparison,
            "requires_schema_decision": (
                schema_comparison is not None
                and schema_comparison.relationship.value != "same_schema"
            ),
        }

    async def select_sheets(
        self, file_id: str, selected_sheets: list[int]
    ) -> IngestionResult:
        """Handle user's sheet selection for Excel files."""
        stored = _ingestion_store.get(file_id)
        if not stored:
            return IngestionResult(
                success=False,
                file_id=file_id,
                metadata=FileMetadata(
                    file_id=file_id,
                    original_filename="unknown",
                    file_size_bytes=0,
                    format=FileFormat.UNKNOWN,
                ),
                errors=["File not found in ingestion store. Please re-upload."],
                justification="File session expired or not found.",
            )

        parser = stored["parser"]
        parse_result = parser.parse(selected_sheets=selected_sheets)

        df = parse_result.dataframe
        file_path = Path(stored["file_path"])
        malformed_report = MalformedHandler.analyze(df, parse_result.malformed)

        preview_data = (
            df.head(self.config.PREVIEW_ROWS).fillna("").to_dict(orient="records")
            if not df.empty else []
        )

        columns = []
        for col in df.columns:
            series = df[col]
            columns.append(ColumnInfo(
                name=str(col),
                dtype=str(series.dtype),
                non_null_count=int(series.notna().sum()),
                null_count=int(series.isna().sum()),
                sample_values=series.dropna().head(5).tolist(),
                unique_count=int(series.nunique()) if len(df) < 100000 else None,
            ))

        metadata = FileMetadata(
            file_id=file_id,
            original_filename=stored["original_filename"],
            file_size_bytes=file_path.stat().st_size,
            format=stored["format"],
            encoding=stored.get("encoding_info"),
            row_count=len(df),
            col_count=len(df.columns),
            columns=columns,
            ingestion_time_seconds=round(time.time() - stored["start_time"], 3),
        )

        _ingestion_store[file_id] = {
            "dataframe": df,
            "file_path": str(file_path),
            "metadata": metadata,
            "malformed_report": malformed_report,
        }

        return IngestionResult(
            success=True,
            file_id=file_id,
            metadata=metadata,
            preview_data=preview_data,
            malformed_report=malformed_report,
            justification=parse_result.justification,
        )

    def _create_parser(
        self, file_format: FileFormat, file_path: Path, encoding: str
    ) -> Optional[BaseParser]:
        """Factory: create the appropriate parser for a format."""
        parser_map = {
            FileFormat.CSV: lambda: CSVParser(file_path, encoding),
            FileFormat.EXCEL: lambda: ExcelParser(file_path, encoding),
            FileFormat.JSON: lambda: JSONParser(file_path, encoding),
            FileFormat.PARQUET: lambda: ParquetParser(file_path, encoding),
            FileFormat.FEATHER: lambda: ParquetParser(file_path, encoding),
            FileFormat.XML: lambda: XMLParser(file_path, encoding),
            FileFormat.SQL: lambda: SQLParser(file_path, encoding),
        }
        factory = parser_map.get(file_format)
        return factory() if factory else None

    def _handle_compressed(
        self, file_path: Path, file_id: str
    ) -> Optional[IngestionResult]:
        """
        Handle compressed files by decompressing and re-ingesting.
        Returns None if the orchestrator should continue with decompressed content,
        or an IngestionResult if the compressed file handling is complete.
        """
        parser = CompressedParser(file_path)
        is_valid, msg = parser.validate()
        if not is_valid:
            return self._error_result(
                file_id, file_path, file_path.name, time.time(), msg
            )

        # The compressed parser needs a factory to create inner parsers
        def parser_factory(inner_path: Path) -> BaseParser:
            inner_format = FormatDetector.detect(inner_path)
            encoding_info = EncodingDetector.detect(inner_path)
            parser = self._create_parser(inner_format, inner_path, encoding_info.encoding)
            if parser is None:
                return CSVParser(inner_path, encoding_info.encoding)  # Fallback
            return parser

        result = parser.parse(parser_factory=parser_factory)

        if result.metadata.get("requires_multi_file_handling"):
            # ZIP with multiple files — handle as multi-file
            pass  # Will be handled by orchestrator's multi-file flow

        return None  # Signal to continue with normal flow

    def _error_result(
        self, file_id: str, file_path: Path, filename: str, start_time: float, error: str
    ) -> IngestionResult:
        """Build a standardized error result."""
        return IngestionResult(
            success=False,
            file_id=file_id,
            metadata=FileMetadata(
                file_id=file_id,
                original_filename=filename,
                file_size_bytes=file_path.stat().st_size if file_path.exists() else 0,
                format=FileFormat.UNKNOWN,
                ingestion_time_seconds=round(time.time() - start_time, 3),
            ),
            errors=[error],
            justification=f"Ingestion failed: {error}",
        )


def get_stored_data(file_id: str) -> Optional[dict]:
    """Retrieve stored ingestion data by file ID."""
    return _ingestion_store.get(file_id)


def get_stored_dataframe(file_id: str) -> Optional[pd.DataFrame]:
    """Retrieve stored DataFrame by file ID."""
    stored = _ingestion_store.get(file_id)
    if stored and "dataframe" in stored:
        return stored["dataframe"]
    return None


def update_stored_dataframe(file_id: str, df: pd.DataFrame) -> bool:
    """Update the stored DataFrame after cleaning operations."""
    if file_id in _ingestion_store:
        _ingestion_store[file_id]["dataframe"] = df
        return True
    return False
