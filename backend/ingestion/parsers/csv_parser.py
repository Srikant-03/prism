"""
CSV / TSV / TXT parser with auto-delimiter detection,
malformed row handling, and chunked reading support.
"""

from __future__ import annotations

import csv
import io
import os
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from config import IngestionConfig
from ingestion.detector import DelimiterDetector
from models.schemas import MalformedReport, MalformedRow, MalformedSeverity
from ingestion.parsers.base import BaseParser, ParseResult


class CSVParser(BaseParser):
    """
    Parser for CSV, TSV, and plaintext tabular files.
    Auto-detects delimiters and handles malformed rows gracefully.
    """

    def __init__(self, file_path: Path, encoding: str = "utf-8", delimiter: Optional[str] = None):
        super().__init__(file_path, encoding)
        self._delimiter = delimiter
        self._delimiter_info = None

    def validate(self) -> tuple[bool, str]:
        """Check that the file exists, is readable, and has content."""
        if not self.file_path.exists():
            return False, f"File not found: {self.file_path}"
        if self.file_path.stat().st_size == 0:
            return False, "File is empty"
        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                first_line = f.readline()
            if not first_line.strip():
                return False, "File has no readable content"
            return True, "File is valid"
        except Exception as e:
            return False, f"Cannot read file: {str(e)}"

    def get_metadata(self) -> dict:
        """Return delimiter detection info and file stats."""
        if self._delimiter_info is None:
            self._delimiter_info = DelimiterDetector.detect(self.file_path, self.encoding)
        return {
            "delimiter": self._delimiter_info.delimiter,
            "delimiter_confidence": self._delimiter_info.confidence,
            "delimiter_alternatives": self._delimiter_info.alternatives,
        }

    def parse(self, **kwargs) -> ParseResult:
        """Parse the entire CSV file, detecting and reporting malformed rows."""
        config = IngestionConfig()

        # Auto-detect delimiter if not provided
        if self._delimiter is None:
            self._delimiter_info = DelimiterDetector.detect(self.file_path, self.encoding)
            self._delimiter = self._delimiter_info.delimiter
        else:
            self._delimiter_info = None

        justification_parts = [
            f"Detected file encoding: {self.encoding}.",
            f"Detected delimiter: {repr(self._delimiter)}.",
        ]

        # First pass: detect malformed rows
        malformed_report = self._detect_malformed_rows(config)

        # Main parse with error handling
        try:
            df = pd.read_csv(
                self.file_path,
                sep=self._delimiter,
                encoding=self.encoding,
                on_bad_lines="warn",
                engine="python",
                dtype=str,  # Read everything as string first to avoid data loss
                keep_default_na=False,
            )

            justification_parts.append(
                f"Successfully parsed {len(df)} rows × {len(df.columns)} columns."
            )

            if malformed_report.has_issues:
                justification_parts.append(
                    f"Found {malformed_report.total_issues} malformed rows. "
                    f"Best-effort parse retained {malformed_report.best_effort_rows_parsed} rows."
                )

            # Attempt type inference on the string data
            df = self._infer_types(df)

            return ParseResult(
                dataframe=df,
                metadata=self.get_metadata(),
                malformed=malformed_report,
                justification=" ".join(justification_parts),
            )

        except Exception as e:
            # If pandas fails, try a more lenient approach
            return self._fallback_parse(str(e), malformed_report, justification_parts)

    def parse_chunked(
        self,
        chunk_size: int = 10000,
        progress_callback: Optional[Callable[[float, int, int], None]] = None,
        **kwargs,
    ) -> ParseResult:
        """Parse a large CSV file in chunks, streaming progress updates."""
        if self._delimiter is None:
            self._delimiter_info = DelimiterDetector.detect(self.file_path, self.encoding)
            self._delimiter = self._delimiter_info.delimiter

        total_bytes = self.file_path.stat().st_size
        bytes_read = 0
        chunks = []
        malformed_rows = []

        try:
            reader = pd.read_csv(
                self.file_path,
                sep=self._delimiter,
                encoding=self.encoding,
                on_bad_lines="warn",
                engine="python",
                chunksize=chunk_size,
                dtype=str,
                keep_default_na=False,
            )

            for i, chunk in enumerate(reader):
                chunks.append(chunk)
                # Estimate bytes read based on rows processed
                rows_so_far = sum(len(c) for c in chunks)
                bytes_read = min(
                    int(total_bytes * (rows_so_far / max(rows_so_far + chunk_size, 1))),
                    total_bytes,
                )

                if progress_callback:
                    pct = (bytes_read / total_bytes) * 100 if total_bytes > 0 else 0
                    progress_callback(pct, bytes_read, total_bytes)

            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
            df = self._infer_types(df)

            if progress_callback:
                progress_callback(100.0, total_bytes, total_bytes)

            return ParseResult(
                dataframe=df,
                metadata=self.get_metadata(),
                justification=(
                    f"Parsed large CSV in {len(chunks)} chunks. "
                    f"Result: {len(df)} rows × {len(df.columns)} columns."
                ),
            )

        except Exception as e:
            return ParseResult(
                warnings=[f"Chunked parsing failed: {str(e)}"],
                justification=f"Chunked parsing encountered an error: {str(e)}",
            )

    def _detect_malformed_rows(self, config: IngestionConfig) -> MalformedReport:
        """
        Scan the file for rows that don't match the expected structure.
        Returns a detailed report of all issues found.
        """
        issues: list[MalformedRow] = []

        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                lines = f.readlines()

            if not lines:
                return MalformedReport()

            # Detect expected field count from header
            header_fields = len(lines[0].split(self._delimiter))

            for i, line in enumerate(lines[1:], start=2):  # 1-indexed, skip header
                if len(issues) >= config.MAX_MALFORMED_REPORT_ROWS:
                    break

                stripped = line.strip()
                if not stripped:
                    continue

                field_count = len(stripped.split(self._delimiter))

                if field_count != header_fields:
                    issues.append(MalformedRow(
                        row_number=i,
                        raw_content=stripped[:500],  # Truncate very long lines
                        issue=(
                            f"Expected {header_fields} fields but found {field_count}. "
                            f"{'Extra' if field_count > header_fields else 'Missing'} "
                            f"{abs(field_count - header_fields)} field(s)."
                        ),
                        severity=MalformedSeverity.WARNING if abs(field_count - header_fields) <= 2
                        else MalformedSeverity.ERROR,
                        suggested_fix=(
                            "Extra fields will be truncated" if field_count > header_fields
                            else "Missing fields will be filled with empty values"
                        ),
                    ))

                # Check for encoding issues
                if "\ufffd" in line:
                    issues.append(MalformedRow(
                        row_number=i,
                        raw_content=stripped[:500],
                        issue="Contains replacement characters (encoding errors detected)",
                        severity=MalformedSeverity.WARNING,
                        suggested_fix="Characters that could not be decoded were replaced with '�'",
                    ))

        except Exception as e:
            return MalformedReport(
                has_issues=True,
                total_issues=1,
                issues=[MalformedRow(
                    row_number=0,
                    raw_content="",
                    issue=f"Could not scan file for malformed rows: {str(e)}",
                    severity=MalformedSeverity.ERROR,
                )],
                summary=f"File scanning failed: {str(e)}",
            )

        if not issues:
            return MalformedReport(
                has_issues=False,
                summary="No malformed rows detected. All rows have consistent structure.",
            )

        return MalformedReport(
            has_issues=True,
            total_issues=len(issues),
            issues=issues,
            summary=(
                f"Found {len(issues)} row(s) with structural issues. "
                f"{sum(1 for i in issues if i.severity == MalformedSeverity.ERROR)} critical, "
                f"{sum(1 for i in issues if i.severity == MalformedSeverity.WARNING)} warnings."
            ),
            best_effort_rows_parsed=len(lines) - 1 - sum(
                1 for i in issues if i.severity == MalformedSeverity.ERROR
            ),
            best_effort_rows_dropped=sum(
                1 for i in issues if i.severity == MalformedSeverity.ERROR
            ),
        )

    def _infer_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Attempt intelligent type inference on string columns.
        Converts to numeric/datetime where appropriate, leaving strings untouched.
        """
        for col in df.columns:
            # Try numeric
            try:
                numeric = pd.to_numeric(df[col], errors="coerce")
                if numeric.notna().sum() / max(len(df), 1) > 0.5:
                    df[col] = numeric
                    continue
            except Exception:
                pass

            # Try datetime
            try:
                dt = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
                if dt.notna().sum() / max(len(df), 1) > 0.5:
                    df[col] = dt
                    continue
            except Exception:
                pass

        return df

    def _fallback_parse(
        self, error: str, malformed: MalformedReport, justification_parts: list[str]
    ) -> ParseResult:
        """
        Fallback parsing when pandas main parser fails.
        Reads line by line, skipping unparseable rows.
        """
        justification_parts.append(
            f"Primary parser failed ({error}). Falling back to line-by-line parsing."
        )
        rows = []
        header = None
        fallback_issues = []

        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                for i, line in enumerate(f, 1):
                    fields = line.strip().split(self._delimiter)
                    if header is None:
                        header = fields
                    elif len(fields) == len(header):
                        rows.append(fields)
                    else:
                        fallback_issues.append(MalformedRow(
                            row_number=i,
                            raw_content=line.strip()[:500],
                            issue=f"Row skipped: expected {len(header)} fields, got {len(fields)}",
                            severity=MalformedSeverity.ERROR,
                        ))

            df = pd.DataFrame(rows, columns=header) if header and rows else pd.DataFrame()
            df = self._infer_types(df)

            if fallback_issues:
                malformed.issues.extend(fallback_issues)
                malformed.total_issues = len(malformed.issues)
                malformed.has_issues = True

            justification_parts.append(
                f"Fallback parse recovered {len(df)} valid rows, skipped {len(fallback_issues)} malformed rows."
            )

            return ParseResult(
                dataframe=df,
                metadata=self.get_metadata(),
                malformed=malformed,
                warnings=[f"Used fallback parser due to: {error}"],
                justification=" ".join(justification_parts),
            )

        except Exception as e2:
            return ParseResult(
                warnings=[f"All parsing methods failed. Primary: {error}. Fallback: {str(e2)}"],
                justification=f"Complete parsing failure. Primary error: {error}. Fallback error: {str(e2)}",
            )
