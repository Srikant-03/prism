"""
File format, encoding, and delimiter auto-detection engine.
All detection is evidence-based — uses magic bytes, statistical analysis, and
confidence scoring. Never assumes anything about the file.
"""

from __future__ import annotations

import io
import os
import struct
from pathlib import Path
from typing import Optional, Tuple

from charset_normalizer import from_bytes
import clevercsv

from config import IngestionConfig
from models.schemas import (
    DelimiterInfo,
    EncodingInfo,
    FileFormat,
)


# ──────────────────────────────────────────
# Magic Bytes Signatures
# ──────────────────────────────────────────

# Common file signatures (magic bytes) for format detection
MAGIC_SIGNATURES: list[Tuple[bytes, FileFormat]] = [
    # ZIP archive (also xlsx, xlsm, docx, etc.)
    (b"PK\x03\x04", FileFormat.COMPRESSED),
    # GZIP
    (b"\x1f\x8b", FileFormat.COMPRESSED),
    # Parquet
    (b"PAR1", FileFormat.PARQUET),
    # Feather / Arrow IPC (starts with ARROW1)
    (b"ARROW1", FileFormat.FEATHER),
    # Excel legacy (.xls) — Compound File Binary Format
    (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", FileFormat.EXCEL),
]

# ZIP-based formats that need further checking
ZIP_SUBTYPES = {
    "xl/": FileFormat.EXCEL,  # xlsx, xlsm
}


class FormatDetector:
    """Detects file format using magic bytes, extension, and content analysis."""

    @staticmethod
    def detect(file_path: Path, content_sample: Optional[bytes] = None) -> FileFormat:
        """
        Detect the format of a file using a multi-signal approach:
        1. Magic bytes (most reliable)
        2. File extension (fallback)
        3. Content analysis (for ambiguous cases like CSV vs TSV vs TXT)
        """
        # Read first bytes for magic detection
        if content_sample is None:
            with open(file_path, "rb") as f:
                content_sample = f.read(8192)

        # 1. Magic bytes detection
        detected = FormatDetector._detect_by_magic(content_sample, file_path)
        if detected != FileFormat.UNKNOWN:
            return detected

        # 2. Extension-based detection
        ext = file_path.suffix.lower()
        config = IngestionConfig()
        if ext in config.FORMAT_REGISTRY:
            parser_id = config.FORMAT_REGISTRY[ext]
            format_map = {
                "csv": FileFormat.CSV,
                "excel": FileFormat.EXCEL,
                "json": FileFormat.JSON,
                "parquet": FileFormat.PARQUET,
                "xml": FileFormat.XML,
                "sql": FileFormat.SQL,
                "compressed": FileFormat.COMPRESSED,
            }
            return format_map.get(parser_id, FileFormat.UNKNOWN)

        # 3. Content heuristics for plaintext formats
        return FormatDetector._detect_by_content(content_sample)

    @staticmethod
    def _detect_by_magic(sample: bytes, file_path: Path) -> FileFormat:
        """Detect format by magic byte signatures."""
        for sig, fmt in MAGIC_SIGNATURES:
            if sample[:len(sig)] == sig:
                # Special handling: ZIP might be an xlsx/xlsm
                if fmt == FileFormat.COMPRESSED and file_path.suffix.lower() in (".xlsx", ".xlsm"):
                    return FileFormat.EXCEL
                if fmt == FileFormat.COMPRESSED:
                    # Check if it's a ZIP-based format (xlsx, xlsm)
                    return FormatDetector._check_zip_subtype(file_path, fmt)
                return fmt
        return FileFormat.UNKNOWN

    @staticmethod
    def _check_zip_subtype(file_path: Path, default: FileFormat) -> FileFormat:
        """Check if a ZIP file is actually an Office format."""
        import zipfile
        try:
            if zipfile.is_zipfile(str(file_path)):
                with zipfile.ZipFile(str(file_path), "r") as zf:
                    names = zf.namelist()
                    for prefix, fmt in ZIP_SUBTYPES.items():
                        if any(n.startswith(prefix) for n in names):
                            return fmt
        except Exception:
            pass
        return default

    @staticmethod
    def _detect_by_content(sample: bytes) -> FileFormat:
        """Detect plaintext format by content heuristics."""
        try:
            text = sample.decode("utf-8", errors="ignore").strip()
        except Exception:
            return FileFormat.UNKNOWN

        if not text:
            return FileFormat.UNKNOWN

        # JSON detection
        if text.startswith(("{", "[")):
            return FileFormat.JSON

        # XML detection
        if text.startswith("<?xml") or text.startswith("<"):
            # More rigorous XML check
            if "</" in text or "/>" in text:
                return FileFormat.XML

        # SQL detection
        sql_keywords = ["CREATE TABLE", "INSERT INTO", "DROP TABLE", "ALTER TABLE"]
        text_upper = text.upper()
        if any(kw in text_upper for kw in sql_keywords):
            return FileFormat.SQL

        # Default to CSV for tabular text
        return FileFormat.CSV


class EncodingDetector:
    """Auto-detects file encoding using statistical analysis."""

    @staticmethod
    def detect(file_path: Path, sample_bytes: Optional[int] = None) -> EncodingInfo:
        """
        Detect file encoding by sampling bytes and running charset-normalizer.
        Returns encoding name, confidence score, and detected language.
        """
        config = IngestionConfig()
        sample_size = sample_bytes or config.ENCODING_SAMPLE_BYTES

        with open(file_path, "rb") as f:
            raw = f.read(sample_size)

        if not raw:
            return EncodingInfo(encoding="utf-8", confidence=1.0, language=None)

        # Check for BOM markers first
        bom_result = EncodingDetector._check_bom(raw)
        if bom_result:
            return bom_result

        # Use charset-normalizer for statistical detection
        results = from_bytes(raw)
        best = results.best()

        if best is None:
            # Fallback to utf-8
            return EncodingInfo(encoding="utf-8", confidence=0.5, language=None)

        return EncodingInfo(
            encoding=best.encoding,
            confidence=round(1.0 - best.chaos, 4),  # chaos is inverse of confidence
            language=best.language,
        )

    @staticmethod
    def _check_bom(raw: bytes) -> Optional[EncodingInfo]:
        """Check for Byte Order Mark (BOM) at the start of the file."""
        bom_map = [
            (b"\xff\xfe\x00\x00", "utf-32-le"),
            (b"\x00\x00\xfe\xff", "utf-32-be"),
            (b"\xff\xfe", "utf-16-le"),
            (b"\xfe\xff", "utf-16-be"),
            (b"\xef\xbb\xbf", "utf-8-sig"),
        ]
        for bom, encoding in bom_map:
            if raw.startswith(bom):
                return EncodingInfo(encoding=encoding, confidence=1.0, language=None)
        return None


class DelimiterDetector:
    """Auto-detects CSV/TSV delimiter using statistical analysis."""

    # Common delimiters to test, ordered by typical frequency
    CANDIDATES = [",", ";", "\t", "|", " "]

    @staticmethod
    def detect(file_path: Path, encoding: str = "utf-8") -> DelimiterInfo:
        """
        Detect the delimiter of a text-based tabular file.
        Uses clevercsv for statistical analysis, with a fallback heuristic.
        """
        config = IngestionConfig()

        try:
            # Read sample for analysis
            with open(file_path, "r", encoding=encoding, errors="replace") as f:
                sample_lines = []
                for i, line in enumerate(f):
                    if i >= config.DETECTION_SAMPLE_ROWS:
                        break
                    sample_lines.append(line)

            sample_text = "".join(sample_lines)

            # Try clevercsv first — it uses a Bayesian approach
            try:
                dialect = clevercsv.Sniffer().sniffer(sample_text)
                if dialect and dialect.delimiter:
                    alternatives = DelimiterDetector._find_alternatives(sample_text, dialect.delimiter)
                    return DelimiterInfo(
                        delimiter=dialect.delimiter,
                        confidence=0.95,
                        alternatives=alternatives,
                    )
            except Exception:
                pass

            # Fallback: frequency-based detection
            return DelimiterDetector._heuristic_detect(sample_lines)

        except Exception as e:
            # Ultimate fallback
            return DelimiterInfo(delimiter=",", confidence=0.3, alternatives=[";", "\t"])

    @staticmethod
    def _heuristic_detect(lines: list[str]) -> DelimiterInfo:
        """
        Heuristic delimiter detection based on consistency of field counts.
        The best delimiter produces the most consistent column count across rows.
        """
        if not lines:
            return DelimiterInfo(delimiter=",", confidence=0.3, alternatives=[])

        best_delim = ","
        best_score = -1.0
        scores: dict[str, float] = {}

        for delim in DelimiterDetector.CANDIDATES:
            counts = [len(line.split(delim)) for line in lines if line.strip()]
            if not counts:
                continue

            # A good delimiter produces:
            # 1. More than 1 column
            # 2. Consistent column count
            avg = sum(counts) / len(counts)
            if avg <= 1.0:
                scores[delim] = 0.0
                continue

            # Coefficient of variation (lower = more consistent)
            variance = sum((c - avg) ** 2 for c in counts) / len(counts)
            std_dev = variance ** 0.5
            cv = std_dev / avg if avg > 0 else float("inf")

            # Score: reward more columns, penalize inconsistency
            score = avg * (1.0 - min(cv, 1.0))
            scores[delim] = score

            if score > best_score:
                best_score = score
                best_delim = delim

        # Calculate confidence from score distribution
        total = sum(scores.values())
        confidence = scores.get(best_delim, 0) / total if total > 0 else 0.3

        alternatives = [
            d for d, s in sorted(scores.items(), key=lambda x: -x[1])
            if d != best_delim and s > 0
        ]

        return DelimiterInfo(
            delimiter=best_delim,
            confidence=round(min(confidence, 1.0), 4),
            alternatives=alternatives[:3],
        )

    @staticmethod
    def _find_alternatives(text: str, primary: str) -> list[str]:
        """Find alternative delimiters that could also work."""
        alternatives = []
        for delim in DelimiterDetector.CANDIDATES:
            if delim != primary and text.count(delim) > len(text.splitlines()) * 0.5:
                alternatives.append(delim)
        return alternatives[:3]
