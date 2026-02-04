"""
Compressed file handler for ZIP and GZIP archives.
Decompresses and routes to the appropriate parser.
"""

from __future__ import annotations

import gzip
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Callable, Optional

from config import IngestionConfig
from ingestion.parsers.base import BaseParser, ParseResult


class CompressedParser(BaseParser):
    """
    Handler for compressed files (.zip, .gz).
    Decompresses to a temp directory and re-routes to the appropriate parser.
    """

    def validate(self) -> tuple[bool, str]:
        """Check that the file is a valid archive."""
        if not self.file_path.exists():
            return False, f"File not found: {self.file_path}"
        if self.file_path.stat().st_size == 0:
            return False, "File is empty"

        ext = self.file_path.suffix.lower()
        if ext == ".zip":
            if zipfile.is_zipfile(str(self.file_path)):
                return True, "Valid ZIP archive"
            return False, "File is not a valid ZIP archive"
        elif ext in (".gz", ".gzip"):
            try:
                with gzip.open(str(self.file_path), "rb") as f:
                    f.read(100)  # Try reading a bit
                return True, "Valid GZIP archive"
            except Exception as e:
                return False, f"Invalid GZIP: {str(e)}"
        return False, f"Unsupported compression format: {ext}"

    def get_metadata(self) -> dict:
        """Extract archive metadata (file list, sizes)."""
        ext = self.file_path.suffix.lower()
        if ext == ".zip":
            return self._zip_metadata()
        elif ext in (".gz", ".gzip"):
            return self._gzip_metadata()
        return {}

    def _zip_metadata(self) -> dict:
        """Get ZIP archive contents metadata."""
        try:
            with zipfile.ZipFile(str(self.file_path), "r") as zf:
                files = []
                for info in zf.infolist():
                    if not info.is_dir():
                        files.append({
                            "name": info.filename,
                            "size": info.file_size,
                            "compressed_size": info.compress_size,
                            "extension": Path(info.filename).suffix.lower(),
                        })
                return {
                    "format": "zip",
                    "file_count": len(files),
                    "files": files,
                    "total_uncompressed": sum(f["size"] for f in files),
                }
        except Exception as e:
            return {"error": str(e)}

    def _gzip_metadata(self) -> dict:
        """Get GZIP metadata."""
        # GZIP typically wraps a single file
        inner_name = self.file_path.stem  # e.g., "data.csv" from "data.csv.gz"
        return {
            "format": "gzip",
            "inner_filename": inner_name,
            "inner_extension": Path(inner_name).suffix.lower(),
            "compressed_size": self.file_path.stat().st_size,
        }

    def parse(self, parser_factory: Optional[Callable] = None, **kwargs) -> ParseResult:
        """
        Decompress and parse the contained file(s).
        
        Args:
            parser_factory: A callable(file_path) -> BaseParser that creates
                           the appropriate parser for a decompressed file.
                           This is injected by the orchestrator.
        """
        config = IngestionConfig()
        config.ensure_dirs()

        ext = self.file_path.suffix.lower()

        if ext == ".zip":
            return self._parse_zip(parser_factory, **kwargs)
        elif ext in (".gz", ".gzip"):
            return self._parse_gzip(parser_factory, **kwargs)
        else:
            return ParseResult(
                warnings=[f"Unsupported compression format: {ext}"],
                justification=f"Cannot decompress {ext} files.",
            )

    def _parse_zip(self, parser_factory: Optional[Callable], **kwargs) -> ParseResult:
        """Decompress a ZIP and parse its contents."""
        config = IngestionConfig()
        temp_dir = Path(tempfile.mkdtemp(dir=str(config.TEMP_DIR)))

        try:
            with zipfile.ZipFile(str(self.file_path), "r") as zf:
                zf.extractall(str(temp_dir))

            # Find all extracted files
            extracted_files = [
                f for f in temp_dir.rglob("*") if f.is_file()
            ]

            if not extracted_files:
                return ParseResult(
                    warnings=["ZIP archive is empty"],
                    justification="ZIP archive contained no files.",
                )

            justification_parts = [
                f"Decompressed ZIP archive: {len(extracted_files)} file(s)."
            ]

            if len(extracted_files) == 1 and parser_factory:
                # Single file — parse it directly
                inner_file = extracted_files[0]
                inner_parser = parser_factory(inner_file)
                result = inner_parser.parse(**kwargs)
                result.justification = (
                    f"Decompressed ZIP → {inner_file.name}. {result.justification}"
                )
                result.metadata["compressed_source"] = self.file_path.name
                return result
            elif parser_factory:
                # Multiple files — return metadata for multi-file handling
                file_info = []
                for f in extracted_files:
                    file_info.append({
                        "path": str(f),
                        "name": f.name,
                        "size": f.stat().st_size,
                        "extension": f.suffix.lower(),
                    })
                justification_parts.append(
                    f"Multiple files found: {', '.join(f.name for f in extracted_files)}. "
                    f"Each will be processed separately."
                )
                return ParseResult(
                    metadata={
                        **self.get_metadata(),
                        "extracted_files": file_info,
                        "temp_dir": str(temp_dir),
                        "requires_multi_file_handling": True,
                    },
                    justification=" ".join(justification_parts),
                )
            else:
                return ParseResult(
                    metadata={
                        **self.get_metadata(),
                        "temp_dir": str(temp_dir),
                    },
                    justification="ZIP decompressed but no parser factory available.",
                    warnings=["Internal: parser_factory not provided to CompressedParser"],
                )

        except Exception as e:
            return ParseResult(
                warnings=[f"ZIP decompression failed: {str(e)}"],
                justification=f"Failed to decompress ZIP: {str(e)}",
            )

    def _parse_gzip(self, parser_factory: Optional[Callable], **kwargs) -> ParseResult:
        """Decompress GZIP and parse the inner file."""
        config = IngestionConfig()
        config.ensure_dirs()

        # Determine inner filename
        inner_name = self.file_path.stem  # "data.csv" from "data.csv.gz"
        temp_file = config.TEMP_DIR / inner_name

        try:
            with gzip.open(str(self.file_path), "rb") as gz:
                with open(str(temp_file), "wb") as out:
                    shutil.copyfileobj(gz, out)

            if parser_factory:
                inner_parser = parser_factory(temp_file)
                result = inner_parser.parse(**kwargs)
                result.justification = (
                    f"Decompressed GZIP → {inner_name}. {result.justification}"
                )
                result.metadata["compressed_source"] = self.file_path.name
                return result
            else:
                return ParseResult(
                    metadata={
                        **self.get_metadata(),
                        "temp_file": str(temp_file),
                    },
                    justification=f"GZIP decompressed to {inner_name}.",
                    warnings=["Internal: parser_factory not provided"],
                )

        except Exception as e:
            return ParseResult(
                warnings=[f"GZIP decompression failed: {str(e)}"],
                justification=f"Failed to decompress GZIP: {str(e)}",
            )
        finally:
            # Cleanup temp file (but not for ZIP dirs — orchestrator handles that)
            if temp_file.exists():
                try:
                    os.remove(str(temp_file))
                except Exception:
                    pass
