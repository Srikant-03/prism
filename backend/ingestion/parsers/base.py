"""
Abstract base parser defining the interface every format-specific parser must implement.
"""

from __future__ import annotations

import abc
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from models.schemas import FileMetadata, MalformedReport


class ParseResult:
    """Structured output from a parser."""

    def __init__(
        self,
        dataframe: Optional[pd.DataFrame] = None,
        metadata: Optional[dict[str, Any]] = None,
        malformed: Optional[MalformedReport] = None,
        warnings: Optional[list[str]] = None,
        justification: str = "",
    ):
        self.dataframe = dataframe if dataframe is not None else pd.DataFrame()
        self.metadata = metadata or {}
        self.malformed = malformed or MalformedReport()
        self.warnings = warnings or []
        self.justification = justification


class BaseParser(abc.ABC):
    """
    Abstract base class for all file format parsers.

    Every parser must implement:
    - parse(): Read the complete file into a DataFrame
    - get_metadata(): Extract format-specific metadata
    - validate(): Pre-parse validation to check if the file is processable

    Optionally:
    - parse_chunked(): Read the file in chunks for large file support
    """

    def __init__(self, file_path: Path, encoding: str = "utf-8"):
        self.file_path = file_path
        self.encoding = encoding

    @abc.abstractmethod
    def parse(self, **kwargs) -> ParseResult:
        """
        Parse the file and return a structured result.
        Must never raise — all errors should be captured in the result.
        """
        ...

    @abc.abstractmethod
    def get_metadata(self) -> dict:
        """
        Extract format-specific metadata (e.g., sheet names for Excel,
        compression info for Parquet, etc.)
        """
        ...

    @abc.abstractmethod
    def validate(self) -> tuple[bool, str]:
        """
        Pre-parse validation. Returns (is_valid, reason).
        Checks that the file can be read by this parser.
        """
        ...

    def parse_chunked(
        self,
        chunk_size: int = 10000,
        progress_callback: Optional[Callable[[float, int, int], None]] = None,
        **kwargs,
    ) -> ParseResult:
        """
        Parse the file in chunks for large file support.
        Default implementation reads the whole file — parsers should
        override this if they support streaming.

        Args:
            chunk_size: Number of rows per chunk
            progress_callback: Called with (progress_pct, bytes_read, total_bytes)
        """
        # Default: just parse the whole file
        return self.parse(**kwargs)

    def _safe_execute(self, fn, fallback, error_msg: str):
        """
        Execute a function safely, returning a fallback value on error
        and logging the warning.
        """
        try:
            return fn(), None
        except Exception as e:
            return fallback, f"{error_msg}: {str(e)}"
