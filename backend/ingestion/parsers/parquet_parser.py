"""
Parquet and Feather parser using PyArrow.
Columnar formats with rich schema metadata extraction.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather

from ingestion.parsers.base import BaseParser, ParseResult


class ParquetParser(BaseParser):
    """
    Parser for Apache Parquet and Arrow Feather (IPC) files.
    Extracts rich schema metadata including column types, compression, and row groups.
    """

    def __init__(self, file_path: Path, encoding: str = "utf-8"):
        super().__init__(file_path, encoding)
        self._is_feather = file_path.suffix.lower() == ".feather"

    def validate(self) -> tuple[bool, str]:
        """Check that the file is a valid Parquet or Feather file."""
        if not self.file_path.exists():
            return False, f"File not found: {self.file_path}"
        if self.file_path.stat().st_size == 0:
            return False, "File is empty"
        try:
            if self._is_feather:
                table = feather.read_table(str(self.file_path))
                return True, f"Valid Feather file with {table.num_rows} rows"
            else:
                pf = pq.ParquetFile(str(self.file_path))
                return True, f"Valid Parquet file with {pf.metadata.num_rows} rows"
        except Exception as e:
            return False, f"Invalid file: {str(e)}"

    def get_metadata(self) -> dict:
        """Extract schema and structural metadata."""
        try:
            if self._is_feather:
                table = feather.read_table(str(self.file_path))
                return {
                    "format": "feather",
                    "num_rows": table.num_rows,
                    "num_columns": table.num_columns,
                    "schema": {
                        field.name: str(field.type) for field in table.schema
                    },
                    "column_names": table.column_names,
                }
            else:
                pf = pq.ParquetFile(str(self.file_path))
                meta = pf.metadata
                schema = pf.schema_arrow
                return {
                    "format": "parquet",
                    "num_rows": meta.num_rows,
                    "num_columns": meta.num_columns,
                    "num_row_groups": meta.num_row_groups,
                    "created_by": meta.created_by or "unknown",
                    "format_version": meta.format_version,
                    "schema": {
                        field.name: str(field.type) for field in schema
                    },
                    "column_names": schema.names,
                    "row_group_sizes": [
                        meta.row_group(i).num_rows for i in range(meta.num_row_groups)
                    ],
                    "serialized_size": meta.serialized_size,
                }
        except Exception as e:
            return {"error": str(e)}

    def parse(self, **kwargs) -> ParseResult:
        """Parse the Parquet or Feather file into a DataFrame."""
        try:
            if self._is_feather:
                df = feather.read_feather(str(self.file_path))
                justification = (
                    f"Read Feather file: {len(df)} rows × {len(df.columns)} columns. "
                    f"Arrow IPC format with native type preservation."
                )
            else:
                df = pq.read_table(str(self.file_path)).to_pandas()
                metadata = self.get_metadata()
                justification = (
                    f"Read Parquet file: {len(df)} rows × {len(df.columns)} columns. "
                    f"{metadata.get('num_row_groups', 'N/A')} row groups. "
                    f"Created by: {metadata.get('created_by', 'unknown')}."
                )

            return ParseResult(
                dataframe=df,
                metadata=self.get_metadata(),
                justification=justification,
            )
        except Exception as e:
            return ParseResult(
                warnings=[f"Failed to parse: {str(e)}"],
                justification=f"Parsing failed: {str(e)}",
            )

    def parse_chunked(
        self,
        chunk_size: int = 10000,
        progress_callback: Optional[Callable[[float, int, int], None]] = None,
        **kwargs,
    ) -> ParseResult:
        """
        Parse Parquet in chunks using row groups.
        Feather doesn't support row-group-level reading, so falls back to full read.
        """
        if self._is_feather:
            return self.parse(**kwargs)

        try:
            pf = pq.ParquetFile(str(self.file_path))
            total_rows = pf.metadata.num_rows
            num_groups = pf.metadata.num_row_groups
            chunks = []
            rows_read = 0

            for i in range(num_groups):
                table = pf.read_row_group(i)
                chunks.append(table.to_pandas())
                rows_read += table.num_rows

                if progress_callback:
                    pct = (rows_read / total_rows) * 100 if total_rows > 0 else 0
                    progress_callback(pct, rows_read, total_rows)

            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

            if progress_callback:
                progress_callback(100.0, total_rows, total_rows)

            return ParseResult(
                dataframe=df,
                metadata=self.get_metadata(),
                justification=(
                    f"Read Parquet file in {num_groups} row groups. "
                    f"Result: {len(df)} rows × {len(df.columns)} columns."
                ),
            )
        except Exception as e:
            return self.parse(**kwargs)
