"""
Data Exporter — Export datasets in multiple formats.
Supports: CSV, Excel, JSON, Parquet, Feather, SQL INSERT statements.
"""

from __future__ import annotations

import io
import json
from typing import Optional

import pandas as pd


class DataExporter:
    """Export a DataFrame in various formats."""

    @staticmethod
    def to_csv(df: pd.DataFrame) -> bytes:
        """Export as CSV."""
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        return buf.getvalue()

    @staticmethod
    def to_excel(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
        """Export as Excel (.xlsx)."""
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        return buf.getvalue()

    @staticmethod
    def to_json(df: pd.DataFrame, orient: str = "records") -> str:
        """Export as JSON."""
        return df.to_json(orient=orient, indent=2, date_format="iso")

    @staticmethod
    def to_parquet(df: pd.DataFrame) -> bytes:
        """Export as Parquet."""
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        return buf.getvalue()

    @staticmethod
    def to_feather(df: pd.DataFrame) -> bytes:
        """Export as Feather."""
        buf = io.BytesIO()
        df.to_feather(buf)
        return buf.getvalue()

    @staticmethod
    def to_sql_inserts(df: pd.DataFrame, table_name: str = "data") -> str:
        """Export as SQL INSERT statements."""
        lines = [
            f"-- SQL INSERT statements for table: {table_name}",
            f"-- {len(df):,} rows",
            "",
        ]

        # CREATE TABLE
        col_defs = []
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DOUBLE"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "VARCHAR"
            col_defs.append(f'    "{col}" {sql_type}')

        lines.append(f'CREATE TABLE IF NOT EXISTS "{table_name}" (')
        lines.append(",\n".join(col_defs))
        lines.append(");")
        lines.append("")

        # INSERT statements (batched)
        cols_str = ", ".join(f'"{c}"' for c in df.columns)

        for _, row in df.iterrows():
            vals = []
            for v in row:
                if pd.isna(v):
                    vals.append("NULL")
                elif isinstance(v, str):
                    vals.append(f"'{v.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(v, bool):
                    vals.append("TRUE" if v else "FALSE")
                else:
                    vals.append(str(v))

            lines.append(
                f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({", ".join(vals)});'
            )

        return "\n".join(lines)

    @staticmethod
    def get_supported_formats() -> list[dict]:
        """List supported export formats with metadata."""
        return [
            {"format": "csv", "label": "CSV", "extension": ".csv", "mime": "text/csv"},
            {"format": "excel", "label": "Excel (.xlsx)", "extension": ".xlsx",
             "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            {"format": "json", "label": "JSON", "extension": ".json", "mime": "application/json"},
            {"format": "parquet", "label": "Parquet", "extension": ".parquet",
             "mime": "application/octet-stream"},
            {"format": "feather", "label": "Feather", "extension": ".feather",
             "mime": "application/octet-stream"},
            {"format": "sql", "label": "SQL INSERT", "extension": ".sql", "mime": "text/plain"},
        ]
