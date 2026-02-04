"""
SQL dump file parser. Extracts CREATE TABLE schemas and INSERT INTO data.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
import sqlparse

from models.schemas import MalformedReport, MalformedRow, MalformedSeverity
from ingestion.parsers.base import BaseParser, ParseResult


class SQLParser(BaseParser):
    """
    Parser for SQL dump files (.sql).
    Extracts table schemas from CREATE TABLE and data from INSERT INTO statements.
    Handles multiple tables in a single dump.
    """

    def validate(self) -> tuple[bool, str]:
        """Check that the file contains SQL statements."""
        if not self.file_path.exists():
            return False, f"File not found: {self.file_path}"
        if self.file_path.stat().st_size == 0:
            return False, "File is empty"

        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                sample = f.read(8192).upper()

            has_sql = any(
                kw in sample
                for kw in ["CREATE TABLE", "INSERT INTO", "SELECT ", "DROP TABLE"]
            )
            if not has_sql:
                return False, "File does not appear to contain SQL statements"
            return True, "SQL statements detected"
        except Exception as e:
            return False, f"Cannot read file: {str(e)}"

    def get_metadata(self) -> dict:
        """Extract table names and statement counts."""
        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                content = f.read()

            tables = set()
            create_count = 0
            insert_count = 0

            for statement in sqlparse.parse(content):
                stmt_type = statement.get_type()
                if stmt_type == "CREATE":
                    create_count += 1
                    table_name = self._extract_table_name(str(statement))
                    if table_name:
                        tables.add(table_name)
                elif stmt_type == "INSERT":
                    insert_count += 1
                    table_name = self._extract_table_name(str(statement))
                    if table_name:
                        tables.add(table_name)

            return {
                "tables": sorted(tables),
                "create_statements": create_count,
                "insert_statements": insert_count,
            }
        except Exception as e:
            return {"error": str(e)}

    def parse(self, **kwargs) -> ParseResult:
        """
        Parse the SQL dump, extracting:
        1. Table schemas from CREATE TABLE
        2. Data from INSERT INTO statements
        """
        justification_parts = []
        warnings = []

        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                content = f.read()
        except Exception as e:
            return ParseResult(
                warnings=[f"Cannot read SQL file: {str(e)}"],
                justification=f"Failed to read file: {str(e)}",
            )

        # Parse schemas and data
        schemas = self._extract_schemas(content)
        table_data = self._extract_inserts(content)

        if not table_data:
            justification_parts.append(
                "No INSERT INTO statements found. "
            )
            if schemas:
                justification_parts.append(
                    f"Found {len(schemas)} CREATE TABLE statement(s): "
                    f"{', '.join(schemas.keys())}. "
                    "Schema extracted but no data rows available."
                )
                # Create empty DataFrames with schema columns
                for table_name, columns in schemas.items():
                    if table_name not in table_data:
                        table_data[table_name] = {
                            "columns": columns,
                            "rows": [],
                        }

        # Build DataFrames for each table
        all_dfs: dict[str, pd.DataFrame] = {}
        for table_name, info in table_data.items():
            columns = info.get("columns", [])
            rows = info.get("rows", [])

            if columns and rows:
                # Ensure all rows match column count
                valid_rows = []
                for row in rows:
                    if len(row) == len(columns):
                        valid_rows.append(row)
                    elif len(row) < len(columns):
                        valid_rows.append(row + [None] * (len(columns) - len(row)))
                    else:
                        valid_rows.append(row[:len(columns)])

                df = pd.DataFrame(valid_rows, columns=columns)
            elif columns:
                df = pd.DataFrame(columns=columns)
            elif rows:
                df = pd.DataFrame(rows)
            else:
                df = pd.DataFrame()

            all_dfs[table_name] = df

        # Combine all tables
        if len(all_dfs) == 0:
            return ParseResult(
                dataframe=pd.DataFrame(),
                metadata=self.get_metadata(),
                warnings=["No extractable data found in SQL dump"],
                justification="SQL dump contained no extractable table data.",
            )
        elif len(all_dfs) == 1:
            table_name = list(all_dfs.keys())[0]
            result_df = all_dfs[table_name]
            justification_parts.append(
                f"Extracted table '{table_name}': "
                f"{len(result_df)} rows × {len(result_df.columns)} columns."
            )
        else:
            # Multiple tables — combine with source column
            combined = []
            for name, df in all_dfs.items():
                df = df.copy()
                df["__source_table__"] = name
                combined.append(df)
                justification_parts.append(
                    f"Table '{name}': {len(df)} rows × {len(df.columns) - 1} columns."
                )

            result_df = pd.concat(combined, ignore_index=True)
            justification_parts.append(
                f"Combined {len(all_dfs)} tables with '__source_table__' column."
            )

        return ParseResult(
            dataframe=result_df,
            metadata=self.get_metadata(),
            justification=" ".join(justification_parts),
            warnings=warnings,
        )

    def _extract_schemas(self, content: str) -> dict[str, list[str]]:
        """Extract column names from CREATE TABLE statements."""
        schemas: dict[str, list[str]] = {}

        # Regex for CREATE TABLE ... (...) 
        pattern = re.compile(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"']?(\w+)[`\"']?\s*\((.*?)\)",
            re.IGNORECASE | re.DOTALL,
        )

        for match in pattern.finditer(content):
            table_name = match.group(1)
            columns_str = match.group(2)

            columns = []
            for line in columns_str.split(","):
                line = line.strip()
                if not line:
                    continue
                # Skip constraints
                if line.upper().startswith(("PRIMARY", "UNIQUE", "INDEX", "KEY", "CONSTRAINT", "FOREIGN", "CHECK")):
                    continue
                # Extract column name (first word, possibly quoted)
                col_match = re.match(r"[`\"']?(\w+)[`\"']?", line)
                if col_match:
                    columns.append(col_match.group(1))

            if columns:
                schemas[table_name] = columns

        return schemas

    def _extract_inserts(self, content: str) -> dict[str, dict]:
        """Extract data from INSERT INTO statements."""
        table_data: dict[str, dict] = {}

        # Parse with sqlparse for better handling
        statements = sqlparse.parse(content)

        for statement in statements:
            stmt_str = str(statement).strip()
            if not stmt_str:
                continue

            # Check for INSERT
            if statement.get_type() != "INSERT":
                continue

            table_name = self._extract_table_name(stmt_str)
            if not table_name:
                continue

            # Initialize table entry
            if table_name not in table_data:
                table_data[table_name] = {"columns": [], "rows": []}

            # Extract column names if specified
            col_match = re.search(
                r"INSERT\s+INTO\s+[`\"']?\w+[`\"']?\s*\(([^)]+)\)",
                stmt_str,
                re.IGNORECASE,
            )
            if col_match and not table_data[table_name]["columns"]:
                cols = col_match.group(1)
                table_data[table_name]["columns"] = [
                    c.strip().strip("`\"'") for c in cols.split(",")
                ]

            # Extract VALUES
            values_match = re.findall(
                r"VALUES\s*(\((?:[^()]*(?:\([^()]*\))?[^()]*)*\)(?:\s*,\s*\((?:[^()]*(?:\([^()]*\))?[^()]*)*\))*)",
                stmt_str,
                re.IGNORECASE,
            )

            for values_block in values_match:
                # Parse individual value tuples
                tuples = re.findall(r"\(([^)]+)\)", values_block)
                for tup in tuples:
                    row = self._parse_value_tuple(tup)
                    table_data[table_name]["rows"].append(row)

        return table_data

    @staticmethod
    def _extract_table_name(stmt: str) -> Optional[str]:
        """Extract table name from a SQL statement."""
        patterns = [
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"']?(\w+)[`\"']?",
            r"INSERT\s+INTO\s+[`\"']?(\w+)[`\"']?",
        ]
        for pattern in patterns:
            match = re.search(pattern, stmt, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _parse_value_tuple(value_str: str) -> list[Any]:
        """Parse a SQL VALUES tuple into a list of Python values."""
        values = []
        current = ""
        in_string = False
        quote_char = None

        for char in value_str:
            if in_string:
                if char == quote_char and current and current[-1] != "\\":
                    in_string = False
                    values.append(current)
                    current = ""
                else:
                    current += char
            elif char in ("'", '"'):
                in_string = True
                quote_char = char
                current = ""
            elif char == ",":
                if current.strip():
                    values.append(SQLParser._parse_sql_value(current.strip()))
                elif not in_string and not current:
                    # Already appended a string value
                    pass
                current = ""
            else:
                current += char

        # Last value
        if current.strip():
            values.append(SQLParser._parse_sql_value(current.strip()))

        return values

    @staticmethod
    def _parse_sql_value(val: str) -> Any:
        """Convert a SQL value string to Python type."""
        if val.upper() == "NULL":
            return None
        if val.upper() in ("TRUE", "FALSE"):
            return val.upper() == "TRUE"
        try:
            if "." in val:
                return float(val)
            return int(val)
        except ValueError:
            return val.strip("'\"")
