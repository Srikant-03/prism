"""
SQL Engine — DuckDB-powered in-process SQL engine.
Registers DataFrames as queryable tables and provides schema introspection.
All operations are read-only.
"""

from __future__ import annotations

import re
import hashlib
import threading
from typing import Any, Optional

import duckdb
import pandas as pd
import numpy as np


class SQLEngine:
    """
    In-process DuckDB SQL engine.
    Registers raw and cleaned DataFrames as named tables for querying.
    """

    def __init__(self):
        # Disable external access to prevent arbitrary file read via read_csv, etc.
        # Set max_memory to 2GB to guard against unbounded memory exhaustion (SEC-04, REL-01).
        self.conn = duckdb.connect(
            database=":memory:",
            config={
                "enable_external_access": "false",
                "max_memory": "2GB",
            }
        )
        self._tables: dict[str, dict[str, Any]] = {}  # name -> metadata
        self._query_cache: dict[str, dict] = {}       # SQL hash -> query result

    # ── Table registration ────────────────────────────────────────────

    def register_dataframe(
        self,
        df: pd.DataFrame,
        name: str,
        source: str = "raw",
        file_id: Optional[str] = None,
    ) -> str:
        """Register a DataFrame as a queryable table. Returns the safe table name."""
        safe_name = self._safe_table_name(name)

        # Handle collision
        if safe_name in self._tables:
            suffix = hashlib.md5(name.encode()).hexdigest()[:6]
            safe_name = f"{safe_name}_{suffix}"

        # Register with DuckDB
        self.conn.register(safe_name, df)

        self._tables[safe_name] = {
            "original_name": name,
            "safe_name": safe_name,
            "source": source,
            "file_id": file_id,
            "n_rows": len(df),
            "n_cols": len(df.columns),
            "columns": self._extract_column_info(df),
        }

        return safe_name

    def unregister_table(self, name: str) -> bool:
        """Remove a table from the engine."""
        if name not in self._tables:
            return False
        try:
            self.conn.unregister(name)
        except Exception:
            pass
        del self._tables[name]
        return True

    # ── Schema introspection ──────────────────────────────────────────

    def list_tables(self) -> list[dict[str, Any]]:
        """List all registered tables with metadata."""
        result = []
        for name, meta in self._tables.items():
            result.append({
                "name": name,
                "original_name": meta["original_name"],
                "source": meta["source"],
                "file_id": meta["file_id"],
                "n_rows": meta["n_rows"],
                "n_cols": meta["n_cols"],
            })
        return result

    def get_columns(self, table_name: str) -> list[dict[str, Any]]:
        """Get column details for a table."""
        if table_name not in self._tables:
            return []
        return self._tables[table_name]["columns"]

    def get_column_values(
        self, table_name: str, column_name: str, limit: int = 50,
    ) -> list[Any]:
        """Get top distinct values for a column (for autocomplete)."""
        if table_name not in self._tables:
            return []
            
        # Strict validation: column_name must exist in the table's registered columns
        valid_columns = {col["name"] for col in self._tables[table_name]["columns"]}
        if column_name not in valid_columns:
            return []
            
        safe_table = table_name.replace('"', '""')
        safe_col = column_name.replace('"', '""')
        
        try:
            sql = f'SELECT DISTINCT "{safe_col}" FROM "{safe_table}" WHERE "{safe_col}" IS NOT NULL ORDER BY "{safe_col}" LIMIT {limit}'
            result = self.conn.execute(sql).fetchall()
            return [row[0] for row in result]
        except Exception:
            return []

    def get_table_preview(self, table_name: str, limit: int = 10) -> dict:
        """Get a preview of a table."""
        if table_name not in self._tables:
            return {"columns": [], "rows": [], "total": 0}
        try:
            sql = f'SELECT * FROM "{table_name}" LIMIT {limit}'
            result = self.conn.execute(sql)
            cols = [desc[0] for desc in result.description]
            rows = result.fetchall()

            return {
                "columns": cols,
                "rows": [dict(zip(cols, self._serialize_row(r))) for r in rows],
                "total": self._tables[table_name]["n_rows"],
            }
        except Exception as e:
            return {"columns": [], "rows": [], "total": 0, "error": str(e)}

    # ── Query execution ───────────────────────────────────────────────

    def execute(self, sql: str, params: Optional[list] = None, timeout_seconds: float = 30.0) -> dict:
        """
        Execute a SQL query and return results.
        Only SELECT statements and CREATE VIEW are allowed.
        """
        # Remove all string literals and quoted identifiers to safely check keywords
        clean_sql = re.sub(r"'(?:''|[^'])*'", "", sql)
        clean_sql = re.sub(r'"(?:""|[^"])*"', "", clean_sql)
        clean_sql = clean_sql.upper()
        
        forbidden_keywords = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", 
                              "GRANT", "REVOKE", "PRAGMA", "COPY", "ATTACH", "DETACH", "CALL"}
        
        words = re.findall(r'[A-Z]+', clean_sql)
        if any(w in forbidden_keywords for w in words):
            return {
                "success": False,
                "error": "Write operations and restricted commands are not allowed.",
                "columns": [], "rows": [], "row_count": 0,
            }
        
        if "CREATE" in words:
            if not re.search(r'\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b', clean_sql):
                return {
                    "success": False,
                    "error": "Only CREATE VIEW is allowed.",
                    "columns": [], "rows": [], "row_count": 0,
                }

        timer = threading.Timer(timeout_seconds, self.conn.interrupt)
        timer.start()
        try:
            import time
            start = time.time()

            if params:
                result = self.conn.execute(sql, params)
            else:
                result = self.conn.execute(sql)

            elapsed = round(time.time() - start, 4)

            if result.description is None:
                return {
                    "success": True,
                    "columns": [], "column_types": [],
                    "rows": [], "row_count": 0,
                    "execution_time_s": elapsed,
                }

            cols = [desc[0] for desc in result.description]
            col_types = [str(desc[1]) if len(desc) > 1 else "unknown" for desc in result.description]
            rows = result.fetchall()

            serialized_rows = [
                dict(zip(cols, self._serialize_row(r))) for r in rows
            ]

            return {
                "success": True,
                "columns": cols,
                "column_types": col_types,
                "rows": serialized_rows,
                "row_count": len(rows),
                "execution_time_s": elapsed,
                "sql": sql,
            }

        except (duckdb.InterruptException, Exception) as e:
            if isinstance(e, duckdb.InterruptException) or "interrupted" in str(e).lower():
                return {
                    "success": False,
                    "error": f"Query execution timed out after {timeout_seconds}s.",
                    "columns": [], "rows": [], "row_count": 0,
                    "sql": sql,
                }
            return {
                "success": False,
                "error": str(e),
                "columns": [], "rows": [], "row_count": 0,
                "sql": sql,
            }
        finally:
            timer.cancel()

    # ── Explain Plan ──────────────────────────────────────────────────

    def explain_query(self, sql: str) -> dict:
        """Get the query execution plan."""
        try:
            explain_sql = f"EXPLAIN ANALYZE {sql}"
            result = self.conn.execute(explain_sql)
            rows = result.fetchall()

            plan_text = "\n".join(str(r[0]) if r else "" for r in rows)

            # Parse into tree nodes
            nodes = []
            for line in plan_text.split("\n"):
                line_stripped = line.rstrip()
                if not line_stripped:
                    continue
                indent = len(line_stripped) - len(line_stripped.lstrip("─│├└ "))
                depth = indent // 3

                nodes.append({
                    "depth": depth,
                    "text": line_stripped.strip("─│├└ ").strip(),
                    "raw": line_stripped,
                })

            return {
                "success": True,
                "plan_text": plan_text,
                "nodes": nodes,
            }
        except Exception as e:
            return {"success": False, "error": str(e), "plan_text": "", "nodes": []}

    # ── Views ─────────────────────────────────────────────────────────

    def create_view(self, name: str, sql: str) -> dict:
        """Create a named virtual view."""
        safe_name = self._safe_table_name(name)
        try:
            self.conn.execute(f'CREATE OR REPLACE VIEW "{safe_name}" AS {sql}')
            return {"success": True, "view_name": safe_name}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def list_views(self) -> list[str]:
        """List all user-created views."""
        try:
            result = self.conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"
            ).fetchall()
            return [r[0] for r in result]
        except Exception:
            return []

    def drop_view(self, name: str) -> dict:
        """Drop a view."""
        safe_name = self._safe_table_name(name)
        try:
            self.conn.execute(f'DROP VIEW IF EXISTS "{safe_name}"')
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Query Cache ───────────────────────────────────────────────────

    def execute_cached(self, sql: str) -> dict:
        """Execute with result caching (hash-keyed)."""
        cache_key = hashlib.md5(sql.strip().encode()).hexdigest()

        if cache_key in self._query_cache:
            cached = self._query_cache[cache_key]
            cached["from_cache"] = True
            return cached

        result = self.execute(sql)
        if result.get("success"):
            self._query_cache[cache_key] = result

        return result

    def clear_cache(self) -> int:
        """Clear the query cache. Returns number of entries cleared."""
        count = len(self._query_cache)
        self._query_cache.clear()
        return count

    # ── Export ─────────────────────────────────────────────────────────

    def execute_to_dataframe(self, sql: str) -> pd.DataFrame:
        """Execute a query and return as DataFrame (for export)."""
        return self.conn.execute(sql).fetchdf()

    # ── Private helpers ───────────────────────────────────────────────

    @staticmethod
    def _safe_table_name(name: str) -> str:
        """Convert a filename into a safe SQL table name."""
        # Remove extension
        name = re.sub(r"\.[^.]+$", "", name)
        # Replace non-alphanumeric with underscore
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        # Remove leading digits
        name = re.sub(r"^[0-9]+", "", name)
        # Collapse multiple underscores
        name = re.sub(r"_+", "_", name).strip("_")
        
        # Guard against DuckDB SQL reserved keywords
        reserved = {
            "select", "from", "where", "group", "by", "order", "having", 
            "limit", "table", "view", "index", "as", "join", "on", 
            "using", "insert", "update", "delete", "create", "drop", 
            "alter", "with", "all", "and", "or", "not", "is", "null"
        }
        if name.lower() in reserved:
            name = f"tbl_{name}"
            
        # Ensure non-empty
        return name or "table"

    @staticmethod
    def _extract_column_info(df: pd.DataFrame) -> list[dict]:
        """Extract column metadata from a DataFrame."""
        columns = []
        for col in df.columns:
            s = df[col]
            dtype = str(s.dtype)

            # Classify type for UI
            if pd.api.types.is_bool_dtype(s):
                ui_type = "boolean"
            elif pd.api.types.is_integer_dtype(s):
                ui_type = "integer"
            elif pd.api.types.is_float_dtype(s):
                ui_type = "float"
            elif pd.api.types.is_datetime64_any_dtype(s):
                ui_type = "datetime"
            elif pd.api.types.is_categorical_dtype(s) or (s.dtype == object and s.nunique() <= 50):
                ui_type = "categorical"
            elif s.dtype == object:
                ui_type = "text"
            else:
                ui_type = "other"

            columns.append({
                "name": col,
                "dtype": dtype,
                "ui_type": ui_type,
                "null_count": int(s.isnull().sum()),
                "null_pct": round(float(s.isnull().mean() * 100), 1),
                "unique_count": int(s.nunique()),
                "sample_values": [
                    _safe_val(v) for v in s.dropna().head(5).tolist()
                ],
            })
        return columns

    @staticmethod
    def _serialize_row(row: tuple) -> list:
        """Serialize a DuckDB row to JSON-safe values."""
        return [_safe_val(v) for v in row]


def _safe_val(v: Any) -> Any:
    """Convert a value to JSON-safe type."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, (pd.Timestamp, np.datetime64)):
        return str(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v
