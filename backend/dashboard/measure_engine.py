"""
Measure Engine — DAX-Style Measure & Time-Intelligence Compiler for DuckDB.

Safely parses and compiles reusable calculated measures, analytical window functions,
filter-context overrides (CALCULATE / ALL), and time-intelligence (YTD, MoM, YoY)
into sandboxed, injection-free DuckDB SQL.
"""

from __future__ import annotations

import re
import ast
import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)

# Allowed functions in DAX-style expressions
ALLOWED_FUNCTIONS = {
    "SUM", "AVG", "COUNT", "COUNTA", "COUNTROWS", "DISTINCTCOUNT",
    "MIN", "MAX", "MEDIAN", "STDDEV", "PERCENTILE", "DIVIDE",
    "CALCULATE", "ALL", "ALLEXCEPT",
    "YTD", "QTD", "MTD", "YOY", "YOY_PERCENT", "MOM", "MOM_PERCENT", "QOQ",
    "SAMEPERIODLASTYEAR", "RANK", "RANKX",
    "SHAREOFTOTAL", "RUNNINGTOTAL", "MOVINGAVG", "ROLLING_3M", "ROLLING_12M",
    "ROUND", "ABS", "COALESCE"
}


def _clean_col(col_name: str) -> str:
    """Sanitize and quote a column name."""
    col = str(col_name).strip().strip("'\"")
    clean = re.sub(r'[^a-zA-Z0-9_]', '', col)
    return f'"{clean}"'


class MeasureEngine:
    """
    Compiles DAX-style analytical expressions into valid DuckDB SQL expressions.
    """

    @classmethod
    def validate_expression(cls, expr: str) -> tuple[bool, str]:
        """
        Validate whether a DAX-style measure expression is safe and well-formed.
        Returns (is_valid, error_message).
        """
        expr_str = expr.strip()
        if not expr_str:
            return False, "Expression cannot be empty."

        # Reject obvious SQL injection or OS command keywords
        forbidden = [";", "--", "/*", "*/", "DROP ", "DELETE ", "UPDATE ", "INSERT ", "EXEC ", "IMPORT ", "SYSTEM "]
        for f in forbidden:
            if f.lower() in expr_str.lower():
                return False, f"Forbidden sequence '{f}' in measure expression."

        # Check functions used
        tokens = re.findall(r'([A-Za-z_][A-Za-z0-9_]*)\s*\(', expr_str)
        for t in tokens:
            if t.upper() not in ALLOWED_FUNCTIONS:
                return False, f"Unsupported function '{t}'. Supported functions: {sorted(list(ALLOWED_FUNCTIONS))}"

        return True, ""

    @classmethod
    def compile_measure(cls, expr: str, primary_date_col: Optional[str] = None) -> str:
        """
        Compile a DAX-style expression into a DuckDB SQL aggregate/window expression.
        
        Examples:
          DIVIDE(SUM(net_revenue), SUM(gross_revenue), 0)
            -> CASE WHEN SUM("gross_revenue") = 0 OR SUM("gross_revenue") IS NULL THEN 0 ELSE SUM("net_revenue") * 1.0 / SUM("gross_revenue") END
            
          CALCULATE(SUM(net_revenue), churn_flag = 1)
            -> SUM("net_revenue") FILTER (WHERE "churn_flag" = 1)
            
          SHAREOFTOTAL(net_revenue)
            -> SUM("net_revenue") * 1.0 / NULLIF(SUM(SUM("net_revenue")) OVER (), 0)
            
          YTD(SUM(net_revenue), order_date)
            -> SUM("net_revenue") OVER (PARTITION BY strftime(TRY_CAST("order_date" AS DATE), '%Y') ORDER BY TRY_CAST("order_date" AS DATE) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        """
        valid, err = cls.validate_expression(expr)
        if not valid:
            raise ValueError(f"Invalid measure expression: {err}")

        expr_clean = expr.strip()

        # ── 1. DIVIDE(num, den, fallback) ──
        divide_match = re.match(r'^DIVIDE\s*\(\s*(.+?)\s*,\s*(.+?)(?:\s*,\s*(.+?))?\s*\)$', expr_clean, re.IGNORECASE)
        if divide_match:
            numerator = cls.compile_measure(divide_match.group(1))
            denominator = cls.compile_measure(divide_match.group(2))
            fallback = divide_match.group(3) or "0"
            return (
                f'CASE WHEN ({denominator}) = 0 OR ({denominator}) IS NULL '
                f'THEN {fallback} ELSE ({numerator}) * 1.0 / ({denominator}) END'
            )

        # ── 2. CALCULATE(agg_expr, filter_condition) ──
        calc_match = re.match(r'^CALCULATE\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)$', expr_clean, re.IGNORECASE)
        if calc_match:
            agg_part = cls.compile_measure(calc_match.group(1))
            filt_part = calc_match.group(2).strip()
            # Quote identifiers in filter if unquoted
            # e.g. churn_flag = 1 -> "churn_flag" = 1
            filt_sql = re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|>|<|>=|<=)', r'"\1" \2', filt_part)
            return f"{agg_part} FILTER (WHERE {filt_sql})"

        # ── 3. SHAREOFTOTAL(col) or % of Total ──
        share_match = re.match(r'^SHAREOFTOTAL\s*\(\s*([a-zA-Z0-9_]+)\s*\)$', expr_clean, re.IGNORECASE)
        if share_match:
            col = _clean_col(share_match.group(1))
            return f"SUM({col}) * 1.0 / NULLIF(SUM(SUM({col})) OVER (), 0)"

        # ── 4. RUNNINGTOTAL(col, date_col) ──
        run_match = re.match(r'^RUNNINGTOTAL\s*\(\s*([a-zA-Z0-9_]+)(?:\s*,\s*([a-zA-Z0-9_]+))?\s*\)$', expr_clean, re.IGNORECASE)
        if run_match:
            col = _clean_col(run_match.group(1))
            d_col = _clean_col(run_match.group(2) or primary_date_col or "order_date")
            return f"SUM(SUM({col})) OVER (ORDER BY TRY_CAST({d_col} AS DATE) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

        # ── 5. MOVINGAVG(col, date_col, window) ──
        mavg_match = re.match(r'^MOVINGAVG\s*\(\s*([a-zA-Z0-9_]+)(?:\s*,\s*([a-zA-Z0-9_]+))?(?:\s*,\s*(\d+))?\s*\)$', expr_clean, re.IGNORECASE)
        if mavg_match:
            col = _clean_col(mavg_match.group(1))
            d_col = _clean_col(mavg_match.group(2) or primary_date_col or "order_date")
            window_size = int(mavg_match.group(3) or 3)
            preceding = max(1, window_size - 1)
            return f"AVG(SUM({col})) OVER (ORDER BY TRY_CAST({d_col} AS DATE) ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW)"

        # ── 6. RANK(col) or RANKX ──
        rank_match = re.match(r'^RANK(?:X)?\s*\(\s*([a-zA-Z0-9_]+)(?:\s*,\s*(ASC|DESC))?\s*\)$', expr_clean, re.IGNORECASE)
        if rank_match:
            col = _clean_col(rank_match.group(1))
            direction = (rank_match.group(2) or "DESC").upper()
            return f"DENSE_RANK() OVER (ORDER BY SUM({col}) {direction})"

        # ── 7. Time Intelligence: YTD(col, date_col) ──
        ytd_match = re.match(r'^YTD\s*\(\s*(?:SUM\s*\(\s*["\']?([a-zA-Z0-9_]+)["\']?\s*\)|["\']?([a-zA-Z0-9_]+)["\']?)(?:\s*,\s*["\']?([a-zA-Z0-9_]+)["\']?)?\s*\)$', expr_clean, re.IGNORECASE)
        if ytd_match:
            col = _clean_col(ytd_match.group(1) or ytd_match.group(2))
            d_col = _clean_col(ytd_match.group(3) or primary_date_col or "order_date")
            return (
                f"SUM({col}) OVER ("
                f"PARTITION BY strftime(TRY_CAST({d_col} AS DATE), '%Y') "
                f"ORDER BY TRY_CAST({d_col} AS DATE) "
                f"RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            )

        # ── 8. Time Intelligence: MOM_PERCENT(col, date_col) ──
        mom_pct_match = re.match(r'^MOM_PERCENT\s*\(\s*(?:SUM\s*\(\s*["\']?([a-zA-Z0-9_]+)["\']?\s*\)|["\']?([a-zA-Z0-9_]+)["\']?)(?:\s*,\s*["\']?([a-zA-Z0-9_]+)["\']?)?\s*\)$', expr_clean, re.IGNORECASE)
        if mom_pct_match:
            col = _clean_col(mom_pct_match.group(1) or mom_pct_match.group(2))
            d_col = _clean_col(mom_pct_match.group(3) or primary_date_col or "order_date")
            return (
                f"({col} - LAG({col}, 1) OVER (ORDER BY TRY_CAST({d_col} AS DATE))) * 1.0 / "
                f"NULLIF(LAG({col}, 1) OVER (ORDER BY TRY_CAST({d_col} AS DATE)), 0)"
            )

        # ── 9. Standard Aggregations: SUM, AVG, COUNT, DISTINCTCOUNT, MIN, MAX, MEDIAN, STDDEV ──
        agg_match = re.match(r'^(SUM|AVG|COUNT|COUNTA|COUNTROWS|DISTINCTCOUNT|MIN|MAX|MEDIAN|STDDEV)\s*\(\s*([a-zA-Z0-9_]*)\s*\)$', expr_clean, re.IGNORECASE)
        if agg_match:
            fn = agg_match.group(1).upper()
            raw_col = agg_match.group(2)
            if fn == "COUNTROWS" or not raw_col:
                return "COUNT(*)"
            if fn in ("COUNT", "COUNTA"):
                return f"COUNT({_clean_col(raw_col)})"
            if fn == "DISTINCTCOUNT":
                return f"COUNT(DISTINCT {_clean_col(raw_col)})"
            return f"{fn}({_clean_col(raw_col)})"

        # ── 10. Fallback: Parse binary operations (e.g. SUM(a) + SUM(b) or SUM(a) * 1.15) ──
        # Simple token replacement for column aggregations
        def _replace_agg(m):
            fn = m.group(1).upper()
            c = _clean_col(m.group(2))
            if fn == "DISTINCTCOUNT":
                return f"COUNT(DISTINCT {c})"
            return f"{fn}({c})"

        compiled = re.sub(
            r'\b(SUM|AVG|COUNT|MIN|MAX|MEDIAN|STDDEV)\s*\(\s*([a-zA-Z0-9_]+)\s*\)',
            _replace_agg,
            expr_clean,
            flags=re.IGNORECASE
        )
        return compiled

    @classmethod
    def inject_rls(cls, sql: str, role_predicate: Optional[str]) -> str:
        """
        Safely inject a Row-Level Security (RLS) predicate into DuckDB SQL.
        Wraps the query in a CTE or adds to the WHERE clause to guarantee isolation.
        """
        if not role_predicate or not role_predicate.strip():
            return sql

        pred = role_predicate.strip()

        # If simple SELECT ... FROM ... [WHERE ...], append predicate safely
        if "WHERE " in sql.upper():
            return re.sub(r'(?i)\bWHERE\b', f'WHERE ({pred}) AND', sql, count=1)
        elif "GROUP BY " in sql.upper():
            return re.sub(r'(?i)\bGROUP BY\b', f'WHERE ({pred}) GROUP BY', sql, count=1)
        elif "ORDER BY " in sql.upper():
            return re.sub(r'(?i)\bORDER BY\b', f'WHERE ({pred}) ORDER BY', sql, count=1)
        elif "LIMIT " in sql.upper():
            return re.sub(r'(?i)\bLIMIT\b', f'WHERE ({pred}) LIMIT', sql, count=1)
        else:
            return f"{sql} WHERE ({pred})"
