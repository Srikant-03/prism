"""
Query Builder — Converts structured JSON query specs into valid DuckDB SQL.
Handles SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT,
JOINs, window functions, expressions, subqueries, and CTEs.
"""

from __future__ import annotations

import re
from typing import Any, Optional


def _quote(identifier: str) -> str:
    """Quote a SQL identifier."""
    return f'"{identifier}"'


def _escape_value(value: Any) -> str:
    """Escape a literal value for SQL."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    # String — escape single quotes
    s = str(value).replace("'", "''")
    return f"'{s}'"


class QueryBuilder:
    """
    Builds SQL from a structured query specification (dict/JSON).

    Query spec structure:
    {
        "from": {"table": "...", "alias": "..."},
        "ctes": [{"name": "...", "query": {...}}],
        "columns": [
            {"column": "col_name", "alias": "alias"},
            {"expression": "col_a * 2", "alias": "double_a"},
            {"aggregate": "SUM", "column": "revenue", "alias": "total_rev"},
            {"window": {"func": "ROW_NUMBER", "partition_by": [...], "order_by": [...]}},
        ],
        "joins": [
            {"type": "LEFT", "table": "...", "on": [{"left": "a.id", "right": "b.id"}]},
        ],
        "where": [
            {"column": "age", "op": ">", "value": 30, "logic": "AND"},
            {"group": [...], "logic": "OR"},
        ],
        "group_by": ["col1", "col2"],
        "having": [...],  # same structure as where
        "order_by": [{"column": "col1", "direction": "ASC", "nulls": "LAST"}],
        "limit": 100,
        "offset": 0,
    }
    """

    @staticmethod
    def build(spec: dict) -> str:
        """Build SQL from a query specification."""
        parts: list[str] = []

        # CTEs
        ctes = spec.get("ctes", [])
        if ctes:
            cte_parts = []
            for cte in ctes:
                cte_sql = QueryBuilder.build(cte["query"])
                cte_parts.append(f'{_quote(cte["name"])} AS (\n  {cte_sql}\n)')
            parts.append("WITH " + ",\n".join(cte_parts))

        # SELECT
        columns = spec.get("columns", [])
        if not columns:
            select_clause = "*"
        else:
            select_items = []
            for col_spec in columns:
                select_items.append(QueryBuilder._build_column(col_spec))
            select_clause = ", ".join(select_items)

        parts.append(f"SELECT {select_clause}")

        # FROM
        from_spec = spec.get("from", {})
        if isinstance(from_spec, str):
            parts.append(f"FROM {_quote(from_spec)}")
        elif isinstance(from_spec, dict):
            table = from_spec.get("table", "")
            alias = from_spec.get("alias", "")
            from_str = f"FROM {_quote(table)}"
            if alias:
                from_str += f" AS {_quote(alias)}"
            parts.append(from_str)

        # JOINs
        joins = spec.get("joins", [])
        for join in joins:
            parts.append(QueryBuilder._build_join(join))

        # WHERE
        where = spec.get("where", [])
        if where:
            where_clause = QueryBuilder._build_conditions(where)
            if where_clause:
                parts.append(f"WHERE {where_clause}")

        # GROUP BY
        group_by = spec.get("group_by", [])
        if group_by:
            gb_cols = ", ".join(_quote(c) for c in group_by)
            parts.append(f"GROUP BY {gb_cols}")

        # HAVING
        having = spec.get("having", [])
        if having:
            having_clause = QueryBuilder._build_conditions(having)
            if having_clause:
                parts.append(f"HAVING {having_clause}")

        # ORDER BY
        order_by = spec.get("order_by", [])
        if order_by:
            ob_parts = []
            for ob in order_by:
                col = _quote(ob["column"]) if not ob.get("is_expression") else ob["column"]
                direction = ob.get("direction", "ASC").upper()
                nulls = ob.get("nulls", "")
                ob_str = f"{col} {direction}"
                if nulls:
                    ob_str += f" NULLS {nulls.upper()}"
                ob_parts.append(ob_str)
            parts.append(f"ORDER BY {', '.join(ob_parts)}")

        # LIMIT / OFFSET
        limit = spec.get("limit")
        if limit is not None:
            parts.append(f"LIMIT {int(limit)}")
        offset = spec.get("offset")
        if offset is not None and offset > 0:
            parts.append(f"OFFSET {int(offset)}")

        return "\n".join(parts)

    # ── Column builders ───────────────────────────────────────────────

    @staticmethod
    def _build_column(col_spec: dict) -> str:
        """Build a single SELECT column/expression."""
        alias = col_spec.get("alias", "")
        alias_clause = f' AS {_quote(alias)}' if alias else ""

        # Simple column
        if "column" in col_spec and "aggregate" not in col_spec and "window" not in col_spec:
            table_prefix = f'{_quote(col_spec["table"])}.' if col_spec.get("table") else ""
            return f'{table_prefix}{_quote(col_spec["column"])}{alias_clause}'

        # Raw expression
        if "expression" in col_spec:
            return f'{col_spec["expression"]}{alias_clause}'

        # Aggregate
        if "aggregate" in col_spec:
            func = col_spec["aggregate"].upper()
            col = col_spec.get("column", "*")
            distinct = "DISTINCT " if col_spec.get("distinct") else ""
            if col == "*":
                return f'{func}({distinct}*){alias_clause}'
            return f'{func}({distinct}{_quote(col)}){alias_clause}'

        # Window function
        if "window" in col_spec:
            return QueryBuilder._build_window(col_spec["window"]) + alias_clause

        # Fallback
        return col_spec.get("column", "*") + alias_clause

    # ── Window function builder ───────────────────────────────────────

    @staticmethod
    def _build_window(win_spec: dict) -> str:
        """Build a window function expression."""
        func = win_spec.get("func", "ROW_NUMBER").upper()
        col = win_spec.get("column", "")
        distinct = "DISTINCT " if win_spec.get("distinct") else ""

        # Function call
        if col and func in ("SUM", "AVG", "COUNT", "MIN", "MAX"):
            func_call = f"{func}({distinct}{_quote(col)})"
        elif func in ("ROW_NUMBER", "RANK", "DENSE_RANK"):
            func_call = f"{func}()"
        elif func == "NTILE":
            n = win_spec.get("n", 4)
            func_call = f"NTILE({n})"
        elif func in ("LAG", "LEAD"):
            offset = win_spec.get("offset", 1)
            default = win_spec.get("default_value")
            args = [_quote(col), str(offset)]
            if default is not None:
                args.append(_escape_value(default))
            func_call = f"{func}({', '.join(args)})"
        elif func in ("FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"):
            if func == "NTH_VALUE":
                n = win_spec.get("n", 1)
                func_call = f"{func}({_quote(col)}, {n})"
            else:
                func_call = f"{func}({_quote(col)})"
        else:
            func_call = f"{func}()"

        # OVER clause
        over_parts = []

        partition_by = win_spec.get("partition_by", [])
        if partition_by:
            pb_cols = ", ".join(_quote(c) for c in partition_by)
            over_parts.append(f"PARTITION BY {pb_cols}")

        order_by = win_spec.get("order_by", [])
        if order_by:
            ob_parts = []
            for ob in order_by:
                if isinstance(ob, str):
                    ob_parts.append(_quote(ob))
                else:
                    col_name = _quote(ob["column"])
                    direction = ob.get("direction", "ASC")
                    ob_parts.append(f"{col_name} {direction}")
            over_parts.append(f"ORDER BY {', '.join(ob_parts)}")

        frame = win_spec.get("frame")
        if frame:
            over_parts.append(frame)

        over_clause = " ".join(over_parts)
        return f"{func_call} OVER ({over_clause})"

    # ── JOIN builder ──────────────────────────────────────────────────

    @staticmethod
    def _build_join(join_spec: dict) -> str:
        """Build a JOIN clause."""
        join_type = join_spec.get("type", "INNER").upper()
        table = join_spec.get("table", "")
        alias = join_spec.get("alias", "")

        # Map friendly names
        type_map = {
            "INNER": "INNER JOIN",
            "LEFT": "LEFT JOIN",
            "RIGHT": "RIGHT JOIN",
            "FULL": "FULL OUTER JOIN",
            "FULL OUTER": "FULL OUTER JOIN",
            "CROSS": "CROSS JOIN",
            "SEMI": "LEFT SEMI JOIN",
            "ANTI": "LEFT ANTI JOIN",
        }
        join_keyword = type_map.get(join_type, "JOIN")

        table_str = _quote(table)
        if alias:
            table_str += f" AS {_quote(alias)}"

        # ON conditions
        on_conditions = join_spec.get("on", [])
        if join_type == "CROSS" or not on_conditions:
            return f"{join_keyword} {table_str}"

        on_parts = []
        for cond in on_conditions:
            left = cond.get("left", "")
            right = cond.get("right", "")
            # Handle dot notation for table.column
            left_sql = QueryBuilder._qualify_column(left)
            right_sql = QueryBuilder._qualify_column(right)
            on_parts.append(f"{left_sql} = {right_sql}")

        on_clause = " AND ".join(on_parts)
        return f"{join_keyword} {table_str} ON {on_clause}"

    @staticmethod
    def _qualify_column(col_ref: str) -> str:
        """Handle table.column dot notation."""
        if "." in col_ref:
            parts = col_ref.split(".", 1)
            return f'{_quote(parts[0])}.{_quote(parts[1])}'
        return _quote(col_ref)

    # ── WHERE / HAVING condition builder ──────────────────────────────

    @staticmethod
    def _build_conditions(conditions: list[dict]) -> str:
        """Build WHERE/HAVING conditions from a list of condition specs."""
        if not conditions:
            return ""

        parts = []
        for i, cond in enumerate(conditions):
            logic = cond.get("logic", "AND").upper() if i > 0 else ""

            # Nested group
            if "group" in cond:
                inner = QueryBuilder._build_conditions(cond["group"])
                if inner:
                    clause = f"({inner})"
                else:
                    continue
            else:
                clause = QueryBuilder._build_single_condition(cond)

            if logic and parts:
                parts.append(f"{logic} {clause}")
            else:
                parts.append(clause)

        return " ".join(parts)

    @staticmethod
    def _build_single_condition(cond: dict) -> str:
        """Build a single filter condition."""
        col = _quote(cond.get("column", ""))
        op = cond.get("op", "=").upper()
        value = cond.get("value")
        values = cond.get("values", [])

        # Map UI operator names to SQL
        op_map = {
            "=": "=", "!=": "!=", "≠": "!=",
            ">": ">", "<": "<", ">=": ">=", "<=": "<=",
            "≥": ">=", "≤": "<=",
            "LIKE": "LIKE", "NOT LIKE": "NOT LIKE",
            "IS NULL": "IS NULL", "IS NOT NULL": "IS NOT NULL",
            "IS TRUE": "= TRUE", "IS FALSE": "= FALSE",
            "STARTS WITH": "LIKE",
            "ENDS WITH": "LIKE",
            "CONTAINS": "LIKE",
            "DOES NOT CONTAIN": "NOT LIKE",
            "MATCHES REGEX": "~",
            "BEFORE": "<", "AFTER": ">",
            "IN": "IN", "NOT IN": "NOT IN",
            "BETWEEN": "BETWEEN",
            "IN LAST N DAYS": ">=",
            "IN LAST N MONTHS": ">=",
            "THIS WEEK": ">=",
            "THIS MONTH": ">=",
            "THIS YEAR": ">=",
        }

        sql_op = op_map.get(op, op)

        # Handle special operators
        if op in ("IS NULL", "IS NOT NULL"):
            return f"{col} {sql_op}"

        if op in ("IS TRUE", "IS FALSE"):
            return f"{col} {sql_op}"

        if op == "BETWEEN":
            low = _escape_value(values[0]) if len(values) > 0 else "NULL"
            high = _escape_value(values[1]) if len(values) > 1 else "NULL"
            return f"{col} BETWEEN {low} AND {high}"

        if op in ("IN", "NOT IN"):
            escaped = ", ".join(_escape_value(v) for v in values)
            return f"{col} {sql_op} ({escaped})"

        if op == "STARTS WITH":
            return f"{col} LIKE {_escape_value(str(value) + '%')}"

        if op == "ENDS WITH":
            return f"{col} LIKE {_escape_value('%' + str(value))}"

        if op == "CONTAINS":
            return f"{col} LIKE {_escape_value('%' + str(value) + '%')}"

        if op == "DOES NOT CONTAIN":
            return f"{col} NOT LIKE {_escape_value('%' + str(value) + '%')}"

        if op == "IN LAST N DAYS":
            n = int(value) if value else 7
            return f"{col} >= CURRENT_DATE - INTERVAL '{n} days'"

        if op == "IN LAST N MONTHS":
            n = int(value) if value else 1
            return f"{col} >= CURRENT_DATE - INTERVAL '{n} months'"

        if op == "THIS WEEK":
            return f"{col} >= DATE_TRUNC('week', CURRENT_DATE)"

        if op == "THIS MONTH":
            return f"{col} >= DATE_TRUNC('month', CURRENT_DATE)"

        if op == "THIS YEAR":
            return f"{col} >= DATE_TRUNC('year', CURRENT_DATE)"

        # Subquery filter
        if cond.get("subquery"):
            sub_sql = QueryBuilder.build(cond["subquery"])
            return f"{col} {sql_op} ({sub_sql})"

        # Standard comparison
        return f"{col} {sql_op} {_escape_value(value)}"

    # ── Validation ────────────────────────────────────────────────────

    @staticmethod
    def validate_spec(spec: dict) -> list[str]:
        """Validate a query spec and return errors."""
        errors = []
        if not spec.get("from"):
            errors.append("Query must specify a 'from' table.")
        return errors
