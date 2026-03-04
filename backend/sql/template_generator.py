"""
Template Generator — Schema-aware, context-driven query template library.
Analyzes column names, types, cardinality, and patterns to auto-generate
categorized query templates with parameterized SQL.
"""

from __future__ import annotations

import re
from typing import Any, Optional


# ── Pattern detectors ─────────────────────────────────────────────────

_DATE_PATTERNS = re.compile(r"(date|time|timestamp|created|updated|modified|at$|_at$|_on$)", re.IGNORECASE)
_ID_PATTERNS = re.compile(r"(^id$|_id$|_key$|_code$|^pk$)", re.IGNORECASE)
_USER_PATTERNS = re.compile(r"(user|customer|client|member|subscriber|patient|employee|student)", re.IGNORECASE)
_EVENT_PATTERNS = re.compile(r"(event|action|activity|status|step|stage|funnel|type)", re.IGNORECASE)
_AMOUNT_PATTERNS = re.compile(r"(amount|price|cost|revenue|salary|total|fee|charge|payment|spend|value)", re.IGNORECASE)
_GEO_PATTERNS = re.compile(r"(city|state|country|region|zip|postal|lat|lon|address|location)", re.IGNORECASE)
_NAME_PATTERNS = re.compile(r"(name|title|label|description|category|brand|product)", re.IGNORECASE)
_METRIC_PATTERNS = re.compile(r"(score|rating|count|quantity|views|clicks|impressions|conversion|rate)", re.IGNORECASE)


def _q(name: str) -> str:
    """Quote identifier."""
    return f'"{name}"'


class TemplateGenerator:
    """
    Generates context-aware query templates by analyzing loaded table schemas.
    Templates are never hardcoded — they are dynamically generated from the data.
    """

    def __init__(self, engine):
        self.engine = engine

    def generate_templates(self, table_name: str) -> list[dict]:
        """
        Generate all applicable templates for a table.
        Returns a list of template dicts with category, description, sql, params.
        """
        columns = self.engine.get_columns(table_name)
        table_meta = None
        for t in self.engine.list_tables():
            if t["name"] == table_name:
                table_meta = t
                break

        if not columns or not table_meta:
            return []

        # Classify columns
        ctx = self._analyze_columns(columns, table_name)

        templates = []

        # Always generate these categories
        templates.extend(self._data_audit_templates(table_name, ctx))
        templates.extend(self._topn_templates(table_name, ctx))
        templates.extend(self._distribution_templates(table_name, ctx))
        templates.extend(self._completeness_templates(table_name, ctx))

        # Conditional categories based on detected patterns
        if ctx["date_cols"]:
            templates.extend(self._trend_templates(table_name, ctx))

        if ctx["amount_cols"] and ctx["category_cols"]:
            templates.extend(self._performance_templates(table_name, ctx))

        if ctx["event_cols"] and ctx["user_cols"]:
            templates.extend(self._funnel_templates(table_name, ctx))

        if ctx["user_cols"] and ctx["date_cols"]:
            templates.extend(self._cohort_templates(table_name, ctx))

        if ctx["amount_cols"]:
            templates.extend(self._outlier_templates(table_name, ctx))

        if ctx["geo_cols"]:
            templates.extend(self._segmentation_templates(table_name, ctx))

        if ctx["numeric_cols"] and ctx["category_cols"]:
            templates.extend(self._ranking_templates(table_name, ctx))

        # Cross-table relationship queries
        tables = self.engine.list_tables()
        if len(tables) > 1:
            templates.extend(self._relationship_templates(table_name, ctx, tables))

        return templates

    # ── Column analysis ───────────────────────────────────────────────

    def _analyze_columns(self, columns: list[dict], table_name: str) -> dict:
        """Classify columns by semantic role."""
        ctx = {
            "all_cols": [c["name"] for c in columns],
            "date_cols": [],
            "id_cols": [],
            "user_cols": [],
            "event_cols": [],
            "amount_cols": [],
            "geo_cols": [],
            "name_cols": [],
            "metric_cols": [],
            "numeric_cols": [],
            "text_cols": [],
            "category_cols": [],
            "bool_cols": [],
            "high_card_cols": [],
            "low_card_cols": [],
        }

        for c in columns:
            name = c["name"]
            utype = c["ui_type"]
            unique = c.get("unique_count", 0)

            # Type-based
            if utype in ("integer", "float"):
                ctx["numeric_cols"].append(name)
            if utype == "text":
                ctx["text_cols"].append(name)
            if utype == "boolean":
                ctx["bool_cols"].append(name)
            if utype == "categorical":
                ctx["category_cols"].append(name)
            if utype == "datetime":
                ctx["date_cols"].append(name)

            # Cardinality
            if unique <= 20:
                ctx["low_card_cols"].append(name)
            elif unique > 100:
                ctx["high_card_cols"].append(name)

            # Semantic patterns
            if _DATE_PATTERNS.search(name) and utype != "boolean":
                if name not in ctx["date_cols"]:
                    ctx["date_cols"].append(name)
            if _ID_PATTERNS.search(name):
                ctx["id_cols"].append(name)
            if _USER_PATTERNS.search(name):
                ctx["user_cols"].append(name)
            if _EVENT_PATTERNS.search(name):
                ctx["event_cols"].append(name)
            if _AMOUNT_PATTERNS.search(name):
                ctx["amount_cols"].append(name)
            if _GEO_PATTERNS.search(name):
                ctx["geo_cols"].append(name)
            if _NAME_PATTERNS.search(name):
                ctx["name_cols"].append(name)
            if _METRIC_PATTERNS.search(name):
                ctx["metric_cols"].append(name)

        return ctx

    # ── Template generators by category ───────────────────────────────

    def _data_audit_templates(self, table: str, ctx: dict) -> list[dict]:
        """Data Audit Queries: null counts, duplicates, type checks."""
        t = _q(table)
        templates = []

        # Null counts per column
        null_selects = ", ".join(
            f"SUM(CASE WHEN {_q(c)} IS NULL THEN 1 ELSE 0 END) AS {_q(c + '_nulls')}"
            for c in ctx["all_cols"]
        )
        templates.append({
            "category": "Data Audit",
            "title": "Null Counts Per Column",
            "description": "Count null values in every column to identify data completeness issues.",
            "sql": f"SELECT\n  COUNT(*) AS total_rows,\n  {null_selects}\nFROM {t}",
            "params": [],
        })

        # Duplicate row finder
        cols = ", ".join(_q(c) for c in ctx["all_cols"])
        templates.append({
            "category": "Data Audit",
            "title": "Duplicate Row Finder",
            "description": "Find exact duplicate rows across all columns.",
            "sql": f"SELECT {cols}, COUNT(*) AS dup_count\nFROM {t}\nGROUP BY {cols}\nHAVING COUNT(*) > 1\nORDER BY dup_count DESC\nLIMIT {{{{limit}}}}",
            "params": [{"name": "limit", "type": "number", "default": 100, "label": "Max rows"}],
        })

        # Record count summary
        if ctx["date_cols"]:
            dc = _q(ctx["date_cols"][0])
            templates.append({
                "category": "Data Audit",
                "title": f"Record Count by Date ({ctx['date_cols'][0]})",
                "description": f"Count records grouped by {ctx['date_cols'][0]} to detect data gaps.",
                "sql": f"SELECT DATE_TRUNC('{{{{period}}}}', {dc}) AS period, COUNT(*) AS record_count\nFROM {t}\nGROUP BY 1\nORDER BY 1",
                "params": [{"name": "period", "type": "select", "default": "month", "options": ["day", "week", "month", "quarter", "year"], "label": "Time period"}],
            })

        # Value range validators for numeric columns
        for nc in ctx["numeric_cols"][:3]:
            templates.append({
                "category": "Data Audit",
                "title": f"Value Range: {nc}",
                "description": f"Show min, max, avg, median, and percentiles for {nc}.",
                "sql": f"SELECT\n  MIN({_q(nc)}) AS min_val,\n  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {_q(nc)}) AS p25,\n  MEDIAN({_q(nc)}) AS median,\n  AVG({_q(nc)}) AS mean,\n  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {_q(nc)}) AS p75,\n  MAX({_q(nc)}) AS max_val,\n  STDDEV({_q(nc)}) AS std_dev\nFROM {t}\nWHERE {_q(nc)} IS NOT NULL",
                "params": [],
            })

        return templates

    def _topn_templates(self, table: str, ctx: dict) -> list[dict]:
        """Top-N Analysis templates."""
        t = _q(table)
        templates = []

        # Top N by any numeric column
        for nc in ctx["numeric_cols"][:3]:
            display_col = ctx["name_cols"][0] if ctx["name_cols"] else ctx["all_cols"][0]
            templates.append({
                "category": "Top-N Analysis",
                "title": f"Top N by {nc}",
                "description": f"Find the top N rows ranked by {nc}.",
                "sql": f"SELECT *\nFROM {t}\nORDER BY {_q(nc)} DESC\nLIMIT {{{{n}}}}",
                "params": [{"name": "n", "type": "number", "default": 10, "label": "N"}],
            })

        # Top N within each group
        if ctx["category_cols"] and ctx["numeric_cols"]:
            cat = ctx["category_cols"][0]
            metric = ctx["numeric_cols"][0]
            templates.append({
                "category": "Top-N Analysis",
                "title": f"Top N {metric} per {cat}",
                "description": f"Rank rows by {metric} within each {cat} group.",
                "sql": f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {_q(cat)} ORDER BY {_q(metric)} DESC) AS rank\nFROM {t}\nQUALIFY rank <= {{{{n}}}}",
                "params": [{"name": "n", "type": "number", "default": 5, "label": "N per group"}],
            })

        return templates

    def _distribution_templates(self, table: str, ctx: dict) -> list[dict]:
        """Distribution Analysis templates."""
        t = _q(table)
        templates = []

        # Value frequency for categorical columns
        for cc in ctx["low_card_cols"][:3]:
            templates.append({
                "category": "Distribution Analysis",
                "title": f"Value Frequency: {cc}",
                "description": f"Show how often each value of {cc} appears.",
                "sql": f"SELECT {_q(cc)}, COUNT(*) AS frequency,\n  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct\nFROM {t}\nGROUP BY {_q(cc)}\nORDER BY frequency DESC",
                "params": [],
            })

        # Histogram bins for numeric columns
        for nc in ctx["numeric_cols"][:2]:
            templates.append({
                "category": "Distribution Analysis",
                "title": f"Histogram: {nc}",
                "description": f"Create {nc} histogram with configurable bucket count.",
                "sql": f"WITH bounds AS (\n  SELECT MIN({_q(nc)}) AS mn, MAX({_q(nc)}) AS mx FROM {t} WHERE {_q(nc)} IS NOT NULL\n)\nSELECT\n  FLOOR(({_q(nc)} - bounds.mn) / NULLIF((bounds.mx - bounds.mn) / {{{{buckets}}}}, 0)) AS bucket,\n  COUNT(*) AS freq\nFROM {t}, bounds\nWHERE {_q(nc)} IS NOT NULL\nGROUP BY 1\nORDER BY 1",
                "params": [{"name": "buckets", "type": "number", "default": 20, "label": "Buckets"}],
            })

        return templates

    def _completeness_templates(self, table: str, ctx: dict) -> list[dict]:
        """Completeness Queries."""
        t = _q(table)
        templates = []

        # Rows with N or more nulls
        null_sum = " + ".join(
            f"CASE WHEN {_q(c)} IS NULL THEN 1 ELSE 0 END"
            for c in ctx["all_cols"]
        )
        templates.append({
            "category": "Completeness",
            "title": "Rows Missing N+ Fields",
            "description": "Find rows that have N or more null values across all columns.",
            "sql": f"SELECT * FROM (\n  SELECT *, ({null_sum}) AS null_count\n  FROM {t}\n) sub\nWHERE null_count >= {{{{min_nulls}}}}\nORDER BY null_count DESC\nLIMIT {{{{limit}}}}",
            "params": [
                {"name": "min_nulls", "type": "number", "default": 3, "label": "Min nulls"},
                {"name": "limit", "type": "number", "default": 100, "label": "Max rows"},
            ],
        })

        # Most incomplete columns
        null_cases = ",\n  ".join(
            f"ROUND(100.0 * SUM(CASE WHEN {_q(c)} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS {_q(c)}"
            for c in ctx["all_cols"]
        )
        templates.append({
            "category": "Completeness",
            "title": "Column Completeness Report",
            "description": "Percentage of null values for each column.",
            "sql": f"SELECT\n  COUNT(*) AS total_rows,\n  {null_cases}\nFROM {t}",
            "params": [],
        })

        return templates

    def _trend_templates(self, table: str, ctx: dict) -> list[dict]:
        """Trend Analysis templates (requires date columns)."""
        t = _q(table)
        dc = _q(ctx["date_cols"][0])
        templates = []

        # Time-series aggregation
        metric_col = ctx["amount_cols"][0] if ctx["amount_cols"] else (
            ctx["numeric_cols"][0] if ctx["numeric_cols"] else None
        )
        if metric_col:
            mc = _q(metric_col)
            templates.append({
                "category": "Trend Analysis",
                "title": f"{metric_col} Over Time",
                "description": f"Aggregate {metric_col} by time period.",
                "sql": f"SELECT\n  DATE_TRUNC('{{{{period}}}}', {dc}) AS period,\n  SUM({mc}) AS total,\n  AVG({mc}) AS average,\n  COUNT(*) AS records\nFROM {t}\nGROUP BY 1\nORDER BY 1",
                "params": [{"name": "period", "type": "select", "default": "month", "options": ["day", "week", "month", "quarter", "year"], "label": "Period"}],
            })

            # Period-over-period growth
            templates.append({
                "category": "Trend Analysis",
                "title": f"Period-over-Period Growth ({metric_col})",
                "description": f"Calculate growth rate of {metric_col} between consecutive periods.",
                "sql": f"WITH periods AS (\n  SELECT DATE_TRUNC('{{{{period}}}}', {dc}) AS p, SUM({mc}) AS total\n  FROM {t}\n  GROUP BY 1\n)\nSELECT p, total,\n  LAG(total) OVER (ORDER BY p) AS prev_total,\n  ROUND(100.0 * (total - LAG(total) OVER (ORDER BY p)) / NULLIF(LAG(total) OVER (ORDER BY p), 0), 2) AS growth_pct\nFROM periods\nORDER BY p",
                "params": [{"name": "period", "type": "select", "default": "month", "options": ["day", "week", "month", "quarter", "year"], "label": "Period"}],
            })

            # Rolling average
            templates.append({
                "category": "Trend Analysis",
                "title": f"Rolling Average ({metric_col})",
                "description": f"Calculate a rolling average of {metric_col} over N periods.",
                "sql": f"WITH daily AS (\n  SELECT DATE_TRUNC('{{{{period}}}}', {dc}) AS p, AVG({mc}) AS avg_val\n  FROM {t}\n  GROUP BY 1\n)\nSELECT p, avg_val,\n  AVG(avg_val) OVER (ORDER BY p ROWS BETWEEN {{{{window}}}} PRECEDING AND CURRENT ROW) AS rolling_avg\nFROM daily\nORDER BY p",
                "params": [
                    {"name": "period", "type": "select", "default": "day", "options": ["day", "week", "month"], "label": "Period"},
                    {"name": "window", "type": "number", "default": 7, "label": "Window size"},
                ],
            })

        # Record count trend
        templates.append({
            "category": "Trend Analysis",
            "title": "Record Count Trend",
            "description": f"Count records per time period using {ctx['date_cols'][0]}.",
            "sql": f"SELECT DATE_TRUNC('{{{{period}}}}', {dc}) AS period, COUNT(*) AS count\nFROM {t}\nGROUP BY 1\nORDER BY 1",
            "params": [{"name": "period", "type": "select", "default": "month", "options": ["day", "week", "month", "quarter", "year"], "label": "Period"}],
        })

        return templates

    def _performance_templates(self, table: str, ctx: dict) -> list[dict]:
        """Performance Queries (metrics x dimensions)."""
        t = _q(table)
        templates = []

        cat = ctx["category_cols"][0] if ctx["category_cols"] else ctx["name_cols"][0]
        met = ctx["amount_cols"][0]

        templates.append({
            "category": "Performance",
            "title": f"{met} Scorecard by {cat}",
            "description": f"Performance metrics for {met} broken down by {cat}.",
            "sql": f"SELECT {_q(cat)},\n  COUNT(*) AS count,\n  SUM({_q(met)}) AS total,\n  AVG({_q(met)}) AS average,\n  MIN({_q(met)}) AS min_val,\n  MAX({_q(met)}) AS max_val\nFROM {t}\nGROUP BY {_q(cat)}\nORDER BY total DESC",
            "params": [],
        })

        return templates

    def _funnel_templates(self, table: str, ctx: dict) -> list[dict]:
        """Funnel & Conversion templates (user + event patterns)."""
        t = _q(table)
        templates = []

        event_col = ctx["event_cols"][0]
        user_col = ctx["user_cols"][0]

        # Step completion rates
        templates.append({
            "category": "Funnel & Conversion",
            "title": f"Funnel Analysis by {event_col}",
            "description": f"Count unique {user_col}s at each {event_col} stage.",
            "sql": f"SELECT {_q(event_col)},\n  COUNT(DISTINCT {_q(user_col)}) AS unique_users\nFROM {t}\nGROUP BY {_q(event_col)}\nORDER BY unique_users DESC",
            "params": [],
        })

        # Users who did X but not Y
        templates.append({
            "category": "Funnel & Conversion",
            "title": f"Users Who Did Step A but Not Step B",
            "description": f"Find {user_col}s who appear with one {event_col} value but not another.",
            "sql": f"SELECT DISTINCT {_q(user_col)}\nFROM {t}\nWHERE {_q(event_col)} = '{{{{step_a}}}}'\n  AND {_q(user_col)} NOT IN (\n    SELECT DISTINCT {_q(user_col)} FROM {t} WHERE {_q(event_col)} = '{{{{step_b}}}}'\n  )\nLIMIT {{{{limit}}}}",
            "params": [
                {"name": "step_a", "type": "column_value", "column": event_col, "label": "Step A"},
                {"name": "step_b", "type": "column_value", "column": event_col, "label": "Step B"},
                {"name": "limit", "type": "number", "default": 100, "label": "Max rows"},
            ],
        })

        return templates

    def _cohort_templates(self, table: str, ctx: dict) -> list[dict]:
        """Cohort & Retention templates (user + date patterns)."""
        t = _q(table)
        templates = []

        user_col = ctx["user_cols"][0]
        date_col = ctx["date_cols"][0]

        templates.append({
            "category": "Cohort Analysis",
            "title": "Monthly Cohort Retention",
            "description": f"Retention matrix: track {user_col} activity by signup cohort month.",
            "sql": f"WITH cohorts AS (\n  SELECT {_q(user_col)},\n    DATE_TRUNC('month', MIN({_q(date_col)})) AS cohort_month\n  FROM {t}\n  GROUP BY {_q(user_col)}\n),\nactivity AS (\n  SELECT {_q(user_col)},\n    DATE_TRUNC('month', {_q(date_col)}) AS active_month\n  FROM {t}\n  GROUP BY 1, 2\n)\nSELECT c.cohort_month,\n  DATE_DIFF('month', c.cohort_month, a.active_month) AS months_since,\n  COUNT(DISTINCT a.{_q(user_col)}) AS active_users\nFROM cohorts c\nJOIN activity a ON c.{_q(user_col)} = a.{_q(user_col)}\nGROUP BY 1, 2\nORDER BY 1, 2",
            "params": [],
        })

        return templates

    def _outlier_templates(self, table: str, ctx: dict) -> list[dict]:
        """Outlier & Anomaly Queries."""
        t = _q(table)
        templates = []

        for ac in ctx["amount_cols"][:2]:
            templates.append({
                "category": "Outlier Detection",
                "title": f"Outliers in {ac} (Z-score)",
                "description": f"Find rows where {ac} deviates more than N standard deviations from the mean.",
                "sql": f"WITH stats AS (\n  SELECT AVG({_q(ac)}) AS mu, STDDEV({_q(ac)}) AS sigma FROM {t}\n)\nSELECT t.*, ROUND(ABS(({_q(ac)} - stats.mu) / NULLIF(stats.sigma, 0)), 2) AS z_score\nFROM {t} t, stats\nWHERE ABS(({_q(ac)} - stats.mu) / NULLIF(stats.sigma, 0)) > {{{{threshold}}}}\nORDER BY z_score DESC\nLIMIT {{{{limit}}}}",
                "params": [
                    {"name": "threshold", "type": "number", "default": 3, "label": "Z-score threshold"},
                    {"name": "limit", "type": "number", "default": 100, "label": "Max rows"},
                ],
            })

        return templates

    def _segmentation_templates(self, table: str, ctx: dict) -> list[dict]:
        """Segmentation Queries (geographic or categorical)."""
        t = _q(table)
        templates = []

        for gc in ctx["geo_cols"][:2]:
            met = ctx["amount_cols"][0] if ctx["amount_cols"] else "1"
            met_q = _q(met) if met != "1" else "1"
            met_label = met if met != "1" else "records"

            templates.append({
                "category": "Segmentation",
                "title": f"Breakdown by {gc}",
                "description": f"Count and aggregate {met_label} by {gc}.",
                "sql": f"SELECT {_q(gc)},\n  COUNT(*) AS count" +
                       (f",\n  SUM({met_q}) AS total,\n  AVG({met_q}) AS average" if met != "1" else "") +
                       f"\nFROM {t}\nGROUP BY {_q(gc)}\nORDER BY count DESC\nLIMIT {{{{limit}}}}",
                "params": [{"name": "limit", "type": "number", "default": 50, "label": "Max rows"}],
            })

        return templates

    def _ranking_templates(self, table: str, ctx: dict) -> list[dict]:
        """Ranking & Comparison templates."""
        t = _q(table)
        templates = []

        cat = ctx["category_cols"][0]
        met = ctx["numeric_cols"][0]

        templates.append({
            "category": "Ranking",
            "title": f"Rank {cat} by {met}",
            "description": f"Rank each {cat} by total/average {met}.",
            "sql": f"SELECT {_q(cat)},\n  SUM({_q(met)}) AS total,\n  AVG({_q(met)}) AS average,\n  RANK() OVER (ORDER BY SUM({_q(met)}) DESC) AS rank\nFROM {t}\nGROUP BY {_q(cat)}\nORDER BY rank\nLIMIT {{{{n}}}}",
            "params": [{"name": "n", "type": "number", "default": 20, "label": "Top N"}],
        })

        return templates

    def _relationship_templates(self, table: str, ctx: dict, all_tables: list) -> list[dict]:
        """Cross-table Relationship Queries."""
        templates = []

        other_tables = [t for t in all_tables if t["name"] != table]
        if not other_tables:
            return templates

        # For each other table, suggest a join if there are matching column names
        for other in other_tables[:3]:
            other_cols = self.engine.get_columns(other["name"])
            other_col_names = {c["name"] for c in other_cols}
            common = set(ctx["all_cols"]) & other_col_names

            if common:
                join_col = list(common)[0]
                templates.append({
                    "category": "Relationships",
                    "title": f"Join {table} with {other['name']} on {join_col}",
                    "description": f"Join tables on common column {join_col}.",
                    "sql": f"SELECT a.*, b.*\nFROM {_q(table)} a\nLEFT JOIN {_q(other['name'])} b ON a.{_q(join_col)} = b.{_q(join_col)}\nLIMIT {{{{limit}}}}",
                    "params": [{"name": "limit", "type": "number", "default": 100, "label": "Max rows"}],
                })

                # Orphan record detection
                templates.append({
                    "category": "Relationships",
                    "title": f"Orphan Records: {table} without {other['name']}",
                    "description": f"Find rows in {table} that have no match in {other['name']} via {join_col}.",
                    "sql": f"SELECT a.*\nFROM {_q(table)} a\nLEFT JOIN {_q(other['name'])} b ON a.{_q(join_col)} = b.{_q(join_col)}\nWHERE b.{_q(join_col)} IS NULL\nLIMIT {{{{limit}}}}",
                    "params": [{"name": "limit", "type": "number", "default": 100, "label": "Max rows"}],
                })

        return templates
