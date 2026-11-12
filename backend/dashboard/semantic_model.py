"""
Semantic Model — Builds semantic layer metadata, hierarchies, and default measures
for the AI-Powered Dashboard and Power BI parity.

Inspects stored DataFrames, profiling results, and registered SQLEngine tables
to discover:
  1. Date Hierarchies (Year -> Quarter -> Month -> Day)
  2. Geographic Hierarchies (Region -> Country -> State -> City)
  3. Categorical Hierarchies (Functional Dependencies)
  4. Auto-Generated Measures & Time-Intelligence Definitions
  5. Multi-Table Join Relationships
"""

from __future__ import annotations

import logging
from typing import Optional, Any
import pandas as pd
from pydantic import BaseModel, Field

from dashboard.chart_config_models import (
    HierarchyDefinition,
    MeasureDefinition,
)

logger = logging.getLogger(__name__)


class SemanticColumn(BaseModel):
    name: str
    data_type: str
    semantic_role: str  # dimension, measure, date, geo, identifier
    cardinality: int = 0
    sample_values: list[str] = Field(default_factory=list)


class TableRelationship(BaseModel):
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str = "1:N"  # 1:1, 1:N, N:1


class SemanticModel(BaseModel):
    file_id: str
    primary_table_name: str
    columns: list[SemanticColumn] = Field(default_factory=list)
    hierarchies: list[HierarchyDefinition] = Field(default_factory=list)
    measures: list[MeasureDefinition] = Field(default_factory=list)
    relationships: list[TableRelationship] = Field(default_factory=list)
    date_columns: list[str] = Field(default_factory=list)
    geographic_columns: list[str] = Field(default_factory=list)
    categorical_columns: list[str] = Field(default_factory=list)
    numeric_columns: list[str] = Field(default_factory=list)


def build_semantic_model(file_id: str, df: Optional[pd.DataFrame] = None) -> SemanticModel:
    """
    Build a complete SemanticModel for a given file_id.
    """
    if df is None:
        from state import get_stored_dataframe
        df = get_stored_dataframe(file_id)

    if df is None:
        return SemanticModel(
            file_id=file_id,
            primary_table_name=f"upload_{file_id}",
        )

    # 1. Analyze columns
    columns_info: list[SemanticColumn] = []
    date_cols: list[str] = []
    geo_cols: list[str] = []
    cat_cols: list[str] = []
    num_cols: list[str] = []

    geo_keywords = {"region", "country", "country_code", "state", "city", "province", "postal_code", "zip"}
    id_keywords = {"id", "uuid", "key", "code", "hash"}

    for col in df.columns:
        c_lower = str(col).lower()
        non_null = df[col].dropna()
        n_unique = int(non_null.nunique())
        dtype_str = str(df[col].dtype)
        samples = [str(v)[:40] for v in non_null.head(5).tolist()]

        # Classify role
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            role = "date"
            date_cols.append(col)
        elif any(c_lower.endswith(k) or c_lower == k for k in ["_date", "date", "_time", "timestamp"]):
            role = "date"
            date_cols.append(col)
        elif any(g in c_lower for g in geo_keywords):
            role = "geo"
            geo_cols.append(col)
        elif pd.api.types.is_numeric_dtype(df[col]):
            is_int_like = pd.api.types.is_integer_dtype(df[col]) or (len(non_null) > 0 and (non_null % 1 == 0).all())
            has_id_keyword = any(k in c_lower for k in id_keywords)
            has_dim_keyword = any(k in c_lower for k in ["code", "tier", "status", "flag", "type", "level", "grade", "group", "category", "year", "quarter"])
            if has_id_keyword and n_unique > len(df) * 0.7:
                role = "identifier"
            elif n_unique <= 2 or has_dim_keyword or (is_int_like and n_unique <= 10):
                role = "dimension"
                cat_cols.append(col)
            else:
                role = "measure"
                num_cols.append(col)
        else:
            if any(k in c_lower for k in id_keywords) and n_unique > len(df) * 0.8:
                role = "identifier"
            else:
                role = "dimension"
                cat_cols.append(col)

        columns_info.append(SemanticColumn(
            name=col,
            data_type=dtype_str,
            semantic_role=role,
            cardinality=n_unique,
            sample_values=samples,
        ))

    # 2. Discover Date Hierarchies
    hierarchies: list[HierarchyDefinition] = []

    # Check for pre-existing separate temporal columns (e.g. order_year, order_quarter, order_month_name, order_date)
    year_cols = [c for c in df.columns if "year" in c.lower()]
    quarter_cols = [c for c in df.columns if "quarter" in c.lower()]
    month_cols = [c for c in df.columns if "month" in c.lower()]
    day_cols = [c for c in df.columns if "date" in c.lower() or "day" in c.lower()]

    if year_cols and quarter_cols and month_cols:
        levels = [year_cols[0], quarter_cols[0], month_cols[0]]
        if day_cols and day_cols[0] not in levels:
            levels.append(day_cols[0])
        hierarchies.append(HierarchyDefinition(
            name="Calendar Hierarchy",
            levels=levels,
            hierarchy_type="date",
        ))
    elif date_cols:
        # Build virtual date hierarchy on primary date column
        primary_date = date_cols[0]
        hierarchies.append(HierarchyDefinition(
            name=f"{primary_date} Hierarchy",
            levels=[f"{primary_date} (Year)", f"{primary_date} (Quarter)", f"{primary_date} (Month)", primary_date],
            hierarchy_type="date",
        ))

    # 3. Discover Geographic Hierarchies
    # Standard ordering: region -> country -> state -> city
    geo_ordered = []
    for g_key in ["region", "country", "country_code", "state", "city"]:
        for c in geo_cols:
            if g_key in c.lower() and c not in geo_ordered:
                geo_ordered.append(c)

    if len(geo_ordered) >= 2:
        hierarchies.append(HierarchyDefinition(
            name="Geographic Hierarchy",
            levels=geo_ordered,
            hierarchy_type="geographic",
        ))

    # 4. Discover Category Hierarchies (e.g. product_category -> sub_category)
    cat_ordered = []
    for c_key in ["department", "category", "product_category", "sub_category", "type", "tier"]:
        for c in cat_cols:
            if c_key in c.lower() and c not in cat_ordered:
                cat_ordered.append(c)
    if len(cat_ordered) >= 2:
        hierarchies.append(HierarchyDefinition(
            name="Category Hierarchy",
            levels=cat_ordered,
            hierarchy_type="categorical",
        ))

    # 5. Build Default Measures
    measures: list[MeasureDefinition] = []

    for num in num_cols:
        nl = num.lower()
        if "rate" in nl or "pct" in nl or "percent" in nl or "ratio" in nl:
            measures.append(MeasureDefinition(
                name=f"Avg {num.replace('_', ' ').title()}",
                expression=f'AVG("{num}")',
                format="percent",
                description=f"Average {num}",
                category="aggregation",
            ))
        else:
            fmt = "currency" if any(curr in nl for curr in ["revenue", "price", "cost", "sales", "amount", "fee", "val"]) else "compact"
            measures.append(MeasureDefinition(
                name=f"Total {num.replace('_', ' ').title()}",
                expression=f'SUM("{num}")',
                format=fmt,
                description=f"Total sum of {num}",
                category="aggregation",
            ))

    # Add ratio / margin measures if gross and net revenue exist
    has_gross = any(c.lower() == "gross_revenue" for c in df.columns)
    has_net = any(c.lower() == "net_revenue" for c in df.columns)
    if has_gross and has_net:
        measures.append(MeasureDefinition(
            name="Net Margin %",
            expression='DIVIDE(SUM("net_revenue"), SUM("gross_revenue"), 0)',
            format="percent",
            description="Net Revenue divided by Gross Revenue",
            category="financial",
        ))

    # Add Time-Intelligence measures on the primary date column
    primary_date = date_cols[0] if date_cols else None
    primary_revenue = "net_revenue" if has_net else (num_cols[0] if num_cols else None)

    if primary_date and primary_revenue:
        measures.append(MeasureDefinition(
            name=f"{primary_revenue.replace('_', ' ').title()} YTD",
            expression=f'YTD(SUM("{primary_revenue}"), "{primary_date}")',
            format="currency" if "revenue" in primary_revenue else "compact",
            description=f"Year to Date {primary_revenue}",
            category="time_intelligence",
        ))
        measures.append(MeasureDefinition(
            name=f"{primary_revenue.replace('_', ' ').title()} MoM %",
            expression=f'MOM_PERCENT(SUM("{primary_revenue}"), "{primary_date}")',
            format="percent",
            description=f"Month over Month % Change for {primary_revenue}",
            category="time_intelligence",
        ))

    # 6. Multi-Table Join Discovery
    relationships: list[TableRelationship] = []
    try:
        from api.sql import get_engine
        engine = get_engine()
        tables = engine.list_tables()
        if len(tables) > 1:
            for i in range(len(tables)):
                for j in range(i + 1, len(tables)):
                    t1_name = tables[i]["name"]
                    t2_name = tables[j]["name"]
                    t1_cols = {c["name"]: c for c in engine.get_columns(t1_name)}
                    t2_cols = {c["name"]: c for c in engine.get_columns(t2_name)}

                    common = set(t1_cols.keys()).intersection(set(t2_cols.keys()))
                    for k in common:
                        if any(id_w in k.lower() for id_w in ["id", "uuid", "key", "code"]):
                            relationships.append(TableRelationship(
                                source_table=t1_name,
                                source_column=k,
                                target_table=t2_name,
                                target_column=k,
                                relationship_type="1:N",
                            ))
    except Exception as e:
        logger.debug("Multi-table relationship detection skipped: %s", e)

    return SemanticModel(
        file_id=file_id,
        primary_table_name=f"upload_{file_id}",
        columns=columns_info,
        hierarchies=hierarchies,
        measures=measures,
        relationships=relationships,
        date_columns=date_cols,
        geographic_columns=geo_cols,
        categorical_columns=cat_cols,
        numeric_columns=num_cols,
    )
