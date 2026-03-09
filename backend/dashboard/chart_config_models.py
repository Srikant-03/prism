"""
Dashboard Chart Config Models — Pydantic models for the AI Dashboard Builder.

Defines ChartConfig (the structured output from prompt interpretation),
DashboardModel, WidgetModel, and request/response types.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel, Field
import uuid as _uuid


# ── Chart Type Enum ──

class ChartType(str, Enum):
    BAR = "bar"
    LINE = "line"
    AREA = "area"
    PIE = "pie"
    DONUT = "donut"
    SCATTER = "scatter"
    HEATMAP = "heatmap"
    TREEMAP = "treemap"
    FUNNEL = "funnel"
    KPI = "kpi"
    TABLE = "table"


class AggregationType(str, Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    NONE = "none"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class ColorScheme(str, Enum):
    DEFAULT = "default"
    DARK = "dark"
    VIBRANT = "vibrant"
    PASTEL = "pastel"
    MONOCHROME = "monochrome"
    OCEAN = "ocean"
    SUNSET = "sunset"
    FOREST = "forest"


# ── Filter Model ──

class FilterCondition(BaseModel):
    """A single filter condition applied to the data."""
    column: str
    operator: str = "="  # =, !=, >, <, >=, <=, in, not_in, between, like, is_null, is_not_null
    value: Any = None
    values: Optional[list[Any]] = None  # For 'in', 'not_in', 'between'


# ── Chart Config (core model) ──

class ChartConfig(BaseModel):
    """
    Structured chart configuration produced by the prompt interpreter.
    Fully serialisable to JSON — no functions, no closures.
    """
    chart_type: ChartType = ChartType.BAR
    title: str = ""
    subtitle: Optional[str] = None

    # Axis mapping
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    y_axis_secondary: Optional[str] = None  # For dual-axis charts
    group_by: Optional[str] = None
    size_by: Optional[str] = None  # For scatter/treemap

    # Aggregation
    aggregation: AggregationType = AggregationType.SUM

    # Filters
    filters: list[FilterCondition] = Field(default_factory=list)

    # Sort
    sort_by: Optional[str] = None
    sort_direction: SortDirection = SortDirection.ASC

    # Display options
    limit: Optional[int] = None
    color_scheme: ColorScheme = ColorScheme.DEFAULT
    show_legend: bool = True
    show_grid: bool = True
    show_values: bool = False
    stacked: bool = False
    trend_line: bool = False
    smooth: bool = False

    # KPI-specific
    kpi_value_column: Optional[str] = None
    kpi_comparison_column: Optional[str] = None
    kpi_aggregation: AggregationType = AggregationType.SUM


# ── Clarification Request ──

class ClarificationRequest(BaseModel):
    """Returned when the prompt is ambiguous and needs user clarification."""
    clarification: str
    suggestions: list[str] = Field(default_factory=list)


# ── API Request / Response Models ──

class InterpretRequest(BaseModel):
    """Request to interpret a natural language prompt into a ChartConfig."""
    message: str
    file_id: str
    current_config: Optional[ChartConfig] = None
    conversation_history: list[str] = Field(default_factory=list)


class InterpretResponse(BaseModel):
    """Response from prompt interpretation — either a config or a clarification."""
    success: bool = True
    config: Optional[ChartConfig] = None
    clarification: Optional[ClarificationRequest] = None
    sql: Optional[str] = None
    data: Optional[list[dict]] = None


class QueryRequest(BaseModel):
    """Request to execute a ChartConfig against a dataset."""
    config: ChartConfig
    file_id: str
    global_filters: list[FilterCondition] = Field(default_factory=list)


class SuggestPromptsRequest(BaseModel):
    """Request for AI-generated follow-up prompt suggestions."""
    file_id: str
    current_config: Optional[ChartConfig] = None


# ── Widget Model ──

class WidgetModel(BaseModel):
    """A single widget on the dashboard canvas."""
    id: str = Field(default_factory=lambda: str(_uuid.uuid4())[:8])
    config: ChartConfig
    source_prompt: str = ""
    prompt_history: list[str] = Field(default_factory=list)
    # react-grid-layout position
    layout: dict = Field(default_factory=lambda: {"x": 0, "y": 0, "w": 6, "h": 4})


# ── Dashboard Model ──

class DashboardModel(BaseModel):
    """A complete dashboard with widgets and layout."""
    id: str = Field(default_factory=lambda: str(_uuid.uuid4()))
    title: str = "Untitled Dashboard"
    description: str = ""
    file_id: str = ""
    widgets: list[WidgetModel] = Field(default_factory=list)
    global_filters: list[FilterCondition] = Field(default_factory=list)
    created_at: float = 0.0
    updated_at: float = 0.0
