"""
Dashboard Chart Config Models — Pydantic models for the AI Dashboard Builder.

Defines ChartConfig (the structured output from prompt interpretation),
multi-page DashboardModel, WidgetModel, MeasureDefinition, BookmarkModel,
and request/response types for Power BI parity.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel, Field
import uuid as _uuid


# ── Chart Type Enum (Full Power BI Parity) ──

class ChartType(str, Enum):
    KPI = "kpi"
    MULTI_KPI = "multi_kpi"
    GAUGE = "gauge"
    BULLET = "bullet"
    BAR = "bar"
    STACKED_BAR = "stacked_bar"
    STACKED_BAR_100 = "stacked_bar_100"
    COLUMN = "column"
    STACKED_COLUMN = "stacked_column"
    STACKED_COLUMN_100 = "stacked_column_100"
    LINE = "line"
    AREA = "area"
    STACKED_AREA = "stacked_area"
    RIBBON = "ribbon"
    COMBO_BAR_LINE = "combo_bar_line"
    COMBO_STACKED_LINE = "combo_stacked_line"
    PIE = "pie"
    DONUT = "donut"
    TREEMAP = "treemap"
    SUNBURST = "sunburst"
    FUNNEL = "funnel"
    WATERFALL = "waterfall"
    SCATTER = "scatter"
    BUBBLE = "bubble"
    HISTOGRAM = "histogram"
    BOXPLOT = "boxplot"
    HEATMAP = "heatmap"
    TABLE = "table"
    MATRIX = "matrix"
    BUBBLE_MAP = "bubble_map"
    CHOROPLETH_MAP = "choropleth_map"
    DECOMPOSITION_TREE = "decomposition_tree"
    KEY_INFLUENCERS = "key_influencers"
    SMART_NARRATIVE = "smart_narrative"
    QA_VISUAL = "qa_visual"
    SLICER = "slicer"


class AggregationType(str, Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    NONE = "none"


class TimeGrain(str, Enum):
    """Temporal bucketing granularity for date/time x-axes."""
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"
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
    operator: str = "="  # =, !=, >, <, >=, <=, in, not_in, between, contains, starts_with, is_null, is_not_null, top_n, relative_date
    value: Any = None
    values: Optional[list[Any]] = None  # For 'in', 'not_in', 'between'


# ── Metric Spec Model (Multi-Metric Support) ──

class MetricSpec(BaseModel):
    """Specification for an aggregated metric in a multi-metric chart, combo, or KPI."""
    column: str
    aggregation: AggregationType = AggregationType.SUM
    label: Optional[str] = None
    alias: Optional[str] = None
    format: Optional[str] = None  # currency, percent, number, compact
    y_axis_index: int = 0         # 0 for primary axis, 1 for secondary combo axis
    measure_expression: Optional[str] = None  # Optional custom DAX/measure expression


# ── Hierarchy Definition ──

class HierarchyDefinition(BaseModel):
    """Definition of a drillable multi-level hierarchy."""
    id: str = Field(default_factory=lambda: str(_uuid.uuid4())[:8])
    name: str
    levels: list[str]  # e.g. ["order_year", "order_quarter", "order_month_name"] or ["region", "country_code"]
    hierarchy_type: str = "custom"  # date, geographic, categorical, custom


# ── Measure Definition ──

class MeasureDefinition(BaseModel):
    """A reusable calculated measure defined with DAX-style syntax."""
    id: str = Field(default_factory=lambda: str(_uuid.uuid4())[:8])
    name: str
    expression: str  # e.g. "DIVIDE(SUM(net_revenue), SUM(gross_revenue))"
    format: str = "number"  # currency, percent, compact, number
    description: Optional[str] = None
    category: str = "custom"


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
    y_axis_secondary: Optional[str] = None  # For dual-axis combo charts
    group_by: Optional[str] = None
    color_by: Optional[str] = None
    size_by: Optional[str] = None  # For scatter/bubble/treemap
    play_axis: Optional[str] = None  # For animated timeline scatter/bubble charts

    # Multi-metric support
    metrics: Optional[list[MetricSpec]] = None

    # Hierarchies and drilldown
    hierarchy_levels: Optional[list[str]] = None
    active_drill_level: Optional[int] = None

    # Aggregation
    aggregation: AggregationType = AggregationType.SUM

    # Temporal x-axis bucketing (e.g. 'month' → GROUP BY YYYY-MM)
    x_axis_time_grain: Optional[TimeGrain] = Field(
        None,
        description="Temporal bucket for the x-axis: 'day', 'week', 'month', 'quarter', 'year'. "
                    "When set, dates are truncated to this granularity before aggregation.",
    )

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

    # Advanced formatting & analytics
    conditional_formatting: Optional[list[dict]] = None
    reference_lines: Optional[list[dict]] = None
    forecast_config: Optional[dict] = None  # {"periods": 6, "confidence": 0.95}
    anomaly_detection: bool = False
    styling: Optional[dict] = None
    interactions: Optional[dict[str, str]] = None  # widget_id -> "filter" | "highlight" | "none"

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
    edit_mode: bool = False


class InterpretResponse(BaseModel):
    """Response from prompt interpretation — either a config or a clarification."""
    success: bool = True
    error: Optional[str] = None
    config: Optional[ChartConfig] = None
    clarification: Optional[ClarificationRequest] = None
    sql: Optional[str] = None
    data: Optional[list[dict]] = None
    intent: Optional[str] = None
    measure: Optional[MeasureDefinition] = None
    multi_configs: Optional[list[ChartConfig]] = None


class QueryRequest(BaseModel):
    """Request to execute a ChartConfig against a dataset."""
    config: ChartConfig
    file_id: str
    global_filters: list[FilterCondition] = Field(default_factory=list)
    active_role: Optional[str] = None


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


# ── Bookmark Model ──

class BookmarkModel(BaseModel):
    """Snapshot of report state for interactive presentation and navigation."""
    id: str = Field(default_factory=lambda: str(_uuid.uuid4())[:8])
    name: str = "Bookmark"
    page_id: str = ""
    filters: list[FilterCondition] = Field(default_factory=list)
    slicers: dict[str, Any] = Field(default_factory=dict)
    active_widgets: list[str] = Field(default_factory=list)
    spotlight_widget_id: Optional[str] = None


# ── Page Model (Multi-Page Reports) ──

class DashboardPageModel(BaseModel):
    """A single page within a multi-page dashboard report."""
    id: str = Field(default_factory=lambda: str(_uuid.uuid4())[:8])
    title: str = "Page 1"
    description: Optional[str] = None
    widgets: list[WidgetModel] = Field(default_factory=list)
    page_filters: list[FilterCondition] = Field(default_factory=list)
    is_drillthrough: bool = False
    drillthrough_field: Optional[str] = None


# ── Dashboard Model (Full Multi-Page Report) ──

class DashboardModel(BaseModel):
    """A complete multi-page dashboard report with semantic metadata."""
    id: str = Field(default_factory=lambda: str(_uuid.uuid4()))
    title: str = "Untitled Dashboard"
    description: str = ""
    file_id: str = ""
    widgets: list[WidgetModel] = Field(default_factory=list)  # Preserved for single-page backwards compatibility
    pages: list[DashboardPageModel] = Field(default_factory=list)
    measures: list[MeasureDefinition] = Field(default_factory=list)
    bookmarks: list[BookmarkModel] = Field(default_factory=list)
    slicers: list[dict] = Field(default_factory=list)
    hierarchies: list[HierarchyDefinition] = Field(default_factory=list)
    rls_roles: list[dict] = Field(default_factory=list)  # [{"role_name": "...", "filter_expression": "..."}]
    global_filters: list[FilterCondition] = Field(default_factory=list)
    created_at: float = 0.0
    updated_at: float = 0.0

    def model_post_init(self, __context: Any) -> None:
        """Ensure backward compatibility between single-page widgets and multi-page pages."""
        if not self.pages and self.widgets:
            self.pages = [
                DashboardPageModel(
                    id="page_1",
                    title="Page 1",
                    widgets=self.widgets,
                    page_filters=[],
                )
            ]
        elif self.pages and not self.widgets:
            self.widgets = self.pages[0].widgets
