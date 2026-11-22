/**
 * TypeScript types for the AI Dashboard Builder.
 * Mirrors the backend Pydantic models in chart_config_models.py.
 */

export type ChartType =
    | 'kpi'
    | 'multi_kpi'
    | 'gauge'
    | 'bullet'
    | 'bar'
    | 'stacked_bar'
    | 'stacked_bar_100'
    | 'column'
    | 'stacked_column'
    | 'stacked_column_100'
    | 'line'
    | 'area'
    | 'stacked_area'
    | 'ribbon'
    | 'combo_bar_line'
    | 'combo_stacked_line'
    | 'pie'
    | 'donut'
    | 'treemap'
    | 'sunburst'
    | 'funnel'
    | 'waterfall'
    | 'scatter'
    | 'bubble'
    | 'histogram'
    | 'boxplot'
    | 'heatmap'
    | 'table'
    | 'matrix'
    | 'bubble_map'
    | 'choropleth_map'
    | 'decomposition_tree'
    | 'key_influencers'
    | 'smart_narrative'
    | 'qa_visual'
    | 'slicer';

export type AggregationType = 'sum' | 'avg' | 'count' | 'distinct_count' | 'min' | 'max' | 'median' | 'stddev' | 'none';
export type TimeGrain = 'day' | 'week' | 'month' | 'quarter' | 'year' | 'none';
export type SortDirection = 'asc' | 'desc';
export type ColorScheme = 'default' | 'dark' | 'vibrant' | 'pastel' | 'monochrome' | 'ocean' | 'sunset' | 'forest';

export interface FilterCondition {
    column: string;
    operator: string;
    value?: any;
    values?: any[];
}

export interface MetricSpec {
    column: string;
    aggregation: AggregationType;
    alias?: string;
    label?: string;
    format?: string;
    y_axis_index?: number;
    measure_expression?: string;
}

export interface ReferenceLine {
    type: 'constant' | 'average' | 'median' | 'min' | 'max' | 'percentile';
    value?: number;
    label?: string;
    color?: string;
    line_style?: 'solid' | 'dashed' | 'dotted';
}

export interface ForecastConfig {
    enabled: boolean;
    periods?: number;
    confidence_interval?: number; // 0.80 or 0.95
    seasonality?: number;
}

export interface AnomalyDetectionConfig {
    enabled: boolean;
    sensitivity?: number; // e.g. 2.5
}

export interface ChartConfig {
    chart_type: ChartType;
    title: string;
    subtitle?: string;
    x_axis?: string;
    y_axis?: string;
    y_axis_secondary?: string;
    group_by?: string;
    size_by?: string;
    color_by?: string;
    play_axis?: string;
    hierarchy_levels?: string[];
    active_drill_level?: number;
    metrics?: MetricSpec[];
    aggregation: AggregationType;
    x_axis_time_grain?: TimeGrain | null;
    filters: FilterCondition[];
    sort_by?: string;
    sort_direction: SortDirection;
    limit?: number;
    color_scheme: ColorScheme;
    show_legend: boolean;
    show_grid: boolean;
    show_values: boolean;
    stacked: boolean;
    trend_line: boolean;
    smooth: boolean;
    kpi_value_column?: string;
    kpi_comparison_column?: string;
    kpi_aggregation?: AggregationType;
    conditional_formatting?: any[];
    reference_lines?: ReferenceLine[];
    forecast_config?: ForecastConfig;
    anomaly_detection?: AnomalyDetectionConfig;
    styling?: Record<string, any>;
    interactions?: Record<string, any>;
}

export function formatCompactNumber(value: number | null | undefined): string {
    if (value === null || value === undefined || isNaN(value)) return '—';
    const abs = Math.abs(value);
    if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
    if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
    if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
    return Number.isInteger(value) ? value.toString() : value.toFixed(2);
}

export interface ClarificationRequest {
    clarification: string;
    suggestions: string[];
}

export interface InterpretResponse {
    success: boolean;
    error?: string;
    intent?: 'create_chart' | 'modify_chart' | 'create_measure' | 'clarify' | 'qa_query';
    config?: ChartConfig;
    clarification?: ClarificationRequest;
    sql?: string;
    data?: Record<string, any>[];
    measure?: MeasureDefinition;
}

export interface Widget {
    id: string;
    config: ChartConfig;
    source_prompt: string;
    prompt_history: string[];
    layout: { x: number; y: number; w: number; h: number };
    data?: Record<string, any>[];
    sql?: string;
    loading?: boolean;
    error?: string;
    hidden?: boolean;
    locked?: boolean;
}

export interface HierarchyDefinition {
    name: string;
    dimension_type: 'date' | 'geography' | 'category' | 'custom';
    levels: string[];
}

export interface MeasureDefinition {
    name: string;
    expression: string;
    format: string;
    description?: string;
    folder?: string;
}

export interface Bookmark {
    id: string;
    name: string;
    page_id?: string;
    filters?: FilterCondition[];
    cross_filters?: Record<string, any>;
    visible_widget_ids?: string[];
    created_at?: number;
}

export interface RLSRole {
    role_name: string;
    filter_expression: string;
    assigned_users?: string[];
}

export interface CrossFilterState {
    sourceWidgetId: string;
    column: string;
    value: any;
}

export interface DashboardPage {
    id: string;
    title: string;
    widgets: Widget[];
    order?: number;
    is_drillthrough_target?: boolean;
    drillthrough_fields?: string[];
}

export interface Dashboard {
    id: string;
    title: string;
    description: string;
    file_id: string;
    pages?: DashboardPage[];
    widgets: Widget[];
    global_filters: FilterCondition[];
    measures?: MeasureDefinition[];
    bookmarks?: Bookmark[];
    rls_roles?: RLSRole[];
    created_at: number;
    updated_at: number;
}

// Color palettes for each scheme
export const COLOR_PALETTES: Record<ColorScheme, string[]> = {
    default: ['#6366f1', '#8b5cf6', '#a78bfa', '#c4b5fd', '#818cf8', '#4f46e5', '#7c3aed', '#5b21b6'],
    dark: ['#60a5fa', '#34d399', '#fbbf24', '#f87171', '#a78bfa', '#fb923c', '#38bdf8', '#4ade80'],
    vibrant: ['#ef4444', '#f97316', '#eab308', '#22c55e', '#3b82f6', '#8b5cf6', '#ec4899', '#14b8a6'],
    pastel: ['#93c5fd', '#86efac', '#fde68a', '#fca5a5', '#c4b5fd', '#fdba74', '#67e8f9', '#a5f3fc'],
    monochrome: ['#1e293b', '#334155', '#475569', '#64748b', '#94a3b8', '#cbd5e1', '#e2e8f0', '#f1f5f9'],
    ocean: ['#0ea5e9', '#06b6d4', '#14b8a6', '#0d9488', '#059669', '#0891b2', '#0284c7', '#0369a1'],
    sunset: ['#f97316', '#ef4444', '#ec4899', '#f43f5e', '#e11d48', '#db2777', '#c026d3', '#a855f7'],
    forest: ['#16a34a', '#15803d', '#166534', '#065f46', '#047857', '#059669', '#10b981', '#34d399'],
};
