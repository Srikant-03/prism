/**
 * TypeScript types for the AI Dashboard Builder.
 * Mirrors the backend Pydantic models in chart_config_models.py.
 */

export type ChartType = 'bar' | 'line' | 'area' | 'pie' | 'donut' | 'scatter' | 'heatmap' | 'treemap' | 'funnel' | 'kpi' | 'table';
export type AggregationType = 'sum' | 'avg' | 'count' | 'min' | 'max' | 'median' | 'none';
export type SortDirection = 'asc' | 'desc';
export type ColorScheme = 'default' | 'dark' | 'vibrant' | 'pastel' | 'monochrome' | 'ocean' | 'sunset' | 'forest';

export interface FilterCondition {
    column: string;
    operator: string;
    value?: any;
    values?: any[];
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
    aggregation: AggregationType;
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
}

export interface ClarificationRequest {
    clarification: string;
    suggestions: string[];
}

export interface InterpretResponse {
    success: boolean;
    config?: ChartConfig;
    clarification?: ClarificationRequest;
    sql?: string;
    data?: Record<string, any>[];
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
}

export interface Dashboard {
    id: string;
    title: string;
    description: string;
    file_id: string;
    widgets: Widget[];
    global_filters: FilterCondition[];
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
