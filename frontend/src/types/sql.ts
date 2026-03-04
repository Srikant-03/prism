/**
 * SQL Types — TypeScript types for the SQL Query Engine.
 */

// ── Column & Table Types ──────────────────────────────────────────────

export type UIColumnType = 'integer' | 'float' | 'text' | 'datetime' | 'boolean' | 'categorical' | 'other';

export interface ColumnInfo {
    name: string;
    dtype: string;
    ui_type: UIColumnType;
    null_count: number;
    null_pct: number;
    unique_count: number;
    sample_values: any[];
}

export interface TableInfo {
    name: string;
    original_name: string;
    source: 'raw' | 'cleaned';
    file_id: string | null;
    n_rows: number;
    n_cols: number;
}

// ── Query Spec Types ──────────────────────────────────────────────────

export interface ColumnSpec {
    column?: string;
    table?: string;
    alias?: string;
    expression?: string;
    aggregate?: string;
    distinct?: boolean;
    window?: WindowSpec;
}

export interface FilterCondition {
    column?: string;
    op?: string;
    value?: any;
    values?: any[];
    logic?: 'AND' | 'OR';
    group?: FilterCondition[];
    subquery?: QuerySpec;
}

export interface JoinSpec {
    type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS' | 'SEMI' | 'ANTI';
    table: string;
    alias?: string;
    on: Array<{ left: string; right: string }>;
}

export interface OrderBySpec {
    column: string;
    direction: 'ASC' | 'DESC';
    nulls?: 'FIRST' | 'LAST';
    is_expression?: boolean;
}

export interface WindowSpec {
    func: string;
    column?: string;
    distinct?: boolean;
    partition_by?: string[];
    order_by?: Array<string | { column: string; direction: string }>;
    frame?: string;
    n?: number;
    offset?: number;
    default_value?: any;
}

export interface CTESpec {
    name: string;
    query: QuerySpec;
}

export interface QuerySpec {
    from?: { table: string; alias?: string } | string;
    columns?: ColumnSpec[];
    joins?: JoinSpec[];
    where?: FilterCondition[];
    group_by?: string[];
    having?: FilterCondition[];
    order_by?: OrderBySpec[];
    limit?: number | null;
    offset?: number;
    ctes?: CTESpec[];
}

// ── Result Types ──────────────────────────────────────────────────────

export interface QueryResult {
    success: boolean;
    columns: string[];
    column_types?: string[];
    rows: Record<string, any>[];
    row_count: number;
    execution_time_s?: number;
    sql?: string;
    error?: string;
    errors?: string[];
}

// ── Operator Types ────────────────────────────────────────────────────

export const OPERATORS_BY_TYPE: Record<UIColumnType, string[]> = {
    integer: ['=', '≠', '>', '<', '≥', '≤', 'BETWEEN', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL'],
    float: ['=', '≠', '>', '<', '≥', '≤', 'BETWEEN', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL'],
    text: ['=', '≠', 'LIKE', 'NOT LIKE', 'STARTS WITH', 'ENDS WITH', 'CONTAINS', 'DOES NOT CONTAIN', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL', 'MATCHES REGEX'],
    datetime: ['=', '≠', 'BEFORE', 'AFTER', 'BETWEEN', 'IN LAST N DAYS', 'IN LAST N MONTHS', 'THIS WEEK', 'THIS MONTH', 'THIS YEAR', 'IS NULL', 'IS NOT NULL'],
    boolean: ['IS TRUE', 'IS FALSE', 'IS NULL', 'IS NOT NULL'],
    categorical: ['IN', 'NOT IN', 'IS NULL', 'IS NOT NULL'],
    other: ['=', '≠', 'IS NULL', 'IS NOT NULL'],
};

export const AGGREGATE_FUNCTIONS = [
    'COUNT', 'COUNT DISTINCT', 'SUM', 'AVG', 'MIN', 'MAX',
    'MEDIAN', 'MODE', 'STDDEV', 'VARIANCE',
];

export const WINDOW_FUNCTIONS = [
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE',
    'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE',
    'SUM', 'AVG', 'COUNT', 'MIN', 'MAX',
];

// ── Natural Language Query Types ─────────────────────────────────────

export interface NLQueryResult {
    sql: string;
    explanation: string;
    assumptions: string[];
    confidence: 'high' | 'medium' | 'low';
    clarification_needed: string | null;
    schema_context: string;
    success: boolean;
    error: string | null;
}

// ── Template Types ───────────────────────────────────────────────────

export interface TemplateParam {
    name: string;
    type: 'number' | 'select' | 'column_value' | 'text';
    default?: any;
    label: string;
    options?: string[];
    column?: string;
}

export interface QueryTemplate {
    category: string;
    title: string;
    description: string;
    sql: string;
    params: TemplateParam[];
}

export interface TemplateLibraryResponse {
    table: string;
    template_count: number;
    categories: Record<string, QueryTemplate[]>;
}
