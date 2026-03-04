/**
 * SQL API Client — Communicates with the SQL backend.
 */

import type { QuerySpec, QueryResult, TableInfo, ColumnInfo } from '../types/sql';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

export async function fetchTables(): Promise<TableInfo[]> {
    const res = await fetch(`${API_BASE}/api/sql/tables`);
    const data = await res.json();
    return data.tables || [];
}

export async function fetchColumns(table: string): Promise<ColumnInfo[]> {
    const res = await fetch(`${API_BASE}/api/sql/columns/${encodeURIComponent(table)}`);
    const data = await res.json();
    return data.columns || [];
}

export async function fetchColumnValues(table: string, column: string, limit = 50): Promise<any[]> {
    const res = await fetch(
        `${API_BASE}/api/sql/values/${encodeURIComponent(table)}/${encodeURIComponent(column)}?limit=${limit}`
    );
    const data = await res.json();
    return data.values || [];
}

export async function fetchPreview(table: string, limit = 10): Promise<any> {
    const res = await fetch(
        `${API_BASE}/api/sql/preview/${encodeURIComponent(table)}?limit=${limit}`
    );
    return res.json();
}

export async function executeQuery(payload: { sql?: string; query_spec?: QuerySpec }): Promise<QueryResult> {
    const res = await fetch(`${API_BASE}/api/sql/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    return res.json();
}

export async function previewSQL(querySpec: QuerySpec): Promise<{ success: boolean; sql: string; errors?: string[] }> {
    const res = await fetch(`${API_BASE}/api/sql/preview-sql`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query_spec: querySpec }),
    });
    return res.json();
}

export async function exportResults(
    payload: { sql?: string; query_spec?: QuerySpec; format: string }
): Promise<Blob> {
    const res = await fetch(`${API_BASE}/api/sql/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    return res.blob();
}

// ── Natural Language Query ────────────────────────────────────────────

export async function nlQuery(question: string, conversationHistory?: any[]): Promise<any> {
    const res = await fetch(`${API_BASE}/api/sql/nl-query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question, conversation_history: conversationHistory }),
    });
    return res.json();
}

export async function nlRefine(
    originalQuestion: string, originalSql: string, refinement: string
): Promise<any> {
    const res = await fetch(`${API_BASE}/api/sql/nl-refine`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            original_question: originalQuestion,
            original_sql: originalSql,
            refinement: refinement,
        }),
    });
    return res.json();
}

// ── Template Library ─────────────────────────────────────────────────

export async function fetchTemplates(tableName: string): Promise<any> {
    const res = await fetch(`${API_BASE}/api/sql/templates/${encodeURIComponent(tableName)}`);
    return res.json();
}

export async function executeTemplate(sql: string, params?: Record<string, any>): Promise<any> {
    const res = await fetch(`${API_BASE}/api/sql/template-execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
    });
    return res.json();
}

