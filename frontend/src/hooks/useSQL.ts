/**
 * useSQL — React hook for the SQL Query Engine.
 * Manages query state, execution, results, and history.
 */

import { useState, useCallback, useEffect } from 'react';
import type { QuerySpec, QueryResult, TableInfo, ColumnInfo } from '../types/sql';
import * as sqlApi from '../api/sql';

export interface SQLState {
    tables: TableInfo[];
    columnsCache: Record<string, ColumnInfo[]>;
    querySpec: QuerySpec;
    rawSQL: string;
    generatedSQL: string;
    result: QueryResult | null;
    loading: boolean;
    error: string | null;
    history: Array<{ sql: string; timestamp: number; rowCount: number }>;
    mode: 'visual' | 'raw';
}

const defaultQuery: QuerySpec = {
    from: undefined,
    columns: [],
    where: [],
    joins: [],
    group_by: [],
    having: [],
    order_by: [],
    limit: 100,
    offset: 0,
};

export function useSQL() {
    const [state, setState] = useState<SQLState>({
        tables: [],
        columnsCache: {},
        querySpec: { ...defaultQuery },
        rawSQL: '',
        generatedSQL: '',
        result: null,
        loading: false,
        error: null,
        history: [],
        mode: 'visual',
    });

    // Load tables on mount
    const loadTables = useCallback(async () => {
        try {
            const tables = await sqlApi.fetchTables();
            setState(s => ({ ...s, tables }));
        } catch (e: any) {
            setState(s => ({ ...s, error: e.message }));
        }
    }, []);

    useEffect(() => { loadTables(); }, [loadTables]);

    // Load columns for a table (with cache)
    const loadColumns = useCallback(async (tableName: string) => {
        if (state.columnsCache[tableName]) return state.columnsCache[tableName];
        try {
            const columns = await sqlApi.fetchColumns(tableName);
            setState(s => ({
                ...s,
                columnsCache: { ...s.columnsCache, [tableName]: columns },
            }));
            return columns;
        } catch {
            return [];
        }
    }, [state.columnsCache]);

    // Update query spec
    const updateQuery = useCallback((updates: Partial<QuerySpec>) => {
        setState(s => ({
            ...s,
            querySpec: { ...s.querySpec, ...updates },
        }));
    }, []);

    // Set FROM table
    const setFromTable = useCallback((tableName: string) => {
        updateQuery({ from: { table: tableName } });
        loadColumns(tableName);
    }, [updateQuery, loadColumns]);

    // Set mode
    const setMode = useCallback((mode: 'visual' | 'raw') => {
        setState(s => ({ ...s, mode }));
    }, []);

    // Set raw SQL
    const setRawSQL = useCallback((sql: string) => {
        setState(s => ({ ...s, rawSQL: sql }));
    }, []);

    // Generate SQL preview from visual builder
    const generateSQL = useCallback(async () => {
        try {
            const res = await sqlApi.previewSQL(state.querySpec);
            if (res.success) {
                setState(s => ({ ...s, generatedSQL: res.sql, error: null }));
            } else {
                setState(s => ({ ...s, error: res.errors?.join(', ') || 'Failed to generate SQL' }));
            }
        } catch (e: any) {
            setState(s => ({ ...s, error: e.message }));
        }
    }, [state.querySpec]);

    // Execute query
    const execute = useCallback(async () => {
        setState(s => ({ ...s, loading: true, error: null }));
        try {
            const payload = state.mode === 'raw'
                ? { sql: state.rawSQL }
                : { query_spec: state.querySpec };

            const result = await sqlApi.executeQuery(payload);

            setState(s => ({
                ...s,
                result,
                loading: false,
                error: result.success ? null : result.error || 'Query failed',
                generatedSQL: result.sql || s.generatedSQL,
                history: result.success
                    ? [
                        { sql: result.sql || s.rawSQL, timestamp: Date.now(), rowCount: result.row_count },
                        ...s.history.slice(0, 49),
                    ]
                    : s.history,
            }));
        } catch (e: any) {
            setState(s => ({ ...s, loading: false, error: e.message }));
        }
    }, [state.mode, state.rawSQL, state.querySpec]);

    // Export results
    const exportResults = useCallback(async (format: string) => {
        try {
            const payload = state.mode === 'raw'
                ? { sql: state.rawSQL, format }
                : { query_spec: state.querySpec, format };
            const blob = await sqlApi.exportResults(payload);
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `query_results.${format}`;
            a.click();
            URL.revokeObjectURL(url);
        } catch (e: any) {
            setState(s => ({ ...s, error: e.message }));
        }
    }, [state.mode, state.rawSQL, state.querySpec]);

    // Reset query
    const resetQuery = useCallback(() => {
        setState(s => ({
            ...s,
            querySpec: { ...defaultQuery },
            rawSQL: '',
            generatedSQL: '',
            result: null,
            error: null,
        }));
    }, []);

    return {
        state,
        loadTables,
        loadColumns,
        updateQuery,
        setFromTable,
        setMode,
        setRawSQL,
        generateSQL,
        execute,
        exportResults,
        resetQuery,
    };
}
