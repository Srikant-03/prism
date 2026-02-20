/**
 * QueryWorkbench — Main SQL workbench with all panels.
 * Left: Table Explorer | Center: Visual Builder / Raw SQL / NL Query / Templates
 * Bottom: Results + AutoViz + Library + History + Compare + Explain
 */

import React, { useEffect, useState, useCallback } from 'react';
import { InputNumber, Space, Tag, Button, Tooltip, Segmented, Tabs } from 'antd';
import {
    TableOutlined, CodeOutlined, PlayCircleOutlined, ReloadOutlined,
    ThunderboltOutlined, AppstoreOutlined, BuildOutlined,
    BarChartOutlined, SaveOutlined, HistoryOutlined,
    ColumnWidthOutlined, ApartmentOutlined,
} from '@ant-design/icons';
import { useSQL } from '../../hooks/useSQL';
import TableExplorer from './TableExplorer';
import ColumnSelector from './ColumnSelector';
import FilterBuilder from './FilterBuilder';
import AggregationBuilder from './AggregationBuilder';
import JoinBuilder from './JoinBuilder';
import SortBuilder from './SortBuilder';
import WindowBuilder from './WindowBuilder';
import SQLEditor from './SQLEditor';
import ResultsGrid from './ResultsGrid';
import NLQueryPanel from './NLQueryPanel';
import TemplateLibrary from './TemplateLibrary';
import AutoVizPanel from './AutoVizPanel';
import QueryLibrary from './QueryLibrary';
import QueryHistory from './QueryHistory';
import type { HistoryEntry } from './QueryHistory';
import QueryComparison from './QueryComparison';
import ExplainPanel from './ExplainPanel';
import type { ColumnSpec, QueryResult } from '../../types/sql';
import * as sqlApi from '../../api/sql';

const QueryWorkbench: React.FC = () => {
    const {
        state, loadColumns, updateQuery,
        setFromTable, setMode, setRawSQL, generateSQL,
        execute, exportResults, resetQuery,
    } = useSQL();

    const [activeTab, setActiveTab] = useState<string>('visual');
    const [bottomTab, setBottomTab] = useState('results');
    const [queryHistory, setQueryHistory] = useState<HistoryEntry[]>([]);
    const [externalResult, setExternalResult] = useState<QueryResult | null>(null);

    const fromTable = typeof state.querySpec.from === 'string'
        ? state.querySpec.from
        : state.querySpec.from?.table || '';

    const currentColumns = state.columnsCache[fromTable] || [];

    // Use external result if available, else state result
    const activeResult = externalResult || state.result;

    // Sync mode with tab
    useEffect(() => {
        if (activeTab === 'raw') setMode('raw');
        else setMode('visual');
    }, [activeTab, setMode]);

    // Auto-generate SQL when query spec changes
    useEffect(() => {
        if (activeTab === 'visual' && fromTable) {
            const timer = setTimeout(() => generateSQL(), 300);
            return () => clearTimeout(timer);
        }
    }, [state.querySpec, activeTab, fromTable]);

    // Record to history when result changes
    useEffect(() => {
        if (state.result?.sql) {
            const entry: HistoryEntry = {
                id: Date.now().toString(36),
                sql: state.result.sql,
                timestamp: Date.now(),
                rowCount: state.result.row_count || 0,
                executionTime: state.result.execution_time_s || 0,
                success: state.result.success,
                error: state.result.error,
            };
            setQueryHistory(prev => [entry, ...prev.slice(0, 99)]);
        }
    }, [state.result]);

    // Extract window columns from selectedColumns
    const regularColumns = (state.querySpec.columns || []).filter(c => !c.window);
    const windowColumns = (state.querySpec.columns || []).filter(c => c.window);
    const measures = (state.querySpec.columns || []).filter(c => c.aggregate);

    const updateColumns = (cols: ColumnSpec[]) => {
        updateQuery({ columns: [...cols.filter(c => !c.window), ...windowColumns] });
    };

    const updateWindowColumns = (winCols: ColumnSpec[]) => {
        updateQuery({ columns: [...regularColumns, ...winCols] });
    };

    const updateMeasures = (newMeasures: ColumnSpec[]) => {
        const nonAgg = (state.querySpec.columns || []).filter(c => !c.aggregate);
        updateQuery({ columns: [...nonAgg, ...newMeasures] });
    };

    // NL and Template result handlers
    const handleExternalResult = useCallback((result: QueryResult) => {
        setExternalResult(result);
        // Also record to history
        if (result.sql) {
            setQueryHistory(prev => [{
                id: Date.now().toString(36),
                sql: result.sql || '',
                timestamp: Date.now(),
                rowCount: result.row_count || 0,
                executionTime: result.execution_time_s || 0,
                success: result.success,
                error: result.error,
            }, ...prev.slice(0, 99)]);
        }
    }, []);

    // Clear external result when switching to visual/raw tabs
    useEffect(() => {
        if (activeTab === 'visual' || activeTab === 'raw') {
            setExternalResult(null);
        }
    }, [activeTab]);

    const handleHistoryRerun = useCallback(async (sql: string) => {
        const result = await sqlApi.executeQuery({ sql });
        setExternalResult(result);
    }, []);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 0, height: '100%', minHeight: '80vh' }}>
            {/* Header */}
            <div
                className="glass-panel"
                style={{
                    padding: '12px 20px',
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    borderRadius: '12px 12px 0 0',
                    marginBottom: 1,
                }}
            >
                <Space>
                    <CodeOutlined style={{ color: '#6366f1', fontSize: 18 }} />
                    <span style={{ fontWeight: 700, fontSize: 16 }}>SQL Query Engine</span>
                    <Tag color="purple" style={{ fontSize: 10 }}>DuckDB</Tag>
                </Space>
                <Space size={8}>
                    <Segmented
                        size="small"
                        value={activeTab}
                        onChange={v => setActiveTab(v as string)}
                        options={[
                            { value: 'visual', label: <span><BuildOutlined /> Visual</span> },
                            { value: 'raw', label: <span><CodeOutlined /> Raw SQL</span> },
                            { value: 'nl', label: <span><ThunderboltOutlined /> Ask AI</span> },
                            { value: 'templates', label: <span><AppstoreOutlined /> Templates</span> },
                        ]}
                    />
                    <Tooltip title="Reset query">
                        <Button size="small" icon={<ReloadOutlined />} onClick={resetQuery} />
                    </Tooltip>
                    {(activeTab === 'visual' || activeTab === 'raw') && (
                        <Button
                            type="primary"
                            icon={<PlayCircleOutlined />}
                            onClick={execute}
                            loading={state.loading}
                        >
                            Execute
                        </Button>
                    )}
                </Space>
            </div>

            {/* Main body */}
            <div style={{ display: 'flex', flex: 1, gap: 1, minHeight: 400 }}>
                {/* Left: Table Explorer */}
                <div
                    className="glass-panel"
                    style={{
                        width: 260, minWidth: 220, borderRadius: '0 0 0 12px',
                        overflow: 'hidden',
                    }}
                >
                    <TableExplorer
                        tables={state.tables}
                        columnsCache={state.columnsCache}
                        onLoadColumns={loadColumns}
                        onSelectTable={setFromTable}
                        selectedTable={fromTable}
                    />
                </div>

                {/* Center: Builder / Editor */}
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 1 }}>
                    <div
                        className="glass-panel"
                        style={{
                            flex: 1, overflow: 'auto',
                            padding: 12,
                            borderRadius: 0,
                        }}
                    >
                        {activeTab === 'raw' && (
                            <SQLEditor
                                rawSQL={state.rawSQL}
                                generatedSQL={state.generatedSQL}
                                onRawSQLChange={setRawSQL}
                                onExecute={execute}
                                onGenerateSQL={generateSQL}
                                loading={state.loading}
                                mode="raw"
                            />
                        )}

                        {activeTab === 'nl' && (
                            <NLQueryPanel
                                onResultReady={handleExternalResult}
                                onSQLGenerated={sql => setRawSQL(sql)}
                            />
                        )}

                        {activeTab === 'templates' && (
                            <TemplateLibrary
                                tableName={fromTable}
                                onResultReady={handleExternalResult}
                                onSQLGenerated={sql => setRawSQL(sql)}
                            />
                        )}

                        {activeTab === 'visual' && (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                                {!fromTable ? (
                                    <div style={{
                                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                                        height: 200, color: 'rgba(255,255,255,0.3)', fontSize: 14,
                                    }}>
                                        <Space direction="vertical" align="center">
                                            <TableOutlined style={{ fontSize: 32 }} />
                                            <span>Select a table from the left panel to start building your query.</span>
                                        </Space>
                                    </div>
                                ) : (
                                    <>
                                        {/* FROM indicator */}
                                        <div style={{
                                            display: 'flex', alignItems: 'center', gap: 8,
                                            padding: '6px 12px', background: 'rgba(99,102,241,0.06)',
                                            borderRadius: 8, fontSize: 12,
                                        }}>
                                            <Tag color="purple">FROM</Tag>
                                            <strong>{fromTable}</strong>
                                            <span style={{ color: 'rgba(255,255,255,0.4)', marginLeft: 'auto' }}>
                                                {state.columnsCache[fromTable]?.length || 0} columns
                                            </span>
                                        </div>

                                        <ColumnSelector
                                            columns={currentColumns}
                                            selectedColumns={regularColumns.filter(c => !c.aggregate)}
                                            onChange={updateColumns}
                                            tableName={fromTable}
                                        />

                                        {state.tables.length >= 2 && (
                                            <JoinBuilder
                                                tables={state.tables}
                                                columnsCache={state.columnsCache}
                                                joins={state.querySpec.joins || []}
                                                onChange={j => updateQuery({ joins: j })}
                                                currentTable={fromTable}
                                            />
                                        )}

                                        <FilterBuilder
                                            columns={currentColumns}
                                            conditions={state.querySpec.where || []}
                                            onChange={c => updateQuery({ where: c })}
                                            tableName={fromTable}
                                            label="WHERE"
                                        />

                                        <AggregationBuilder
                                            columns={currentColumns}
                                            groupBy={state.querySpec.group_by || []}
                                            measures={measures}
                                            onGroupByChange={gb => updateQuery({ group_by: gb })}
                                            onMeasuresChange={updateMeasures}
                                        />

                                        {(state.querySpec.group_by || []).length > 0 && (
                                            <FilterBuilder
                                                columns={currentColumns}
                                                conditions={state.querySpec.having || []}
                                                onChange={c => updateQuery({ having: c })}
                                                tableName={fromTable}
                                                label="HAVING"
                                            />
                                        )}

                                        <WindowBuilder
                                            columns={currentColumns}
                                            windowColumns={windowColumns}
                                            onChange={updateWindowColumns}
                                        />

                                        <SortBuilder
                                            columns={currentColumns}
                                            orderBy={state.querySpec.order_by || []}
                                            onChange={ob => updateQuery({ order_by: ob })}
                                        />

                                        <div className="glass-panel" style={{ padding: 12 }}>
                                            <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                                                <span style={{ fontWeight: 600, fontSize: 13 }}>LIMIT</span>
                                                <InputNumber
                                                    size="small"
                                                    min={1}
                                                    value={state.querySpec.limit ?? 100}
                                                    onChange={v => updateQuery({ limit: v })}
                                                    style={{ width: 80 }}
                                                />
                                                <span style={{ fontWeight: 600, fontSize: 13 }}>OFFSET</span>
                                                <InputNumber
                                                    size="small"
                                                    min={0}
                                                    value={state.querySpec.offset || 0}
                                                    onChange={v => updateQuery({ offset: v || 0 })}
                                                    style={{ width: 80 }}
                                                />
                                                <Button
                                                    size="small"
                                                    type="link"
                                                    onClick={() => updateQuery({ limit: null })}
                                                    danger
                                                >
                                                    Show All
                                                </Button>
                                            </div>
                                        </div>

                                        <SQLEditor
                                            rawSQL={state.rawSQL}
                                            generatedSQL={state.generatedSQL}
                                            onRawSQLChange={setRawSQL}
                                            onExecute={execute}
                                            onGenerateSQL={generateSQL}
                                            loading={state.loading}
                                            mode="visual"
                                        />
                                    </>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* Bottom: Tabbed panel (Results + AutoViz + Library + History + Compare + Explain) */}
            <div style={{ marginTop: 1 }}>
                <Tabs
                    activeKey={bottomTab}
                    onChange={setBottomTab}
                    size="small"
                    tabBarStyle={{ margin: 0, padding: '0 16px' }}
                    items={[
                        {
                            key: 'results',
                            label: <span><TableOutlined /> Results</span>,
                            children: (
                                <ResultsGrid
                                    result={activeResult}
                                    onExport={exportResults}
                                    loading={state.loading}
                                />
                            ),
                        },
                        {
                            key: 'viz',
                            label: <span><BarChartOutlined /> Auto-Viz</span>,
                            children: <AutoVizPanel result={activeResult} />,
                        },
                        {
                            key: 'library',
                            label: <span><SaveOutlined /> Library</span>,
                            children: (
                                <div className="glass-panel" style={{ padding: 12 }}>
                                    <QueryLibrary
                                        currentSQL={state.rawSQL || state.generatedSQL || ''}
                                        onLoadQuery={sql => setRawSQL(sql)}
                                        onExecuteQuery={handleHistoryRerun}
                                    />
                                </div>
                            ),
                        },
                        {
                            key: 'history',
                            label: <span><HistoryOutlined /> History</span>,
                            children: (
                                <div className="glass-panel" style={{ padding: 12 }}>
                                    <QueryHistory
                                        history={queryHistory}
                                        onRerun={handleHistoryRerun}
                                        onLoad={sql => setRawSQL(sql)}
                                        onClear={() => setQueryHistory([])}
                                    />
                                </div>
                            ),
                        },
                        {
                            key: 'compare',
                            label: <span><ColumnWidthOutlined /> Compare</span>,
                            children: (
                                <div className="glass-panel" style={{ padding: 12 }}>
                                    <QueryComparison
                                        initialSQL={state.rawSQL || state.generatedSQL || ''}
                                    />
                                </div>
                            ),
                        },
                        {
                            key: 'explain',
                            label: <span><ApartmentOutlined /> Explain</span>,
                            children: (
                                <div className="glass-panel" style={{ padding: 12 }}>
                                    <ExplainPanel />
                                </div>
                            ),
                        },
                    ]}
                />
            </div>
        </div>
    );
};

export default QueryWorkbench;
