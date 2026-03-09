/**
 * LiveDataGrid — Excel-like interactive data grid using AG Grid.
 * Full-featured: sort, filter, pin, resize, reorder, context menus,
 * conditional formatting, cell quality indicators, inline editing.
 */

import React, { useState, useMemo, useCallback, useRef } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, type ColDef, type GridReadyEvent } from 'ag-grid-community';
import { Input, Button, Space, Tag, Badge, Tooltip, Modal, Select, InputNumber } from 'antd';
import {
    SearchOutlined, FilterOutlined, ClearOutlined, EyeInvisibleOutlined,
    DownloadOutlined, BarChartOutlined
} from '@ant-design/icons';

import { ClientSideRowModelModule } from 'ag-grid-community';

ModuleRegistry.registerModules([AllCommunityModule, ClientSideRowModelModule]);

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

interface CellQuality {
    row: number;
    col: string;
    type: 'imputed' | 'outlier' | 'modified';
    detail: string;
}

interface ConditionalRule {
    id: string;
    column: string;
    operator: 'gt' | 'lt' | 'eq' | 'neq' | 'contains' | 'between';
    value: any;
    value2?: any;
    bgColor: string;
    textColor: string;
}

interface Props {
    data: any[];
    columns: string[];
    columnTypes?: string[];
    qualityFlags?: CellQuality[];
    onCellEdit?: (row: number, col: string, oldVal: any, newVal: any) => void;
    editable?: boolean;
}

const LiveDataGrid: React.FC<Props> = ({
    data, columns, columnTypes, qualityFlags = [], onCellEdit, editable = false,
}) => {
    const gridRef = useRef<AgGridReact>(null);
    const [searchText, setSearchText] = useState('');
    const [activeFilters, setActiveFilters] = useState(0);
    const [showColPanel, setShowColPanel] = useState(false);
    const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
    const [pageSize, setPageSize] = useState(100);
    const [jumpRow, setJumpRow] = useState<number | null>(null);
    const [_rowNotes, setRowNotes] = useState<Record<number, string>>({});
    const [noteModal, setNoteModal] = useState<{ visible: boolean; rowIndex: number }>({ visible: false, rowIndex: 0 });
    const [noteText, setNoteText] = useState('');
    const [condRules, setCondRules] = useState<ConditionalRule[]>([]);
    const [showRuleModal, setShowRuleModal] = useState(false);
    const [newRule, setNewRule] = useState<Partial<ConditionalRule>>({});
    const [miniStats, setMiniStats] = useState<{ col: string; stats: any } | null>(null);

    // Build quality flag lookup
    const qualityMap = useMemo(() => {
        const m: Record<string, CellQuality> = {};
        qualityFlags.forEach(q => { m[`${q.row}-${q.col}`] = q; });
        return m;
    }, [qualityFlags]);

    // Cell style based on conditional formatting + quality flags
    const getCellStyle = useCallback((params: any) => {
        const style: any = {};
        const col = params.colDef?.field;
        const rowIdx = params.node?.rowIndex;

        // Quality indicator
        const qKey = `${rowIdx}-${col}`;
        if (qualityMap[qKey]) {
            const q = qualityMap[qKey];
            const dotColors = { imputed: '#3b82f6', outlier: '#ef4444', modified: '#f59e0b' };
            style.borderLeft = `3px solid ${dotColors[q.type]}`;
        }

        // Conditional formatting rules
        condRules.forEach(rule => {
            if (rule.column !== col) return;
            const val = params.value;
            let match = false;
            switch (rule.operator) {
                case 'gt': match = typeof val === 'number' && val > rule.value; break;
                case 'lt': match = typeof val === 'number' && val < rule.value; break;
                case 'eq': match = val == rule.value; break;
                case 'neq': match = val != rule.value; break;
                case 'contains': match = String(val).toLowerCase().includes(String(rule.value).toLowerCase()); break;
                case 'between': match = typeof val === 'number' && val >= rule.value && val <= rule.value2; break;
            }
            if (match) {
                style.backgroundColor = rule.bgColor;
                style.color = rule.textColor;
            }
        });

        return style;
    }, [qualityMap, condRules]);

    // Column definitions
    const columnDefs: ColDef[] = useMemo(() => {
        return columns.filter(c => !hiddenCols.has(c)).map((col, i) => ({
            field: col,
            headerName: col,
            sortable: true,
            filter: true,
            resizable: true,
            editable: editable,
            minWidth: 100,
            headerTooltip: columnTypes?.[i] ? `${col} (${columnTypes[i]})` : col,
            cellStyle: getCellStyle,
            tooltipValueGetter: (params: any) => {
                const qKey = `${params.node?.rowIndex}-${col}`;
                const q = qualityMap[qKey];
                return q ? `[${q.type.toUpperCase()}] ${q.detail}` : undefined;
            },
        }));
    }, [columns, columnTypes, hiddenCols, editable, getCellStyle, qualityMap]);

    // Row data with search highlight
    const rowData = useMemo(() => {
        if (!searchText.trim()) return data;
        const lower = searchText.toLowerCase();
        return data.filter(row =>
            columns.some(col => String(row[col] ?? '').toLowerCase().includes(lower))
        );
    }, [data, columns, searchText]);

    const onGridReady = useCallback((params: GridReadyEvent) => {
        params.api.sizeColumnsToFit();
    }, []);

    const handleFilterChanged = useCallback(() => {
        if (!gridRef.current?.api) return;
        const api = gridRef.current.api as any;
        const model = api.getFilterModel();
        setActiveFilters(Object.keys(model).length);
    }, []);

    const clearAllFilters = useCallback(() => {
        const api = gridRef.current?.api as any;
        api?.setFilterModel(null);
        setSearchText('');
        setActiveFilters(0);
    }, []);

    const handleJumpToRow = useCallback(() => {
        if (jumpRow !== null && gridRef.current?.api) {
            gridRef.current.api.ensureIndexVisible(jumpRow, 'middle');
        }
    }, [jumpRow]);

    const handleCellEdit = useCallback((event: any) => {
        if (onCellEdit && event.oldValue !== event.newValue) {
            onCellEdit(event.rowIndex, event.colDef.field, event.oldValue, event.newValue);
        }
    }, [onCellEdit]);

    const exportCurrentView = useCallback((format: 'csv' | 'excel') => {
        if (format === 'csv') {
            const api = gridRef.current?.api as any;
            api?.exportDataAsCsv({ fileName: 'grid_export.csv' });
        }
    }, []);

    const fetchColumnStats = useCallback(async (col: string) => {
        try {
            const res = await fetch(`${API_BASE}/api/grid/column-stats?column=${encodeURIComponent(col)}`);
            const stats = await res.json();
            setMiniStats({ col, stats });
        } catch { setMiniStats(null); }
    }, []);

    const handleAddNote = useCallback(() => {
        setRowNotes(prev => ({ ...prev, [noteModal.rowIndex]: noteText }));
        setNoteModal({ visible: false, rowIndex: 0 });
        setNoteText('');
    }, [noteModal, noteText]);

    const handleAddRule = useCallback(() => {
        if (!newRule.column || !newRule.operator) return;
        const rule: ConditionalRule = {
            id: Date.now().toString(),
            column: newRule.column || '',
            operator: (newRule.operator as any) || 'gt',
            value: newRule.value ?? 0,
            value2: newRule.value2,
            bgColor: newRule.bgColor || '#22c55e30',
            textColor: newRule.textColor || 'inherit',
        };
        setCondRules(prev => [...prev, rule]);
        setShowRuleModal(false);
        setNewRule({});
    }, [newRule]);



    const defaultColDef: ColDef = useMemo(() => ({
        sortable: true,
        filter: true,
        resizable: true,
        floatingFilter: true,
        minWidth: 80,
    }), []);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {/* Toolbar */}
            <div className="glass-panel" style={{
                padding: '8px 12px', display: 'flex', justifyContent: 'space-between',
                alignItems: 'center', flexWrap: 'wrap', gap: 8,
            }}>
                <Space size={8}>
                    <Input
                        prefix={<SearchOutlined />}
                        placeholder="Search all columns..."
                        value={searchText}
                        onChange={e => setSearchText(e.target.value)}
                        style={{ width: 220 }}
                        size="small"
                        allowClear
                        aria-label="Global search"
                    />
                    <Badge count={activeFilters} size="small" offset={[-4, 0]}>
                        <Tag color={activeFilters > 0 ? 'blue' : undefined} style={{ cursor: 'pointer', margin: 0 }}>
                            <FilterOutlined /> {activeFilters > 0 ? `${activeFilters} filters` : 'Filters'}
                        </Tag>
                    </Badge>
                    {activeFilters > 0 && (
                        <Button size="small" icon={<ClearOutlined />} onClick={clearAllFilters} className="cancel-btn">
                            Clear All
                        </Button>
                    )}
                </Space>
                <Space size={4}>
                    <InputNumber
                        size="small"
                        min={1} max={data.length}
                        placeholder="Jump to row"
                        value={jumpRow}
                        onChange={v => setJumpRow(v)}
                        onPressEnter={handleJumpToRow}
                        style={{ width: 110 }}
                    />
                    <Tooltip title="Column visibility">
                        <Button size="small" icon={<EyeInvisibleOutlined />}
                            onClick={() => setShowColPanel(!showColPanel)}
                            type={showColPanel ? 'primary' : 'default'}
                        />
                    </Tooltip>
                    <Tooltip title="Conditional formatting">
                        <Button size="small" icon={<BarChartOutlined />}
                            onClick={() => setShowRuleModal(true)}
                        />
                    </Tooltip>
                    <Select size="small" value={pageSize} onChange={setPageSize} style={{ width: 80 }}
                        options={[
                            { value: 25, label: '25' }, { value: 50, label: '50' },
                            { value: 100, label: '100' }, { value: 500, label: '500' },
                            { value: data.length, label: 'All' },
                        ]}
                    />
                    <Button size="small" icon={<DownloadOutlined />} onClick={() => exportCurrentView('csv')}>
                        Export
                    </Button>
                </Space>
            </div>

            {/* Column visibility panel */}
            {showColPanel && (
                <div className="glass-panel" style={{
                    padding: '8px 12px', display: 'flex', flexWrap: 'wrap', gap: 4,
                }}>
                    {columns.map(col => (
                        <Tag
                            key={col}
                            color={hiddenCols.has(col) ? undefined : 'blue'}
                            style={{ cursor: 'pointer', fontSize: 11 }}
                            onClick={() => {
                                setHiddenCols(prev => {
                                    const next = new Set(prev);
                                    next.has(col) ? next.delete(col) : next.add(col);
                                    return next;
                                });
                            }}
                        >
                            {hiddenCols.has(col) ? <EyeInvisibleOutlined /> : null} {col}
                        </Tag>
                    ))}
                </div>
            )}

            {/* Mini stats panel */}
            {miniStats && (
                <div className="glass-panel" style={{ padding: 12 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                        <strong>{miniStats.col} — Quick Stats</strong>
                        <Button size="small" onClick={() => setMiniStats(null)}>✕</Button>
                    </div>
                    <Space wrap>
                        {Object.entries(miniStats.stats).map(([k, v]) => (
                            <Tag key={k}>
                                <strong>{k}:</strong> {typeof v === 'number' ? (v as number).toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(v)}
                            </Tag>
                        ))}
                    </Space>
                </div>
            )}

            {/* Conditional formatting rules */}
            {condRules.length > 0 && (
                <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', padding: '0 4px' }}>
                    {condRules.map(rule => (
                        <Tag
                            key={rule.id}
                            closable
                            onClose={() => setCondRules(prev => prev.filter(r => r.id !== rule.id))}
                            style={{
                                fontSize: 10,
                                background: rule.bgColor,
                                color: rule.textColor,
                            }}
                        >
                            {rule.column} {rule.operator} {rule.value}
                        </Tag>
                    ))}
                </div>
            )}

            {/* AG Grid */}
            <div style={{ height: 'calc(100vh - 320px)', minHeight: 400 }}>
                <AgGridReact
                    ref={gridRef}
                    rowData={rowData}
                    columnDefs={columnDefs}
                    defaultColDef={defaultColDef}
                    pagination={true}
                    paginationPageSize={pageSize}
                    paginationPageSizeSelector={false}
                    onGridReady={onGridReady}
                    onFilterChanged={handleFilterChanged}
                    onCellValueChanged={handleCellEdit}
                    rowSelection={{ mode: "multiRow" }}
                    onColumnHeaderClicked={(e: any) => fetchColumnStats(e.column.getColId())}

                    enableCellTextSelection={true}
                    animateRows={true}
                    suppressMenuHide={true}
                    tooltipShowDelay={300}
                    theme="legacy"
                    className="ag-theme-alpine-dark custom-ag-grid"
                />
            </div>

            {/* Row Note Modal */}
            <Modal
                title={`Note for Row ${noteModal.rowIndex}`}
                open={noteModal.visible}
                onOk={handleAddNote}
                onCancel={() => setNoteModal({ visible: false, rowIndex: 0 })}
                width={400}
            >
                <Input.TextArea
                    value={noteText}
                    onChange={e => setNoteText(e.target.value)}
                    rows={3}
                    placeholder="Add a note to this row..."
                />
            </Modal>

            {/* Conditional Formatting Rule Modal */}
            <Modal
                title="Add Conditional Formatting Rule"
                open={showRuleModal}
                onOk={handleAddRule}
                onCancel={() => setShowRuleModal(false)}
                width={500}
            >
                <Space direction="vertical" style={{ width: '100%' }}>
                    <Select
                        placeholder="Column"
                        style={{ width: '100%' }}
                        value={newRule.column}
                        onChange={v => setNewRule(p => ({ ...p, column: v }))}
                        options={columns.map(c => ({ value: c, label: c }))}
                    />
                    <Select
                        placeholder="Condition"
                        style={{ width: '100%' }}
                        value={newRule.operator}
                        onChange={v => setNewRule(p => ({ ...p, operator: v }))}
                        options={[
                            { value: 'gt', label: 'Greater than (>)' },
                            { value: 'lt', label: 'Less than (<)' },
                            { value: 'eq', label: 'Equals (=)' },
                            { value: 'neq', label: 'Not equals (≠)' },
                            { value: 'contains', label: 'Contains' },
                            { value: 'between', label: 'Between' },
                        ]}
                    />
                    <InputNumber
                        placeholder="Value"
                        style={{ width: '100%' }}
                        value={newRule.value}
                        onChange={v => setNewRule(p => ({ ...p, value: v }))}
                    />
                    {newRule.operator === 'between' && (
                        <InputNumber
                            placeholder="Value 2"
                            style={{ width: '100%' }}
                            value={newRule.value2}
                            onChange={v => setNewRule(p => ({ ...p, value2: v }))}
                        />
                    )}
                    <Space>
                        <span>BG Color:</span>
                        <input type="color" value={newRule.bgColor || '#22c55e'}
                            onChange={e => setNewRule(p => ({ ...p, bgColor: e.target.value + '30' }))} />
                        <span>Text Color:</span>
                        <input type="color" value={newRule.textColor || '#ffffff'}
                            onChange={e => setNewRule(p => ({ ...p, textColor: e.target.value }))} />
                    </Space>
                </Space>
            </Modal>
        </div>
    );
};

export default LiveDataGrid;

