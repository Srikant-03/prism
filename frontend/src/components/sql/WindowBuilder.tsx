/**
 * WindowBuilder — Window function builder (ROW_NUMBER, RANK, LAG, etc.)
 */

import React from 'react';
import { Select, Button, Input, InputNumber } from 'antd';
import { PlusOutlined, DeleteOutlined, BarChartOutlined } from '@ant-design/icons';
import type { ColumnSpec, ColumnInfo, WindowSpec } from '../../types/sql';
import { WINDOW_FUNCTIONS } from '../../types/sql';

interface Props {
    columns: ColumnInfo[];
    windowColumns: ColumnSpec[];
    onChange: (cols: ColumnSpec[]) => void;
}

const FRAME_OPTIONS = [
    { value: '', label: 'Default' },
    { value: 'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW', label: 'Running Total' },
    { value: 'ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING', label: 'Moving Avg (±2)' },
    { value: 'ROWS BETWEEN 6 PRECEDING AND CURRENT ROW', label: 'Last 7 Rows' },
    { value: 'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING', label: 'Entire Partition' },
    { value: 'RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW', label: 'Range Running' },
];

const WindowBuilder: React.FC<Props> = ({ columns, windowColumns, onChange }) => {
    const colOptions = columns.map(c => ({ value: c.name, label: c.name }));

    const addWindow = () => {
        onChange([...windowColumns, {
            window: {
                func: 'ROW_NUMBER',
                partition_by: [],
                order_by: [],
            },
            alias: `win_${windowColumns.length + 1}`,
        }]);
    };

    const updateWindow = (index: number, winUpdates: Partial<WindowSpec>) => {
        const updated = [...windowColumns];
        updated[index] = {
            ...updated[index],
            window: { ...updated[index].window!, ...winUpdates },
        };
        onChange(updated);
    };

    const updateAlias = (index: number, alias: string) => {
        const updated = [...windowColumns];
        updated[index] = { ...updated[index], alias };
        onChange(updated);
    };

    const remove = (index: number) => {
        onChange(windowColumns.filter((_, i) => i !== index));
    };

    const needsColumn = (func: string) =>
        ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE'].includes(func);

    const needsOffset = (func: string) =>
        ['LAG', 'LEAD'].includes(func);

    return (
        <div className="glass-panel" style={{ padding: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <span style={{ fontWeight: 600, fontSize: 13 }}>
                    <BarChartOutlined style={{ marginRight: 6 }} />
                    Window Functions
                </span>
                <Button size="small" icon={<PlusOutlined />} onClick={addWindow} type="dashed">
                    Add
                </Button>
            </div>

            {windowColumns.map((wc, i) => {
                const win = wc.window!;
                return (
                    <div
                        key={i}
                        style={{
                            border: '1px solid rgba(99,102,241,0.15)',
                            borderRadius: 8, padding: 10, marginBottom: 8,
                            background: 'rgba(99,102,241,0.03)',
                        }}
                    >
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 6 }}>
                            {/* Function */}
                            <Select
                                size="small"
                                value={win.func}
                                onChange={v => updateWindow(i, { func: v })}
                                style={{ width: 130 }}
                                options={WINDOW_FUNCTIONS.map(f => ({ value: f, label: f }))}
                            />
                            {/* Column */}
                            {needsColumn(win.func) && (
                                <>
                                    <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11, alignSelf: 'center' }}>(</span>
                                    <Select
                                        size="small"
                                        value={win.column || undefined}
                                        onChange={v => updateWindow(i, { column: v })}
                                        style={{ width: 100 }}
                                        placeholder="Column"
                                        options={colOptions}
                                    />
                                    <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11, alignSelf: 'center' }}>)</span>
                                </>
                            )}
                            {/* LAG/LEAD offset */}
                            {needsOffset(win.func) && (
                                <InputNumber
                                    size="small"
                                    min={1}
                                    value={win.offset || 1}
                                    onChange={v => updateWindow(i, { offset: v || 1 })}
                                    style={{ width: 60 }}
                                    addonBefore="±"
                                />
                            )}
                            <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11, alignSelf: 'center' }}>AS</span>
                            <Input
                                size="small"
                                value={wc.alias || ''}
                                onChange={e => updateAlias(i, e.target.value)}
                                style={{ width: 100 }}
                                placeholder="alias"
                            />
                            <DeleteOutlined
                                style={{ marginLeft: 'auto', color: 'rgba(255,100,100,0.5)', cursor: 'pointer' }}
                                onClick={() => remove(i)}
                            />
                        </div>

                        {/* OVER clause */}
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                            <div style={{ flex: 1, minWidth: 140 }}>
                                <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', marginBottom: 2 }}>PARTITION BY</div>
                                <Select
                                    mode="multiple"
                                    size="small"
                                    value={win.partition_by || []}
                                    onChange={v => updateWindow(i, { partition_by: v })}
                                    style={{ width: '100%' }}
                                    placeholder="Partition by..."
                                    options={colOptions}
                                />
                            </div>
                            <div style={{ flex: 1, minWidth: 140 }}>
                                <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', marginBottom: 2 }}>ORDER BY</div>
                                <Select
                                    mode="multiple"
                                    size="small"
                                    value={(win.order_by || []).map(o => typeof o === 'string' ? o : o.column)}
                                    onChange={v => updateWindow(i, { order_by: v })}
                                    style={{ width: '100%' }}
                                    placeholder="Order by..."
                                    options={colOptions}
                                />
                            </div>
                            <div style={{ minWidth: 140 }}>
                                <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', marginBottom: 2 }}>Frame</div>
                                <Select
                                    size="small"
                                    value={win.frame || ''}
                                    onChange={v => updateWindow(i, { frame: v || undefined })}
                                    style={{ width: '100%' }}
                                    options={FRAME_OPTIONS}
                                />
                            </div>
                        </div>
                    </div>
                );
            })}

            {windowColumns.length === 0 && (
                <div style={{ color: 'rgba(255,255,255,0.3)', fontSize: 12, padding: 4 }}>
                    Add ROW_NUMBER, RANK, LAG, running totals, etc.
                </div>
            )}
        </div>
    );
};

export default WindowBuilder;
