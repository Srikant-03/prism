/**
 * JoinBuilder â€” Visual join designer for multi-table queries.
 */

import React from 'react';
import { Select, Button, Tag } from 'antd';
import { PlusOutlined, DeleteOutlined, LinkOutlined } from '@ant-design/icons';
import type { JoinSpec, TableInfo, ColumnInfo } from '../../types/sql';

interface Props {
    tables: TableInfo[];
    columnsCache: Record<string, ColumnInfo[]>;
    joins: JoinSpec[];
    onChange: (joins: JoinSpec[]) => void;
    currentTable?: string;
}

const JOIN_TYPES = [
    { value: 'INNER', label: 'INNER JOIN', color: '#69b1ff' },
    { value: 'LEFT', label: 'LEFT JOIN', color: '#95de64' },
    { value: 'RIGHT', label: 'RIGHT JOIN', color: '#ffc069' },
    { value: 'FULL', label: 'FULL OUTER', color: '#b37feb' },
    { value: 'CROSS', label: 'CROSS JOIN', color: '#ff7875' },
    { value: 'SEMI', label: 'SEMI JOIN', color: '#5cdbd3' },
    { value: 'ANTI', label: 'ANTI JOIN', color: '#ff9c6e' },
];

const JoinBuilder: React.FC<Props> = ({ tables, columnsCache, joins, onChange, currentTable }) => {
    const addJoin = () => {
        onChange([...joins, { type: 'INNER', table: '', on: [{ left: '', right: '' }] }]);
    };

    const updateJoin = (index: number, updates: Partial<JoinSpec>) => {
        const updated = [...joins];
        updated[index] = { ...updated[index], ...updates } as JoinSpec;
        onChange(updated);
    };

    const removeJoin = (index: number) => {
        onChange(joins.filter((_, i) => i !== index));
    };

    const addOnCondition = (joinIdx: number) => {
        const updated = [...joins];
        updated[joinIdx] = {
            ...updated[joinIdx],
            on: [...updated[joinIdx].on, { left: '', right: '' }],
        };
        onChange(updated);
    };

    const updateOnCondition = (joinIdx: number, condIdx: number, side: 'left' | 'right', value: string) => {
        const updated = [...joins];
        const onConds = [...updated[joinIdx].on];
        onConds[condIdx] = { ...onConds[condIdx], [side]: value };
        updated[joinIdx] = { ...updated[joinIdx], on: onConds };
        onChange(updated);
    };

    const removeOnCondition = (joinIdx: number, condIdx: number) => {
        const updated = [...joins];
        updated[joinIdx] = {
            ...updated[joinIdx],
            on: updated[joinIdx].on.filter((_, i) => i !== condIdx),
        };
        onChange(updated);
    };

    const getColumnsForTable = (tableName: string) => {
        return (columnsCache[tableName] || []).map(c => ({
            value: `${tableName}.${c.name}`,
            label: `${tableName}.${c.name}`,
        }));
    };

    const currentCols = currentTable ? getColumnsForTable(currentTable) : [];

    return (
        <div className="glass-panel" style={{ padding: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <span style={{ fontWeight: 600, fontSize: 13 }}>
                    <LinkOutlined style={{ marginRight: 6 }} />
                    JOINs
                </span>
                <Button
                    size="small" icon={<PlusOutlined />} onClick={addJoin} type="dashed"
                    disabled={tables.length < 2}
                >
                    Add Join
                </Button>
            </div>

            {tables.length < 2 && (
                <div style={{ color: 'rgba(255,255,255,0.3)', fontSize: 12, padding: 8 }}>
                    Upload 2+ files to enable joins.
                </div>
            )}

            {joins.map((join, ji) => {
                const rightCols = join.table ? getColumnsForTable(join.table) : [];
                return (
                    <div
                        key={ji}
                        style={{
                            border: '1px solid rgba(99,102,241,0.15)',
                            borderRadius: 8, padding: 10, marginBottom: 8,
                            background: 'rgba(99,102,241,0.03)',
                        }}
                    >
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8, flexWrap: 'wrap' }}>
                            <Select
                                size="small"
                                value={join.type}
                                onChange={v => updateJoin(ji, { type: v as JoinSpec['type'] })}
                                style={{ width: 130 }}
                                options={JOIN_TYPES.map(j => ({
                                    value: j.value,
                                    label: <span style={{ color: j.color }}>{j.label}</span>,
                                }))}
                            />
                            <Select
                                size="small"
                                showSearch
                                value={join.table || undefined}
                                onChange={v => updateJoin(ji, { table: v })}
                                style={{ minWidth: 140 }}
                                placeholder="Select table..."
                                options={tables
                                    .filter(t => t.name !== currentTable)
                                    .map(t => ({ value: t.name, label: t.name }))
                                }
                            />
                            <DeleteOutlined
                                style={{ marginLeft: 'auto', color: 'rgba(255,100,100,0.5)', cursor: 'pointer' }}
                                onClick={() => removeJoin(ji)}
                            />
                        </div>

                        {join.type !== 'CROSS' && (
                            <>
                                <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.4)', marginBottom: 4 }}>ON</div>
                                {join.on.map((cond, ci) => (
                                    <div key={ci} style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                                        {ci > 0 && <Tag style={{ fontSize: 10 }}>AND</Tag>}
                                        <Select
                                            size="small"
                                            showSearch
                                            value={cond.left || undefined}
                                            onChange={v => updateOnCondition(ji, ci, 'left', v)}
                                            style={{ flex: 1, minWidth: 120 }}
                                            placeholder="Left column"
                                            options={currentCols}
                                        />
                                        <span style={{ color: 'rgba(255,255,255,0.4)' }}>=</span>
                                        <Select
                                            size="small"
                                            showSearch
                                            value={cond.right || undefined}
                                            onChange={v => updateOnCondition(ji, ci, 'right', v)}
                                            style={{ flex: 1, minWidth: 120 }}
                                            placeholder="Right column"
                                            options={rightCols}
                                        />
                                        {join.on.length > 1 && (
                                            <DeleteOutlined
                                                style={{ color: 'rgba(255,100,100,0.4)', cursor: 'pointer', fontSize: 11 }}
                                                onClick={() => removeOnCondition(ji, ci)}
                                            />
                                        )}
                                    </div>
                                ))}
                                <Button
                                    size="small" type="link" icon={<PlusOutlined />}
                                    onClick={() => addOnCondition(ji)}
                                    style={{ fontSize: 11, padding: 0 }}
                                >
                                    Add condition
                                </Button>
                            </>
                        )}
                    </div>
                );
            })}
        </div>
    );
};

export default JoinBuilder;

