/**
 * AggregationBuilder â€” GROUP BY + aggregate measures builder.
 */

import React from 'react';
import { Select, Button, Input } from 'antd';
import { PlusOutlined, DeleteOutlined } from '@ant-design/icons';
import type { ColumnInfo, ColumnSpec } from '../../types/sql';
import { AGGREGATE_FUNCTIONS } from '../../types/sql';

interface Props {
    columns: ColumnInfo[];
    groupBy: string[];
    measures: ColumnSpec[];
    onGroupByChange: (groupBy: string[]) => void;
    onMeasuresChange: (measures: ColumnSpec[]) => void;
}

const AggregationBuilder: React.FC<Props> = ({
    columns, groupBy, measures, onGroupByChange, onMeasuresChange,
}) => {
    const addMeasure = () => {
        onMeasuresChange([...measures, { aggregate: 'COUNT', column: '*', alias: '' }]);
    };

    const updateMeasure = (index: number, updates: Partial<ColumnSpec>) => {
        const updated = [...measures];
        updated[index] = { ...updated[index], ...updates };
        onMeasuresChange(updated);
    };

    const removeMeasure = (index: number) => {
        onMeasuresChange(measures.filter((_, i) => i !== index));
    };

    const colOptions = columns.map(c => ({ value: c.name, label: c.name }));

    return (
        <div className="glass-panel" style={{ padding: 12 }}>
            <span style={{ fontWeight: 600, fontSize: 13, display: 'block', marginBottom: 8 }}>
                GROUP BY & Aggregates
            </span>

            {/* Group By */}
            <div style={{ marginBottom: 10 }}>
                <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', marginBottom: 4 }}>Group By Columns</div>
                <Select
                    mode="multiple"
                    size="small"
                    placeholder="Drag columns here to group by..."
                    value={groupBy}
                    onChange={onGroupByChange}
                    style={{ width: '100%' }}
                    options={colOptions}
                />
            </div>

            {/* Measures */}
            <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                    <span style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)' }}>Measures</span>
                    <Button size="small" icon={<PlusOutlined />} onClick={addMeasure} type="dashed">
                        Add
                    </Button>
                </div>
                {measures.map((m, i) => (
                    <div
                        key={i}
                        style={{
                            display: 'flex', alignItems: 'center', gap: 6, padding: '4px 0',
                            flexWrap: 'wrap',
                        }}
                    >
                        <Select
                            size="small"
                            value={m.aggregate}
                            onChange={v => updateMeasure(i, { aggregate: v })}
                            style={{ width: 130 }}
                            options={AGGREGATE_FUNCTIONS.map(f => ({ value: f, label: f }))}
                        />
                        <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11 }}>(</span>
                        <Select
                            size="small"
                            value={m.column || '*'}
                            onChange={v => updateMeasure(i, { column: v })}
                            style={{ width: 120 }}
                            options={[
                                { value: '*', label: '*' },
                                ...colOptions,
                            ]}
                        />
                        <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11 }}>)</span>
                        <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11 }}>AS</span>
                        <Input
                            size="small"
                            placeholder="alias"
                            value={m.alias || ''}
                            onChange={e => updateMeasure(i, { alias: e.target.value })}
                            style={{ width: 100 }}
                        />
                        <DeleteOutlined
                            style={{ color: 'rgba(255,100,100,0.5)', cursor: 'pointer', fontSize: 11 }}
                            onClick={() => removeMeasure(i)}
                        />
                    </div>
                ))}
                {measures.length === 0 && (
                    <div style={{ color: 'rgba(255,255,255,0.3)', fontSize: 12, padding: '4px 0' }}>
                        No measures. Add COUNT, SUM, AVG, etc.
                    </div>
                )}
            </div>
        </div>
    );
};

export default AggregationBuilder;

