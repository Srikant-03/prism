/**
 * FilterBuilder — Visual WHERE/HAVING clause builder.
 * Supports dynamic operators by type, AND/OR grouping, value autocomplete.
 */

import React, { useState } from 'react';
import { Space, Select, Input, Button, InputNumber, Tag } from 'antd';
import { PlusOutlined, DeleteOutlined, GroupOutlined } from '@ant-design/icons';
import type { FilterCondition, ColumnInfo } from '../../types/sql';
import { OPERATORS_BY_TYPE } from '../../types/sql';
import * as sqlApi from '../../api/sql';

interface Props {
    columns: ColumnInfo[];
    conditions: FilterCondition[];
    onChange: (conditions: FilterCondition[]) => void;
    tableName?: string;
    label?: string;
}

const FilterBuilder: React.FC<Props> = ({
    columns, conditions, onChange, tableName, label = 'WHERE',
}) => {
    const [valueCache, setValueCache] = useState<Record<string, any[]>>({});

    const loadValues = async (colName: string) => {
        if (valueCache[colName] || !tableName) return;
        const values = await sqlApi.fetchColumnValues(tableName, colName);
        setValueCache(prev => ({ ...prev, [colName]: values }));
    };

    const addCondition = () => {
        onChange([...conditions, { column: '', op: '=', value: '', logic: 'AND' }]);
    };

    const addGroup = () => {
        onChange([...conditions, { group: [{ column: '', op: '=', value: '' }], logic: 'AND' }]);
    };

    const updateCondition = (index: number, updates: Partial<FilterCondition>) => {
        const updated = [...conditions];
        updated[index] = { ...updated[index], ...updates };
        onChange(updated);
    };

    const removeCondition = (index: number) => {
        onChange(conditions.filter((_, i) => i !== index));
    };

    const getColumnType = (colName: string) => {
        const col = columns.find(c => c.name === colName);
        return col?.ui_type || 'other';
    };

    const getOperators = (colName: string) => {
        const type = getColumnType(colName);
        return OPERATORS_BY_TYPE[type] || OPERATORS_BY_TYPE.other;
    };

    const needsNoValue = (op: string) =>
        ['IS NULL', 'IS NOT NULL', 'IS TRUE', 'IS FALSE', 'THIS WEEK', 'THIS MONTH', 'THIS YEAR'].includes(op);

    const renderValueInput = (cond: FilterCondition, index: number) => {
        if (!cond.column || needsNoValue(cond.op || '')) return null;

        const colType = getColumnType(cond.column);
        const op = cond.op || '=';

        // IN / NOT IN — multi-select
        if (op === 'IN' || op === 'NOT IN') {
            const cached = valueCache[cond.column] || [];
            return (
                <Select
                    mode="tags"
                    size="small"
                    placeholder="Select values..."
                    value={cond.values || []}
                    onChange={(vals) => updateCondition(index, { values: vals })}
                    onFocus={() => loadValues(cond.column!)}
                    style={{ minWidth: 160, flex: 1 }}
                    options={cached.map((v: any) => ({ label: String(v), value: v }))}
                />
            );
        }

        // BETWEEN — two inputs
        if (op === 'BETWEEN') {
            const vals = cond.values || ['', ''];
            return (
                <Space size={4}>
                    <InputNumber
                        size="small"
                        placeholder="From"
                        value={vals[0]}
                        onChange={v => updateCondition(index, { values: [v, vals[1]] })}
                        style={{ width: 80 }}
                    />
                    <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 11 }}>AND</span>
                    <InputNumber
                        size="small"
                        placeholder="To"
                        value={vals[1]}
                        onChange={v => updateCondition(index, { values: [vals[0], v] })}
                        style={{ width: 80 }}
                    />
                </Space>
            );
        }

        // IN LAST N DAYS/MONTHS
        if (op === 'IN LAST N DAYS' || op === 'IN LAST N MONTHS') {
            return (
                <InputNumber
                    size="small"
                    min={1}
                    value={cond.value || 7}
                    onChange={v => updateCondition(index, { value: v })}
                    style={{ width: 60 }}
                    addonAfter={op.includes('DAYS') ? 'days' : 'months'}
                />
            );
        }

        // Numeric
        if (colType === 'integer' || colType === 'float') {
            return (
                <InputNumber
                    size="small"
                    value={cond.value}
                    onChange={v => updateCondition(index, { value: v })}
                    style={{ width: 120 }}
                    placeholder="Value"
                />
            );
        }

        // Categorical — show select with actual values
        if (colType === 'categorical') {
            const cached = valueCache[cond.column] || [];
            return (
                <Select
                    size="small"
                    showSearch
                    allowClear
                    value={cond.value}
                    onChange={v => updateCondition(index, { value: v })}
                    onFocus={() => loadValues(cond.column!)}
                    style={{ minWidth: 120 }}
                    options={cached.map((v: any) => ({ label: String(v), value: v }))}
                    placeholder="Select..."
                />
            );
        }

        // Default: text input
        return (
            <Input
                size="small"
                value={cond.value || ''}
                onChange={e => updateCondition(index, { value: e.target.value })}
                style={{ width: 160 }}
                placeholder="Value"
            />
        );
    };

    const renderCondition = (cond: FilterCondition, index: number, depth = 0) => {
        // Nested group
        if (cond.group) {
            return (
                <div
                    key={index}
                    style={{
                        border: '1px solid rgba(99,102,241,0.2)',
                        borderRadius: 8, padding: 8, marginBottom: 4,
                        marginLeft: depth * 12,
                        background: 'rgba(99,102,241,0.03)',
                    }}
                >
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                        {index > 0 && (
                            <Select
                                size="small"
                                value={cond.logic || 'AND'}
                                onChange={v => updateCondition(index, { logic: v })}
                                style={{ width: 70 }}
                                options={[
                                    { value: 'AND', label: 'AND' },
                                    { value: 'OR', label: 'OR' },
                                ]}
                            />
                        )}
                        <Tag color="purple" style={{ fontSize: 10 }}>GROUP</Tag>
                        <DeleteOutlined
                            style={{ marginLeft: 'auto', color: 'rgba(255,100,100,0.5)', cursor: 'pointer', fontSize: 11 }}
                            onClick={() => removeCondition(index)}
                        />
                    </div>
                    {cond.group.map((c, j) => renderCondition(c, j, depth + 1))}
                </div>
            );
        }

        return (
            <div
                key={index}
                style={{
                    display: 'flex', alignItems: 'center', gap: 6,
                    padding: '4px 0', marginLeft: depth * 12,
                    flexWrap: 'wrap',
                }}
            >
                {index > 0 && (
                    <Select
                        size="small"
                        value={cond.logic || 'AND'}
                        onChange={v => updateCondition(index, { logic: v })}
                        style={{ width: 65 }}
                        options={[
                            { value: 'AND', label: 'AND' },
                            { value: 'OR', label: 'OR' },
                        ]}
                    />
                )}
                <Select
                    size="small"
                    showSearch
                    value={cond.column || undefined}
                    onChange={v => {
                        updateCondition(index, { column: v, op: getOperators(v)[0], value: '' });
                        loadValues(v);
                    }}
                    style={{ minWidth: 120 }}
                    placeholder="Column"
                    options={columns.map(c => ({ value: c.name, label: c.name }))}
                />
                <Select
                    size="small"
                    value={cond.op || '='}
                    onChange={v => updateCondition(index, { op: v })}
                    style={{ minWidth: 90 }}
                    options={getOperators(cond.column || '').map(op => ({ value: op, label: op }))}
                />
                {renderValueInput(cond, index)}
                <DeleteOutlined
                    style={{ color: 'rgba(255,100,100,0.5)', cursor: 'pointer', fontSize: 11 }}
                    onClick={() => removeCondition(index)}
                />
            </div>
        );
    };

    return (
        <div className="glass-panel" style={{ padding: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                <span style={{ fontWeight: 600, fontSize: 13 }}>{label}</span>
                <Space size={4}>
                    <Button size="small" icon={<PlusOutlined />} onClick={addCondition} type="dashed">
                        Condition
                    </Button>
                    <Button size="small" icon={<GroupOutlined />} onClick={addGroup} type="dashed">
                        Group
                    </Button>
                </Space>
            </div>
            {conditions.length === 0 ? (
                <div style={{ color: 'rgba(255,255,255,0.3)', fontSize: 12, padding: 8 }}>
                    No filters applied. Click + to add conditions.
                </div>
            ) : (
                conditions.map((cond, i) => renderCondition(cond, i))
            )}
        </div>
    );
};

export default FilterBuilder;

