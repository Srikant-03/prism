/**
 * ColumnSelector — Checkbox column picker with aliasing and expression builder.
 */

import React, { useState } from 'react';
import { Checkbox, Input, Button, Space, Tag, Tooltip, Popover } from 'antd';
import {
    EditOutlined, FunctionOutlined,
    DeleteOutlined, HolderOutlined,
} from '@ant-design/icons';
import type { ColumnSpec, ColumnInfo } from '../../types/sql';

interface Props {
    columns: ColumnInfo[];
    selectedColumns: ColumnSpec[];
    onChange: (columns: ColumnSpec[]) => void;
    tableName?: string;
}

const expressionTemplates = [
    { label: 'Arithmetic', items: ['col_a + col_b', 'col_a * 2', 'col_a / col_b', 'ROUND(col, 2)'] },
    { label: 'String', items: ["UPPER(col)", "LOWER(col)", "LENGTH(col)", "CONCAT(col_a, ' ', col_b)", "TRIM(col)"] },
    { label: 'Date', items: ['YEAR(col)', 'MONTH(col)', 'DAY(col)', "DATE_TRUNC('month', col)", 'DATEDIFF(col_a, col_b)'] },
    { label: 'Conditional', items: ["CASE WHEN col > 0 THEN 'Positive' ELSE 'Negative' END", "COALESCE(col, 0)", "NULLIF(col, 0)"] },
];

const ColumnSelector: React.FC<Props> = ({ columns, selectedColumns, onChange }) => {
    const [showExprModal, setShowExprModal] = useState(false);
    const [editExpr, setEditExpr] = useState('');
    const [editAlias, setEditAlias] = useState('');

    const isSelected = (colName: string) =>
        selectedColumns.some(c => c.column === colName && !c.expression);

    const handleToggle = (colName: string) => {
        if (isSelected(colName)) {
            onChange(selectedColumns.filter(c => c.column !== colName || c.expression));
        } else {
            onChange([...selectedColumns, { column: colName }]);
        }
    };

    const handleSelectAll = () => {
        if (selectedColumns.length === columns.length) {
            onChange([]);
        } else {
            onChange(columns.map(c => ({ column: c.name })));
        }
    };

    const handleSetAlias = (index: number, alias: string) => {
        const updated = [...selectedColumns];
        updated[index] = { ...updated[index], alias };
        onChange(updated);
    };

    const handleRemove = (index: number) => {
        onChange(selectedColumns.filter((_, i) => i !== index));
    };

    const handleAddExpression = () => {
        if (!editExpr) return;
        onChange([...selectedColumns, {
            expression: editExpr,
            alias: editAlias || `expr_${selectedColumns.length + 1}`,
        }]);
        setEditExpr('');
        setEditAlias('');
        setShowExprModal(false);
    };

    const exprPopover = (
        <div style={{ width: 360 }}>
            <div style={{ marginBottom: 8 }}>
                <label style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)' }}>Expression</label>
                <Input.TextArea
                    rows={2}
                    placeholder="e.g. price * quantity"
                    value={editExpr}
                    onChange={e => setEditExpr(e.target.value)}
                    style={{ fontSize: 12 }}
                />
            </div>
            <div style={{ marginBottom: 8 }}>
                <label style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)' }}>Alias</label>
                <Input
                    placeholder="e.g. total_value"
                    value={editAlias}
                    onChange={e => setEditAlias(e.target.value)}
                    size="small"
                />
            </div>
            <div style={{ marginBottom: 8 }}>
                <label style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)' }}>Templates</label>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginTop: 4 }}>
                    {expressionTemplates.map(group =>
                        group.items.map(item => (
                            <Tag
                                key={item}
                                style={{ cursor: 'pointer', fontSize: 10 }}
                                onClick={() => setEditExpr(item)}
                            >
                                {item.length > 20 ? item.slice(0, 20) + '…' : item}
                            </Tag>
                        ))
                    )}
                </div>
            </div>
            <Button type="primary" size="small" onClick={handleAddExpression} block disabled={!editExpr}>
                Add Expression
            </Button>
        </div>
    );

    return (
        <div className="glass-panel" style={{ padding: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                <span style={{ fontWeight: 600, fontSize: 13 }}>SELECT Columns</span>
                <Space size={4}>
                    <Button size="small" type="link" onClick={handleSelectAll}>
                        {selectedColumns.length === columns.length ? 'Deselect All' : 'Select All (*)'}
                    </Button>
                    <Popover
                        content={exprPopover}
                        title="Add Computed Column"
                        trigger="click"
                        open={showExprModal}
                        onOpenChange={setShowExprModal}
                    >
                        <Button size="small" icon={<FunctionOutlined />} type="dashed">
                            Expression
                        </Button>
                    </Popover>
                </Space>
            </div>

            {/* Column checkboxes */}
            <div style={{ maxHeight: 200, overflow: 'auto', marginBottom: 8 }}>
                {columns.map(col => (
                    <div
                        key={col.name}
                        style={{
                            display: 'flex', alignItems: 'center', gap: 6,
                            padding: '2px 4px', borderRadius: 4,
                            background: isSelected(col.name) ? 'rgba(99,102,241,0.08)' : 'transparent',
                        }}
                    >
                        <Checkbox
                            checked={isSelected(col.name)}
                            onChange={() => handleToggle(col.name)}
                        />
                        <span style={{ fontSize: 12 }}>{col.name}</span>
                        <Tag style={{ fontSize: 9, marginLeft: 'auto' }}>{col.ui_type}</Tag>
                    </div>
                ))}
            </div>

            {/* Selected columns (reorderable) */}
            {selectedColumns.length > 0 && (
                <div style={{ borderTop: '1px solid rgba(255,255,255,0.06)', paddingTop: 8 }}>
                    <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.4)', marginBottom: 4 }}>
                        Output order ({selectedColumns.length})
                    </div>
                    {selectedColumns.map((spec, i) => (
                        <div
                            key={i}
                            style={{
                                display: 'flex', alignItems: 'center', gap: 6,
                                padding: '2px 4px', fontSize: 12,
                            }}
                        >
                            <HolderOutlined style={{ color: 'rgba(255,255,255,0.2)', cursor: 'grab' }} />
                            <span style={{ color: spec.expression ? '#ff9c6e' : undefined }}>
                                {spec.expression || spec.column}
                            </span>
                            {spec.alias && (
                                <Tag color="blue" style={{ fontSize: 9 }}>AS {spec.alias}</Tag>
                            )}
                            <Tooltip title="Set alias">
                                <Popover
                                    trigger="click"
                                    content={
                                        <Input
                                            size="small"
                                            placeholder="Alias"
                                            defaultValue={spec.alias || ''}
                                            onPressEnter={(e) => handleSetAlias(i, (e.target as HTMLInputElement).value)}
                                            style={{ width: 150 }}
                                        />
                                    }
                                >
                                    <EditOutlined style={{ color: 'rgba(255,255,255,0.3)', cursor: 'pointer', fontSize: 11 }} />
                                </Popover>
                            </Tooltip>
                            <DeleteOutlined
                                style={{ color: 'rgba(255,100,100,0.5)', cursor: 'pointer', marginLeft: 'auto', fontSize: 11 }}
                                onClick={() => handleRemove(i)}
                            />
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

export default ColumnSelector;

