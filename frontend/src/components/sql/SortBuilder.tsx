/**
 * SortBuilder â€” ORDER BY multi-column sort builder.
 */

import React from 'react';
import { Select, Button } from 'antd';
import { PlusOutlined, DeleteOutlined, SortAscendingOutlined } from '@ant-design/icons';
import type { OrderBySpec, ColumnInfo } from '../../types/sql';

interface Props {
    columns: ColumnInfo[];
    orderBy: OrderBySpec[];
    onChange: (orderBy: OrderBySpec[]) => void;
}

const SortBuilder: React.FC<Props> = ({ columns, orderBy, onChange }) => {
    const add = () => {
        onChange([...orderBy, { column: '', direction: 'ASC' }]);
    };

    const update = (index: number, updates: Partial<OrderBySpec>) => {
        const updated = [...orderBy];
        updated[index] = { ...updated[index], ...updates };
        onChange(updated);
    };

    const remove = (index: number) => {
        onChange(orderBy.filter((_, i) => i !== index));
    };

    return (
        <div className="glass-panel" style={{ padding: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                <span style={{ fontWeight: 600, fontSize: 13 }}>
                    <SortAscendingOutlined style={{ marginRight: 6 }} />
                    ORDER BY
                </span>
                <Button size="small" icon={<PlusOutlined />} onClick={add} type="dashed">Add</Button>
            </div>
            {orderBy.map((ob, i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '3px 0' }}>
                    <Select
                        size="small"
                        showSearch
                        value={ob.column || undefined}
                        onChange={v => update(i, { column: v })}
                        style={{ flex: 1, minWidth: 120 }}
                        placeholder="Column"
                        options={columns.map(c => ({ value: c.name, label: c.name }))}
                    />
                    <Select
                        size="small"
                        value={ob.direction}
                        onChange={v => update(i, { direction: v })}
                        style={{ width: 80 }}
                        options={[
                            { value: 'ASC', label: 'â†‘ ASC' },
                            { value: 'DESC', label: 'â†“ DESC' },
                        ]}
                    />
                    <Select
                        size="small"
                        value={ob.nulls || 'DEFAULT'}
                        onChange={v => update(i, { nulls: v === 'DEFAULT' ? undefined : v as 'FIRST' | 'LAST' })}
                        style={{ width: 100 }}
                        options={[
                            { value: 'DEFAULT', label: 'Default' },
                            { value: 'FIRST', label: 'Nulls First' },
                            { value: 'LAST', label: 'Nulls Last' },
                        ]}
                    />
                    <DeleteOutlined
                        style={{ color: 'rgba(255,100,100,0.5)', cursor: 'pointer', fontSize: 11 }}
                        onClick={() => remove(i)}
                    />
                </div>
            ))}
            {orderBy.length === 0 && (
                <div style={{ color: 'rgba(255,255,255,0.3)', fontSize: 12, padding: 4 }}>
                    No sorting. Results in natural order.
                </div>
            )}
        </div>
    );
};

export default SortBuilder;

