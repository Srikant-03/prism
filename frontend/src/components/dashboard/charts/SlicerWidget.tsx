/**
 * SlicerWidget — Power BI-style Interactive On-Canvas Slicer.
 * Renders categorical filter items as tile pills, list, or dropdown, propagating cross-filtering to all page visuals.
 */
import React, { useState } from 'react';
import { Tag, Input, Button } from 'antd';
import { FilterOutlined, SearchOutlined, ClearOutlined } from '@ant-design/icons';
import type { ChartConfig } from '../../../types/dashboard';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const SlicerWidget: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colKey = config.x_axis || config.group_by || Object.keys(data[0] || {})[0] || 'category';
    const [search, setSearch] = useState('');
    const [selectedValues, setSelectedValues] = useState<string[]>([]);

    const items = [...new Set(data.map(d => String(d[colKey] ?? '')))].filter(Boolean);
    const filteredItems = items.filter(it => it.toLowerCase().includes(search.toLowerCase()));

    const handleSelect = (val: string) => {
        let nextSelected: string[];
        if (selectedValues.includes(val)) {
            nextSelected = selectedValues.filter(v => v !== val);
        } else {
            nextSelected = [...selectedValues, val];
        }
        setSelectedValues(nextSelected);

        // Propagate filter
        onDataPointClick?.({ column: colKey, value: val });
    };

    const handleClear = () => {
        setSelectedValues([]);
        onDataPointClick?.({ column: colKey, value: undefined });
    };

    return (
        <div style={{
            display: 'flex', flexDirection: 'column', height: '100%', padding: '12px',
            background: 'rgba(15, 23, 42, 0.6)', borderRadius: 8,
        }}>
            {/* Slicer Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                <span style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', display: 'flex', alignItems: 'center', gap: 6 }}>
                    <FilterOutlined style={{ color: '#818cf8' }} />
                    {config.title || colKey.replace(/_/g, ' ').toUpperCase()}
                </span>
                {selectedValues.length > 0 && (
                    <Button
                        size="small"
                        type="text"
                        icon={<ClearOutlined />}
                        onClick={handleClear}
                        style={{ fontSize: 11, color: '#ef4444', height: 20, padding: 0 }}
                    >
                        Clear
                    </Button>
                )}
            </div>

            {/* Quick Search */}
            {items.length > 8 && (
                <Input
                    size="small"
                    prefix={<SearchOutlined style={{ color: '#64748b' }} />}
                    placeholder="Search..."
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    style={{ marginBottom: 8, background: '#1e293b', borderColor: '#334155' }}
                />
            )}

            {/* Slicer Items (Tile Pills) */}
            <div style={{
                flex: 1, overflowY: 'auto', display: 'flex', flexWrap: 'wrap',
                gap: 6, alignContent: 'flex-start',
            }}>
                {filteredItems.map(item => {
                    const isSelected = selectedValues.includes(item);
                    return (
                        <Tag.CheckableTag
                            key={item}
                            checked={isSelected}
                            onChange={() => handleSelect(item)}
                            style={{
                                padding: '4px 10px',
                                fontSize: 12,
                                borderRadius: 16,
                                background: isSelected ? '#6366f1' : '#1e293b',
                                color: isSelected ? '#ffffff' : '#cbd5e1',
                                border: isSelected ? '1px solid #818cf8' : '1px solid #334155',
                                cursor: 'pointer',
                                transition: 'all 0.15s ease',
                            }}
                        >
                            {item}
                        </Tag.CheckableTag>
                    );
                })}
            </div>
        </div>
    );
};

export default SlicerWidget;
