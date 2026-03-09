/**
 * GlobalFilterBar — Date range picker + categorical multi-select chips.
 * Propagates filters to all widgets via callback.
 */
import React from 'react';
import { DatePicker, Tag } from 'antd';
import dayjs from 'dayjs';
import { FilterOutlined, CloseCircleOutlined } from '@ant-design/icons';
import type { FilterCondition } from '../../types/dashboard';

const { RangePicker } = DatePicker;

interface Props {
    filters: FilterCondition[];
    columns?: string[];
    onFiltersChange: (filters: FilterCondition[]) => void;
}

const GlobalFilterBar: React.FC<Props> = ({ filters, columns = [], onFiltersChange }) => {

    const findDateColumn = () => {
        return columns.find(c => c.toLowerCase().includes('date') || c.toLowerCase().includes('time')) || 'date';
    };

    const handleDateChange = (_: any, dates: [string, string]) => {
        const dateCol = findDateColumn();
        const newFilters = filters.filter(f => !(f.operator === 'between' && f.column === dateCol));

        if (dates[0] && dates[1]) {
            newFilters.push({ column: dateCol, operator: 'between', values: [dates[0], dates[1]] });
        }
        onFiltersChange(newFilters);
    };

    // Calculate controlled value for RangePicker based on existing filters
    const dateCol = findDateColumn();
    const dateFilter = filters.find(f => f.operator === 'between' && f.column === dateCol);
    const dateRange: any = dateFilter && dateFilter.values?.length === 2
        ? [dayjs(dateFilter.values[0]), dayjs(dateFilter.values[1])]
        : null;

    const removeFilter = (idx: number) => {
        onFiltersChange(filters.filter((_, i) => i !== idx));
    };

    return (
        <div style={{
            display: 'flex', alignItems: 'center', gap: 12, padding: '8px 24px',
            borderBottom: '1px solid rgba(99,102,241,0.08)', flexWrap: 'wrap',
        }}>
            <FilterOutlined style={{ color: '#64748b' }} />
            <RangePicker
                value={dateRange}
                onChange={handleDateChange}
                size="small"
                style={{ background: '#1e293b', borderRadius: 8 }}
            />

            {filters.map((f, i) => (
                <Tag
                    key={i}
                    closable
                    onClose={() => removeFilter(i)}
                    style={{
                        background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.2)',
                        color: '#a5b4fc', borderRadius: 16,
                    }}
                >
                    {f.column} {f.operator} {f.value ?? f.values?.join(', ')}
                </Tag>
            ))}

            {filters.length > 0 && (
                <Tag
                    onClick={() => onFiltersChange([])}
                    style={{ cursor: 'pointer', color: '#ef4444', background: 'transparent', border: 'none' }}
                >
                    <CloseCircleOutlined /> Clear all
                </Tag>
            )}
        </div>
    );
};

export default GlobalFilterBar;
