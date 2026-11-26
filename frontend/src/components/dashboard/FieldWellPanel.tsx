/**
 * FieldWellPanel — Power BI-style Field Wells and Visual Picker panel.
 * Allows switching visuals across all 35 types and binding columns/measures to wells.
 */
import React from 'react';
import { Select, Button, Divider, Tooltip } from 'antd';
import {
    BarChartOutlined, LineChartOutlined, PieChartOutlined,
    DotChartOutlined, TableOutlined, FundOutlined,
    PlusOutlined, DeleteOutlined, AppstoreOutlined,
    ApartmentOutlined, BulbOutlined, CompassOutlined,
} from '@ant-design/icons';
import type { ChartConfig, ChartType, AggregationType, TimeGrain, MetricSpec, MeasureDefinition } from '../../types/dashboard';

interface Props {
    config: ChartConfig;
    columns: string[];
    measures?: MeasureDefinition[];
    onChange: (newConfig: ChartConfig) => void;
    onClose?: () => void;
}

interface VisualCategory {
    title: string;
    items: { type: ChartType; label: string; icon: React.ReactNode }[];
}

const VISUAL_CATEGORIES: VisualCategory[] = [
    {
        title: 'KPI & Gauges',
        items: [
            { type: 'kpi', label: 'KPI Card', icon: <FundOutlined /> },
            { type: 'multi_kpi', label: 'Multi-KPI', icon: <FundOutlined /> },
            { type: 'gauge', label: 'Gauge', icon: <CompassOutlined /> },
            { type: 'bullet', label: 'Bullet Chart', icon: <BarChartOutlined /> },
        ],
    },
    {
        title: 'Bars & Columns',
        items: [
            { type: 'bar', label: 'Bar', icon: <BarChartOutlined /> },
            { type: 'stacked_bar', label: 'Stacked Bar', icon: <BarChartOutlined /> },
            { type: 'stacked_bar_100', label: '100% Stacked Bar', icon: <BarChartOutlined /> },
            { type: 'column', label: 'Column', icon: <BarChartOutlined /> },
            { type: 'stacked_column', label: 'Stacked Column', icon: <BarChartOutlined /> },
            { type: 'stacked_column_100', label: '100% Stacked Column', icon: <BarChartOutlined /> },
        ],
    },
    {
        title: 'Lines & Areas',
        items: [
            { type: 'line', label: 'Line', icon: <LineChartOutlined /> },
            { type: 'area', label: 'Area', icon: <LineChartOutlined /> },
            { type: 'stacked_area', label: 'Stacked Area', icon: <LineChartOutlined /> },
            { type: 'ribbon', label: 'Ribbon', icon: <LineChartOutlined /> },
            { type: 'combo_bar_line', label: 'Combo Bar+Line', icon: <BarChartOutlined /> },
            { type: 'combo_stacked_line', label: 'Combo Stack+Line', icon: <BarChartOutlined /> },
        ],
    },
    {
        title: 'Composition & Distribution',
        items: [
            { type: 'pie', label: 'Pie', icon: <PieChartOutlined /> },
            { type: 'donut', label: 'Donut', icon: <PieChartOutlined /> },
            { type: 'treemap', label: 'Treemap', icon: <AppstoreOutlined /> },
            { type: 'sunburst', label: 'Sunburst', icon: <PieChartOutlined /> },
            { type: 'funnel', label: 'Funnel', icon: <BarChartOutlined /> },
            { type: 'waterfall', label: 'Waterfall', icon: <BarChartOutlined /> },
            { type: 'scatter', label: 'Scatter', icon: <DotChartOutlined /> },
            { type: 'bubble', label: 'Bubble', icon: <DotChartOutlined /> },
            { type: 'histogram', label: 'Histogram', icon: <BarChartOutlined /> },
            { type: 'boxplot', label: 'Box Plot', icon: <BarChartOutlined /> },
        ],
    },
    {
        title: 'Grids & Maps',
        items: [
            { type: 'table', label: 'Data Table', icon: <TableOutlined /> },
            { type: 'matrix', label: 'Matrix', icon: <TableOutlined /> },
            { type: 'heatmap', label: 'Heatmap', icon: <AppstoreOutlined /> },
            { type: 'bubble_map', label: 'Bubble Map', icon: <CompassOutlined /> },
            { type: 'choropleth_map', label: 'Filled Map', icon: <CompassOutlined /> },
        ],
    },
    {
        title: 'AI & Super-Visuals',
        items: [
            { type: 'decomposition_tree', label: 'Decomp Tree', icon: <ApartmentOutlined /> },
            { type: 'key_influencers', label: 'Key Influencers', icon: <BulbOutlined /> },
            { type: 'smart_narrative', label: 'Smart Narrative', icon: <FundOutlined /> },
            { type: 'qa_visual', label: 'Q&A Visual', icon: <BulbOutlined /> },
            { type: 'slicer', label: 'Slicer', icon: <TableOutlined /> },
        ],
    },
];

const AGG_OPTIONS = [
    { value: 'sum', label: 'Sum' },
    { value: 'avg', label: 'Average' },
    { value: 'count', label: 'Count' },
    { value: 'distinct_count', label: 'Distinct Count' },
    { value: 'min', label: 'Min' },
    { value: 'max', label: 'Max' },
    { value: 'median', label: 'Median' },
    { value: 'stddev', label: 'Std Dev' },
];

const TIME_GRAINS: { value: TimeGrain; label: string }[] = [
    { value: 'none', label: 'Exact (None)' },
    { value: 'day', label: 'Day' },
    { value: 'week', label: 'Week' },
    { value: 'month', label: 'Month' },
    { value: 'quarter', label: 'Quarter' },
    { value: 'year', label: 'Year' },
];

const FieldWellPanel: React.FC<Props> = ({
    config,
    columns,
    measures = [],
    onChange,
    onClose,
}) => {
    const handleTypeChange = (newType: ChartType) => {
        onChange({ ...config, chart_type: newType });
    };

    const handleXAxisChange = (val?: string) => {
        onChange({ ...config, x_axis: val || undefined });
    };

    const handleGroupByChange = (val?: string) => {
        onChange({ ...config, group_by: val || undefined, color_by: val || undefined });
    };

    const handleSizeByChange = (val?: string) => {
        onChange({ ...config, size_by: val || undefined });
    };

    const handleTimeGrainChange = (val: TimeGrain) => {
        onChange({ ...config, x_axis_time_grain: val === 'none' ? null : val });
    };

    const handleAddMetric = () => {
        const firstCol = columns[0] || 'value';
        const newMetric: MetricSpec = {
            column: firstCol,
            aggregation: 'sum',
            label: `Sum of ${firstCol}`,
        };
        const currentMetrics = config.metrics || [];
        onChange({ ...config, metrics: [...currentMetrics, newMetric] });
    };

    const handleUpdateMetric = (index: number, updates: Partial<MetricSpec>) => {
        const currentMetrics = [...(config.metrics || [])];
        if (currentMetrics[index]) {
            currentMetrics[index] = { ...currentMetrics[index], ...updates };
            onChange({ ...config, metrics: currentMetrics });
        }
    };

    const handleRemoveMetric = (index: number) => {
        const currentMetrics = (config.metrics || []).filter((_, i) => i !== index);
        onChange({ ...config, metrics: currentMetrics });
    };

    return (
        <div
            style={{
                width: 320,
                background: '#0f172a',
                borderLeft: '1px solid rgba(99, 102, 241, 0.2)',
                display: 'flex',
                flexDirection: 'column',
                height: '100%',
                overflowY: 'auto',
                padding: '16px',
                color: '#e2e8f0',
            }}
        >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                <span style={{ fontWeight: 700, fontSize: 14, color: '#f1f5f9' }}>Visualizations & Fields</span>
                {onClose && (
                    <Button size="small" type="text" onClick={onClose} style={{ color: '#64748b' }}>
                        ✕
                    </Button>
                )}
            </div>

            {/* Visual Type Grid */}
            <div style={{ marginBottom: 16 }}>
                <span style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.5, color: '#64748b', fontWeight: 600 }}>
                    Visual Type
                </span>
                <div style={{ maxHeight: 220, overflowY: 'auto', marginTop: 8, paddingRight: 4 }}>
                    {VISUAL_CATEGORIES.map((cat) => (
                        <div key={cat.title} style={{ marginBottom: 8 }}>
                            <div style={{ fontSize: 10, color: '#475569', marginBottom: 4 }}>{cat.title}</div>
                            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 4 }}>
                                {cat.items.map((item) => {
                                    const isSelected = config.chart_type === item.type;
                                    return (
                                        <Tooltip key={item.type} title={item.label} mouseEnterDelay={0.3}>
                                            <button
                                                type="button"
                                                onClick={() => handleTypeChange(item.type)}
                                                style={{
                                                    display: 'flex',
                                                    flexDirection: 'column',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    height: 38,
                                                    background: isSelected ? 'rgba(99, 102, 241, 0.25)' : '#1e293b',
                                                    border: isSelected ? '1.5px solid #6366f1' : '1px solid #334155',
                                                    borderRadius: 6,
                                                    color: isSelected ? '#a5b4fc' : '#94a3b8',
                                                    cursor: 'pointer',
                                                    fontSize: 14,
                                                    transition: 'all 0.15s ease',
                                                }}
                                            >
                                                {item.icon}
                                                <span style={{ fontSize: 8, marginTop: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 54 }}>
                                                    {item.label}
                                                </span>
                                            </button>
                                        </Tooltip>
                                    );
                                })}
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            <Divider style={{ borderColor: 'rgba(99, 102, 241, 0.15)', margin: '8px 0 14px' }} />

            {/* Field Wells */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
                {/* X-Axis / Category */}
                <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                        <span style={{ fontSize: 12, fontWeight: 600, color: '#cbd5e1' }}>X-Axis / Category</span>
                    </div>
                    <Select
                        allowClear
                        placeholder="Select category or date column"
                        value={config.x_axis}
                        onChange={handleXAxisChange}
                        style={{ width: '100%' }}
                        options={columns.map(c => ({ value: c, label: c }))}
                    />

                    {/* Time Grain selector when x_axis looks temporal */}
                    {config.x_axis && (config.x_axis.toLowerCase().includes('date') || config.x_axis.toLowerCase().includes('year') || config.x_axis.toLowerCase().includes('month')) && (
                        <div style={{ marginTop: 6 }}>
                            <span style={{ fontSize: 11, color: '#64748b' }}>Time Granularity</span>
                            <Select
                                value={config.x_axis_time_grain || 'none'}
                                onChange={handleTimeGrainChange}
                                style={{ width: '100%', marginTop: 2 }}
                                options={TIME_GRAINS}
                            />
                        </div>
                    )}
                </div>

                {/* Values / Metrics Well */}
                <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                        <span style={{ fontSize: 12, fontWeight: 600, color: '#cbd5e1' }}>Values / Metrics</span>
                        <Button
                            size="small"
                            type="dashed"
                            icon={<PlusOutlined />}
                            onClick={handleAddMetric}
                            style={{ fontSize: 11, height: 22 }}
                        >
                            Add Metric
                        </Button>
                    </div>

                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                        {(config.metrics && config.metrics.length > 0) ? (
                            config.metrics.map((m, idx) => (
                                <div
                                    key={idx}
                                    style={{
                                        background: '#1e293b',
                                        padding: 8,
                                        borderRadius: 6,
                                        border: '1px solid #334155',
                                        display: 'flex',
                                        flexDirection: 'column',
                                        gap: 6,
                                    }}
                                >
                                    <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                                        <Select
                                            size="small"
                                            value={m.column}
                                            onChange={(col) => handleUpdateMetric(idx, { column: col, label: `${m.aggregation.toUpperCase()} of ${col}` })}
                                            style={{ flex: 1 }}
                                            options={[
                                                ...columns.map(c => ({ value: c, label: c })),
                                                ...measures.map(meas => ({ value: meas.name, label: `fx ${meas.name}` })),
                                            ]}
                                        />
                                        <Button
                                            size="small"
                                            type="text"
                                            danger
                                            icon={<DeleteOutlined />}
                                            onClick={() => handleRemoveMetric(idx)}
                                        />
                                    </div>
                                    <div style={{ display: 'flex', gap: 6 }}>
                                        <Select
                                            size="small"
                                            value={m.aggregation}
                                            onChange={(agg: AggregationType) => handleUpdateMetric(idx, { aggregation: agg, label: `${agg.toUpperCase()} of ${m.column}` })}
                                            style={{ width: '50%' }}
                                            options={AGG_OPTIONS}
                                        />
                                        <Select
                                            size="small"
                                            placeholder="Format"
                                            value={m.format || 'auto'}
                                            onChange={(fmt) => handleUpdateMetric(idx, { format: fmt })}
                                            style={{ width: '50%' }}
                                            options={[
                                                { value: 'auto', label: 'Auto' },
                                                { value: 'compact', label: 'Compact (1.2M)' },
                                                { value: 'currency', label: 'Currency ($)' },
                                                { value: 'percent', label: 'Percent (%)' },
                                                { value: 'integer', label: 'Integer' },
                                            ]}
                                        />
                                    </div>
                                </div>
                            ))
                        ) : (
                            <div style={{ fontSize: 12, color: '#64748b', fontStyle: 'italic', padding: '4px 0' }}>
                                Using default metric: {config.y_axis || 'auto'} ({config.aggregation})
                            </div>
                        )}
                    </div>
                </div>

                {/* Legend / Group By */}
                <div>
                    <span style={{ fontSize: 12, fontWeight: 600, color: '#cbd5e1', display: 'block', marginBottom: 4 }}>
                        Legend / Color By
                    </span>
                    <Select
                        allowClear
                        placeholder="Select categorical grouping"
                        value={config.group_by || config.color_by}
                        onChange={handleGroupByChange}
                        style={{ width: '100%' }}
                        options={columns.map(c => ({ value: c, label: c }))}
                    />
                </div>

                {/* Size By */}
                <div>
                    <span style={{ fontSize: 12, fontWeight: 600, color: '#cbd5e1', display: 'block', marginBottom: 4 }}>
                        Size By (Scatter/Bubble/Treemap)
                    </span>
                    <Select
                        allowClear
                        placeholder="Select numeric sizing column"
                        value={config.size_by}
                        onChange={handleSizeByChange}
                        style={{ width: '100%' }}
                        options={columns.map(c => ({ value: c, label: c }))}
                    />
                </div>
            </div>
        </div>
    );
};

export default FieldWellPanel;
