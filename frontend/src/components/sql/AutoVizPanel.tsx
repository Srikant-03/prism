/**
 * AutoVizPanel — Auto-detects result structure and renders the best chart.
 * Analyzes columns/types/cardinality to pick from 12+ chart types.
 * Fully interactive with customization controls.
 */

import React, { useMemo, useState, useCallback } from 'react';
import { Select, Space, Tag, Button, Input, Tooltip, Segmented, Empty } from 'antd';
import {
    BarChartOutlined, LineChartOutlined, PieChartOutlined, DotChartOutlined,
    TableOutlined, HeatMapOutlined, AreaChartOutlined, DownloadOutlined,
    SettingOutlined, EyeOutlined,
} from '@ant-design/icons';
import {
    ResponsiveContainer, BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
    ScatterChart, Scatter, AreaChart, Area,
    XAxis, YAxis, Tooltip as RechartsTooltip, Legend, CartesianGrid,
    FunnelChart, Funnel, LabelList,
} from 'recharts';
import type { QueryResult } from '../../types/sql';

// ── Chart type detection ─────────────────────────────────────────────

type ChartType =
    | 'kpi' | 'bar' | 'horizontal_bar' | 'line' | 'multi_line'
    | 'grouped_bar' | 'scatter' | 'pie' | 'area'
    | 'heatmap_table' | 'funnel' | 'histogram' | 'table';

interface ChartRecommendation {
    type: ChartType;
    label: string;
    icon: React.ReactNode;
    confidence: number;
    config: {
        xKey?: string;
        yKeys?: string[];
        categoryKey?: string;
        valueKey?: string;
    };
}

// Okabe-Ito colorblind-safe palette as default
const CHART_PALETTE = [
    '#56B4E9', '#E69F00', '#009E73', '#CC79A7', '#0072B2',
    '#D55E00', '#F0E442', '#999999', '#6366f1', '#a78bfa',
    '#f472b6', '#34d399', '#fbbf24', '#f97316', '#06b6d4',
];

function classifyColumns(result: QueryResult) {
    if (!result?.columns?.length) return { numeric: [], datetime: [], categorical: [], boolean: [] };

    const numeric: string[] = [];
    const datetime: string[] = [];
    const categorical: string[] = [];
    const boolean: string[] = [];

    result.columns.forEach((col, i) => {
        const type = result.column_types?.[i]?.toLowerCase() || '';
        // Sample values to determine type
        const colVals = result.rows.slice(0, 50).map(r => r[col]);
        const nonNull = colVals.filter(v => v !== null && v !== undefined);

        if (type.includes('bool')) {
            boolean.push(col);
        } else if (type.includes('int') || type.includes('float') || type.includes('double') || type.includes('decimal') || type.includes('numeric') || type.includes('bigint')) {
            numeric.push(col);
        } else if (type.includes('date') || type.includes('time') || type.includes('timestamp')) {
            datetime.push(col);
        } else {
            // Check if values are numeric
            const numericCount = nonNull.filter(v => typeof v === 'number' || !isNaN(Number(v))).length;
            if (numericCount > nonNull.length * 0.8 && nonNull.length > 0) {
                numeric.push(col);
            } else {
                categorical.push(col);
            }
        }
    });

    return { numeric, datetime, categorical, boolean };
}

function detectChartType(result: QueryResult): ChartRecommendation[] {
    if (!result?.rows?.length || !result?.columns?.length) return [];

    const { numeric, datetime, categorical } = classifyColumns(result);
    const rowCount = result.row_count;
    const colCount = result.columns.length;
    const recommendations: ChartRecommendation[] = [];

    // 1. Single numeric result → KPI card
    if (rowCount === 1 && colCount <= 3 && numeric.length >= 1) {
        recommendations.push({
            type: 'kpi', label: 'KPI Card', icon: <EyeOutlined />,
            confidence: 95,
            config: { yKeys: numeric },
        });
    }

    // 2. One datetime + one measure → line chart
    if (datetime.length >= 1 && numeric.length === 1) {
        recommendations.push({
            type: 'line', label: 'Line Chart', icon: <LineChartOutlined />,
            confidence: 90,
            config: { xKey: datetime[0], yKeys: [numeric[0]] },
        });
    }

    // 3. One datetime + multiple measures → multi-line
    if (datetime.length >= 1 && numeric.length > 1) {
        recommendations.push({
            type: 'multi_line', label: 'Multi-Line Chart', icon: <LineChartOutlined />,
            confidence: 88,
            config: { xKey: datetime[0], yKeys: numeric },
        });
        recommendations.push({
            type: 'area', label: 'Area Chart', icon: <AreaChartOutlined />,
            confidence: 70,
            config: { xKey: datetime[0], yKeys: numeric },
        });
    }

    // 4. One categorical + one numeric → bar chart
    if (categorical.length >= 1 && numeric.length >= 1 && datetime.length === 0) {
        const catCardinality = new Set(result.rows.map(r => r[categorical[0]])).size;
        if (catCardinality > 15) {
            recommendations.push({
                type: 'horizontal_bar', label: 'Horizontal Bar', icon: <BarChartOutlined />,
                confidence: 85,
                config: { xKey: numeric[0], yKeys: [categorical[0]], categoryKey: categorical[0], valueKey: numeric[0] },
            });
        } else {
            recommendations.push({
                type: 'bar', label: 'Bar Chart', icon: <BarChartOutlined />,
                confidence: 85,
                config: { xKey: categorical[0], yKeys: [numeric[0]] },
            });
        }

        // Pie chart for low cardinality
        if (catCardinality < 8) {
            recommendations.push({
                type: 'pie', label: 'Pie Chart', icon: <PieChartOutlined />,
                confidence: 75,
                config: { categoryKey: categorical[0], valueKey: numeric[0] },
            });
        }
    }

    // 5. Two categoricals + one numeric → grouped bar
    if (categorical.length >= 2 && numeric.length >= 1) {
        recommendations.push({
            type: 'grouped_bar', label: 'Grouped Bar', icon: <BarChartOutlined />,
            confidence: 80,
            config: { xKey: categorical[0], yKeys: [numeric[0]], categoryKey: categorical[1] },
        });
    }

    // 6. Two numeric columns → scatter plot
    if (numeric.length >= 2 && categorical.length === 0 && datetime.length === 0) {
        recommendations.push({
            type: 'scatter', label: 'Scatter Plot', icon: <DotChartOutlined />,
            confidence: 82,
            config: { xKey: numeric[0], yKeys: [numeric[1]] },
        });
    }

    // 7. Funnel detection (decreasing values)
    if (categorical.length === 1 && numeric.length === 1 && rowCount <= 10) {
        const vals = result.rows.map(r => Number(r[numeric[0]])).filter(v => !isNaN(v));
        let isDecreasing = true;
        for (let i = 1; i < vals.length; i++) {
            if (vals[i] > vals[i - 1]) { isDecreasing = false; break; }
        }
        if (isDecreasing && vals.length >= 3) {
            recommendations.push({
                type: 'funnel', label: 'Funnel Chart', icon: <BarChartOutlined />,
                confidence: 78,
                config: { categoryKey: categorical[0], valueKey: numeric[0] },
            });
        }
    }

    // 8. Many columns → heatmap table
    if (colCount > 5 && rowCount > 5) {
        recommendations.push({
            type: 'heatmap_table', label: 'Heatmap Table', icon: <HeatMapOutlined />,
            confidence: 60,
            config: { yKeys: numeric },
        });
    }

    // Always provide table as fallback
    recommendations.push({
        type: 'table', label: 'Data Table', icon: <TableOutlined />,
        confidence: 50,
        config: {},
    });

    // Sort by confidence
    recommendations.sort((a, b) => b.confidence - a.confidence);
    return recommendations;
}

// ── Component ────────────────────────────────────────────────────────

interface Props {
    result: QueryResult | null;
}

const AutoVizPanel: React.FC<Props> = ({ result }) => {
    const recommendations = useMemo(() => result ? detectChartType(result) : [], [result]);
    const [selectedType, setSelectedType] = useState<string | null>(null);
    const [chartTitle, setChartTitle] = useState('');
    const [showSettings, setShowSettings] = useState(false);
    const [colorScheme, setColorScheme] = useState(0);
    const [showDataTable, setShowDataTable] = useState(false);

    const activeRec = useMemo(() => {
        if (!recommendations.length) return null;
        if (selectedType) return recommendations.find(r => r.type === selectedType) || recommendations[0];
        return recommendations[0];
    }, [recommendations, selectedType]);

    const colors = useMemo(() => {
        const schemes = [
            CHART_PALETTE,
            ['#3b82f6', '#60a5fa', '#93c5fd', '#bfdbfe', '#dbeafe', '#2563eb', '#1d4ed8'],
            ['#10b981', '#34d399', '#6ee7b7', '#a7f3d0', '#d1fae5', '#059669', '#047857'],
            ['#f59e0b', '#fbbf24', '#fcd34d', '#fde68a', '#fef3c7', '#d97706', '#b45309'],
            ['#ef4444', '#f87171', '#fca5a5', '#fecaca', '#fee2e2', '#dc2626', '#b91c1c'],
        ];
        return schemes[colorScheme % schemes.length];
    }, [colorScheme]);

    const exportChart = useCallback((format: string) => {
        const svg = document.querySelector('.auto-viz-chart svg');
        if (!svg) return;

        if (format === 'svg') {
            const blob = new Blob([svg.outerHTML], { type: 'image/svg+xml' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url; a.download = 'chart.svg'; a.click();
        } else if (format === 'png') {
            const svgData = new XMLSerializer().serializeToString(svg);
            const canvas = document.createElement('canvas');
            canvas.width = 800; canvas.height = 450;
            const ctx = canvas.getContext('2d')!;
            const img = new Image();
            img.onload = () => {
                ctx.fillStyle = '#1a1a2e';
                ctx.fillRect(0, 0, 800, 450);
                ctx.drawImage(img, 0, 0);
                canvas.toBlob(blob => {
                    if (!blob) return;
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url; a.download = 'chart.png'; a.click();
                });
            };
            img.src = 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(svgData)));
        }
    }, []);

    if (!result || !result.success || !result.rows.length) {
        return null;
    }

    const renderChart = () => {
        if (!activeRec) return null;
        const { type, config } = activeRec;
        const rows = result.rows;

        switch (type) {
            case 'kpi':
                return (
                    <div style={{
                        display: 'flex', gap: 24, justifyContent: 'center',
                        padding: 24, flexWrap: 'wrap',
                    }}>
                        {(config.yKeys || []).map((key, i) => (
                            <div key={key} style={{
                                textAlign: 'center', padding: '20px 32px',
                                background: `linear-gradient(135deg, ${colors[i]}15, ${colors[i]}08)`,
                                borderRadius: 16,
                                border: `1px solid ${colors[i]}30`,
                                minWidth: 160,
                            }}>
                                <div style={{
                                    fontSize: 36, fontWeight: 800, color: colors[i],
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>
                                    {typeof rows[0][key] === 'number'
                                        ? rows[0][key].toLocaleString(undefined, { maximumFractionDigits: 2 })
                                        : rows[0][key]}
                                </div>
                                <div style={{ fontSize: 13, color: 'rgba(255,255,255,0.5)', marginTop: 4 }}>
                                    {key}
                                </div>
                            </div>
                        ))}
                    </div>
                );

            case 'bar':
            case 'horizontal_bar':
                return (
                    <ResponsiveContainer width="100%" height={350}>
                        <BarChart
                            data={rows}
                            layout={type === 'horizontal_bar' ? 'vertical' : 'horizontal'}
                        >
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                            {type === 'horizontal_bar' ? (
                                <>
                                    <YAxis dataKey={config.categoryKey || config.xKey} type="category" width={120}
                                        tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                                    <XAxis type="number"
                                        tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                                </>
                            ) : (
                                <>
                                    <XAxis dataKey={config.xKey} tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                                    <YAxis tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                                </>
                            )}
                            <RechartsTooltip
                                contentStyle={{
                                    background: 'rgba(20,20,40,0.95)', border: '1px solid rgba(99,102,241,0.3)',
                                    borderRadius: 8, fontSize: 12,
                                }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                            {(config.yKeys || []).map((key, i) => (
                                <Bar key={key} dataKey={key} fill={colors[i]} radius={[4, 4, 0, 0]} />
                            ))}
                        </BarChart>
                    </ResponsiveContainer>
                );

            case 'line':
            case 'multi_line':
                return (
                    <ResponsiveContainer width="100%" height={350}>
                        <LineChart data={rows}>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                            <XAxis dataKey={config.xKey} tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <YAxis tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <RechartsTooltip
                                contentStyle={{
                                    background: 'rgba(20,20,40,0.95)', border: '1px solid rgba(99,102,241,0.3)',
                                    borderRadius: 8, fontSize: 12,
                                }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                            {(config.yKeys || []).map((key, i) => (
                                <Line key={key} type="monotone" dataKey={key}
                                    stroke={colors[i]} strokeWidth={2}
                                    dot={{ r: 3, fill: colors[i] }}
                                    activeDot={{ r: 6 }}
                                />
                            ))}
                        </LineChart>
                    </ResponsiveContainer>
                );

            case 'area':
                return (
                    <ResponsiveContainer width="100%" height={350}>
                        <AreaChart data={rows}>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                            <XAxis dataKey={config.xKey} tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <YAxis tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <RechartsTooltip
                                contentStyle={{
                                    background: 'rgba(20,20,40,0.95)', border: '1px solid rgba(99,102,241,0.3)',
                                    borderRadius: 8, fontSize: 12,
                                }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                            {(config.yKeys || []).map((key, i) => (
                                <Area key={key} type="monotone" dataKey={key}
                                    stroke={colors[i]} fill={`${colors[i]}30`} strokeWidth={2}
                                />
                            ))}
                        </AreaChart>
                    </ResponsiveContainer>
                );

            case 'pie':
                return (
                    <ResponsiveContainer width="100%" height={350}>
                        <PieChart>
                            <Pie
                                data={rows.map(r => ({
                                    name: r[config.categoryKey!],
                                    value: Number(r[config.valueKey!]) || 0,
                                }))}
                                cx="50%" cy="50%" outerRadius={130} innerRadius={60}
                                dataKey="value" nameKey="name"
                                label={({ name, percent }: any) => `${name} (${((percent || 0) * 100).toFixed(0)}%)`}
                                labelLine={{ stroke: 'rgba(255,255,255,0.3)' }}
                            >
                                {rows.map((_, i) => (
                                    <Cell key={i} fill={colors[i % colors.length]} />
                                ))}
                            </Pie>
                            <RechartsTooltip
                                contentStyle={{
                                    background: 'rgba(20,20,40,0.95)', border: '1px solid rgba(99,102,241,0.3)',
                                    borderRadius: 8, fontSize: 12,
                                }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                        </PieChart>
                    </ResponsiveContainer>
                );

            case 'scatter':
                return (
                    <ResponsiveContainer width="100%" height={350}>
                        <ScatterChart>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                            <XAxis dataKey={config.xKey} name={config.xKey}
                                tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <YAxis dataKey={config.yKeys?.[0]} name={config.yKeys?.[0]}
                                tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <RechartsTooltip
                                contentStyle={{
                                    background: 'rgba(20,20,40,0.95)', border: '1px solid rgba(99,102,241,0.3)',
                                    borderRadius: 8, fontSize: 12,
                                }}
                                cursor={{ strokeDasharray: '3 3' }}
                            />
                            <Scatter data={rows} fill={colors[0]} />
                        </ScatterChart>
                    </ResponsiveContainer>
                );

            case 'funnel':
                return (
                    <ResponsiveContainer width="100%" height={350}>
                        <FunnelChart>
                            <RechartsTooltip
                                contentStyle={{
                                    background: 'rgba(20,20,40,0.95)', border: '1px solid rgba(99,102,241,0.3)',
                                    borderRadius: 8, fontSize: 12,
                                }}
                            />
                            <Funnel
                                dataKey={config.valueKey!}
                                nameKey={config.categoryKey!}
                                data={rows.map((r, i) => ({
                                    ...r,
                                    fill: colors[i % colors.length],
                                }))}
                            >
                                <LabelList position="center" fill="#fff" style={{ fontSize: 12, fontWeight: 600 }} />
                            </Funnel>
                        </FunnelChart>
                    </ResponsiveContainer>
                );

            case 'grouped_bar': {
                // Pivot data by category
                const cat2 = config.categoryKey!;
                const valueKey = config.yKeys?.[0] || '';
                const groups = [...new Set(rows.map(r => r[cat2]))];
                const xValues = [...new Set(rows.map(r => r[config.xKey!]))];

                const pivoted = xValues.map(x => {
                    const entry: any = { [config.xKey!]: x };
                    groups.forEach(g => {
                        const row = rows.find(r => r[config.xKey!] === x && r[cat2] === g);
                        entry[String(g)] = row ? row[valueKey] : 0;
                    });
                    return entry;
                });

                return (
                    <ResponsiveContainer width="100%" height={350}>
                        <BarChart data={pivoted}>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                            <XAxis dataKey={config.xKey} tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <YAxis tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }} />
                            <RechartsTooltip
                                contentStyle={{
                                    background: 'rgba(20,20,40,0.95)', border: '1px solid rgba(99,102,241,0.3)',
                                    borderRadius: 8, fontSize: 12,
                                }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11 }} />
                            {groups.map((g, i) => (
                                <Bar key={String(g)} dataKey={String(g)} fill={colors[i % colors.length]} radius={[4, 4, 0, 0]} />
                            ))}
                        </BarChart>
                    </ResponsiveContainer>
                );
            }

            case 'heatmap_table': {
                const nums = config.yKeys || [];
                // Find min/max for each numeric column
                const ranges: Record<string, { min: number; max: number }> = {};
                nums.forEach(n => {
                    const vals = rows.map(r => Number(r[n])).filter(v => !isNaN(v));
                    ranges[n] = { min: Math.min(...vals), max: Math.max(...vals) };
                });

                const getHeatColor = (val: number, col: string) => {
                    if (isNaN(val) || !ranges[col]) return 'transparent';
                    const { min, max } = ranges[col];
                    const ratio = max === min ? 0.5 : (val - min) / (max - min);
                    const r = Math.round(30 + ratio * 70);
                    const g = Math.round(130 - ratio * 80);
                    const b = Math.round(240 - ratio * 100);
                    return `rgba(${r}, ${g}, ${b}, 0.3)`;
                };

                return (
                    <div style={{ overflow: 'auto', maxHeight: 400 }}>
                        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                            <thead>
                                <tr>
                                    {result.columns.map(col => (
                                        <th key={col} style={{
                                            padding: '6px 10px', textAlign: 'left',
                                            borderBottom: '1px solid rgba(255,255,255,0.1)',
                                            fontWeight: 600, fontSize: 11,
                                        }}>{col}</th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {rows.slice(0, 100).map((row, ri) => (
                                    <tr key={ri}>
                                        {result.columns.map(col => {
                                            const val = row[col];
                                            const isNum = nums.includes(col) && typeof val === 'number';
                                            return (
                                                <td key={col} style={{
                                                    padding: '4px 10px',
                                                    borderBottom: '1px solid rgba(255,255,255,0.04)',
                                                    background: isNum ? getHeatColor(val, col) : 'transparent',
                                                    fontFamily: isNum ? 'monospace' : 'inherit',
                                                }}>
                                                    {val === null ? <span style={{ opacity: 0.2 }}>NULL</span> :
                                                        typeof val === 'number' ? val.toLocaleString(undefined, { maximumFractionDigits: 2 }) :
                                                            String(val)}
                                                </td>
                                            );
                                        })}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                );
            }

            default:
                return null;
        }
    };

    return (
        <div className="glass-panel" style={{ padding: 0, marginTop: 1 }}>
            {/* Chart selector bar */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 16px',
                borderBottom: '1px solid rgba(255,255,255,0.06)',
                flexWrap: 'wrap', gap: 8,
            }}>
                <Space size={6}>
                    <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>Auto-Viz</Tag>
                    <Segmented
                        size="small"
                        value={selectedType || activeRec?.type || 'table'}
                        onChange={v => setSelectedType(v as string)}
                        options={recommendations.slice(0, 5).map(r => ({
                            value: r.type,
                            label: (
                                <Tooltip title={`${r.label} (${r.confidence}% match)`}>
                                    <span>{r.icon} {r.label}</span>
                                </Tooltip>
                            ),
                        }))}
                    />
                </Space>
                <Space size={4}>
                    <Tooltip title="Chart settings">
                        <Button size="small" icon={<SettingOutlined />}
                            onClick={() => setShowSettings(!showSettings)}
                            type={showSettings ? 'primary' : 'default'}
                        />
                    </Tooltip>
                    <Tooltip title="Export PNG">
                        <Button size="small" icon={<DownloadOutlined />}
                            onClick={() => exportChart('png')} />
                    </Tooltip>
                    <Tooltip title="Export SVG">
                        <Button size="small" onClick={() => exportChart('svg')}>SVG</Button>
                    </Tooltip>
                </Space>
            </div>

            {/* Settings */}
            {showSettings && (
                <div style={{
                    padding: '8px 16px', display: 'flex', gap: 12, alignItems: 'center',
                    borderBottom: '1px solid rgba(255,255,255,0.04)', flexWrap: 'wrap',
                }}>
                    <Input
                        size="small" placeholder="Chart title"
                        value={chartTitle} onChange={e => setChartTitle(e.target.value)}
                        style={{ width: 200 }}
                    />
                    <Select size="small" value={colorScheme} onChange={setColorScheme}
                        style={{ width: 120 }}
                        options={[
                            { value: 0, label: '🎨 Default' },
                            { value: 1, label: '🔵 Blue' },
                            { value: 2, label: '🟢 Green' },
                            { value: 3, label: '🟡 Amber' },
                            { value: 4, label: '🔴 Red' },
                        ]}
                    />
                </div>
            )}

            {/* Chart */}
            <div className="auto-viz-chart" style={{ padding: '12px 16px' }} role="figure" aria-label={`${activeRec?.label || 'Chart'} visualization`}>
                {chartTitle && (
                    <div style={{
                        textAlign: 'center', fontSize: 15, fontWeight: 700,
                        marginBottom: 8, color: 'rgba(255,255,255,0.8)',
                    }}>
                        {chartTitle}
                    </div>
                )}
                {activeRec?.type === 'table' ? (
                    <Empty description="Select a chart type above" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                ) : (
                    renderChart()
                )}

                {/* Data table alternative (WCAG accessibility) */}
                {activeRec?.type !== 'table' && activeRec?.type !== 'heatmap_table' && (
                    <div style={{ marginTop: 8, textAlign: 'right' }}>
                        <button
                            className="chart-data-table-toggle"
                            onClick={() => setShowDataTable(!showDataTable)}
                            aria-label={showDataTable ? 'Hide data table' : 'Show data as table'}
                        >
                            <TableOutlined /> {showDataTable ? 'Hide table' : 'View as table'}
                        </button>
                        {showDataTable && (
                            <table className="chart-data-table" role="table">
                                <thead>
                                    <tr>
                                        {result.columns.map(c => <th key={c}>{c}</th>)}
                                    </tr>
                                </thead>
                                <tbody>
                                    {result.rows.slice(0, 50).map((row, i) => (
                                        <tr key={i}>
                                            {result.columns.map(c => (
                                                <td key={c}>
                                                    {row[c] === null ? 'NULL' :
                                                        typeof row[c] === 'number'
                                                            ? row[c].toLocaleString(undefined, { maximumFractionDigits: 2 })
                                                            : String(row[c])}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
};

export default AutoVizPanel;
