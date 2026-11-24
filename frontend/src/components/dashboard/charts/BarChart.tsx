/**
 * BarChart — Comprehensive Power BI-style Bar & Column Visualizer.
 * Supports Column, Bar, Stacked (standard & 100%), Multi-Metric, Reference Lines, and Cross-Filtering.
 */
import React from 'react';
import {
    ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer, LabelList,
    ReferenceLine as RechartsRefLine,
} from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatCompactNumber, formatValue, formatAxisTick } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardBarChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    // Filter out null rows for x_axis
    const rawData = (data || []).filter(d => {
        if (config.x_axis && (d[config.x_axis] === null || d[config.x_axis] === undefined)) return false;
        return true;
    });

    const isHorizontal = config.chart_type === 'bar' || config.chart_type === 'stacked_bar' || config.chart_type === 'stacked_bar_100';
    const is100Percent = config.chart_type === 'stacked_bar_100' || config.chart_type === 'stacked_column_100';
    const isStacked = config.stacked || config.chart_type.includes('stacked');
    const hasMultiMetrics = Boolean(config.metrics && config.metrics.length > 0);

    // Normalize for 100% stacked if requested
    let chartData = rawData;
    if (is100Percent && hasMultiMetrics && config.metrics) {
        chartData = rawData.map(row => {
            const sum = config.metrics!.reduce((acc, m) => acc + (Number(row[m.label || m.column]) || 0), 0);
            if (sum === 0) return row;
            const normalized = { ...row };
            config.metrics!.forEach(m => {
                const key = m.label || m.column;
                normalized[key] = Math.round(((Number(row[key]) || 0) / sum) * 1000) / 10; // percentage to 1 decimal
            });
            return normalized;
        });
    }

    // Compute reference line numeric values if needed
    const refLines = (config.reference_lines || []).map(ref => {
        if (ref.type === 'constant') return { ...ref, computedValue: ref.value };
        const primaryMetric = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';
        const vals = chartData.map(d => Number(d[primaryMetric])).filter(v => !isNaN(v));
        if (vals.length === 0) return { ...ref, computedValue: undefined };

        if (ref.type === 'average') {
            const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
            return { ...ref, computedValue: avg };
        }
        if (ref.type === 'min') return { ...ref, computedValue: Math.min(...vals) };
        if (ref.type === 'max') return { ...ref, computedValue: Math.max(...vals) };
        if (ref.type === 'median') {
            const sorted = [...vals].sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            const med = sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
            return { ...ref, computedValue: med };
        }
        return { ...ref, computedValue: undefined };
    });

    const handleBarClick = (entry: any) => {
        if (!entry) return;
        const val = entry[config.x_axis || ''] || entry.payload?.[config.x_axis || ''];
        if (val !== undefined) {
            onDataPointClick?.({ column: config.x_axis, value: val });
        }
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
                layout={isHorizontal ? 'vertical' : 'horizontal'}
                data={chartData}
                margin={{ top: 20, right: 30, left: isHorizontal ? 40 : 20, bottom: isHorizontal ? 10 : 30 }}
            >
                <defs>
                    <linearGradient id="barGlassCoral" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#fb923c" stopOpacity={0.95} />
                        <stop offset="100%" stopColor="#ea580c" stopOpacity={0.7} />
                    </linearGradient>
                    <linearGradient id="barGlassViolet" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#c084fc" stopOpacity={0.95} />
                        <stop offset="100%" stopColor="#7c3aed" stopOpacity={0.7} />
                    </linearGradient>
                    <linearGradient id="barGlassEmerald" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#34d399" stopOpacity={0.95} />
                        <stop offset="100%" stopColor="#059669" stopOpacity={0.7} />
                    </linearGradient>
                    <linearGradient id="barGlassGold" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor="#fbbf24" stopOpacity={0.95} />
                        <stop offset="100%" stopColor="#d97706" stopOpacity={0.7} />
                    </linearGradient>
                </defs>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />}

                {isHorizontal ? (
                    <>
                        <XAxis
                            type="number"
                            tick={{ fill: '#94a3b8', fontSize: 11 }}
                            axisLine={{ stroke: '#334155' }}
                            tickFormatter={is100Percent ? (v) => `${v}%` : formatAxisTick}
                        />
                        <YAxis
                            type="category"
                            dataKey={config.x_axis || undefined}
                            tick={{ fill: '#94a3b8', fontSize: 11 }}
                            axisLine={{ stroke: '#334155' }}
                            width={100}
                        />
                    </>
                ) : (
                    <>
                        <XAxis
                            dataKey={config.x_axis || undefined}
                            tick={{ fill: '#94a3b8', fontSize: 11 }}
                            axisLine={{ stroke: '#334155' }}
                            interval={0}
                            angle={chartData.length > 5 ? -25 : 0}
                            textAnchor={chartData.length > 5 ? 'end' : 'middle'}
                        />
                        <YAxis
                            tick={{ fill: '#94a3b8', fontSize: 11 }}
                            axisLine={{ stroke: '#334155' }}
                            tickFormatter={is100Percent ? (v) => `${v}%` : formatAxisTick}
                        />
                    </>
                )}

                <Tooltip
                    contentStyle={{
                        background: 'rgba(15, 23, 42, 0.92)',
                        backdropFilter: 'blur(16px)',
                        border: '1px solid rgba(255, 255, 255, 0.15)',
                        borderRadius: 10,
                        color: '#f8fafc',
                        boxShadow: '0 8px 32px rgba(0, 0, 0, 0.5)',
                    }}
                    formatter={(val: any, name: any) => [
                        is100Percent ? `${val}%` : formatValue(val),
                        String(name),
                    ]}
                />

                {config.show_legend && <Legend wrapperStyle={{ color: 'rgba(255, 255, 255, 0.65)', paddingTop: 8 }} />}

                {/* Render Reference Lines */}
                {refLines.map((ref, idx) => {
                    if (ref.computedValue === undefined) return null;
                    return (
                        <RechartsRefLine
                            key={idx}
                            y={!isHorizontal ? ref.computedValue : undefined}
                            x={isHorizontal ? ref.computedValue : undefined}
                            stroke={ref.color || '#ef4444'}
                            strokeDasharray={ref.line_style === 'dashed' ? '4 4' : undefined}
                            label={{
                                value: `${ref.label || ref.type}: ${formatCompactNumber(ref.computedValue)}`,
                                fill: ref.color || '#ef4444',
                                fontSize: 10,
                                position: isHorizontal ? 'insideTopLeft' : 'insideTopRight',
                            }}
                        />
                    );
                })}

                {/* Bars Rendering with Glass Gradients & Optical Easing */}
                {hasMultiMetrics ? (
                    config.metrics!.map((m, i) => (
                        <Bar
                            key={m.column}
                            dataKey={m.label || m.column}
                            name={(m.label || m.column).replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                            fill={i === 0 ? 'url(#barGlassCoral)' : i === 1 ? 'url(#barGlassViolet)' : colors[i % colors.length]}
                            stackId={isStacked ? 'stack' : undefined}
                            radius={isStacked ? [0, 0, 0, 0] : [6, 6, 0, 0]}
                            onClick={handleBarClick}
                            cursor="pointer"
                            animationDuration={1300}
                            animationEasing="ease-out"
                        >
                            {config.show_values && (
                                <LabelList
                                    position={isHorizontal ? 'right' : 'top'}
                                    fill="#94a3b8"
                                    fontSize={10}
                                    formatter={(val: any) => (is100Percent ? `${val}%` : formatCompactNumber(Number(val)))}
                                />
                            )}
                        </Bar>
                    ))
                ) : config.group_by ? (
                    [...new Set(chartData.map(d => d[config.group_by!]))].map((group, i) => (
                        <Bar
                            key={String(group)}
                            dataKey={String(group)}
                            fill={i === 0 ? 'url(#barGlassCoral)' : i === 1 ? 'url(#barGlassViolet)' : colors[i % colors.length]}
                            stackId={isStacked ? 'stack' : undefined}
                            radius={isStacked ? [0, 0, 0, 0] : [6, 6, 0, 0]}
                            onClick={handleBarClick}
                            cursor="pointer"
                            animationDuration={1300}
                            animationEasing="ease-out"
                        >
                            {config.show_values && (
                                <LabelList
                                    position={isHorizontal ? 'right' : 'top'}
                                    fill="#94a3b8"
                                    fontSize={10}
                                    formatter={(val: any) => formatCompactNumber(Number(val))}
                                />
                            )}
                        </Bar>
                    ))
                ) : (
                    <Bar
                        dataKey={config.y_axis || 'value'}
                        fill={colors[0]}
                        radius={[4, 4, 0, 0]}
                        onClick={handleBarClick}
                        cursor="pointer"
                    >
                        {config.show_values && (
                            <LabelList
                                position={isHorizontal ? 'right' : 'top'}
                                fill="#94a3b8"
                                fontSize={10}
                                formatter={(val: any) => formatCompactNumber(Number(val))}
                            />
                        )}
                    </Bar>
                )}

                {config.trend_line && !config.group_by && chartData.length > 1 && (
                    <Line
                        type="monotone"
                        dataKey="_trend"
                        stroke="#fbbf24"
                        strokeDasharray="5 5"
                        dot={false}
                        name="Trend"
                    />
                )}
            </ComposedChart>
        </ResponsiveContainer>
    );
};

export default DashboardBarChart;
