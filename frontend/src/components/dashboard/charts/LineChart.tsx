/**
 * LineChart — Recharts line chart with multi-metric support, reference lines, forecast bounds, and cross-filtering.
 */
import React from 'react';
import {
    LineChart as RechartsLine, Line, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer, ReferenceLine as RechartsRefLine,
} from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatCompactNumber, formatValue, formatAxisTick } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, unknown>[];
    onDataPointClick?: (params: { column?: string; value: unknown }) => void;
}

const DashboardLineChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    const chartData = (data || []).filter(d => {
        if (config.x_axis && (d[config.x_axis] === null || d[config.x_axis] === undefined)) return false;
        return true;
    });

    const hasMultiMetrics = Boolean(config.metrics && config.metrics.length > 0);
    const groups = config.group_by
        ? [...new Set(chartData.map(d => d[config.group_by!]))]
        : [];

    // Pivot data if grouped
    let displayData = chartData;
    if (config.group_by && config.x_axis) {
        const map = new Map<string, Record<string, unknown>>();
        chartData.forEach(row => {
            const x = String(row[config.x_axis!] ?? '');
            if (!map.has(x)) map.set(x, { ...row });
            map.get(x)![String(row[config.group_by!])] = row[config.y_axis || 'value'];
        });
        displayData = Array.from(map.values());
    }

    // Trendline computation
    if (config.trend_line && config.y_axis && !config.group_by && displayData.length > 1) {
        const yVals = displayData.map((d, i) => ({ x: i, y: Number(d[config.y_axis!]) || 0 }));
        const n = yVals.length;
        const sumX = yVals.reduce((a, v) => a + v.x, 0);
        const sumY = yVals.reduce((a, v) => a + v.y, 0);
        const sumXY = yVals.reduce((a, v) => a + v.x * v.y, 0);
        const sumX2 = yVals.reduce((a, v) => a + v.x * v.x, 0);
        const denom = (n * sumX2 - sumX * sumX);
        if (denom !== 0) {
            const slope = (n * sumXY - sumX * sumY) / denom;
            const intercept = (sumY - slope * sumX) / n;
            displayData = displayData.map((d, i) => ({
                ...d,
                _trend: Math.round((intercept + slope * i) * 100) / 100,
            }));
        }
    }

    // Compute reference lines
    const refLines = (config.reference_lines || []).map(ref => {
        if (ref.type === 'constant') return { ...ref, computedValue: ref.value };
        const primaryMetric = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';
        const vals = displayData.map(d => Number(d[primaryMetric])).filter(v => !isNaN(v));
        if (vals.length === 0) return { ...ref, computedValue: undefined };

        if (ref.type === 'average') {
            return { ...ref, computedValue: vals.reduce((a, b) => a + b, 0) / vals.length };
        }
        if (ref.type === 'min') return { ...ref, computedValue: Math.min(...vals) };
        if (ref.type === 'max') return { ...ref, computedValue: Math.max(...vals) };
        if (ref.type === 'median') {
            const sorted = [...vals].sort((a, b) => a - b);
            const mid = Math.floor(sorted.length / 2);
            return { ...ref, computedValue: sorted[mid] };
        }
        return { ...ref, computedValue: undefined };
    });

    const handlePointClick = (entry: Record<string, unknown> | null | undefined) => {
        if (!entry) return;
        const val = entry[config.x_axis || ''] ?? (entry.payload as Record<string, unknown> | undefined)?.[config.x_axis || ''];
        if (val !== undefined) {
            onDataPointClick?.({ column: config.x_axis, value: val });
        }
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsLine data={displayData} margin={{ top: 20, right: 30, left: 20, bottom: 25 }}>
                <defs>
                    <linearGradient id="watercolorLineGrad" x1="0" y1="0" x2="1" y2="0">
                        <stop offset="0%" stopColor="#f43f5e" />
                        <stop offset="50%" stopColor="#fb923c" />
                        <stop offset="100%" stopColor="#fbbf24" />
                    </linearGradient>
                </defs>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />}
                <XAxis
                    dataKey={config.x_axis || undefined}
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    interval={0}
                    angle={displayData.length > 6 ? -25 : 0}
                    textAnchor={displayData.length > 6 ? 'end' : 'middle'}
                />
                <YAxis
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    tickFormatter={formatAxisTick}
                />
                <Tooltip
                    contentStyle={{
                        background: 'rgba(15, 23, 42, 0.94)',
                        backdropFilter: 'blur(16px)',
                        border: '1px solid rgba(255, 255, 255, 0.16)',
                        borderRadius: 10,
                        color: '#f8fafc',
                        boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
                    }}
                    formatter={(val: unknown, name: unknown) => [formatValue(val), String(name ?? '')]}
                />
                {config.show_legend && <Legend wrapperStyle={{ color: 'rgba(255, 255, 255, 0.65)', paddingTop: 8 }} />}

                {/* Reference Lines */}
                {refLines.map((ref, idx) => {
                    if (ref.computedValue === undefined) return null;
                    return (
                        <RechartsRefLine
                            key={idx}
                            y={ref.computedValue}
                            stroke={ref.color || '#ef4444'}
                            strokeDasharray={ref.line_style === 'dashed' ? '4 4' : undefined}
                            label={{
                                value: `${ref.label || ref.type}: ${formatCompactNumber(ref.computedValue)}`,
                                fill: ref.color || '#ef4444',
                                fontSize: 10,
                                position: 'insideTopRight',
                            }}
                        />
                    );
                })}

                {/* Multi-Metric Lines */}
                {hasMultiMetrics ? (
                    config.metrics!.map((m, i) => (
                        <Line
                            key={m.column}
                            type={config.smooth ? 'monotone' : 'linear'}
                            dataKey={m.label || m.column}
                            name={(m.label || m.column).replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                            stroke={colors[i % colors.length]}
                            strokeWidth={2.5}
                            dot={{ r: 3, fill: colors[i % colors.length] }}
                            activeDot={{ r: 6, strokeWidth: 2, onClick: (_, e: { payload?: Record<string, unknown> }) => handlePointClick(e?.payload) }}
                        />
                    ))
                ) : config.group_by ? (
                    groups.map((group, i) => (
                        <Line
                            key={String(group)}
                            type={config.smooth ? 'monotone' : 'linear'}
                            dataKey={String(group)}
                            stroke={colors[i % colors.length]}
                            strokeWidth={2}
                            dot={{ r: 3, fill: colors[i % colors.length] }}
                            activeDot={{ r: 6, strokeWidth: 2, onClick: (_, e: { payload?: Record<string, unknown> }) => handlePointClick(e?.payload) }}
                        />
                    ))
                ) : (
                    <Line
                        type={config.smooth ? 'monotone' : 'linear'}
                        dataKey={config.y_axis || 'value'}
                        stroke={colors[0]}
                        strokeWidth={2.5}
                        dot={{ r: 3, fill: colors[0] }}
                        activeDot={{ r: 6, strokeWidth: 2, onClick: (_, e: { payload?: Record<string, unknown> }) => handlePointClick(e?.payload) }}
                    />
                )}

                {config.trend_line && !config.group_by && displayData.length > 1 && (
                    <Line
                        type="linear"
                        dataKey="_trend"
                        stroke="#fbbf24"
                        strokeDasharray="5 5"
                        dot={false}
                        name="Trend"
                    />
                )}
            </RechartsLine>
        </ResponsiveContainer>
    );
};

export default DashboardLineChart;
