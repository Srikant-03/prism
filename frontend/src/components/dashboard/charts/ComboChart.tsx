/**
 * ComboChart — Power BI-style Dual-Axis Combination Chart (Column + Line / Stacked + Line).
 * Maps primary volume metrics to Bar/Column on Left Y-Axis, and rate/margin metrics to Line on Right Y-Axis.
 */
import React from 'react';
import {
    ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatCompactNumber, formatValue, formatAxisTick } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardComboChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    const chartData = (data || []).filter(d => {
        if (config.x_axis && (d[config.x_axis] === null || d[config.x_axis] === undefined)) return false;
        return true;
    });

    const metrics = config.metrics || [];
    // Primary metrics (bars): those with y_axis_index === 0 or first metric
    // Secondary metrics (lines): those with y_axis_index === 1 or second metric
    const barMetrics = metrics.length > 1
        ? metrics.filter((m, i) => m.y_axis_index === 0 || (m.y_axis_index === undefined && i === 0))
        : metrics.length === 1
        ? [metrics[0]]
        : [{ column: config.y_axis || 'value', aggregation: config.aggregation, label: config.y_axis || 'Value' }];

    const lineMetrics = metrics.length > 1
        ? metrics.filter((m, i) => m.y_axis_index === 1 || (m.y_axis_index === undefined && i > 0))
        : config.y_axis_secondary
        ? [{ column: config.y_axis_secondary, aggregation: 'avg', label: config.y_axis_secondary }]
        : [];

    const handlePointClick = (entry: any) => {
        if (!entry) return;
        const val = entry[config.x_axis || ''] || entry.payload?.[config.x_axis || ''];
        if (val !== undefined) {
            onDataPointClick?.({ column: config.x_axis, value: val });
        }
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 25 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />}

                <XAxis
                    dataKey={config.x_axis || undefined}
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    interval={0}
                    angle={chartData.length > 6 ? -25 : 0}
                    textAnchor={chartData.length > 6 ? 'end' : 'middle'}
                />

                {/* Left Y-Axis (Volume / Primary) */}
                <YAxis
                    yAxisId="left"
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    tickFormatter={formatAxisTick}
                />

                {/* Right Y-Axis (Rate / Secondary) */}
                <YAxis
                    yAxisId="right"
                    orientation="right"
                    tick={{ fill: '#818cf8', fontSize: 11 }}
                    axisLine={{ stroke: '#4f46e5' }}
                    tickFormatter={(v) => (Math.abs(v) <= 1 && v !== 0 ? `${(v * 100).toFixed(0)}%` : formatCompactNumber(v))}
                />

                <Tooltip
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                    formatter={(val: any, name: any) => [formatValue(val), String(name)]}
                />

                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8', paddingTop: 8 }} />}

                {/* Primary Bars */}
                {barMetrics.map((m, i) => (
                    <Bar
                        key={m.column}
                        yAxisId="left"
                        dataKey={m.label || m.column}
                        name={(m.label || m.column).replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                        fill={colors[i % colors.length]}
                        radius={[4, 4, 0, 0]}
                        onClick={handlePointClick}
                        cursor="pointer"
                    />
                ))}

                {/* Secondary Lines */}
                {lineMetrics.map((m, i) => (
                    <Line
                        key={m.column}
                        yAxisId="right"
                        type={config.smooth ? 'monotone' : 'linear'}
                        dataKey={m.label || m.column}
                        name={(m.label || m.column).replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                        stroke={colors[(barMetrics.length + i) % colors.length]}
                        strokeWidth={2.5}
                        dot={{ r: 4, fill: colors[(barMetrics.length + i) % colors.length] }}
                        activeDot={{ r: 6, onClick: (_, e: any) => handlePointClick(e?.payload) }}
                    />
                ))}
            </ComposedChart>
        </ResponsiveContainer>
    );
};

export default DashboardComboChart;
