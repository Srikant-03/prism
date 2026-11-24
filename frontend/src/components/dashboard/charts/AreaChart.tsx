/**
 * AreaChart — Recharts stacked and regular area chart with multi-metric and cross-filtering support.
 */
import React from 'react';
import {
    AreaChart as RechartsArea, Area, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import { COLOR_PALETTES } from '../../../types/dashboard';
import type { ChartConfig } from '../../../types/dashboard';
import { formatValue, formatAxisTick } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardAreaChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const isStacked = config.stacked || config.chart_type === 'stacked_area';
    const hasMultiMetrics = Boolean(config.metrics && config.metrics.length > 0);

    const chartData = (data || []).filter(d => {
        if (config.x_axis && (d[config.x_axis] === null || d[config.x_axis] === undefined)) return false;
        return true;
    });

    const groups = config.group_by
        ? [...new Set(chartData.map(d => d[config.group_by!]))]
        : [];

    let displayData = chartData;
    if (config.group_by && config.x_axis) {
        const pivoted: Record<string, any> = {};
        chartData.forEach(row => {
            const x = row[config.x_axis!];
            if (!pivoted[x]) pivoted[x] = { [config.x_axis!]: x };
            pivoted[x][row[config.group_by!]] = row[config.y_axis || 'value'];
        });
        displayData = Object.values(pivoted);
    }

    const handlePointClick = (entry: any) => {
        if (!entry) return;
        const val = entry[config.x_axis || ''] || entry.payload?.[config.x_axis || ''];
        if (val !== undefined) {
            onDataPointClick?.({ column: config.x_axis, value: val });
        }
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsArea data={displayData} margin={{ top: 20, right: 30, left: 20, bottom: 25 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />}
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
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                    formatter={(val: any, name: any) => [formatValue(val), String(name)]}
                />
                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8', paddingTop: 8 }} />}

                {hasMultiMetrics ? (
                    config.metrics!.map((m, i) => (
                        <Area
                            key={m.column}
                            type={config.smooth ? 'monotone' : 'linear'}
                            dataKey={m.label || m.column}
                            name={(m.label || m.column).replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                            stackId={isStacked ? 'stack' : undefined}
                            stroke={colors[i % colors.length]}
                            fill={colors[i % colors.length]}
                            fillOpacity={0.25}
                            activeDot={{ r: 5, onClick: (_, e: any) => handlePointClick(e?.payload) }}
                        />
                    ))
                ) : config.group_by ? (
                    groups.map((key, i) => (
                        <Area
                            key={String(key)}
                            type={config.smooth ? 'monotone' : 'linear'}
                            dataKey={String(key)}
                            stackId={isStacked ? 'stack' : undefined}
                            stroke={colors[i % colors.length]}
                            fill={colors[i % colors.length]}
                            fillOpacity={0.25}
                            activeDot={{ r: 5, onClick: (_, e: any) => handlePointClick(e?.payload) }}
                        />
                    ))
                ) : (
                    <Area
                        type={config.smooth ? 'monotone' : 'linear'}
                        dataKey={config.y_axis || 'value'}
                        stroke={colors[0]}
                        fill={colors[0]}
                        fillOpacity={0.25}
                        activeDot={{ r: 5, onClick: (_, e: any) => handlePointClick(e?.payload) }}
                    />
                )}
            </RechartsArea>
        </ResponsiveContainer>
    );
};

export default DashboardAreaChart;
