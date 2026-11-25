/**
 * RibbonChart — Power BI Ribbon Chart visual.
 * Connects ranked categorical slices across time periods, visualizing changes in rank.
 */
import React from 'react';
import {
    AreaChart, Area, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatValue, formatAxisTick } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardRibbonChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    const chartData = (data || []).filter(d => {
        if (config.x_axis && (d[config.x_axis] === null || d[config.x_axis] === undefined)) return false;
        return true;
    });

    const groupKey = config.group_by || 'category';
    const groups = [...new Set(chartData.map(d => d[groupKey]))].filter(Boolean);

    // Pivot table for Recharts
    const pivoted: Record<string, any> = {};
    chartData.forEach(row => {
        const x = row[config.x_axis!];
        if (!pivoted[x]) pivoted[x] = { [config.x_axis!]: x };
        pivoted[x][row[groupKey]] = row[config.y_axis || 'value'];
    });
    const displayData = Object.values(pivoted);

    const handlePointClick = (entry: any) => {
        if (!entry) return;
        const val = entry[config.x_axis || ''] || entry.payload?.[config.x_axis || ''];
        if (val !== undefined) {
            onDataPointClick?.({ column: config.x_axis, value: val });
        }
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={displayData} margin={{ top: 20, right: 30, left: 20, bottom: 25 }}>
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

                {groups.map((g, i) => (
                    <Area
                        key={String(g)}
                        type="monotone"
                        dataKey={String(g)}
                        stackId="ribbon"
                        stroke={colors[i % colors.length]}
                        fill={colors[i % colors.length]}
                        fillOpacity={0.65}
                        strokeWidth={2}
                        activeDot={{ r: 5, onClick: (_, e: any) => handlePointClick(e?.payload) }}
                    />
                ))}
            </AreaChart>
        </ResponsiveContainer>
    );
};

export default DashboardRibbonChart;
