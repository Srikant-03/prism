/**
 * AreaChart — Recharts stacked/regular area chart.
 */
import React from 'react';
import {
    AreaChart as RechartsArea, Area, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import { COLOR_PALETTES } from "../../../types/dashboard";
import type { ChartConfig } from "../../../types/dashboard";

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const DashboardAreaChart: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const groups = config.group_by
        ? [...new Set(data.map(d => d[config.group_by!]))]
        : [config.y_axis || 'value'];

    let chartData = data;
    if (config.group_by && config.x_axis) {
        const pivoted: Record<string, any> = {};
        data.forEach(row => {
            const x = row[config.x_axis!];
            if (!pivoted[x]) pivoted[x] = { [config.x_axis!]: x };
            pivoted[x][row[config.group_by!]] = row[config.y_axis || 'value'];
        });
        chartData = Object.values(pivoted);
    }

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsArea data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />}
                <XAxis dataKey={config.x_axis || undefined} tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }} />
                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8' }} />}
                {(config.group_by ? groups : [config.y_axis || 'value']).map((key, i) => (
                    <Area
                        key={String(key)}
                        type={config.smooth ? 'monotone' : 'linear'}
                        dataKey={String(key)}
                        stackId={config.stacked ? 'stack' : undefined}
                        stroke={colors[i % colors.length]}
                        fill={colors[i % colors.length]}
                        fillOpacity={0.3}
                    />
                ))}
            </RechartsArea>
        </ResponsiveContainer>
    );
};

export default DashboardAreaChart;
