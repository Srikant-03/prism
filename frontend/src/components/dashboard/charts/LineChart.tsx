/**
 * LineChart — Recharts line chart with optional trend line and smooth curves.
 */
import React from 'react';
import {
    LineChart as RechartsLine, Line, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
}

const DashboardLineChart: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    const groups = config.group_by
        ? [...new Set(data.map(d => d[config.group_by!]))]
        : [config.y_axis || 'value'];

    let chartData = data;
    if (config.group_by && config.x_axis) {
        const map = new Map<any, any>();
        data.forEach(row => {
            const x = row[config.x_axis!];
            if (!map.has(x)) map.set(x, { ...row });
            map.get(x)[row[config.group_by!]] = row[config.y_axis || 'value'];
        });
        chartData = Array.from(map.values());
    }

    if (config.trend_line && config.y_axis && !config.group_by && chartData.length > 1) {
        const yVals = chartData.map((d, i) => ({ x: i, y: Number(d[config.y_axis!]) || 0 }));
        const n = yVals.length;
        const sumX = yVals.reduce((a, v) => a + v.x, 0);
        const sumY = yVals.reduce((a, v) => a + v.y, 0);
        const sumXY = yVals.reduce((a, v) => a + v.x * v.y, 0);
        const sumX2 = yVals.reduce((a, v) => a + v.x * v.x, 0);
        const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        const intercept = (sumY - slope * sumX) / n;

        chartData = chartData.map((d, i) => ({
            ...d,
            _trend: intercept + slope * i,
        }));
    }

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsLine data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />}
                <XAxis dataKey={config.x_axis || undefined} tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }} />
                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8' }} />}

                {config.group_by ? (
                    groups.map((group, i) => (
                        <Line
                            key={String(group)}
                            type={config.smooth ? 'monotone' : 'linear'}
                            dataKey={String(group)}
                            stroke={colors[i % colors.length]}
                            strokeWidth={2}
                            dot={{ r: 3, fill: colors[i % colors.length] }}
                            activeDot={{ r: 5, strokeWidth: 2 }}
                        />
                    ))
                ) : (
                    <Line
                        type={config.smooth ? 'monotone' : 'linear'}
                        dataKey={config.y_axis || 'value'}
                        stroke={colors[0]}
                        strokeWidth={2}
                        dot={{ r: 3, fill: colors[0] }}
                        activeDot={{ r: 5, strokeWidth: 2 }}
                    />
                )}

                {config.trend_line && !config.group_by && chartData.length > 1 && (
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
