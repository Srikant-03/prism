/**
 * BarChart — Recharts bar chart with grouped/stacked variants.
 */
import React from 'react';
import {
    BarChart as RechartsBar, Bar, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer, ReferenceLine, LabelList,
} from 'recharts';
import { ChartConfig, COLOR_PALETTES } from '../../types/dashboard';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
}

const DashboardBarChart: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    // If grouped, get unique group values
    const groups = config.group_by
        ? [...new Set(data.map(d => d[config.group_by!]))]
        : [config.y_axis || 'value'];

    // Pivot data for grouped bars
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

    // Compute trend line (simple linear regression on y values)
    let trendSlope = 0, trendIntercept = 0;
    if (config.trend_line && config.y_axis && !config.group_by) {
        const yVals = chartData.map((d, i) => ({ x: i, y: Number(d[config.y_axis!]) || 0 }));
        const n = yVals.length;
        if (n > 1) {
            const sumX = yVals.reduce((a, v) => a + v.x, 0);
            const sumY = yVals.reduce((a, v) => a + v.y, 0);
            const sumXY = yVals.reduce((a, v) => a + v.x * v.y, 0);
            const sumX2 = yVals.reduce((a, v) => a + v.x * v.x, 0);
            trendSlope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
            trendIntercept = (sumY - trendSlope * sumX) / n;
        }
    }

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsBar data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />}
                <XAxis
                    dataKey={config.x_axis || undefined}
                    tick={{ fill: '#94a3b8', fontSize: 12 }}
                    axisLine={{ stroke: '#334155' }}
                />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                <Tooltip
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                />
                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8' }} />}

                {config.group_by ? (
                    groups.map((group, i) => (
                        <Bar
                            key={String(group)}
                            dataKey={String(group)}
                            fill={colors[i % colors.length]}
                            stackId={config.stacked ? 'stack' : undefined}
                            radius={[4, 4, 0, 0]}
                        >
                            {config.show_values && <LabelList position="top" fill="#94a3b8" fontSize={11} />}
                        </Bar>
                    ))
                ) : (
                    <Bar
                        dataKey={config.y_axis || 'value'}
                        fill={colors[0]}
                        radius={[4, 4, 0, 0]}
                    >
                        {config.show_values && <LabelList position="top" fill="#94a3b8" fontSize={11} />}
                    </Bar>
                )}

                {config.trend_line && !config.group_by && chartData.length > 1 && (
                    <ReferenceLine
                        y={trendIntercept + trendSlope * (chartData.length / 2)}
                        stroke="#fbbf24"
                        strokeDasharray="5 5"
                        label={{ value: 'Trend', fill: '#fbbf24', fontSize: 11 }}
                    />
                )}
            </RechartsBar>
        </ResponsiveContainer>
    );
};

export default DashboardBarChart;
