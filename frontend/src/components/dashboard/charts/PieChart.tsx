/**
 * PieChart — Recharts pie/donut chart.
 */
import React from 'react';
import { PieChart as RechartsPie, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { COLOR_PALETTES } from "../../../types/dashboard";
import type { ChartConfig } from "../../../types/dashboard";

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const DashboardPieChart: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const isDonut = config.chart_type === 'donut';
    const nameKey = config.x_axis || config.group_by || Object.keys(data[0] || {})[0];
    const valueKey = config.y_axis || 'value';

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsPie>
                <Pie
                    data={data}
                    dataKey={valueKey}
                    nameKey={nameKey}
                    cx="50%" cy="50%"
                    innerRadius={isDonut ? '50%' : 0}
                    outerRadius="75%"
                    paddingAngle={2}
                    label={config.show_values ? ({ name, percent }: any) => `${name} ${(percent * 100).toFixed(0)}%` : false}
                    labelLine={config.show_values}
                >
                    {data.map((_, i) => (
                        <Cell key={i} fill={colors[i % colors.length]} stroke="transparent" />
                    ))}
                </Pie>
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }} />
                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8' }} />}
            </RechartsPie>
        </ResponsiveContainer>
    );
};

export default DashboardPieChart;
