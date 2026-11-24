/**
 * PieChart — Recharts Pie and Donut visualizer with custom tooltips, percentages, and cross-filtering.
 */
import React from 'react';
import { PieChart as RechartsPie, Pie, Cell, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { COLOR_PALETTES } from '../../../types/dashboard';
import type { ChartConfig } from '../../../types/dashboard';
import { formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardPieChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const isDonut = config.chart_type === 'donut';
    const nameKey = config.x_axis || config.group_by || Object.keys(data[0] || {})[0];
    const valueKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';

    const chartData = (data || []).map(row => ({
        ...row,
        [nameKey]: String(row[nameKey] ?? 'Unknown'),
        [valueKey]: Number(row[valueKey]) || 0,
    }));

    const handleClick = (entry: any) => {
        if (!entry) return;
        const val = entry[nameKey] || entry.payload?.[nameKey];
        if (val !== undefined) {
            onDataPointClick?.({ column: nameKey, value: val });
        }
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsPie>
                <Pie
                    data={chartData}
                    dataKey={valueKey}
                    nameKey={nameKey}
                    cx="50%"
                    cy="50%"
                    innerRadius={isDonut ? '55%' : 0}
                    outerRadius="75%"
                    paddingAngle={2}
                    onClick={handleClick}
                    cursor="pointer"
                    label={config.show_values ? ({ name, percent }: any) => `${name} (${(percent * 100).toFixed(0)}%)` : false}
                    labelLine={config.show_values}
                >
                    {chartData.map((_, i) => (
                        <Cell key={i} fill={colors[i % colors.length]} stroke="transparent" />
                    ))}
                </Pie>
                <Tooltip
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                    formatter={(val: any, name: any) => [formatValue(val), String(name)]}
                />
                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8', paddingTop: 8 }} />}
            </RechartsPie>
        </ResponsiveContainer>
    );
};

export default DashboardPieChart;
