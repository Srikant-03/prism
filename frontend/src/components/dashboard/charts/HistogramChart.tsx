/**
 * HistogramChart — Statistical Frequency Distribution Visualizer.
 * Automatically computes numeric bins and frequency counts.
 */
import React from 'react';
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid,
    Tooltip, ResponsiveContainer,
} from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatCompactNumber } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardHistogramChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const colKey = config.x_axis || config.y_axis || Object.keys(data[0] || {})[0];

    const values = (data || [])
        .map(d => Number(d[colKey]))
        .filter(v => !isNaN(v) && v !== null);

    if (values.length === 0) {
        return (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#94a3b8' }}>
                No numeric data for histogram
            </div>
        );
    }

    const min = Math.min(...values);
    const max = Math.max(...values);
    const numBins = Math.min(10, Math.max(5, Math.ceil(Math.sqrt(values.length))));
    const binWidth = (max - min) / numBins || 1;

    const bins = Array.from({ length: numBins }, (_, i) => {
        const binStart = min + i * binWidth;
        const binEnd = min + (i + 1) * binWidth;
        return {
            label: `${formatCompactNumber(binStart)} - ${formatCompactNumber(binEnd)}`,
            count: 0,
            start: binStart,
            end: binEnd,
        };
    });

    values.forEach(v => {
        const idx = Math.min(numBins - 1, Math.floor((v - min) / binWidth));
        if (bins[idx]) bins[idx].count += 1;
    });

    const handleClick = (entry: any) => {
        if (!entry) return;
        onDataPointClick?.({ column: colKey, value: entry.label });
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <BarChart data={bins} margin={{ top: 20, right: 30, left: 20, bottom: 25 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />}
                <XAxis
                    dataKey="label"
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    interval={0}
                    angle={-20}
                    textAnchor="end"
                />
                <YAxis
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    allowDecimals={false}
                />
                <Tooltip
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                    formatter={(val: any) => [`${val} records`, 'Frequency']}
                />
                <Bar
                    dataKey="count"
                    name="Frequency"
                    fill={colors[0]}
                    radius={[4, 4, 0, 0]}
                    onClick={handleClick}
                    cursor="pointer"
                />
            </BarChart>
        </ResponsiveContainer>
    );
};

export default DashboardHistogramChart;
