/**
 * FunnelChart — Recharts Funnel visualization with drop-off rates and cross-filtering.
 */
import React from 'react';
import { FunnelChart as RechartsFunnel, Funnel, Tooltip, LabelList, ResponsiveContainer, Cell } from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatCompactNumber, formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardFunnelChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    if (!data || data.length === 0) return null;

    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const nameKey = config.x_axis || Object.keys(data[0] || {})[0];
    const valueKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';

    const maxVal = Math.max(...data.map(d => Number(d[valueKey]) || 0), 1);

    const funnelData = data.map((d, i) => {
        const val = Number(d[valueKey]) || 0;
        const pct = Math.round((val / maxVal) * 100);
        return {
            name: String(d[nameKey]),
            value: val,
            percentage: `${pct}%`,
            fill: colors[i % colors.length],
        };
    });

    const handleClick = (entry: any) => {
        if (!entry) return;
        const val = entry.name;
        if (val !== undefined) {
            onDataPointClick?.({ column: nameKey, value: val });
        }
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsFunnel>
                <Tooltip
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                    formatter={(val: any, name: any) => [formatValue(val), String(name)]}
                />
                <Funnel dataKey="value" data={funnelData} isAnimationActive onClick={handleClick} cursor="pointer">
                    <LabelList position="right" fill="#94a3b8" fontSize={11} dataKey="name" />
                    <LabelList position="center" fill="#ffffff" fontSize={12} fontWeight={600} formatter={(v: any) => formatCompactNumber(Number(v))} />
                    {funnelData.map((entry, i) => (
                        <Cell key={i} fill={entry.fill} />
                    ))}
                </Funnel>
            </RechartsFunnel>
        </ResponsiveContainer>
    );
};

export default DashboardFunnelChart;
