/**
 * FunnelChart — Recharts funnel visualization.
 */
import React from 'react';
import { FunnelChart as RechartsFunnel, Funnel, Tooltip, LabelList, ResponsiveContainer, Cell } from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const DashboardFunnelChart: React.FC<Props> = ({ config, data }) => {
    if (!data || data.length === 0) return null;

    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const nameKey = config.x_axis || Object.keys(data[0] || {})[0];
    const valueKey = config.y_axis || 'value';

    const funnelData = data.map((d, i) => ({
        name: String(d[nameKey]),
        value: Number(d[valueKey]) || 0,
        fill: colors[i % colors.length],
    }));

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsFunnel>
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }} />
                <Funnel dataKey="value" data={funnelData} isAnimationActive>
                    <LabelList position="right" fill="#94a3b8" fontSize={12} dataKey="name" />
                    <LabelList position="center" fill="#fff" fontSize={13} fontWeight={600} dataKey="value" />
                    {funnelData.map((entry, i) => (
                        <Cell key={i} fill={entry.fill} />
                    ))}
                </Funnel>
            </RechartsFunnel>
        </ResponsiveContainer>
    );
};

export default DashboardFunnelChart;
