/**
 * TreemapChart — Recharts treemap.
 */
import React from 'react';
import { Treemap, ResponsiveContainer, Tooltip } from 'recharts';
import { COLOR_PALETTES } from "../../../types/dashboard";
import type { ChartConfig } from "../../../types/dashboard";

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const DashboardTreemapChart: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const nameKey = config.x_axis || config.group_by || Object.keys(data[0] || {})[0];
    const valueKey = config.y_axis || 'value';

    const treeData = data.map((d, i) => ({
        name: String(d[nameKey]),
        size: Number(d[valueKey]) || 0,
        fill: colors[i % colors.length],
    }));

    const CustomContent = (props: any) => {
        const { x, y, width, height, name, fill } = props;
        if (width < 30 || height < 20) return null;
        return (
            <g>
                <rect x={x} y={y} width={width} height={height} fill={fill} stroke="#0f172a" strokeWidth={2} rx={4} />
                <text x={x + width / 2} y={y + height / 2} textAnchor="middle" dominantBaseline="central" fill="#fff" fontSize={Math.min(12, width / 6)} fontWeight={600}>
                    {String(name).slice(0, 15)}
                </text>
            </g>
        );
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <Treemap data={treeData} dataKey="size" nameKey="name" content={<CustomContent />}>
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }} />
            </Treemap>
        </ResponsiveContainer>
    );
};

export default DashboardTreemapChart;
