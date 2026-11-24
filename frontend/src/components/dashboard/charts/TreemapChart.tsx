/**
 * TreemapChart — Recharts Treemap visualizer with cell styling and cross-filtering.
 */
import React from 'react';
import { Treemap, ResponsiveContainer, Tooltip } from 'recharts';
import { COLOR_PALETTES } from '../../../types/dashboard';
import type { ChartConfig } from '../../../types/dashboard';
import { formatCompactNumber, formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

// Stable top-level custom content component to prevent React Hooks warnings
const CustomTreemapContent = (props: any) => {
    const { x, y, width, height, name, size, fill, onCellClick } = props;
    if (width < 30 || height < 20) return null;

    return (
        <g
            style={{ cursor: 'pointer' }}
            onClick={() => onCellClick?.(name)}
        >
            <rect
                x={x}
                y={y}
                width={width}
                height={height}
                fill={fill}
                stroke="#0f172a"
                strokeWidth={2}
                rx={6}
            />
            <text
                x={x + width / 2}
                y={y + height / 2 - 6}
                textAnchor="middle"
                dominantBaseline="central"
                fill="#ffffff"
                fontSize={Math.min(12, Math.max(9, width / 8))}
                fontWeight={600}
            >
                {String(name).slice(0, 16)}
            </text>
            <text
                x={x + width / 2}
                y={y + height / 2 + 8}
                textAnchor="middle"
                dominantBaseline="central"
                fill="#cbd5e1"
                fontSize={Math.min(11, Math.max(8, width / 9))}
            >
                {formatCompactNumber(size)}
            </text>
        </g>
    );
};

const DashboardTreemapChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const nameKey = config.x_axis || config.group_by || Object.keys(data[0] || {})[0];
    const valueKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';

    const treeData = (data || []).map((d, i) => ({
        name: String(d[nameKey] ?? 'Unknown'),
        size: Number(d[valueKey]) || 0,
        fill: colors[i % colors.length],
    }));

    const handleCellClick = (name: string) => {
        onDataPointClick?.({ column: nameKey, value: name });
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <Treemap
                data={treeData}
                dataKey="size"
                nameKey="name"
                content={<CustomTreemapContent onCellClick={handleCellClick} />}
            >
                <Tooltip
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                    formatter={(val: any, name: any) => [formatValue(val), String(name)]}
                />
            </Treemap>
        </ResponsiveContainer>
    );
};

export default DashboardTreemapChart;
