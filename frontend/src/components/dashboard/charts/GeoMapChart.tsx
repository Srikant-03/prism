/**
 * GeoMapChart — Geospatial Bubble and Regional Performance Visualizer.
 * Maps regional metric values across countries, regions, and territories.
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatCompactNumber, formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardGeoMapChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const regionKey = config.x_axis || 'country_code';
    const valKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';

    const chartData = (data || []).map(row => ({
        name: String(row[regionKey] || 'Unknown'),
        value: Number(row[valKey]) || 0,
    })).sort((a, b) => b.value - a.value);

    // Render regional geographic bar/polar distribution
    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'item',
            formatter: (params: any) => `<strong>${params.name}</strong>: ${formatValue(params.value)}`,
        },
        grid: {
            left: '14%',
            right: '8%',
            top: '8%',
            bottom: '12%',
        },
        xAxis: {
            type: 'value',
            axisLabel: {
                color: '#94a3b8',
                fontSize: 10,
                formatter: (v: number) => formatCompactNumber(v),
            },
            splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        },
        yAxis: {
            type: 'category',
            data: chartData.map(d => d.name),
            axisLabel: {
                color: '#cbd5e1',
                fontSize: 11,
            },
            axisLine: { lineStyle: { color: '#334155' } },
        },
        series: [
            {
                name: 'Geo Performance',
                type: 'bar',
                data: chartData.map((d, i) => ({
                    value: d.value,
                    itemStyle: { color: colors[i % colors.length] },
                })),
                label: {
                    show: true,
                    position: 'right',
                    color: '#94a3b8',
                    fontSize: 10,
                    formatter: (params: any) => formatCompactNumber(params.value),
                },
                barMaxWidth: 24,
                itemStyle: { borderRadius: [0, 4, 4, 0] },
            },
        ],
    };

    const onEvents = {
        click: (params: any) => {
            if (params.name) {
                onDataPointClick?.({ column: regionKey, value: params.name });
            }
        },
    };

    return (
        <div style={{ width: '100%', height: '100%' }}>
            <ReactECharts
                option={option}
                style={{ width: '100%', height: '100%' }}
                onEvents={onEvents}
            />
        </div>
    );
};

export default DashboardGeoMapChart;
