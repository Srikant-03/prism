/**
 * HeatmapChart — ECharts heatmap since Recharts lacks native heatmap.
 */
import React, { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import { ChartConfig, COLOR_PALETTES } from '../../types/dashboard';

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const DashboardHeatmapChart: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    const option = useMemo(() => {
        const xKey = config.x_axis || Object.keys(data[0] || {})[0];
        const yKey = config.group_by || config.y_axis || Object.keys(data[0] || {})[1];
        const valueKey = config.y_axis || 'value';

        const xLabels = [...new Set(data.map(d => String(d[xKey])))];
        const yLabels = [...new Set(data.map(d => String(d[yKey])))];

        const heatData = data.map(d => [
            xLabels.indexOf(String(d[xKey])),
            yLabels.indexOf(String(d[yKey])),
            Number(d[valueKey]) || 0,
        ]);

        const values = heatData.map(d => d[2]);
        const min = Math.min(...values);
        const max = Math.max(...values);

        return {
            tooltip: { position: 'top', backgroundColor: '#1e293b', borderColor: '#334155', textStyle: { color: '#e2e8f0' } },
            grid: { top: 40, right: 30, bottom: 60, left: 80 },
            xAxis: { type: 'category', data: xLabels, axisLabel: { color: '#94a3b8', fontSize: 11, rotate: 45 }, axisLine: { lineStyle: { color: '#334155' } } },
            yAxis: { type: 'category', data: yLabels, axisLabel: { color: '#94a3b8', fontSize: 11 }, axisLine: { lineStyle: { color: '#334155' } } },
            visualMap: { min, max, calculable: true, orient: 'horizontal', left: 'center', bottom: 0, inRange: { color: [colors[7] || '#e2e8f0', colors[0]] }, textStyle: { color: '#94a3b8' } },
            series: [{
                type: 'heatmap',
                data: heatData,
                label: { show: config.show_values, color: '#fff', fontSize: 10 },
                emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.5)' } },
            }],
        };
    }, [config, data, colors]);

    return <ReactECharts option={option} style={{ width: '100%', height: '100%' }} />;
};

export default DashboardHeatmapChart;
