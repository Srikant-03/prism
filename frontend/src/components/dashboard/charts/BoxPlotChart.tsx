/**
 * BoxPlotChart — Apache ECharts Statistical Box & Whisker Visualizer.
 * Renders distributions with Min, 25th percentile (Q1), Median, 75th percentile (Q3), Max, and Outliers.
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import type { ChartConfig } from '../../../types/dashboard';
import { formatCompactNumber } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardBoxPlotChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const catKey = config.x_axis || config.group_by || 'category';
    const valKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';

    // Group numeric values by category
    const catMap = new Map<string, number[]>();
    data.forEach(row => {
        const cat = String(row[catKey] ?? 'All');
        const v = Number(row[valKey]);
        if (!isNaN(v) && row[valKey] !== null) {
            if (!catMap.has(cat)) catMap.set(cat, []);
            catMap.get(cat)!.push(v);
        }
    });

    const categories = Array.from(catMap.keys());
    const boxplotData = categories.map(cat => {
        const sorted = [...catMap.get(cat)!].sort((a, b) => a - b);
        if (sorted.length === 0) return [0, 0, 0, 0, 0];
        const min = sorted[0];
        const max = sorted[sorted.length - 1];
        const q1 = sorted[Math.floor(sorted.length * 0.25)];
        const median = sorted[Math.floor(sorted.length * 0.5)];
        const q3 = sorted[Math.floor(sorted.length * 0.75)];
        return [min, q1, median, q3, max];
    });

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'item',
            axisPointer: { type: 'shadow' },
            formatter: (param: any) => {
                const [min, q1, med, q3, max] = param.data;
                return [
                    `<strong>${param.name}</strong>`,
                    `Max: ${formatCompactNumber(max)}`,
                    `Q3 (75%): ${formatCompactNumber(q3)}`,
                    `Median: ${formatCompactNumber(med)}`,
                    `Q1 (25%): ${formatCompactNumber(q1)}`,
                    `Min: ${formatCompactNumber(min)}`,
                ].join('<br/>');
            },
        },
        grid: {
            left: '10%',
            right: '10%',
            bottom: '18%',
            top: '12%',
        },
        xAxis: {
            type: 'category',
            data: categories,
            axisLabel: {
                color: '#94a3b8',
                fontSize: 11,
                rotate: categories.length > 5 ? 25 : 0,
            },
            axisLine: { lineStyle: { color: '#334155' } },
        },
        yAxis: {
            type: 'value',
            axisLabel: {
                color: '#94a3b8',
                fontSize: 11,
                formatter: (val: number) => formatCompactNumber(val),
            },
            splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        },
        series: [
            {
                name: 'Boxplot',
                type: 'boxplot',
                data: boxplotData,
                itemStyle: {
                    color: 'rgba(99, 102, 241, 0.4)',
                    borderColor: '#818cf8',
                    borderWidth: 1.5,
                },
            },
        ],
    };

    const onEvents = {
        click: (params: any) => {
            if (params.name) {
                onDataPointClick?.({ column: catKey, value: params.name });
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

export default DashboardBoxPlotChart;
