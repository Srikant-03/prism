/**
 * GaugeChart — Power BI-style Radial Gauge Visualizer.
 * Renders a circular/semicircular gauge showing current value, target, min, and max thresholds.
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import type { ChartConfig } from '../../../types/dashboard';
import { formatCompactNumber, formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardGaugeChart: React.FC<Props> = ({ config, data }) => {
    const valKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || config.kpi_value_column || 'value';
    const rawVal = data.length > 0 ? Number(data[0][valKey] ?? data[0].value ?? 0) : 0;

    // Optional comparison / target column
    const targetKey = config.kpi_comparison_column;
    const targetVal = targetKey && data.length > 0 ? Number(data[0][targetKey] ?? 0) : undefined;

    const maxVal = targetVal && targetVal > rawVal ? Math.round(targetVal * 1.2) : Math.round(rawVal * 1.3) || 100;

    const option = {
        backgroundColor: 'transparent',
        series: [
            {
                type: 'gauge',
                startAngle: 180,
                endAngle: 0,
                center: ['50%', '75%'],
                radius: '110%',
                min: 0,
                max: maxVal,
                splitNumber: 5,
                axisLine: {
                    lineStyle: {
                        width: 16,
                        color: [
                            [0.3, '#ef4444'],
                            [0.7, '#f59e0b'],
                            [1, '#10b981'],
                        ],
                    },
                },
                pointer: {
                    icon: 'path://M12.8,0.7l12,40.1H0.7L12.8,0.7z',
                    length: '12%',
                    width: 12,
                    offsetCenter: [0, '-60%'],
                    itemStyle: {
                        color: '#f8fafc',
                    },
                },
                axisTick: {
                    length: 8,
                    lineStyle: { color: '#475569', width: 1 },
                },
                splitLine: {
                    length: 14,
                    lineStyle: { color: '#64748b', width: 2 },
                },
                axisLabel: {
                    color: '#94a3b8',
                    fontSize: 10,
                    distance: -35,
                    formatter: (value: number) => formatCompactNumber(value),
                },
                title: {
                    offsetCenter: [0, '-20%'],
                    fontSize: 13,
                    color: '#94a3b8',
                },
                detail: {
                    fontSize: 26,
                    offsetCenter: [0, '0%'],
                    valueAnimation: true,
                    formatter: (value: number) => formatValue(value, config.metrics?.[0]?.format),
                    color: '#f8fafc',
                    fontWeight: 700,
                },
                data: [
                    {
                        value: rawVal,
                        name: config.title || valKey.replace(/_/g, ' ').toUpperCase(),
                    },
                ],
            },
        ],
    };

    return (
        <div style={{ width: '100%', height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
            <ReactECharts option={option} style={{ width: '100%', height: '100%' }} />
            {targetVal !== undefined && (
                <div style={{ marginTop: -20, fontSize: 11, color: '#94a3b8' }}>
                    Target: <strong>{formatValue(targetVal, config.metrics?.[0]?.format)}</strong>
                </div>
            )}
        </div>
    );
};

export default DashboardGaugeChart;
