import React, { useMemo, useState } from 'react';
import { Card, Select, Typography, Space, Empty } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { TemporalAnalysis } from '../../types/profiling';

const { Title, Text, Paragraph } = Typography;
const { Option } = Select;


interface TemporalPatternsPanelProps {
    data: TemporalAnalysis;
}

const TemporalPatternsPanel: React.FC<TemporalPatternsPanelProps> = ({ data }) => {
    const defaultCol = Object.keys(data.decompositions || {})[0] || null;
    const [selectedCol, setSelectedCol] = useState<string | null>(defaultCol);

    const stlOption = useMemo(() => {
        if (!selectedCol || !data.decompositions || !data.decompositions[selectedCol]) return {};

        const comp = data.decompositions[selectedCol];
        const times = comp.timestamps;

        const original = comp.trend.map((t, i) => t + comp.seasonal[i] + comp.residual[i]);

        return {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'cross' }
            },
            legend: {
                top: '5%',
                textStyle: { color: 'rgba(255,255,255,0.7)' },
                data: ['Original (Approx)', 'Trend', 'Seasonality', 'Residual']
            },
            grid: [
                { top: '15%', height: '35%', left: '10%', right: '5%' },
                { top: '55%', height: '35%', left: '10%', right: '5%' }
            ],
            axisPointer: {
                link: [{ xAxisIndex: 'all' }]
            },
            dataZoom: [
                { type: 'slider', xAxisIndex: [0, 1], bottom: '2%', height: 20 }
            ],
            xAxis: [
                { type: 'category', data: times, boundaryGap: false, axisLabel: { color: 'rgba(255,255,255,0.7)' } },
                { type: 'category', data: times, gridIndex: 1, boundaryGap: false, axisLabel: { show: false } }
            ],
            yAxis: [
                { type: 'value', name: 'Value', axisLabel: { color: 'rgba(255,255,255,0.7)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
                { type: 'value', name: 'Seasonality/Resid', gridIndex: 1, axisLabel: { color: 'rgba(255,255,255,0.7)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } }
            ],
            series: [
                {
                    name: 'Original (Approx)',
                    type: 'line',
                    data: original,
                    showSymbol: false,
                    lineStyle: { width: 1, color: '#434343' }
                },
                {
                    name: 'Trend',
                    type: 'line',
                    data: comp.trend,
                    showSymbol: false,
                    lineStyle: { width: 3, color: '#1677ff' }
                },
                {
                    name: 'Seasonality',
                    type: 'line',
                    data: comp.seasonal,
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    showSymbol: false,
                    lineStyle: { width: 1.5, color: '#fa541c' }
                },
                {
                    name: 'Residual',
                    type: 'scatter',
                    data: comp.residual,
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    symbolSize: 3,
                    itemStyle: { color: '#eb2f96' }
                }
            ]
        };
    }, [selectedCol, data.decompositions]);

    return (
        <Space orientation="vertical" size="large" style={{ width: '100%' }}>
            <Card variant="borderless" className="glass-panel">
                <Title level={4} style={{ color: '#fff' }}>Time-Series Decomposition</Title>
                <Paragraph style={{ color: 'rgba(255,255,255,0.7)' }}>
                    Primary Time Column: <Text strong style={{ color: '#bae0ff' }}>{data.primary_time_col}</Text>
                </Paragraph>

                {data.decompositions && Object.keys(data.decompositions).length > 0 ? (
                    <>
                        <div style={{ marginBottom: 16 }}>
                            <Text style={{ marginRight: 8, color: '#fff' }}>Select Feature to Decompose:</Text>
                            <Select
                                value={selectedCol}
                                onChange={setSelectedCol}
                                style={{ width: 250 }}
                            >
                                {Object.keys(data.decompositions).map(col => (
                                    <Option key={col} value={col}>{col}</Option>
                                ))}
                            </Select>
                        </div>
                        <ReactECharts option={stlOption} style={{ height: 600 }} theme="dark" />
                    </>
                ) : (
                    <Empty description="No suitable numeric features found for time-series decomposition." image={Empty.PRESENTED_IMAGE_SIMPLE} />
                )}
            </Card>
        </Space>
    );
};

export default TemporalPatternsPanel;

