import React, { useMemo } from 'react';
import { Card, Table, Typography, Alert, Space, Row, Col } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { CorrelationAnalysis } from '../../types/profiling';

const { Paragraph } = Typography;

interface CorrelationPanelProps {
    data: CorrelationAnalysis;
}

const CorrelationPanel: React.FC<CorrelationPanelProps> = ({ data }) => {
    // Top pairs table
    const columns = [
        { title: 'Feature 1', dataIndex: 'col1', key: 'col1' },
        { title: 'Feature 2', dataIndex: 'col2', key: 'col2' },
        {
            title: 'Score',
            dataIndex: 'score',
            key: 'score',
            render: (val: number) => {
                const color = val > 0.7 || val < -0.7 ? '#faad14' : val > 0.9 || val < -0.9 ? '#ff4d4f' : '#fff';
                return <span style={{ color }}>{val.toFixed(3)}</span>;
            }
        },
        { title: 'Metric', dataIndex: 'metric', key: 'metric' }
    ];

    // Heatmap formatting
    const heatmapOption = useMemo(() => {
        const matrix = data.correlation_matrix;
        const features = Object.keys(matrix);
        // Ensure features have matrix data
        if (features.length === 0) return {};

        const chartData: [number, number, number][] = [];
        features.forEach((f1, i) => {
            features.forEach((f2, j) => {
                const val = matrix[f1][f2] !== undefined ? matrix[f1][f2] : 0;
                chartData.push([i, j, Number(val.toFixed(2))]);
            });
        });

        return {
            tooltip: { position: 'top' },
            grid: { top: '5%', bottom: '25%', left: '25%', right: '5%' },
            xAxis: {
                type: 'category',
                data: features,
                splitArea: { show: true },
                axisLabel: { interval: 0, rotate: 60, color: 'rgba(255,255,255,0.7)', fontSize: 10 }
            },
            yAxis: {
                type: 'category',
                data: features,
                splitArea: { show: true },
                axisLabel: { color: 'rgba(255,255,255,0.7)', fontSize: 10 }
            },
            visualMap: {
                min: -1,
                max: 1,
                calculable: true,
                orient: 'horizontal',
                left: 'center',
                bottom: '0%',
                inRange: {
                    color: ['#0050b3', '#141414', '#a8071a'] // Red positive, Blue negative
                },
                textStyle: { color: 'rgba(255,255,255,0.7)' }
            },
            series: [{
                name: 'Pearson',
                type: 'heatmap',
                data: chartData,
                label: { show: features.length <= 15, color: '#fff' },
                emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.5)' } }
            }]
        };
    }, [data.correlation_matrix]);

    // Network Graph formatting
    const networkOption = useMemo(() => {
        const nodes: { name: string; value: number; symbolSize: number }[] = [];
        const edges: { source: string; target: string; value: number; lineStyle: { width: number; color: string } }[] = [];
        const addedNodes = new Set();

        data.strongest_pairs.forEach(pair => {
            if (Math.abs(pair.score) > 0.4) {
                if (!addedNodes.has(pair.col1)) {
                    nodes.push({ name: pair.col1, value: 1, symbolSize: 30 });
                    addedNodes.add(pair.col1);
                }
                if (!addedNodes.has(pair.col2)) {
                    nodes.push({ name: pair.col2, value: 1, symbolSize: 30 });
                    addedNodes.add(pair.col2);
                }
                edges.push({
                    source: pair.col1,
                    target: pair.col2,
                    value: pair.score,
                    lineStyle: {
                        width: Math.abs(pair.score) * 3 + 1,
                        color: pair.score > 0 ? '#ff7875' : '#69c0ff'
                    }
                });
            }
        });

        return {
            tooltip: {},
            animationDurationUpdate: 1500,
            animationEasingUpdate: 'quinticInOut',
            series: [
                {
                    type: 'graph',
                    layout: 'force',
                    symbolSize: 25,
                    roam: true,
                    label: { show: true, color: '#fff', position: 'right', fontSize: 10 },
                    edgeSymbol: ['circle', 'none'],
                    edgeSymbolSize: [4, 8],
                    data: nodes,
                    links: edges,
                    force: { repulsion: 800, edgeLength: [50, 100] },
                    itemStyle: { color: '#722ed1', borderColor: '#b37feb', borderWidth: 2 }
                }
            ]
        };
    }, [data.strongest_pairs]);


    return (
        <Space orientation="vertical" size="large" style={{ width: '100%' }}>
            {data.multicollinearity.has_multicollinearity && (
                <Alert
                    message="High Multicollinearity Detected"
                    description={
                        <ul>
                            {data.multicollinearity.warnings.map((w, i) => <li key={i}>{w}</li>)}
                        </ul>
                    }
                    type="warning"
                    showIcon
                />
            )}

            <Row gutter={[16, 16]}>
                <Col span={24} lg={12}>
                    <Card title="Correlation Matrix" variant="borderless" className="glass-panel">
                        {Object.keys(data.correlation_matrix).length > 0 ? (
                            <ReactECharts option={heatmapOption} style={{ height: 600 }} theme="dark" />
                        ) : (
                            <Paragraph type="secondary">Not enough numeric columns for a matrix.</Paragraph>
                        )}
                    </Card>
                </Col>
                <Col span={24} lg={12}>
                    <Card title="Association Network (Score > 0.4)" variant="borderless" className="glass-panel">
                        {data.strongest_pairs.some(p => Math.abs(p.score) > 0.4) ? (
                            <ReactECharts option={networkOption} style={{ height: 600 }} theme="dark" />
                        ) : (
                            <Paragraph type="secondary">No strong correlations found to graph.</Paragraph>
                        )}
                    </Card>
                </Col>
            </Row>

            <Card title="Strongest Associations" variant="borderless" className="glass-panel">
                <Table
                    dataSource={data.strongest_pairs.slice(0, 10)}
                    columns={columns}
                    rowKey={(rec) => `${rec.col1}-${rec.col2}`}
                    pagination={false}
                    size="small"
                />
            </Card>
        </Space>
    );
};

export default CorrelationPanel;

