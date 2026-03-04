import React, { useMemo } from 'react';
import { Card, Typography, Row, Col, Statistic, Progress } from 'antd';
import ReactECharts from 'echarts-for-react';
import { AimOutlined, TrophyOutlined } from '@ant-design/icons';
import type { TargetAnalysis } from '../../types/profiling';

const { Title, Text, Paragraph } = Typography;

interface TargetAnalysisPanelProps {
    data: TargetAnalysis;
}

const TargetAnalysisPanel: React.FC<TargetAnalysisPanelProps> = ({ data }) => {

    const classDistOption = useMemo(() => {
        if (!data.class_distribution) return {};

        const chartData = Object.entries(data.class_distribution).map(([name, val]) => ({
            name, value: (val * 100).toFixed(1)
        }));

        return {
            tooltip: { trigger: 'item', formatter: '{b}: {c}%' },
            legend: { top: '5%', left: 'center', textStyle: { color: '#fff' } },
            series: [
                {
                    name: 'Class Distribution',
                    type: 'pie',
                    radius: ['40%', '70%'],
                    avoidLabelOverlap: false,
                    itemStyle: {
                        borderRadius: 10,
                        borderColor: '#141414',
                        borderWidth: 2
                    },
                    label: { show: false, position: 'center' },
                    emphasis: {
                        label: { show: true, fontSize: '20', fontWeight: 'bold', color: '#fff' }
                    },
                    labelLine: { show: false },
                    data: chartData
                }
            ]
        };
    }, [data.class_distribution]);

    const importanceOption = useMemo(() => {
        if (!data.top_predictors || data.top_predictors.length === 0) return {};

        // Reverse for horizontal bar chart (highest at top)
        const sorted = [...data.top_predictors].reverse();

        return {
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
            xAxis: { type: 'value', splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }, axisLabel: { color: 'rgba(255,255,255,0.6)' } },
            yAxis: { type: 'category', data: sorted.map(d => d.feature), axisLabel: { color: 'rgba(255,255,255,0.8)' } },
            series: [
                {
                    name: 'Importance',
                    type: 'bar',
                    data: sorted.map(d => Number(d.importance_score.toFixed(3))),
                    itemStyle: {
                        color: {
                            type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
                            colorStops: [{ offset: 0, color: '#13c2c2' }, { offset: 1, color: '#006d75' }]
                        },
                        borderRadius: [0, 4, 4, 0]
                    }
                }
            ]
        };
    }, [data.top_predictors]);

    return (
        <Card variant="borderless" className="glass-panel target-analysis-panel">
            <Row gutter={[24, 24]}>
                <Col span={24} md={8}>
                    <Statistic
                        title={<span style={{ color: 'rgba(255,255,255,0.7)' }}>Detected Target Variable</span>}
                        value={data.target_column || "None"}
                        prefix={<AimOutlined style={{ color: '#fa541c' }} />}
                        styles={{ content: { color: '#fa541c', fontWeight: 'bold' } }}
                    />
                    <Statistic
                        title={<span style={{ color: 'rgba(255,255,255,0.7)', marginTop: '20px', display: 'block' }}>Problem Type</span>}
                        value={data.problem_type ? data.problem_type.replace('_', ' ').replace(/\b\w/g, c => c.toUpperCase()) : "Unknown"}
                        prefix={<TrophyOutlined style={{ color: '#faad14' }} />}
                        styles={{ content: { color: '#faad14' } }}
                    />

                    <div style={{ marginTop: '24px' }}>
                        <Text type="secondary">Confidence</Text>
                        <Progress percent={Math.round(data.confidence * 100)} strokeColor="#52c41a" status="active" />
                        <Paragraph style={{ marginTop: '12px', color: 'rgba(255,255,255,0.7)' }}>
                            {data.justification}
                        </Paragraph>
                    </div>
                </Col>

                <Col span={24} md={8}>
                    <Title level={5} style={{ color: '#fff' }}>Class Distribution</Title>
                    {data.class_distribution ? (
                        <ReactECharts option={classDistOption} style={{ height: 250 }} theme="dark" />
                    ) : (
                        <Paragraph type="secondary">N/A for Regression</Paragraph>
                    )}
                </Col>

                <Col span={24} md={8}>
                    <Title level={5} style={{ color: '#fff' }}>Highest Associative Features</Title>
                    {data.top_predictors && data.top_predictors.length > 0 ? (
                        <ReactECharts option={importanceOption} style={{ height: 250 }} theme="dark" />
                    ) : (
                        <Paragraph type="secondary">No strong predictors found.</Paragraph>
                    )}
                </Col>
            </Row>
        </Card>
    );
};

export default TargetAnalysisPanel;

