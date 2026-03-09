import React, { useMemo } from 'react';
import { Card, Statistic, Row, Col, Typography } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { DataQualityScore } from '../../types/insight';

const { Title } = Typography;

interface QualityReportCardProps {
    data: DataQualityScore;
}

const QualityReportCard: React.FC<QualityReportCardProps> = ({ data }) => {

    const getGradeColor = (grade: string) => {
        switch (grade) {
            case 'A': return '#52c41a'; // Green
            case 'B': return '#a0d911'; // Light Green
            case 'C': return '#faad14'; // Yellow
            case 'D': return '#fa8c16'; // Orange
            default: return '#cf1322';  // Red
        }
    }

    const radarOption = useMemo(() => {
        const indicators = [
            { name: 'Completeness', max: 100 },
            { name: 'Uniqueness', max: 100 },
            { name: 'Validity', max: 100 },
            { name: 'Consistency', max: 100 },
        ];

        const values = [data.completeness, data.uniqueness, data.validity, data.consistency];

        if (data.timeliness !== null && data.timeliness !== undefined) {
            indicators.push({ name: 'Timeliness', max: 100 });
            values.push(data.timeliness);
        }

        return {
            title: { text: null },
            tooltip: {},
            radar: {
                indicator: indicators,
                splitNumber: 5,
                axisName: { color: 'rgba(255, 255, 255, 0.7)' },
                splitLine: {
                    lineStyle: { color: 'rgba(255, 255, 255, 0.2)' }
                },
                splitArea: {
                    areaStyle: {
                        color: ['rgba(250,250,250,0.05)', 'rgba(200,200,200,0.05)']
                    }
                },
                axisLine: {
                    lineStyle: { color: 'rgba(255, 255, 255, 0.2)' }
                }
            },
            series: [{
                name: 'Quality Attributes',
                type: 'radar',
                data: [{
                    value: values,
                    name: 'Score',
                    areaStyle: {
                        color: getGradeColor(data.grade) + '40' // 25% opacity
                    },
                    lineStyle: {
                        color: getGradeColor(data.grade),
                        width: 2
                    },
                    itemStyle: {
                        color: getGradeColor(data.grade)
                    }
                }]
            }]
        };
    }, [data]);

    return (
        <Card className="glass-panel" title="Data Quality Report Card" variant="borderless" style={{ height: '100%' }}>

            <Row gutter={[24, 24]} justify="space-around" align="middle" style={{ marginBottom: '20px' }}>
                <Col>
                    <div style={{ textAlign: 'center' }}>
                        <Title level={5} type="secondary" style={{ margin: 0 }}>Overall Score</Title>
                        <Title level={1} style={{ margin: 0, color: getGradeColor(data.grade) }}>
                            {data.overall_score.toFixed(1)}
                        </Title>
                    </div>
                </Col>
                <Col>
                    <div style={{ textAlign: 'center' }}>
                        <Title level={5} type="secondary" style={{ margin: 0 }}>Grade</Title>
                        <Title level={1} style={{ margin: 0, color: getGradeColor(data.grade) }}>
                            {data.grade}
                        </Title>
                    </div>
                </Col>
            </Row>

            <div style={{ height: '300px', width: '100%' }}>
                <ReactECharts
                    option={radarOption}
                    style={{ height: '100%', width: '100%' }}
                    opts={{ renderer: 'svg' }}
                />
            </div>

            <Row gutter={16} style={{ marginTop: '20px' }}>
                <Col span={12}><Statistic title="Completeness" value={data.completeness} suffix="/ 100" styles={{ content: { fontSize: '1.2rem' } }} /></Col>
                <Col span={12}><Statistic title="Uniqueness" value={data.uniqueness} suffix="/ 100" styles={{ content: { fontSize: '1.2rem' } }} /></Col>
                <Col span={12}><Statistic title="Validity" value={data.validity} suffix="/ 100" styles={{ content: { fontSize: '1.2rem' } }} /></Col>
                <Col span={12}><Statistic title="Consistency" value={data.consistency} suffix="/ 100" styles={{ content: { fontSize: '1.2rem' } }} /></Col>
                {data.timeliness !== null && data.timeliness !== undefined && (
                    <Col span={24}><Statistic title="Timeliness" value={data.timeliness} suffix="/ 100" styles={{ content: { fontSize: '1.2rem' } }} /></Col>
                )}
            </Row>
        </Card>
    );
};

export default QualityReportCard;

