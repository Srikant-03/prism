import React, { useMemo } from 'react';
import { Card, Typography, Row, Col, Space, Empty } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { GeoAnalysis } from '../../types/profiling';
// IMPORTANT: echarts maps need the geoJSON registered. Since we may not have the
// actual World GeoJSON compiled into ReactECharts by default in a simple setup, 
// we will fallback to a bar chart or scatter plot for distribution if map is unavailable,
// but let's assume we can map distribution to a horizontal bar chart to be safe.

const { Title, Text, Paragraph } = Typography;

interface GeoPatternsPanelProps {
    data: GeoAnalysis;
}

const GeoPatternsPanel: React.FC<GeoPatternsPanelProps> = ({ data }) => {

    const distributionOption = useMemo(() => {
        if (!data.geo_distribution || Object.keys(data.geo_distribution).length === 0) return {};

        // Sort highest to lowest
        const entries = Object.entries(data.geo_distribution)
            .sort((a, b) => a[1] - b[1]) // Ascending for horizontal bar
            .slice(-20); // Top 20

        return {
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            grid: { left: '3%', right: '4%', bottom: '3%', top: '5%', containLabel: true },
            xAxis: { type: 'value', axisLabel: { color: 'rgba(255,255,255,0.6)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } } },
            yAxis: { type: 'category', data: entries.map(e => e[0].length > 15 ? e[0].substring(0, 15) + '...' : e[0]), axisLabel: { color: '#fff' } },
            series: [
                {
                    name: 'Count',
                    type: 'bar',
                    data: entries.map(e => e[1]),
                    itemStyle: {
                        color: {
                            type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
                            colorStops: [{ offset: 0, color: '#312eb5' }, { offset: 1, color: '#531dab' }]
                        },
                        borderRadius: [0, 4, 4, 0]
                    }
                }
            ]
        };
    }, [data.geo_distribution]);

    return (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Card bordered={false} className="glass-panel">
                <Title level={4} style={{ color: '#fff' }}>Geospatial Distribution</Title>
                <Paragraph style={{ color: 'rgba(255,255,255,0.7)' }}>
                    Detected Geo Columns: <Text code style={{ background: 'transparent', color: '#b37feb' }}>{data.geo_columns.join(', ')}</Text>
                </Paragraph>

                {data.bounding_box && (
                    <div style={{ marginBottom: 24 }}>
                        <Text style={{ color: '#fff' }}>Coordinate Bounding Box:</Text>
                        <Row gutter={[16, 16]} style={{ marginTop: 8 }}>
                            <Col span={6}>
                                <div style={{ padding: 12, border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8 }}>
                                    <Text type="secondary" style={{ display: 'block', fontSize: 12 }}>Min Latitude</Text>
                                    <Text strong style={{ color: '#d3adf7' }}>{data.bounding_box.min_lat.toFixed(4)}</Text>
                                </div>
                            </Col>
                            <Col span={6}>
                                <div style={{ padding: 12, border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8 }}>
                                    <Text type="secondary" style={{ display: 'block', fontSize: 12 }}>Max Latitude</Text>
                                    <Text strong style={{ color: '#d3adf7' }}>{data.bounding_box.max_lat.toFixed(4)}</Text>
                                </div>
                            </Col>
                            <Col span={6}>
                                <div style={{ padding: 12, border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8 }}>
                                    <Text type="secondary" style={{ display: 'block', fontSize: 12 }}>Min Longitude</Text>
                                    <Text strong style={{ color: '#d3adf7' }}>{data.bounding_box.min_lon.toFixed(4)}</Text>
                                </div>
                            </Col>
                            <Col span={6}>
                                <div style={{ padding: 12, border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8 }}>
                                    <Text type="secondary" style={{ display: 'block', fontSize: 12 }}>Max Longitude</Text>
                                    <Text strong style={{ color: '#d3adf7' }}>{data.bounding_box.max_lon.toFixed(4)}</Text>
                                </div>
                            </Col>
                        </Row>
                    </div>
                )}

                {data.geo_distribution && Object.keys(data.geo_distribution).length > 0 ? (
                    <>
                        <Title level={5} style={{ color: '#fff', marginTop: 24 }}>Top Locations Distribution</Title>
                        <ReactECharts option={distributionOption} style={{ height: 400 }} theme="dark" />
                    </>
                ) : (
                    <Empty description="No regional distributions found." image={Empty.PRESENTED_IMAGE_SIMPLE} />
                )}
            </Card>
        </Space>
    );
};

export default GeoPatternsPanel;
