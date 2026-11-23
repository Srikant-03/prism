import React, { useMemo, useState } from 'react';
import { Card, Select, Typography, Space, Empty, Alert, Tag, Row, Col, Statistic, Segmented } from 'antd';
import {
    ClockCircleOutlined,
    WarningOutlined,
    CheckCircleOutlined,
    LineChartOutlined,
    CalendarOutlined,
    BranchesOutlined,
    InfoCircleOutlined,
} from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import type { TemporalAnalysis } from '../../types/profiling';

const { Title, Text, Paragraph } = Typography;
const { Option } = Select;

interface TemporalPatternsPanelProps {
    data: TemporalAnalysis;
}

const TemporalPatternsPanel: React.FC<TemporalPatternsPanelProps> = ({ data }) => {
    const diagnostic = data.diagnostic;
    const eventTrendFeatures = Object.keys(data.event_trends || {});
    const decompFeatures = Object.keys(data.decompositions || {});

    // Determine default view mode
    const hasEventTrends = eventTrendFeatures.length > 0;
    const hasDecompositions = decompFeatures.length > 0;

    const [viewMode, setViewMode] = useState<'event_trend' | 'decomposition'>(
        data.analysis_mode === 'classical_decomposition' && hasDecompositions
            ? 'decomposition'
            : 'event_trend'
    );

    const defaultFeature = (viewMode === 'event_trend' ? eventTrendFeatures[0] : decompFeatures[0]) ||
        eventTrendFeatures[0] || decompFeatures[0] || null;

    const [selectedFeature, setSelectedFeature] = useState<string | null>(defaultFeature);

    // Synchronize selected feature if switching modes and feature doesn't exist
    const activeFeature = useMemo(() => {
        const pool = viewMode === 'event_trend' ? eventTrendFeatures : decompFeatures;
        if (selectedFeature && pool.includes(selectedFeature)) {
            return selectedFeature;
        }
        return pool[0] || null;
    }, [viewMode, selectedFeature, eventTrendFeatures, decompFeatures]);

    // -------------------------------------------------------------------
    // Event-Level Trend Option
    // -------------------------------------------------------------------
    const eventTrendOption = useMemo(() => {
        if (!activeFeature || !data.event_trends || !data.event_trends[activeFeature]) return {};

        const comp = data.event_trends[activeFeature];
        const times = comp.timestamps;
        const values = comp.values;
        const rollingMean = comp.rolling_mean;
        const rollingStd = comp.rolling_std;

        const upperBand = rollingMean.map((m, i) => Number((m + (rollingStd[i] || 0)).toFixed(2)));
        const lowerBand = rollingMean.map((m, i) => Number((m - (rollingStd[i] || 0)).toFixed(2)));

        return {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'cross' },
                formatter: (params: any) => {
                    if (!Array.isArray(params) || params.length === 0) return '';
                    const time = params[0].axisValue;
                    let content = `<div style="font-weight:600;margin-bottom:4px;color:#bae0ff">${time}</div>`;
                    params.forEach((item: any) => {
                        const val = typeof item.value === 'number' ? item.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : item.value;
                        content += `<div style="display:flex;justify-content:space-between;gap:16px;">
                            <span>${item.marker} ${item.seriesName}:</span>
                            <span style="font-weight:600">${val}</span>
                        </div>`;
                    });
                    return content;
                }
            },
            legend: {
                top: '2%',
                textStyle: { color: 'rgba(255,255,255,0.85)' },
                data: ['Observed Event', 'Rolling Trend', 'Upper Volatility (+1σ)', 'Lower Volatility (-1σ)']
            },
            grid: {
                top: '12%',
                left: '8%',
                right: '4%',
                bottom: '15%'
            },
            dataZoom: [
                { type: 'inside', xAxisIndex: 0 },
                { type: 'slider', xAxisIndex: 0, bottom: '2%', height: 22, textStyle: { color: 'rgba(255,255,255,0.6)' } }
            ],
            xAxis: {
                type: 'category',
                data: times,
                boundaryGap: false,
                axisLabel: { color: 'rgba(255,255,255,0.7)', rotate: 25 },
                axisLine: { lineStyle: { color: 'rgba(255,255,255,0.2)' } }
            },
            yAxis: {
                type: 'value',
                name: activeFeature,
                axisLabel: { color: 'rgba(255,255,255,0.7)' },
                splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }
            },
            series: [
                {
                    name: 'Observed Event',
                    type: 'scatter',
                    data: values,
                    symbolSize: 6,
                    itemStyle: { color: '#36cfc9', opacity: 0.75 }
                },
                {
                    name: 'Rolling Trend',
                    type: 'line',
                    data: rollingMean,
                    smooth: true,
                    showSymbol: false,
                    lineStyle: { width: 3, color: '#1677ff' }
                },
                {
                    name: 'Upper Volatility (+1σ)',
                    type: 'line',
                    data: upperBand,
                    smooth: true,
                    showSymbol: false,
                    lineStyle: { width: 1.2, color: '#fa8c16', type: 'dashed' }
                },
                {
                    name: 'Lower Volatility (-1σ)',
                    type: 'line',
                    data: lowerBand,
                    smooth: true,
                    showSymbol: false,
                    lineStyle: { width: 1.2, color: '#fa8c16', type: 'dashed' }
                }
            ]
        };
    }, [activeFeature, data.event_trends]);

    // -------------------------------------------------------------------
    // Classical STL Decomposition Option
    // -------------------------------------------------------------------
    const stlOption = useMemo(() => {
        if (!activeFeature || !data.decompositions || !data.decompositions[activeFeature]) return {};

        const comp = data.decompositions[activeFeature];
        const times = comp.timestamps;
        const original = comp.trend.map((t, i) => Number((t + comp.seasonal[i] + comp.residual[i]).toFixed(2)));

        return {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'cross' }
            },
            legend: {
                top: '2%',
                textStyle: { color: 'rgba(255,255,255,0.85)' },
                data: ['Original (Aggregated)', 'Trend', 'Seasonality', 'Residual']
            },
            grid: [
                { top: '12%', height: '36%', left: '8%', right: '4%' },
                { top: '56%', height: '32%', left: '8%', right: '4%' }
            ],
            axisPointer: {
                link: [{ xAxisIndex: 'all' }]
            },
            dataZoom: [
                { type: 'slider', xAxisIndex: [0, 1], bottom: '2%', height: 20, textStyle: { color: 'rgba(255,255,255,0.6)' } }
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
                    name: 'Original (Aggregated)',
                    type: 'line',
                    data: original,
                    showSymbol: false,
                    lineStyle: { width: 1.2, color: 'rgba(255,255,255,0.5)' }
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
                    lineStyle: { width: 1.5, color: '#52c41a' }
                },
                {
                    name: 'Residual',
                    type: 'scatter',
                    data: comp.residual,
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    symbolSize: 3.5,
                    itemStyle: { color: '#eb2f96' }
                }
            ]
        };
    }, [activeFeature, data.decompositions]);

    const activeExplanation = useMemo(() => {
        if (viewMode === 'event_trend' && activeFeature && data.event_trends && data.event_trends[activeFeature]) {
            return data.event_trends[activeFeature].explanation;
        }
        return null;
    }, [viewMode, activeFeature, data.event_trends]);

    return (
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            {/* Diagnostic Header Overview */}
            <Card variant="borderless" className="glass-panel" style={{ background: 'rgba(255,255,255,0.02)' }}>
                <Row gutter={[16, 16]} align="middle">
                    <Col xs={24} md={6}>
                        <Statistic
                            title={<span style={{ color: 'rgba(255,255,255,0.65)' }}><ClockCircleOutlined /> Primary Time Column</span>}
                            value={data.primary_time_col || 'None'}
                            valueStyle={{ color: '#bae0ff', fontSize: 18 }}
                        />
                    </Col>
                    <Col xs={12} md={6}>
                        <Statistic
                            title={<span style={{ color: 'rgba(255,255,255,0.65)' }}><BranchesOutlined /> Series Nature</span>}
                            value={diagnostic?.series_type === 'event_transactions' ? 'Event Transactions' : 'Regular Time Series'}
                            valueStyle={{
                                color: diagnostic?.series_type === 'event_transactions' ? '#faad14' : '#52c41a',
                                fontSize: 16
                            }}
                            prefix={diagnostic?.series_type === 'event_transactions' ? <WarningOutlined /> : <CheckCircleOutlined />}
                        />
                    </Col>
                    <Col xs={12} md={6}>
                        <Statistic
                            title={<span style={{ color: 'rgba(255,255,255,0.65)' }}><CalendarOutlined /> Dates & Duplicates</span>}
                            value={diagnostic ? `${diagnostic.unique_timestamps} dates (${diagnostic.duplicate_count} dups)` : 'N/A'}
                            valueStyle={{ color: '#fff', fontSize: 16 }}
                        />
                    </Col>
                    <Col xs={24} md={6}>
                        <Statistic
                            title={<span style={{ color: 'rgba(255,255,255,0.65)' }}><LineChartOutlined /> Sampling Regularity</span>}
                            value={diagnostic?.is_regular ? 'Uniform Frequency' : 'Irregular / Gaps Detected'}
                            valueStyle={{ color: diagnostic?.is_regular ? '#52c41a' : '#fa8c16', fontSize: 16 }}
                        />
                    </Col>
                </Row>

                {/* Sampling summary badge & details */}
                {diagnostic?.sampling_summary && (
                    <div style={{ marginTop: 12 }}>
                        <Text type="secondary" style={{ color: 'rgba(255,255,255,0.65)' }}>
                            <InfoCircleOutlined style={{ marginRight: 6 }} />
                            {diagnostic.sampling_summary}
                        </Text>
                    </div>
                )}
            </Card>

            {/* Ineligibility Warning or Transformation Explanation */}
            {diagnostic && !diagnostic.decomposition_eligible && (
                <Alert
                    type="warning"
                    showIcon
                    message="Classical STL Decomposition Bypassed for Raw Transactions"
                    description={
                        <div>
                            <p style={{ margin: '4px 0' }}>
                                {diagnostic.decomposition_ineligibility_reason ||
                                    'Classical decomposition assumes regularly-spaced observations without extensive missing intervals. Presenting event-level temporal trend analysis instead.'}
                            </p>
                            {data.transformation_used && (
                                <p style={{ margin: '4px 0', fontStyle: 'italic', color: '#ffd591' }}>
                                    {data.transformation_used}
                                </p>
                            )}
                        </div>
                    }
                    style={{ background: 'rgba(250, 173, 20, 0.08)', border: '1px solid rgba(250, 173, 20, 0.3)' }}
                />
            )}

            {diagnostic && diagnostic.decomposition_eligible && data.transformation_used && (
                <Alert
                    type="info"
                    showIcon
                    message="Data Transformation Applied Before Decomposition"
                    description={data.transformation_used}
                    style={{ background: 'rgba(22, 119, 255, 0.08)', border: '1px solid rgba(22, 119, 255, 0.3)' }}
                />
            )}

            {/* Main Interactive Chart Card */}
            <Card variant="borderless" className="glass-panel">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 12, marginBottom: 16 }}>
                    <div>
                        <Title level={4} style={{ color: '#fff', margin: 0 }}>
                            {viewMode === 'event_trend' ? 'Event-Level Temporal Trend' : 'Classical Time-Series Decomposition'}
                        </Title>
                        <Text type="secondary" style={{ color: 'rgba(255,255,255,0.6)' }}>
                            {viewMode === 'event_trend'
                                ? 'Sorted event transactions with rolling moving average and volatility bands (±1σ)'
                                : 'STL separation into underlying trend, seasonal oscillation, and residual noise'}
                        </Text>
                    </div>

                    <Space wrap>
                        {hasEventTrends && hasDecompositions && (
                            <Segmented
                                value={viewMode}
                                onChange={(val) => setViewMode(val as 'event_trend' | 'decomposition')}
                                options={[
                                    { label: 'Event Trend', value: 'event_trend', icon: <LineChartOutlined /> },
                                    { label: 'STL Decomposition', value: 'decomposition', icon: <BranchesOutlined /> },
                                ]}
                            />
                        )}

                        <Space>
                            <Text style={{ color: '#fff' }}>Metric:</Text>
                            <Select
                                value={activeFeature}
                                onChange={setSelectedFeature}
                                style={{ width: 220 }}
                            >
                                {(viewMode === 'event_trend' ? eventTrendFeatures : decompFeatures).map(col => (
                                    <Option key={col} value={col}>{col}</Option>
                                ))}
                            </Select>
                        </Space>
                    </Space>
                </div>

                {activeExplanation && (
                    <Paragraph style={{ color: '#91caff', marginBottom: 16, background: 'rgba(22,119,255,0.06)', padding: '8px 12px', borderRadius: 4 }}>
                        {activeExplanation}
                    </Paragraph>
                )}

                {/* Render Selected View */}
                {viewMode === 'event_trend' ? (
                    hasEventTrends && activeFeature ? (
                        <ReactECharts option={eventTrendOption} style={{ height: 500 }} theme="dark" />
                    ) : (
                        <Empty description="No event-level temporal trends available." image={Empty.PRESENTED_IMAGE_SIMPLE} />
                    )
                ) : (
                    hasDecompositions && activeFeature ? (
                        <ReactECharts option={stlOption} style={{ height: 600 }} theme="dark" />
                    ) : (
                        <Empty description="Classical decomposition not available for this series." image={Empty.PRESENTED_IMAGE_SIMPLE} />
                    )
                )}

                {/* Periodicities tag list if found */}
                {data.detected_periodicities && data.detected_periodicities.length > 0 && (
                    <div style={{ marginTop: 20, borderTop: '1px solid rgba(255,255,255,0.1)', paddingTop: 16 }}>
                        <Text strong style={{ color: '#bae0ff', marginRight: 8 }}>Detected Recurring Patterns:</Text>
                        <Space wrap size={[6, 8]} style={{ marginTop: 6 }}>
                            {data.detected_periodicities.map((p, idx) => (
                                <Tag key={idx} color="blue">{p}</Tag>
                            ))}
                        </Space>
                    </div>
                )}
            </Card>
        </Space>
    );
};

export default TemporalPatternsPanel;
