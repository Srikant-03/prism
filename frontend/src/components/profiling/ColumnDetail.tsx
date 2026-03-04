/**
 * ColumnDetail — Deep column profiling view with all type-specific stats and charts.
 * Shows universal stats + the appropriate type-specific panel.
 */

import React from 'react';
import { Tag, Typography, Space, Descriptions, Progress, Row, Col, Card, Tooltip, Alert, Badge } from 'antd';
import {
    WarningOutlined,
    ExclamationCircleOutlined, EyeOutlined,
} from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import type { ColumnProfile, NumericProfile, CategoricalProfile, DatetimeProfile, BooleanProfile, TextProfile } from '../../types/profiling';

const { Text } = Typography;

interface ColumnDetailProps {
    column: ColumnProfile;
}

const SEMANTIC_TYPE_COLORS: Record<string, string> = {
    numeric_continuous: '#3b82f6', numeric_discrete: '#6366f1',
    categorical_nominal: '#f59e0b', categorical_ordinal: '#f97316',
    boolean: '#10b981', datetime: '#8b5cf6', duration: '#8b5cf6',
    free_text: '#ec4899', url: '#06b6d4', email: '#14b8a6',
    phone: '#64748b', geo_coordinate: '#22c55e', currency: '#eab308',
    percentage: '#a855f7', id_key: '#94a3b8', binary_encoded: '#64748b',
    hashed: '#475569', ip_address: '#0ea5e9', unknown: '#6b7280',
};

function getQualityColor(score: number): string {
    if (score >= 90) return '#52c41a';
    if (score >= 70) return '#faad14';
    if (score >= 50) return '#fa8c16';
    return '#ff4d4f';
}

// ── Histogram Chart ──
function HistogramChart({ num }: { num: NumericProfile }) {
    if (!num.histogram_bins.length) return null;
    const categories = num.histogram_bins.slice(0, -1).map((b, i) =>
        `${b.toFixed(1)}–${num.histogram_bins[i + 1].toFixed(1)}`
    );
    const option = {
        tooltip: { trigger: 'axis' as const },
        grid: { left: 50, right: 20, top: 30, bottom: 40 },
        xAxis: { type: 'category' as const, data: categories, axisLabel: { rotate: 45, fontSize: 9, color: 'rgba(255,255,255,0.5)' } },
        yAxis: { type: 'value' as const, axisLabel: { color: 'rgba(255,255,255,0.5)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
        series: [
            { type: 'bar' as const, data: num.histogram_counts, itemStyle: { color: '#6366f1', borderRadius: [3, 3, 0, 0] } },
            ...(num.kde_x.length ? [{
                type: 'line' as const, data: num.kde_y.map((y, i) => [num.kde_x[i], y * Math.max(...num.histogram_counts) * (categories.length / (num.kde_y.reduce((a, b) => a + b, 0) || 1))]),
                smooth: true, lineStyle: { color: '#f59e0b', width: 2 }, symbol: 'none' as const,
            }] : []),
        ],
    };
    return <ReactECharts option={option} style={{ height: 250 }} theme="dark" />;
}

// ── Box Plot Chart ──
function BoxPlotChart({ num }: { num: NumericProfile }) {
    if (num.box_q1 == null) return null;
    const option = {
        tooltip: { trigger: 'item' as const },
        grid: { left: 60, right: 30, top: 20, bottom: 20 },
        xAxis: { type: 'category' as const, data: [''], axisLabel: { show: false } },
        yAxis: { type: 'value' as const, axisLabel: { color: 'rgba(255,255,255,0.5)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
        series: [
            {
                type: 'boxplot' as const, data: [[num.box_whisker_low, num.box_q1, num.box_q2, num.box_q3, num.box_whisker_high]],
                itemStyle: { color: 'rgba(99,102,241,0.3)', borderColor: '#6366f1' }
            },
            ...(num.box_outliers.length ? [{
                type: 'scatter' as const, data: num.box_outliers.map(o => ['', o]),
                itemStyle: { color: '#f59e0b' }, symbolSize: 6,
            }] : []),
        ],
    };
    return <ReactECharts option={option} style={{ height: 200 }} theme="dark" />;
}

// ── Q-Q Plot Chart ──
function QQPlotChart({ num }: { num: NumericProfile }) {
    if (!num.qq_theoretical.length) return null;
    const data = num.qq_theoretical.map((t, i) => [t, num.qq_sample[i]]);
    const min = Math.min(...num.qq_theoretical);
    const max = Math.max(...num.qq_theoretical);
    const option = {
        tooltip: { trigger: 'item' as const, formatter: (p: { value: number[] }) => `Theoretical: ${p.value[0].toFixed(2)}<br/>Sample: ${p.value[1].toFixed(2)}` },
        grid: { left: 60, right: 30, top: 20, bottom: 40 },
        xAxis: { name: 'Theoretical Quantiles', nameLocation: 'center' as const, nameGap: 25, axisLabel: { color: 'rgba(255,255,255,0.5)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
        yAxis: { name: 'Sample Quantiles', nameLocation: 'center' as const, nameGap: 40, axisLabel: { color: 'rgba(255,255,255,0.5)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
        series: [
            { type: 'scatter' as const, data, symbolSize: 4, itemStyle: { color: '#6366f1' } },
            { type: 'line' as const, data: [[min, min], [max, max]], lineStyle: { color: '#ef4444', type: 'dashed' as const }, symbol: 'none' as const },
        ],
    };
    return <ReactECharts option={option} style={{ height: 250 }} theme="dark" />;
}

// ── Pie Chart ──
function PieChart({ data }: { data: { name: string; value: number }[] }) {
    if (!data.length) return null;
    const option = {
        tooltip: { trigger: 'item' as const, formatter: '{b}: {c} ({d}%)' },
        series: [{
            type: 'pie' as const, radius: ['40%', '70%'], data,
            label: { color: 'rgba(255,255,255,0.7)', fontSize: 11 },
            itemStyle: { borderColor: 'rgba(0,0,0,0.3)', borderWidth: 2 },
        }],
    };
    return <ReactECharts option={option} style={{ height: 250 }} theme="dark" />;
}

// ── Bar Chart (Top Values) ──
function FrequencyBarChart({ data }: { data: { value: unknown; count: number; percentage: number }[] }) {
    if (!data.length) return null;
    const option = {
        tooltip: { trigger: 'axis' as const },
        grid: { left: 120, right: 30, top: 10, bottom: 20 },
        xAxis: { type: 'value' as const, axisLabel: { color: 'rgba(255,255,255,0.5)' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
        yAxis: { type: 'category' as const, data: data.map(d => String(d.value).slice(0, 25)), inverse: true, axisLabel: { color: 'rgba(255,255,255,0.6)', fontSize: 11 } },
        series: [{ type: 'bar' as const, data: data.map(d => d.count), itemStyle: { color: '#6366f1', borderRadius: [0, 4, 4, 0] }, barWidth: '60%' }],
    };
    return <ReactECharts option={option} style={{ height: Math.max(200, data.length * 28) }} theme="dark" />;
}

// ── Main Component ──
const ColumnDetail: React.FC<ColumnDetailProps> = ({ column }) => {
    const typeColor = SEMANTIC_TYPE_COLORS[column.semantic_type] || '#6b7280';

    return (
        <div className="column-detail">
            {/* Header */}
            <div className="col-detail-header">
                <Space>
                    <Text strong style={{ fontSize: 16 }}>{column.name}</Text>
                    <Tag color={typeColor}>{column.semantic_type.replace(/_/g, ' ')}</Tag>
                    <Tooltip title={`Confidence: ${(column.semantic_type_confidence * 100).toFixed(0)}%`}>
                        <Progress type="circle" percent={Math.round(column.semantic_type_confidence * 100)} size={28}
                            strokeColor={typeColor} format={() => ''} />
                    </Tooltip>
                </Space>
                <Space>
                    <Tooltip title={column.quality_justification}>
                        <Badge count={`${column.quality_score.toFixed(0)}/100`} showZero
                            style={{ backgroundColor: getQualityColor(column.quality_score), fontSize: 11 }} />
                    </Tooltip>
                </Space>
            </div>

            {/* Universal Stats */}
            <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
                <Col xs={8} sm={4}>
                    <div className="mini-stat"><Text type="secondary" style={{ fontSize: 10 }}>Non-Null</Text>
                        <Text strong>{column.non_null_count.toLocaleString()}</Text></div>
                </Col>
                <Col xs={8} sm={4}>
                    <div className="mini-stat"><Text type="secondary" style={{ fontSize: 10 }}>Null %</Text>
                        <Text strong style={{ color: column.null_percentage > 20 ? '#ff4d4f' : undefined }}>{column.null_percentage.toFixed(1)}%</Text></div>
                </Col>
                <Col xs={8} sm={4}>
                    <div className="mini-stat"><Text type="secondary" style={{ fontSize: 10 }}>Distinct</Text>
                        <Text strong>{column.distinct_count.toLocaleString()}</Text></div>
                </Col>
                <Col xs={8} sm={4}>
                    <div className="mini-stat"><Text type="secondary" style={{ fontSize: 10 }}>Distinct %</Text>
                        <Text strong>{column.distinct_percentage.toFixed(1)}%</Text></div>
                </Col>
                <Col xs={8} sm={4}>
                    <div className="mini-stat"><Text type="secondary" style={{ fontSize: 10 }}>Most Freq</Text>
                        <Tooltip title={`${column.most_frequent?.value} (${column.most_frequent?.count})`}>
                            <Text strong style={{ fontSize: 11 }}>{String(column.most_frequent?.value || '—').slice(0, 15)}</Text>
                        </Tooltip>
                    </div>
                </Col>
                <Col xs={8} sm={4}>
                    <div className="mini-stat"><Text type="secondary" style={{ fontSize: 10 }}>Dtype</Text>
                        <Tag style={{ fontSize: 10, margin: 0 }}>{column.inferred_dtype}</Tag></div>
                </Col>
            </Row>

            {/* Samples */}
            {column.sample_values.length > 0 && (
                <div style={{ marginTop: 8 }}>
                    <Text type="secondary" style={{ fontSize: 11 }}><EyeOutlined /> Samples: </Text>
                    <Space wrap size={4}>{column.sample_values.slice(0, 8).map((v, i) =>
                        <Tag key={i} style={{ fontSize: 10 }}>{String(v).slice(0, 30)}</Tag>
                    )}</Space>
                </div>
            )}

            {/* Numeric Panel */}
            {column.numeric && <NumericPanel num={column.numeric} />}

            {/* Categorical Panel */}
            {column.categorical && <CategoricalPanel cat={column.categorical} />}

            {/* Datetime Panel */}
            {column.datetime && <DatetimePanel dt={column.datetime} />}

            {/* Boolean Panel */}
            {column.boolean && <BooleanPanel bool={column.boolean} />}

            {/* Text Panel */}
            {column.text && <TextPanel txt={column.text} />}
        </div>
    );
};

// ── Type-Specific Panels ──

function NumericPanel({ num }: { num: NumericProfile }) {
    return (
        <div className="type-panel">
            <Descriptions size="small" column={{ xs: 2, sm: 3, md: 4 }} bordered className="numeric-stats">
                <Descriptions.Item label="Min">{num.min?.toLocaleString()}</Descriptions.Item>
                <Descriptions.Item label="Max">{num.max?.toLocaleString()}</Descriptions.Item>
                <Descriptions.Item label="Mean">{num.mean?.toFixed(4)}</Descriptions.Item>
                <Descriptions.Item label="Median">{num.median?.toFixed(4)}</Descriptions.Item>
                <Descriptions.Item label="Std Dev">{num.std_dev?.toFixed(4)}</Descriptions.Item>
                <Descriptions.Item label="Variance">{num.variance?.toFixed(4)}</Descriptions.Item>
                <Descriptions.Item label="Skewness">
                    <Space>{num.skewness?.toFixed(3)}
                        {num.skewness_interpretation && <Tag color={num.skewness_interpretation === 'symmetric' ? 'green' : 'orange'} style={{ fontSize: 10 }}>{num.skewness_interpretation.replace(/_/g, ' ')}</Tag>}
                    </Space>
                </Descriptions.Item>
                <Descriptions.Item label="Kurtosis">
                    <Space>{num.kurtosis?.toFixed(3)}
                        {num.kurtosis_interpretation && <Tag color="default" style={{ fontSize: 10 }}>{num.kurtosis_interpretation}</Tag>}
                    </Space>
                </Descriptions.Item>
                <Descriptions.Item label="IQR">{num.iqr?.toFixed(4)}</Descriptions.Item>
                <Descriptions.Item label="CoV">{num.coefficient_of_variation?.toFixed(4)}</Descriptions.Item>
                <Descriptions.Item label="Zeros">{num.zero_count}</Descriptions.Item>
                <Descriptions.Item label="Negatives">{num.negative_count}</Descriptions.Item>
            </Descriptions>
            {num.trimmed_mean_5 != null && (
                <Descriptions size="small" column={{ xs: 2, sm: 4 }} style={{ marginTop: 8 }}>
                    <Descriptions.Item label="Trimmed Mean (5%)">{num.trimmed_mean_5?.toFixed(4)}</Descriptions.Item>
                    {num.geometric_mean != null && <Descriptions.Item label="Geometric Mean">{num.geometric_mean.toFixed(4)}</Descriptions.Item>}
                    {num.harmonic_mean != null && <Descriptions.Item label="Harmonic Mean">{num.harmonic_mean.toFixed(4)}</Descriptions.Item>}
                </Descriptions>
            )}
            {Object.keys(num.percentiles).length > 0 && (
                <div style={{ marginTop: 8 }}>
                    <Text type="secondary" style={{ fontSize: 11 }}>Percentiles: </Text>
                    <Space wrap size={4}>{Object.entries(num.percentiles).map(([k, v]) =>
                        <Tag key={k} style={{ fontSize: 10 }}>{k}: {v.toFixed(2)}</Tag>
                    )}</Space>
                </div>
            )}
            <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
                <Col xs={24} md={12}><Card size="small" title="Histogram + KDE" bordered={false} className="chart-card"><HistogramChart num={num} /></Card></Col>
                <Col xs={24} md={6}><Card size="small" title="Box Plot" bordered={false} className="chart-card"><BoxPlotChart num={num} /></Card></Col>
                <Col xs={24} md={6}><Card size="small" title="Q-Q Plot (Normality)" bordered={false} className="chart-card"><QQPlotChart num={num} /></Card></Col>
            </Row>
            {num.formatting_issues.length > 0 && (
                <Alert type="warning" showIcon message="Formatting Issues" style={{ marginTop: 8 }}
                    description={<ul style={{ margin: 0, paddingLeft: 16 }}>{num.formatting_issues.map((i, idx) => <li key={idx}>{i}</li>)}</ul>} />
            )}
        </div>
    );
}

function CategoricalPanel({ cat }: { cat: CategoricalProfile }) {
    return (
        <div className="type-panel">
            <Space style={{ marginBottom: 8 }}>
                <Tag color="processing">Cardinality: {cat.cardinality}</Tag>
                {cat.cardinality_class && <Tag>{cat.cardinality_class.replace(/_/g, ' ')}</Tag>}
                {cat.suspected_ordinal && <Tag color="warning">Suspected Ordinal</Tag>}
            </Space>
            <Row gutter={[12, 12]}>
                <Col xs={24} md={cat.pie_data.length ? 12 : 24}>
                    <Card size="small" title={`Top ${cat.top_values.length} Values`} bordered={false} className="chart-card">
                        <FrequencyBarChart data={cat.top_values} />
                    </Card>
                </Col>
                {cat.pie_data.length > 0 && (
                    <Col xs={24} md={12}>
                        <Card size="small" title="Distribution" bordered={false} className="chart-card"><PieChart data={cat.pie_data} /></Card>
                    </Col>
                )}
            </Row>
            {cat.case_inconsistencies.length > 0 && (
                <Alert type="warning" showIcon icon={<WarningOutlined />} style={{ marginTop: 8 }}
                    message={`${cat.case_inconsistencies.length} Case Inconsistencies`}
                    description={cat.case_inconsistencies.slice(0, 5).map((c, i) =>
                        <div key={i}><Text code>{c.normalized}</Text>: {c.variants.join(', ')}</div>
                    )} />
            )}
            {cat.whitespace_issues.length > 0 && (
                <Alert type="warning" showIcon style={{ marginTop: 8 }} message="Whitespace Issues Detected"
                    description={cat.whitespace_issues.map((w, i) => <div key={i}>{w.type}: {w.count} values</div>)} />
            )}
        </div>
    );
}

function DatetimePanel({ dt }: { dt: DatetimeProfile }) {
    return (
        <div className="type-panel">
            <Descriptions size="small" column={{ xs: 1, sm: 2, md: 3 }} bordered>
                <Descriptions.Item label="Earliest">{dt.earliest?.slice(0, 19)}</Descriptions.Item>
                <Descriptions.Item label="Latest">{dt.latest?.slice(0, 19)}</Descriptions.Item>
                <Descriptions.Item label="Span">{dt.time_span_days?.toFixed(0)} days</Descriptions.Item>
                <Descriptions.Item label="Frequency"><Tag color={dt.frequency === 'irregular' ? 'warning' : 'processing'}>{dt.frequency || 'unknown'}</Tag></Descriptions.Item>
                <Descriptions.Item label="Timezone">{dt.timezone_info}</Descriptions.Item>
                <Descriptions.Item label="Formats">{dt.detected_formats.join(', ')}{dt.mixed_formats && <Tag color="warning" style={{ marginLeft: 4 }}>Mixed!</Tag>}</Descriptions.Item>
            </Descriptions>
            {(dt.future_dates_count > 0 || dt.implausible_dates_count > 0) && (
                <Space style={{ marginTop: 8 }}>
                    {dt.future_dates_count > 0 && <Tag color="warning">{dt.future_dates_count} future dates</Tag>}
                    {dt.implausible_dates_count > 0 && <Tag color="error">{dt.implausible_dates_count} implausible dates</Tag>}
                </Space>
            )}
            {dt.gap_count > 0 && <Alert type="info" showIcon style={{ marginTop: 8 }} message={`${dt.gap_count} gaps detected in time series`}
                description={dt.frequency_justification} />}
            {dt.seasonality_indicator && <Alert type="info" showIcon style={{ marginTop: 8 }} message={dt.seasonality_indicator} />}
        </div>
    );
}

function BooleanPanel({ bool }: { bool: BooleanProfile }) {
    const pieData = [
        { name: 'True', value: bool.true_count },
        { name: 'False', value: bool.false_count },
    ];
    return (
        <div className="type-panel">
            <Row gutter={[12, 12]}>
                <Col xs={12}><PieChart data={pieData} /></Col>
                <Col xs={12}>
                    <Descriptions size="small" column={1}>
                        <Descriptions.Item label="True">{bool.true_count.toLocaleString()} ({(bool.true_ratio * 100).toFixed(1)}%)</Descriptions.Item>
                        <Descriptions.Item label="False">{bool.false_count.toLocaleString()} ({(bool.false_ratio * 100).toFixed(1)}%)</Descriptions.Item>
                    </Descriptions>
                    {bool.is_disguised && (
                        <Alert type="info" showIcon style={{ marginTop: 8 }} message="Disguised Boolean"
                            description={<div>{Object.entries(bool.disguised_mapping).map(([k, v]) =>
                                <div key={k}><Text code>{k}</Text> → {v}</div>
                            )}</div>} />
                    )}
                </Col>
            </Row>
        </div>
    );
}

function TextPanel({ txt }: { txt: TextProfile }) {
    return (
        <div className="type-panel">
            <Descriptions size="small" column={{ xs: 1, sm: 2, md: 3 }} bordered>
                <Descriptions.Item label="Avg Length">{txt.avg_length.toFixed(1)} chars</Descriptions.Item>
                <Descriptions.Item label="Range">{txt.min_length}–{txt.max_length} chars</Descriptions.Item>
                <Descriptions.Item label="Avg Tokens">{txt.avg_token_count.toFixed(1)}</Descriptions.Item>
                {txt.detected_language && <Descriptions.Item label="Language"><Tag>{txt.detected_language}</Tag> ({(txt.language_confidence * 100).toFixed(0)}%)</Descriptions.Item>}
            </Descriptions>
            {txt.entity_types_found.length > 0 && (
                <div style={{ marginTop: 8 }}><Text type="secondary">Entities found: </Text>
                    <Space wrap>{txt.entity_types_found.map(e => <Tag key={e} color="processing">{e.replace(/_/g, ' ')}</Tag>)}</Space>
                </div>
            )}
            {(txt.html_contamination || txt.markdown_contamination) && (
                <Space style={{ marginTop: 8 }}>
                    {txt.html_contamination && <Tag color="warning">HTML Contamination</Tag>}
                    {txt.markdown_contamination && <Tag color="warning">Markdown Contamination</Tag>}
                </Space>
            )}
            {txt.has_pii_risk && (
                <Alert type="error" showIcon icon={<ExclamationCircleOutlined />} style={{ marginTop: 8 }}
                    message="⚠️ PII Risk Detected" description={
                        <div>{txt.pii_risks.map((r, i) => (
                            <div key={i} style={{ marginBottom: 4 }}>
                                <Tag color="error">{r.type}</Tag>
                                <Text type="secondary">{r.recommendation}</Text>
                            </div>
                        ))}</div>
                    } />
            )}
        </div>
    );
}

export default ColumnDetail;
