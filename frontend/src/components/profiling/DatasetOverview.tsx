/**
 * DatasetOverview â€” Top-level dataset stats, domain inference, key detection.
 */

import React from 'react';
import { Card, Row, Col, Statistic, Tag, Typography, Space, Progress, Collapse, Descriptions, Table } from 'antd';
import {
    DatabaseOutlined, ColumnWidthOutlined, CloudOutlined, HddOutlined,
    KeyOutlined, LinkOutlined,
    IdcardOutlined, FieldTimeOutlined, GlobalOutlined, ClockCircleOutlined,
} from '@ant-design/icons';
import type { DatasetProfile } from '../../types/profiling';

const { Title, Text } = Typography;
const { Panel } = Collapse;

interface DatasetOverviewProps {
    profile: DatasetProfile;
}

function getCompletenessColor(pct: number): string {
    if (pct >= 95) return '#52c41a';
    if (pct >= 80) return '#faad14';
    return '#ff4d4f';
}

const DatasetOverview: React.FC<DatasetOverviewProps> = ({ profile }) => {
    const keyColumns = [
        {
            title: 'Column(s)', dataIndex: 'columns', key: 'columns',
            render: (cols: string[]) => cols.map(c => <Tag key={c} color="processing">{c}</Tag>)
        },
        {
            title: 'Uniqueness', dataIndex: 'uniqueness', key: 'uniqueness',
            render: (v: number) => `${(v * 100).toFixed(0)}%`
        },
        {
            title: 'Justification', dataIndex: 'justification', key: 'justification',
            render: (t: string) => <Text type="secondary" style={{ fontSize: 12 }}>{t}</Text>
        },
    ];

    return (
        <div className="dataset-overview">
            {/* Row 1: Core Stats */}
            <Row gutter={[16, 16]}>
                <Col xs={12} sm={6}>
                    <Card variant="borderless" className="stat-card">
                        <Statistic title="Total Rows" value={profile.total_rows.toLocaleString()} prefix={<DatabaseOutlined />} valueStyle={{ color: '#6366f1' }} />
                    </Card>
                </Col>
                <Col xs={12} sm={6}>
                    <Card variant="borderless" className="stat-card">
                        <Statistic title="Total Columns" value={profile.total_columns} prefix={<ColumnWidthOutlined />} valueStyle={{ color: '#8b5cf6' }} />
                    </Card>
                </Col>
                <Col xs={12} sm={6}>
                    <Card variant="borderless" className="stat-card">
                        <Statistic title="In-Memory" value={profile.memory_size_readable} prefix={<CloudOutlined />} valueStyle={{ color: '#06b6d4', fontSize: 20 }} />
                    </Card>
                </Col>
                <Col xs={12} sm={6}>
                    <Card variant="borderless" className="stat-card">
                        <Statistic title="On-Disk" value={profile.disk_size_readable} prefix={<HddOutlined />} valueStyle={{ color: '#10b981', fontSize: 20 }} />
                    </Card>
                </Col>
            </Row>

            {/* Row 2: Domain & Scores */}
            <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
                <Col xs={24} md={12}>
                    <Card variant="borderless" className="domain-card">
                        <Space direction="vertical" style={{ width: '100%' }}>
                            <Space>
                                <GlobalOutlined style={{ fontSize: 20, color: '#6366f1' }} />
                                <Title level={5} style={{ margin: 0 }}>Estimated Domain</Title>
                            </Space>
                            <Tag color="purple" style={{ fontSize: 14, padding: '4px 12px' }}>{profile.estimated_domain}</Tag>
                            <Progress percent={Math.round(profile.domain_confidence * 100)} strokeColor="#6366f1" size="small"
                                format={p => `${p}% confidence`} />
                            <Text type="secondary" italic style={{ fontSize: 12 }}>{profile.domain_justification}</Text>
                        </Space>
                    </Card>
                </Col>
                <Col xs={12} md={6}>
                    <Card variant="borderless" className="score-card">
                        <Space direction="vertical" align="center" style={{ width: '100%' }}>
                            <Text type="secondary">Completeness</Text>
                            <Progress type="circle" percent={Math.round(profile.structural_completeness)} size={80}
                                strokeColor={getCompletenessColor(profile.structural_completeness)} />
                        </Space>
                    </Card>
                </Col>
                <Col xs={12} md={6}>
                    <Card variant="borderless" className="score-card">
                        <Space direction="vertical" align="center" style={{ width: '100%' }}>
                            <Text type="secondary">Schema Consistency</Text>
                            <Progress type="circle" percent={Math.round(profile.schema_consistency)} size={80}
                                strokeColor={getCompletenessColor(profile.schema_consistency)} />
                        </Space>
                    </Card>
                </Col>
            </Row>

            {/* Row 3: Temporal & Keys */}
            <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
                {profile.temporal_coverage && (
                    <Col xs={24} md={8}>
                        <Card variant="borderless" className="info-card" title={<Space><ClockCircleOutlined /> Temporal Coverage</Space>} size="small">
                            <Descriptions column={1} size="small">
                                <Descriptions.Item label="Column"><Tag>{profile.temporal_coverage.column}</Tag></Descriptions.Item>
                                <Descriptions.Item label="Range">{profile.temporal_coverage.earliest?.slice(0, 10)} â†’ {profile.temporal_coverage.latest?.slice(0, 10)}</Descriptions.Item>
                                <Descriptions.Item label="Span">{profile.temporal_coverage.span_days.toLocaleString()} days</Descriptions.Item>
                            </Descriptions>
                        </Card>
                    </Col>
                )}
                <Col xs={24} md={profile.temporal_coverage ? 16 : 24}>
                    <Collapse ghost>
                        {profile.primary_key_candidates.length > 0 && (
                            <Panel header={<Space><KeyOutlined /> Primary Key Candidates ({profile.primary_key_candidates.length})</Space>} key="pk">
                                <Table dataSource={profile.primary_key_candidates.map((k, i) => ({ ...k, key: i }))}
                                    columns={keyColumns} pagination={false} size="small" />
                            </Panel>
                        )}
                        {profile.foreign_key_candidates.length > 0 && (
                            <Panel header={<Space><LinkOutlined /> Foreign Key Candidates ({profile.foreign_key_candidates.length})</Space>} key="fk">
                                <Table dataSource={profile.foreign_key_candidates.map((k, i) => ({ ...k, key: i }))}
                                    columns={keyColumns} pagination={false} size="small" />
                            </Panel>
                        )}
                        {profile.id_columns.length > 0 && (
                            <Panel header={<Space><IdcardOutlined /> ID Columns ({profile.id_columns.length})</Space>} key="id">
                                <Space wrap>{profile.id_columns.map(c => <Tag key={c} color="warning">{c}</Tag>)}</Space>
                                <Text type="secondary" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
                                    These columns have high uniqueness and no analytical value â€” consider excluding from analysis.
                                </Text>
                            </Panel>
                        )}
                    </Collapse>
                </Col>
            </Row>

            {/* Profiling Time */}
            <div style={{ textAlign: 'right', marginTop: 8 }}>
                <Text type="secondary" style={{ fontSize: 11 }}>
                    <FieldTimeOutlined /> Profiled in {profile.profiling_time_seconds.toFixed(2)}s
                </Text>
            </div>
        </div>
    );
};

export default DatasetOverview;

