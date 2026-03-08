/**
 * MultiFileResolver — Schema comparison UI with merge/separate decision.
 */

import React from 'react';
import { Card, Typography, Button, Table, Tag, Space, Progress } from 'antd';
import {
    MergeOutlined,
    SplitCellsOutlined,
    CheckCircleOutlined,
    CloseCircleOutlined,
    QuestionCircleOutlined,
} from '@ant-design/icons';
import type { SchemaComparison } from '../../types/ingestion';

const { Title, Text, Paragraph } = Typography;

interface MultiFileResolverProps {
    comparison: SchemaComparison;
    fileIds: string[];
    onResolve: (fileIds: string[], action: 'merge' | 'separate' | 'exclude') => void;
    loading?: boolean;
}

const relationshipConfig = {
    same_schema: {
        color: '#52c41a',
        icon: <CheckCircleOutlined />,
        label: 'Same Schema',
        bgColor: 'rgba(82, 196, 26, 0.08)',
    },
    different_schema: {
        color: '#ff4d4f',
        icon: <CloseCircleOutlined />,
        label: 'Different Schemas',
        bgColor: 'rgba(255, 77, 79, 0.08)',
    },
    mixed: {
        color: '#faad14',
        icon: <QuestionCircleOutlined />,
        label: 'Mixed — Your Decision Needed',
        bgColor: 'rgba(250, 173, 20, 0.08)',
    },
};

const MultiFileResolver: React.FC<MultiFileResolverProps> = ({
    comparison,
    fileIds,
    onResolve,
    loading = false,
}) => {
    const config = relationshipConfig[comparison.relationship];

    const fileColumns = [
        {
            title: 'File',
            dataIndex: 'filename',
            key: 'filename',
            render: (name: string) => <Text strong>{name}</Text>,
        },
        {
            title: 'Rows',
            dataIndex: 'row_count',
            key: 'row_count',
            render: (val: number) => val.toLocaleString(),
        },
        {
            title: 'Columns',
            dataIndex: 'columns',
            key: 'columns',
            render: (cols: string[]) => (
                <Space wrap size={[4, 4]}>
                    {cols.slice(0, 8).map((col) => (
                        <Tag
                            key={col}
                            color={comparison.common_columns.includes(col) ? 'processing' : 'default'}
                        >
                            {col}
                        </Tag>
                    ))}
                    {cols.length > 8 && <Tag>+{cols.length - 8} more</Tag>}
                </Space>
            ),
        },
    ];

    return (
        <div className="multi-file-resolver">
            <Card variant="borderless" className="resolver-header-card" style={{ background: config.bgColor }}>
                <Space align="start">
                    <span style={{ fontSize: 28, color: config.color }}>{config.icon}</span>
                    <div>
                        <Title level={4} style={{ margin: 0, color: config.color }}>
                            {config.label}
                        </Title>
                        <Paragraph style={{ margin: '8px 0 0', maxWidth: 600 }}>
                            {comparison.justification}
                        </Paragraph>
                    </div>
                </Space>
                <Progress
                    percent={Math.round(comparison.confidence * 100)}
                    strokeColor={config.color}
                    railColor="rgba(255,255,255,0.1)"
                    size="small"
                    format={(pct) => `${pct}% confidence`}
                    style={{ maxWidth: 300, marginTop: 12 }}
                />
            </Card>

            <Card title="File Schemas" variant="borderless" className="schema-table-card">
                <Table
                    dataSource={comparison.files.map((f, i) => ({ ...f, key: i }))}
                    columns={fileColumns}
                    pagination={false}
                    size="small"
                />

                {comparison.common_columns.length > 0 && (
                    <div style={{ marginTop: 16 }}>
                        <Text strong>Common Columns ({comparison.common_columns.length}):</Text>
                        <div style={{ marginTop: 8 }}>
                            <Space wrap size={[4, 4]}>
                                {comparison.common_columns.map((col) => (
                                    <Tag key={col} color="processing">{col}</Tag>
                                ))}
                            </Space>
                        </div>
                    </div>
                )}

                {Object.keys(comparison.differing_columns).length > 0 && (
                    <div style={{ marginTop: 16 }}>
                        <Text strong>Unique Columns per File:</Text>
                        {Object.entries(comparison.differing_columns).map(([fname, cols]) => (
                            <div key={fname} style={{ marginTop: 8 }}>
                                <Text type="secondary">{fname}:</Text>
                                <Space wrap size={[4, 4]} style={{ marginLeft: 8 }}>
                                    {cols.map((col) => (
                                        <Tag key={col} color="default">{col}</Tag>
                                    ))}
                                </Space>
                            </div>
                        ))}
                    </div>
                )}
            </Card>

            <div className="resolver-actions">
                <Space size="middle">
                    <Button
                        type="primary"
                        icon={<MergeOutlined />}
                        onClick={() => onResolve(fileIds, 'merge')}
                        loading={loading}
                        size="large"
                        disabled={comparison.relationship === 'different_schema'}
                    >
                        Merge into One Dataset
                    </Button>
                    <Button
                        icon={<SplitCellsOutlined />}
                        onClick={() => onResolve(fileIds, 'separate')}
                        loading={loading}
                        size="large"
                    >
                        Keep as Separate Tables
                    </Button>
                </Space>
            </div>
        </div>
    );
};

export default MultiFileResolver;

