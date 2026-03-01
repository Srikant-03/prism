/**
 * ColumnTagger — Smart semantic tagger for columns.
 * Auto-detects column roles (ID, target, feature, date, PII) and lets users override.
 */

import React from 'react';
import { Table, Tag, Select, Space, Button, Tooltip, Alert } from 'antd';
import {
    TagOutlined,
    EyeInvisibleOutlined, ThunderboltOutlined,
} from '@ant-design/icons';

type ColumnTag = 'id' | 'target' | 'feature' | 'datetime' | 'pii' | 'text' | 'numeric' | 'categorical' | 'ignore';

interface TaggedColumn {
    name: string;
    dtype: string;
    autoTag: ColumnTag;
    userTag?: ColumnTag;
    confidence: number;
    reasoning: string;
}

interface Props {
    columns: TaggedColumn[];
    onTagChange: (column: string, tag: ColumnTag) => void;
    onAutoTag: () => void;
}

const tagStyles: Record<ColumnTag, { color: string; label: string; icon: string }> = {
    id: { color: '#8b5cf6', label: 'ID', icon: '🔑' },
    target: { color: '#ef4444', label: 'Target', icon: '🎯' },
    feature: { color: '#3b82f6', label: 'Feature', icon: '⚙️' },
    datetime: { color: '#10b981', label: 'DateTime', icon: '📅' },
    pii: { color: '#f59e0b', label: 'PII', icon: '🔒' },
    text: { color: '#6366f1', label: 'Text', icon: '📝' },
    numeric: { color: '#06b6d4', label: 'Numeric', icon: '🔢' },
    categorical: { color: '#ec4899', label: 'Categorical', icon: '🏷️' },
    ignore: { color: '#6b7280', label: 'Ignore', icon: '⏭️' },
};

const ColumnTagger: React.FC<Props> = ({ columns, onTagChange, onAutoTag }) => {
    const piiCount = columns.filter(c => (c.userTag || c.autoTag) === 'pii').length;
    const ignoredCount = columns.filter(c => (c.userTag || c.autoTag) === 'ignore').length;
    const targetCol = columns.find(c => (c.userTag || c.autoTag) === 'target');

    const tableColumns = [
        {
            title: 'Column',
            dataIndex: 'name',
            render: (v: string) => <code style={{ fontSize: 12 }}>{v}</code>,
        },
        {
            title: 'Type',
            dataIndex: 'dtype',
            width: 80,
            render: (v: string) => <Tag style={{ fontSize: 10 }}>{v}</Tag>,
        },
        {
            title: 'Auto Tag',
            dataIndex: 'autoTag',
            width: 100,
            render: (tag: ColumnTag) => {
                const style = tagStyles[tag];
                return (
                    <Tag color={style.color} style={{ fontSize: 10 }}>
                        {style.icon} {style.label}
                    </Tag>
                );
            },
        },
        {
            title: 'Tag',
            width: 140,
            render: (_: any, record: TaggedColumn) => {
                const currentTag = record.userTag || record.autoTag;
                return (
                    <Select
                        size="small"
                        value={currentTag}
                        onChange={v => onTagChange(record.name, v)}
                        style={{ width: 130 }}
                        options={Object.entries(tagStyles).map(([k, v]) => ({
                            value: k,
                            label: <span>{v.icon} {v.label}</span>,
                        }))}
                    />
                );
            },
        },
        {
            title: 'Confidence',
            dataIndex: 'confidence',
            width: 70,
            render: (v: number) => (
                <span style={{
                    fontSize: 11,
                    color: v >= 0.9 ? '#52c41a' : v >= 0.7 ? '#faad14' : '#ef4444',
                    fontWeight: 600,
                }}>
                    {Math.round(v * 100)}%
                </span>
            ),
        },
        {
            title: 'Reasoning',
            dataIndex: 'reasoning',
            ellipsis: true,
            render: (v: string) => (
                <Tooltip title={v}>
                    <span style={{ fontSize: 11, opacity: 0.6 }}>{v}</span>
                </Tooltip>
            ),
        },
    ];

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {/* Header */}
            <div className="glass-panel" style={{
                padding: '8px 12px', display: 'flex', justifyContent: 'space-between',
                alignItems: 'center', flexWrap: 'wrap', gap: 8,
            }}>
                <Space>
                    <TagOutlined style={{ color: '#6366f1', fontSize: 16 }} />
                    <strong>Smart Column Tags</strong>
                    <Tag color="blue">{columns.length} columns</Tag>
                    {targetCol && (
                        <Tag color="red">🎯 Target: {targetCol.name}</Tag>
                    )}
                    {piiCount > 0 && (
                        <Tag color="orange" icon={<EyeInvisibleOutlined />}>
                            {piiCount} PII
                        </Tag>
                    )}
                    {ignoredCount > 0 && (
                        <Tag>{ignoredCount} ignored</Tag>
                    )}
                </Space>
                <Button size="small" icon={<ThunderboltOutlined />} onClick={onAutoTag}>
                    Re-detect Tags
                </Button>
            </div>

            {piiCount > 0 && (
                <Alert
                    type="warning"
                    showIcon
                    message={`${piiCount} column(s) flagged as PII — these will be excluded from ML training and masked in exports`}
                    style={{ fontSize: 12 }}
                />
            )}

            {/* Table */}
            <div className="glass-panel" style={{ padding: 0 }}>
                <Table
                    columns={tableColumns}
                    dataSource={columns}
                    rowKey="name"
                    size="small"
                    pagination={false}
                    scroll={{ x: true }}
                />
            </div>
        </div>
    );
};

export default ColumnTagger;
