/**
 * CellRepairPanel — Intelligent cell-level repair suggestions.
 */

import React, { useState, useCallback } from 'react';
import { Table, Tag, Button, Space, Tooltip, Slider, Alert, Empty } from 'antd';
import {
    ToolOutlined, CheckCircleOutlined, EditOutlined,
    CloseCircleOutlined, FilterOutlined, ThunderboltOutlined,
} from '@ant-design/icons';

interface RepairSuggestion {
    id: string;
    row_index: number;
    column: string;
    current_value: any;
    suggested_value: any;
    confidence: number;
    type: 'typo' | 'outlier' | 'date_format';
    reasoning: string;
    status: 'pending' | 'accepted' | 'rejected' | 'edited';
}

interface Props {
    suggestions: RepairSuggestion[];
    onAccept: (id: string) => void;
    onReject: (id: string) => void;
    onBulkAccept: (minConfidence: number) => void;
    onJumpToRow: (rowIndex: number) => void;
}

const typeIcons = {
    typo: { color: '#f59e0b', label: 'Typo' },
    outlier: { color: '#ef4444', label: 'Outlier' },
    date_format: { color: '#3b82f6', label: 'Date Format' },
};

const CellRepairPanel: React.FC<Props> = ({
    suggestions, onAccept, onReject, onBulkAccept, onJumpToRow,
}) => {
    const [minConfidence, setMinConfidence] = useState(0.7);
    const [typeFilter, setTypeFilter] = useState<string | null>(null);

    const filtered = suggestions.filter(s => {
        if (typeFilter && s.type !== typeFilter) return false;
        if (s.status !== 'pending') return false;
        return true;
    });

    const pending = suggestions.filter(s => s.status === 'pending');
    const aboveThreshold = pending.filter(s => s.confidence >= minConfidence);

    if (suggestions.length === 0) {
        return (
            <Empty
                image={<ToolOutlined style={{ fontSize: 48, color: '#52c41a' }} />}
                description="No repair suggestions — your data looks good!"
            />
        );
    }

    const columns = [
        {
            title: 'Type',
            dataIndex: 'type',
            width: 80,
            render: (type: string) => {
                const config = typeIcons[type as keyof typeof typeIcons];
                return <Tag color={config?.color} style={{ fontSize: 10 }}>{config?.label || type}</Tag>;
            },
        },
        {
            title: 'Row',
            dataIndex: 'row_index',
            width: 60,
            render: (v: number) => (
                <Button type="link" size="small" onClick={() => onJumpToRow(v)}
                    style={{ fontSize: 11, fontFamily: 'monospace' }}>
                    #{v}
                </Button>
            ),
        },
        {
            title: 'Column',
            dataIndex: 'column',
            width: 120,
            render: (v: string) => <code style={{ fontSize: 11 }}>{v}</code>,
        },
        {
            title: 'Current',
            dataIndex: 'current_value',
            width: 120,
            render: (v: any) => (
                <span style={{
                    fontFamily: 'monospace', fontSize: 11,
                    color: '#ef4444', textDecoration: 'line-through',
                }}>
                    {String(v).slice(0, 20)}
                </span>
            ),
        },
        {
            title: '→ Suggested',
            dataIndex: 'suggested_value',
            width: 120,
            render: (v: any) => (
                <span style={{
                    fontFamily: 'monospace', fontSize: 11,
                    color: '#52c41a', fontWeight: 600,
                }}>
                    {String(v).slice(0, 20)}
                </span>
            ),
        },
        {
            title: 'Confidence',
            dataIndex: 'confidence',
            width: 80,
            render: (v: number) => (
                <Tag color={v >= 0.9 ? 'green' : v >= 0.7 ? 'blue' : 'orange'}
                    style={{ fontSize: 10 }}>
                    {Math.round(v * 100)}%
                </Tag>
            ),
        },
        {
            title: 'Reasoning',
            dataIndex: 'reasoning',
            ellipsis: true,
            render: (v: string) => <span style={{ fontSize: 11, opacity: 0.7 }}>{v}</span>,
        },
        {
            title: '',
            width: 80,
            render: (_: any, record: RepairSuggestion) => (
                <Space size={2}>
                    <Tooltip title="Accept">
                        <Button size="small" icon={<CheckCircleOutlined />}
                            style={{ color: '#52c41a' }}
                            onClick={() => onAccept(record.id)} />
                    </Tooltip>
                    <Tooltip title="Reject">
                        <Button size="small" icon={<CloseCircleOutlined />}
                            danger
                            onClick={() => onReject(record.id)} />
                    </Tooltip>
                </Space>
            ),
        },
    ];

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {/* Summary */}
            <div className="glass-panel" style={{
                padding: '8px 12px', display: 'flex', justifyContent: 'space-between',
                alignItems: 'center', flexWrap: 'wrap', gap: 8,
            }}>
                <Space>
                    <ToolOutlined style={{ color: '#6366f1' }} />
                    <strong>{pending.length} pending repairs</strong>
                    {Object.entries(typeIcons).map(([type, config]) => {
                        const count = pending.filter(s => s.type === type).length;
                        return count > 0 ? (
                            <Tag key={type} color={typeFilter === type ? config.color : undefined}
                                style={{ cursor: 'pointer', fontSize: 10 }}
                                onClick={() => setTypeFilter(typeFilter === type ? null : type)}>
                                {config.label}: {count}
                            </Tag>
                        ) : null;
                    })}
                </Space>
                <Space>
                    <span style={{ fontSize: 11 }}>Bulk accept above:</span>
                    <Slider
                        min={50} max={100} step={5}
                        value={minConfidence * 100}
                        onChange={v => setMinConfidence(v / 100)}
                        style={{ width: 100 }}
                        tooltip={{ formatter: v => `${v}%` }}
                    />
                    <Button size="small" type="primary"
                        icon={<ThunderboltOutlined />}
                        onClick={() => onBulkAccept(minConfidence)}>
                        Accept {aboveThreshold.length}
                    </Button>
                </Space>
            </div>

            {/* Table */}
            <div className="glass-panel" style={{ padding: 0 }}>
                <Table
                    columns={columns}
                    dataSource={filtered}
                    rowKey="id"
                    size="small"
                    pagination={{ pageSize: 20, size: 'small' }}
                    scroll={{ x: true }}
                />
            </div>
        </div>
    );
};

export default CellRepairPanel;
