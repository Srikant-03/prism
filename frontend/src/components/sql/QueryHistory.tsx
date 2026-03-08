/**
 * QueryHistory — Timestamped history of all queries with one-click re-run.
 */

import React, { useState, useMemo } from 'react';
import { Button, Input, Space, Empty, Tag, Badge, Tooltip, Popconfirm } from 'antd';
import {
    HistoryOutlined, PlayCircleOutlined, CopyOutlined, DeleteOutlined,
    SearchOutlined, ClockCircleOutlined, CheckCircleOutlined, CloseCircleOutlined,
} from '@ant-design/icons';

export interface HistoryEntry {
    id: string;
    sql: string;
    timestamp: number;
    rowCount: number;
    executionTime: number;
    success: boolean;
    error?: string;
}

interface Props {
    history: HistoryEntry[];
    onRerun: (sql: string) => void;
    onLoad: (sql: string) => void;
    onClear: () => void;
}

const QueryHistory: React.FC<Props> = ({ history, onRerun, onLoad, onClear }) => {
    const [search, setSearch] = useState('');

    const filtered = useMemo(() => {
        if (!search) return history;
        const s = search.toLowerCase();
        return history.filter(h => h.sql.toLowerCase().includes(s));
    }, [history, search]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 12px',
                background: 'linear-gradient(135deg, rgba(245,158,11,0.06), rgba(251,191,36,0.06))',
                borderRadius: 10, border: '1px solid rgba(245,158,11,0.15)',
            }}>
                <Space>
                    <HistoryOutlined style={{ color: '#fbbf24', fontSize: 16 }} />
                    <span style={{ fontWeight: 700, fontSize: 14 }}>Query History</span>
                    <Badge count={history.length}
                        style={{ backgroundColor: 'rgba(245,158,11,0.2)', color: '#fcd34d', fontSize: 10 }} />
                </Space>
                <Space size={4}>
                    <Input prefix={<SearchOutlined style={{ color: 'rgba(255,255,255,0.3)' }} />}
                        placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)}
                        size="small" style={{ width: 150 }} allowClear />
                    <Popconfirm title="Clear all history?" onConfirm={onClear}>
                        <Button size="small" danger icon={<DeleteOutlined />}>Clear</Button>
                    </Popconfirm>
                </Space>
            </div>

            {/* History entries */}
            {filtered.length === 0 ? (
                <Empty description={history.length ? "No matches" : "No query history yet"}
                    image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 4, maxHeight: 500, overflow: 'auto' }}>
                    {filtered.map(h => (
                        <div key={h.id} className="glass-panel" style={{
                            padding: '8px 12px', display: 'flex', alignItems: 'center', gap: 10,
                        }}>
                            {/* Status + time */}
                            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2, width: 50 }}>
                                {h.success ? (
                                    <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 14 }} />
                                ) : (
                                    <CloseCircleOutlined style={{ color: '#ff4d4f', fontSize: 14 }} />
                                )}
                                <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>
                                    {new Date(h.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                </span>
                            </div>

                            {/* SQL preview */}
                            <div style={{ flex: 1, minWidth: 0 }}>
                                <div style={{
                                    fontSize: 11, fontFamily: 'monospace', color: 'rgba(165,180,252,0.7)',
                                    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                                }}>
                                    {h.sql.replace(/\s+/g, ' ').slice(0, 120)}
                                </div>
                                <div style={{ display: 'flex', gap: 6, marginTop: 2 }}>
                                    {h.success && (
                                        <Tag style={{ fontSize: 9 }}>{h.rowCount.toLocaleString()} rows</Tag>
                                    )}
                                    <Tag style={{ fontSize: 9 }} icon={<ClockCircleOutlined />}>
                                        {h.executionTime < 1 ? `${(h.executionTime * 1000).toFixed(0)}ms`
                                            : `${h.executionTime.toFixed(2)}s`}
                                    </Tag>
                                    <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.2)' }}>
                                        {new Date(h.timestamp).toLocaleDateString()}
                                    </span>
                                </div>
                            </div>

                            {/* Actions */}
                            <Space size={4}>
                                <Tooltip title="Load into editor">
                                    <Button size="small" icon={<CopyOutlined />} onClick={() => onLoad(h.sql)} />
                                </Tooltip>
                                <Button type="primary" size="small" icon={<PlayCircleOutlined />}
                                    onClick={() => onRerun(h.sql)}>Re-run</Button>
                            </Space>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

export default QueryHistory;

