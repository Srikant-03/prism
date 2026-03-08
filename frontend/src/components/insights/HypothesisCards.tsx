/**
 * HypothesisCards — Auto-generated data-driven hypothesis cards.
 */

import React, { useState } from 'react';
import { Tag, Progress, Space, Button, Tooltip, Empty } from 'antd';
import {
    BulbOutlined, CheckCircleOutlined, CloseCircleOutlined,
    QuestionCircleOutlined, SearchOutlined,
} from '@ant-design/icons';

interface Hypothesis {
    id: string;
    observation: string;
    evidence: string;
    question: string;
    confidence: number;
    impact: 'high' | 'medium' | 'low';
    action: { label: string; type: string; payload: string };
    status: 'unreviewed' | 'confirmed' | 'rejected' | 'needs_data';
}

interface Props {
    hypotheses: Hypothesis[];
    onStatusChange: (id: string, status: Hypothesis['status']) => void;
    onAction: (action: Hypothesis['action']) => void;
}

const impactColors = { high: '#ef4444', medium: '#f59e0b', low: '#3b82f6' };
const statusConfig = {
    unreviewed: { color: 'default', icon: <QuestionCircleOutlined /> },
    confirmed: { color: 'success', icon: <CheckCircleOutlined /> },
    rejected: { color: 'error', icon: <CloseCircleOutlined /> },
    needs_data: { color: 'warning', icon: <SearchOutlined /> },
};

const HypothesisCards: React.FC<Props> = ({ hypotheses, onStatusChange, onAction }) => {
    const [filter, setFilter] = useState<string | null>(null);

    const filtered = filter
        ? hypotheses.filter(h => h.status === filter)
        : hypotheses;

    if (hypotheses.length === 0) {
        return (
            <Empty
                image={<BulbOutlined style={{ fontSize: 48, color: '#6366f1' }} />}
                description="No hypotheses generated yet — run profiling first"
            />
        );
    }

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {/* Filter bar */}
            <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                <Tag
                    color={filter === null ? 'blue' : undefined}
                    style={{ cursor: 'pointer' }}
                    onClick={() => setFilter(null)}
                >
                    All ({hypotheses.length})
                </Tag>
                {Object.entries(statusConfig).map(([key, config]) => {
                    const count = hypotheses.filter(h => h.status === key).length;
                    return count > 0 ? (
                        <Tag
                            key={key}
                            color={filter === key ? config.color : undefined}
                            style={{ cursor: 'pointer' }}
                            onClick={() => setFilter(filter === key ? null : key)}
                        >
                            {key} ({count})
                        </Tag>
                    ) : null;
                })}
            </div>

            {/* Cards */}
            {filtered.map(h => (
                <div key={h.id} className="glass-panel" style={{
                    padding: 12,
                    borderLeft: `3px solid ${impactColors[h.impact]}`,
                }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                        <Space>
                            <BulbOutlined style={{ color: '#fbbf24' }} />
                            <Tag color={h.impact === 'high' ? 'red' : h.impact === 'medium' ? 'orange' : 'blue'}
                                style={{ fontSize: 10 }}>
                                {h.impact} impact
                            </Tag>
                        </Space>
                        <Tooltip title={`Confidence: ${Math.round(h.confidence * 100)}%`}>
                            <Progress
                                type="circle"
                                percent={Math.round(h.confidence * 100)}
                                size={28}
                                strokeColor={h.confidence > 0.8 ? '#52c41a' : '#faad14'}
                                format={p => `${p}%`}
                            />
                        </Tooltip>
                    </div>

                    <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>
                        {h.observation}
                    </div>
                    <div style={{ fontSize: 12, opacity: 0.6, marginBottom: 6 }}>
                        📊 {h.evidence}
                    </div>
                    <div style={{ fontSize: 13, fontStyle: 'italic', marginBottom: 10 }}>
                        â“ {h.question}
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Space size={4}>
                            <Button size="small" type={h.status === 'confirmed' ? 'primary' : 'default'}
                                icon={<CheckCircleOutlined />}
                                onClick={() => onStatusChange(h.id, 'confirmed')}>
                                Confirm
                            </Button>
                            <Button size="small" type={h.status === 'rejected' ? 'primary' : 'default'}
                                danger={h.status === 'rejected'}
                                icon={<CloseCircleOutlined />}
                                onClick={() => onStatusChange(h.id, 'rejected')}>
                                Reject
                            </Button>
                            <Button size="small"
                                icon={<SearchOutlined />}
                                onClick={() => onStatusChange(h.id, 'needs_data')}>
                                Need Data
                            </Button>
                        </Space>
                        <Button size="small" type="dashed"
                            onClick={() => onAction(h.action)}>
                            {h.action.label}
                        </Button>
                    </div>
                </div>
            ))}
        </div>
    );
};

export default HypothesisCards;

