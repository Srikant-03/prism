/**
 * ExplainPanel â€” Visual query execution plan tree.
 * Shows EXPLAIN ANALYZE output as an indented tree with timing info.
 */

import React, { useState, useCallback } from 'react';
import { Button, Empty, Spin, Alert, Input, Tooltip } from 'antd';
import {
    ApartmentOutlined, PlayCircleOutlined, CopyOutlined,
    SaveOutlined,
} from '@ant-design/icons';

const { TextArea } = Input;

interface PlanNode {
    depth: number;
    text: string;
    raw: string;
}

interface Props {
    onCreateView?: (name: string, sql: string) => void;
}

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

const ExplainPanel: React.FC<Props> = ({ onCreateView }) => {
    const [sql, setSQL] = useState('');
    const [loading, setLoading] = useState(false);
    const [planText, setPlanText] = useState('');
    const [nodes, setNodes] = useState<PlanNode[]>([]);
    const [error, setError] = useState<string | null>(null);
    const [viewName, setViewName] = useState('');

    const handleExplain = useCallback(async () => {
        if (!sql.trim()) return;
        setLoading(true);
        setError(null);

        try {
            const res = await fetch(`${API_BASE}/api/sql/explain`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql }),
            });
            const data = await res.json();

            if (data.success) {
                setPlanText(data.plan_text);
                setNodes(data.nodes || []);
            } else {
                setError(data.error || 'Failed to explain query');
            }
        } catch (e: any) {
            setError(e.message);
        } finally {
            setLoading(false);
        }
    }, [sql]);

    const handleCreateView = useCallback(async () => {
        if (!viewName.trim() || !sql.trim()) return;

        try {
            const res = await fetch(`${API_BASE}/api/sql/views`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: viewName, sql }),
            });
            const data = await res.json();

            if (data.success) {
                onCreateView?.(data.view_name, sql);
                setViewName('');
            } else {
                setError(data.error);
            }
        } catch (e: any) {
            setError(e.message);
        }
    }, [viewName, sql, onCreateView]);

    const getNodeColor = (text: string) => {
        if (text.includes('SEQ_SCAN') || text.includes('Seq Scan')) return '#ef4444';
        if (text.includes('INDEX') || text.includes('Index')) return '#22c55e';
        if (text.includes('HASH') || text.includes('Hash')) return '#f59e0b';
        if (text.includes('JOIN') || text.includes('Join')) return '#6366f1';
        if (text.includes('FILTER') || text.includes('Filter')) return '#ec4899';
        if (text.includes('SORT') || text.includes('Sort')) return '#06b6d4';
        if (text.includes('AGGREGATE') || text.includes('Aggregate')) return '#8b5cf6';
        return 'rgba(255,255,255,0.6)';
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {/* Header */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: 8,
                padding: '8px 12px',
                background: 'linear-gradient(135deg, rgba(6,182,212,0.06), rgba(99,102,241,0.06))',
                borderRadius: 10, border: '1px solid rgba(6,182,212,0.15)',
            }}>
                <ApartmentOutlined style={{ color: '#06b6d4', fontSize: 16 }} />
                <span style={{ fontWeight: 700, fontSize: 14 }}>Query Plan & Views</span>
            </div>

            {/* SQL input */}
            <TextArea
                value={sql}
                onChange={e => setSQL(e.target.value)}
                placeholder="Enter SQL to analyze execution plan..."
                autoSize={{ minRows: 3, maxRows: 8 }}
                style={{
                    fontFamily: "'Fira Code', monospace", fontSize: 12,
                    background: 'rgba(0,0,0,0.2)', border: '1px solid rgba(6,182,212,0.15)',
                }}
            />

            {/* Actions */}
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
                <Button type="primary" icon={<PlayCircleOutlined />}
                    onClick={handleExplain} loading={loading} disabled={!sql.trim()}>
                    Explain
                </Button>
                <Tooltip title="Copy plan">
                    <Button icon={<CopyOutlined />} size="small"
                        disabled={!planText}
                        onClick={() => navigator.clipboard.writeText(planText)} />
                </Tooltip>
                <div style={{ flex: 1 }} />
                <Input size="small" placeholder="View name" value={viewName}
                    onChange={e => setViewName(e.target.value)}
                    style={{ width: 130 }} />
                <Button size="small" icon={<SaveOutlined />}
                    onClick={handleCreateView}
                    disabled={!viewName.trim() || !sql.trim()}>
                    Save as View
                </Button>
            </div>

            {error && <Alert type="error" message={error} showIcon />}

            {/* Plan tree */}
            {loading && <div style={{ textAlign: 'center', padding: 20 }}><Spin size="large" /></div>}

            {!loading && nodes.length > 0 && (
                <div className="glass-panel" style={{ padding: 12 }}>
                    <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 8 }}>Execution Plan</div>
                    <div style={{ fontFamily: "'Fira Code', monospace", fontSize: 11, lineHeight: 1.8 }}>
                        {nodes.map((node, i) => (
                            <div key={i} style={{
                                paddingLeft: node.depth * 24,
                                display: 'flex', alignItems: 'center', gap: 6,
                            }}>
                                {node.depth > 0 && (
                                    <span style={{ color: 'rgba(255,255,255,0.15)' }}>
                                        {'â””'.repeat(1)}
                                    </span>
                                )}
                                <span style={{ color: getNodeColor(node.text) }}>
                                    {node.text}
                                </span>
                            </div>
                        ))}
                    </div>

                    {/* Raw plan */}
                    <details style={{ marginTop: 12 }}>
                        <summary style={{ fontSize: 11, cursor: 'pointer', color: 'rgba(255,255,255,0.4)' }}>
                            Raw Plan Text
                        </summary>
                        <pre style={{
                            fontSize: 10, color: 'rgba(255,255,255,0.5)',
                            whiteSpace: 'pre-wrap', marginTop: 6,
                            padding: 8, background: 'rgba(0,0,0,0.15)', borderRadius: 6,
                        }}>
                            {planText}
                        </pre>
                    </details>
                </div>
            )}

            {!loading && !nodes.length && !error && (
                <Empty description="Enter SQL and click Explain" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
        </div>
    );
};

export default ExplainPanel;

