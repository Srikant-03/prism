/**
 * ExplainPointDrawer — Power BI-style "Explain the Increase / Decrease" AI drawer.
 * Breaks down point delta into dimension driver contributions and outlier records.
 */
import React, { useState, useEffect } from 'react';
import { Drawer, Spin, Alert, Tag, Typography, Progress } from 'antd';
import { RiseOutlined, FallOutlined, RobotOutlined, WarningOutlined } from '@ant-design/icons';
import { fetchAuth, API_BASE } from '../../api/client';
import { formatCompactNumber } from '../../types/dashboard';

const { Paragraph } = Typography;

interface Props {
    open: boolean;
    fileId: string;
    metric: string;
    dimension: string;
    priorPoint: string;
    currentPoint: string;
    onClose: () => void;
}

interface Contributor {
    name: string;
    delta: number;
    pct_of_change: number;
    prior_value: number;
    current_value: number;
}

interface OutlierRow {
    row_index: number;
    value: number;
    z_score: number;
    description: string;
}

interface ExplainData {
    prior_point: string;
    current_point: string;
    prior_total: number;
    current_total: number;
    total_delta: number;
    pct_change: number;
    direction: 'increase' | 'decrease';
    top_driver_dimension: string;
    narrative: string;
    contributors: Contributor[];
    outliers: OutlierRow[];
}

const ExplainPointDrawer: React.FC<Props> = ({
    open,
    fileId,
    metric,
    dimension,
    priorPoint,
    currentPoint,
    onClose,
}) => {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [data, setData] = useState<ExplainData | null>(null);

    useEffect(() => {
        if (!open || !fileId || !metric || !priorPoint || !currentPoint) return;
        let cancelled = false;

        const runExplain = async () => {
            setLoading(true);
            setError(null);
            try {
                const res = await fetchAuth(`${API_BASE}/api/dashboard/explain-point`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        file_id: fileId,
                        metric,
                        dimension,
                        prior_point: priorPoint,
                        current_point: currentPoint,
                    }),
                });
                if (!res.ok) throw new Error('Failed to compute point explanation');
                const resData = await res.json();
                if (!cancelled) setData(resData);
            } catch (err: any) {
                if (!cancelled) setError(err.message || 'Error explaining point delta');
            } finally {
                if (!cancelled) setLoading(false);
            }
        };

        runExplain();
        return () => { cancelled = true; };
    }, [open, fileId, metric, dimension, priorPoint, currentPoint]);

    return (
        <Drawer
            title={
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#f8fafc' }}>
                    <RobotOutlined style={{ color: '#818cf8', fontSize: 18 }} />
                    <span>Explain the {data?.direction === 'increase' ? 'Increase' : 'Decrease'}</span>
                </div>
            }
            placement="right"
            open={open}
            onClose={onClose}
            width={480}
            styles={{ body: { padding: '16px', background: '#0f172a' }, header: { background: '#0f172a' } }}
        >
            {loading && (
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: 260 }}>
                    <Spin size="large" />
                    <span style={{ marginTop: 12, color: '#94a3b8' }}>Analyzing driver dimensions and records...</span>
                </div>
            )}

            {error && (
                <Alert
                    type="error"
                    showIcon
                    message="Explanation Unavailable"
                    description={error}
                />
            )}

            {!loading && data && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                    {/* Summary Header Card */}
                    <div
                        style={{
                            background: '#1e293b',
                            borderRadius: 10,
                            padding: 16,
                            border: '1px solid #334155',
                        }}
                    >
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
                            <span style={{ fontSize: 13, color: '#94a3b8' }}>
                                {data.prior_point} → {data.current_point}
                            </span>
                            <Tag
                                color={data.total_delta >= 0 ? 'green' : 'red'}
                                icon={data.total_delta >= 0 ? <RiseOutlined /> : <FallOutlined />}
                                style={{ fontSize: 12, padding: '2px 8px' }}
                            >
                                {data.total_delta >= 0 ? '+' : ''}{formatCompactNumber(data.total_delta)} ({data.pct_change.toFixed(1)}%)
                            </Tag>
                        </div>
                        <div style={{ display: 'flex', gap: 24 }}>
                            <div>
                                <span style={{ fontSize: 11, color: '#64748b' }}>Prior Total</span>
                                <div style={{ fontSize: 16, fontWeight: 700, color: '#cbd5e1' }}>
                                    {formatCompactNumber(data.prior_total)}
                                </div>
                            </div>
                            <div>
                                <span style={{ fontSize: 11, color: '#64748b' }}>Current Total</span>
                                <div style={{ fontSize: 16, fontWeight: 700, color: '#f8fafc' }}>
                                    {formatCompactNumber(data.current_total)}
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* AI Narrative */}
                    {data.narrative && (
                        <div
                            style={{
                                background: 'rgba(99, 102, 241, 0.1)',
                                border: '1px solid rgba(99, 102, 241, 0.3)',
                                borderRadius: 10,
                                padding: 14,
                            }}
                        >
                            <span style={{ fontSize: 12, fontWeight: 600, color: '#a5b4fc', display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                                <RobotOutlined /> AI Root Cause Analysis
                            </span>
                            <Paragraph style={{ color: '#e2e8f0', fontSize: 13, margin: 0, lineHeight: 1.5 }}>
                                {data.narrative}
                            </Paragraph>
                        </div>
                    )}

                    {/* Drivers Breakdown */}
                    <div>
                        <span style={{ fontSize: 12, fontWeight: 600, color: '#cbd5e1', textTransform: 'uppercase', letterSpacing: 0.5, display: 'block', marginBottom: 8 }}>
                            Top Category Drivers ({data.top_driver_dimension})
                        </span>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            {data.contributors.slice(0, 6).map((c, i) => {
                                const isPos = c.delta >= 0;
                                return (
                                    <div
                                        key={i}
                                        style={{
                                            background: '#1e293b',
                                            padding: '8px 12px',
                                            borderRadius: 8,
                                            border: '1px solid #334155',
                                        }}
                                    >
                                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                                            <span style={{ fontWeight: 600, color: '#f1f5f9', fontSize: 13 }}>{c.name}</span>
                                            <span style={{ color: isPos ? '#34d399' : '#f87171', fontWeight: 600, fontSize: 13 }}>
                                                {isPos ? '+' : ''}{formatCompactNumber(c.delta)}
                                            </span>
                                        </div>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                            <Progress
                                                percent={Math.min(100, Math.round(c.pct_of_change))}
                                                strokeColor={isPos ? '#10b981' : '#ef4444'}
                                                showInfo={false}
                                                size="small"
                                                style={{ flex: 1, margin: 0 }}
                                            />
                                            <span style={{ fontSize: 11, color: '#94a3b8', width: 44, textAlign: 'right' }}>
                                                {c.pct_of_change.toFixed(1)}%
                                            </span>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>

                    {/* Outlier Transactions */}
                    {data.outliers && data.outliers.length > 0 && (
                        <div>
                            <span style={{ fontSize: 12, fontWeight: 600, color: '#cbd5e1', textTransform: 'uppercase', letterSpacing: 0.5, display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
                                <WarningOutlined style={{ color: '#f59e0b' }} /> Outlier Transactions
                            </span>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                                {data.outliers.map((o, idx) => (
                                    <div
                                        key={idx}
                                        style={{
                                            background: '#1e293b',
                                            padding: '8px 12px',
                                            borderRadius: 6,
                                            border: '1px solid #475569',
                                            display: 'flex',
                                            justifyContent: 'space-between',
                                            alignItems: 'center',
                                        }}
                                    >
                                        <div>
                                            <div style={{ color: '#f8fafc', fontSize: 12, fontWeight: 500 }}>
                                                {o.description}
                                            </div>
                                            <div style={{ fontSize: 11, color: '#94a3b8' }}>
                                                Row #{o.row_index} • Z-Score: {o.z_score.toFixed(2)}
                                            </div>
                                        </div>
                                        <Tag color="orange" style={{ fontWeight: 600 }}>
                                            {formatCompactNumber(o.value)}
                                        </Tag>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            )}
        </Drawer>
    );
};

export default ExplainPointDrawer;
