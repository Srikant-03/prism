/**
 * WhatIfPanel — Sandbox mode for testing preprocessing decisions.
 * Simulates steps on a sample before committing to full dataset.
 */

import React, { useState, useCallback } from 'react';
import { Card, Button, Space, Tag, Progress, Statistic, Row, Col, Alert, Empty, Tooltip, Switch, Select, Divider } from 'antd';
import {
    ExperimentOutlined, CheckCircleOutlined, CloseCircleOutlined,
    ThunderboltOutlined, ArrowRightOutlined, BarChartOutlined,
    PlusOutlined, DeleteOutlined,
} from '@ant-design/icons';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

interface SimStep {
    id: string;
    action: string;
    column?: string;
    params: Record<string, any>;
}

interface SimResult {
    before: Record<string, any>;
    after: Record<string, any>;
    deltas: Record<string, number>;
    readiness_before: number;
    readiness_after: number;
    rows_affected: number;
    sample_size: number;
}

interface Props {
    columns: string[];
    fileId: string;
}

const STEP_OPTIONS = [
    { value: 'fill_nulls', label: 'Fill Missing Values', params: ['strategy'] },
    { value: 'remove_outliers', label: 'Remove Outliers', params: ['method', 'threshold'] },
    { value: 'normalize', label: 'Normalize Column', params: ['method'] },
    { value: 'encode_categorical', label: 'Encode Categorical', params: ['method'] },
    { value: 'drop_column', label: 'Drop Column', params: [] },
    { value: 'drop_duplicates', label: 'Drop Duplicates', params: [] },
    { value: 'standardize', label: 'Standardize Column', params: ['method'] },
    { value: 'log_transform', label: 'Log Transform', params: [] },
];

const WhatIfPanel: React.FC<Props> = ({ columns, fileId }) => {
    const [steps, setSteps] = useState<SimStep[]>([]);
    const [results, setResults] = useState<SimResult | null>(null);
    const [loading, setLoading] = useState(false);
    const [samplePct, setSamplePct] = useState(10);

    const addStep = useCallback(() => {
        setSteps(prev => [...prev, {
            id: Date.now().toString(),
            action: 'fill_nulls',
            column: columns[0],
            params: { strategy: 'mean' },
        }]);
    }, [columns]);

    const updateStep = useCallback((id: string, field: string, value: any) => {
        setSteps(prev => prev.map(s =>
            s.id === id ? { ...s, [field]: value } : s
        ));
    }, []);

    const removeStep = useCallback((id: string) => {
        setSteps(prev => prev.filter(s => s.id !== id));
    }, []);

    const simulate = useCallback(async () => {
        if (steps.length === 0) return;
        setLoading(true);
        try {
            const res = await fetch(`${API_BASE}/api/simulate/chain`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    file_id: fileId,
                    steps: steps.map(s => ({
                        action: s.action,
                        column: s.column,
                        params: s.params,
                    })),
                    sample_pct: samplePct,
                }),
            });
            const data = await res.json();
            setResults(data);
        } catch (e: any) {
            console.error('Simulation error:', e);
        } finally {
            setLoading(false);
        }
    }, [steps, fileId, samplePct]);

    const commitAll = useCallback(async () => {
        try {
            await fetch(`${API_BASE}/api/simulate/commit`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    file_id: fileId,
                    steps: steps.map(s => ({
                        action: s.action,
                        column: s.column,
                        params: s.params,
                    })),
                }),
            });
            setResults(null);
            setSteps([]);
        } catch (e: any) {
            console.error('Commit error:', e);
        }
    }, [steps, fileId]);

    const readinessColor = (score: number) =>
        score >= 80 ? '#52c41a' : score >= 60 ? '#faad14' : '#ff4d4f';

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div className="glass-panel" style={{ padding: 12 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                    <Space>
                        <ExperimentOutlined style={{ color: '#a78bfa', fontSize: 18 }} />
                        <strong style={{ fontSize: 15 }}>What-If Simulator</strong>
                        <Tag color="purple" style={{ fontSize: 10 }}>SANDBOX</Tag>
                    </Space>
                    <Space>
                        <span style={{ fontSize: 11, opacity: 0.6 }}>Sample:</span>
                        <Select size="small" value={samplePct} onChange={setSamplePct}
                            style={{ width: 80 }}
                            options={[
                                { value: 5, label: '5%' },
                                { value: 10, label: '10%' },
                                { value: 25, label: '25%' },
                                { value: 50, label: '50%' },
                            ]}
                        />
                        <Button size="small" icon={<PlusOutlined />} onClick={addStep}>
                            Add Step
                        </Button>
                    </Space>
                </div>

                {/* Steps */}
                {steps.length === 0 ? (
                    <Empty
                        description="Add preprocessing steps to simulate their effect"
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                    >
                        <Button type="dashed" icon={<PlusOutlined />} onClick={addStep}>
                            Add First Step
                        </Button>
                    </Empty>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                        {steps.map((step, i) => (
                            <div key={step.id} className="glass-panel" style={{
                                padding: '8px 12px', display: 'flex', alignItems: 'center', gap: 8,
                                borderLeft: '3px solid #a78bfa',
                            }}>
                                <Tag color="purple" style={{ margin: 0, fontSize: 10 }}>{i + 1}</Tag>
                                <Select
                                    size="small"
                                    value={step.action}
                                    onChange={v => updateStep(step.id, 'action', v)}
                                    style={{ width: 180 }}
                                    options={STEP_OPTIONS.map(o => ({ value: o.value, label: o.label }))}
                                />
                                {step.action !== 'drop_duplicates' && (
                                    <Select
                                        size="small"
                                        value={step.column}
                                        onChange={v => updateStep(step.id, 'column', v)}
                                        style={{ width: 140 }}
                                        options={columns.map(c => ({ value: c, label: c }))}
                                        placeholder="Column"
                                    />
                                )}
                                {(step.action === 'fill_nulls') && (
                                    <Select
                                        size="small"
                                        value={step.params.strategy || 'mean'}
                                        onChange={v => updateStep(step.id, 'params', { ...step.params, strategy: v })}
                                        style={{ width: 100 }}
                                        options={[
                                            { value: 'mean', label: 'Mean' },
                                            { value: 'median', label: 'Median' },
                                            { value: 'mode', label: 'Mode' },
                                            { value: 'zero', label: 'Zero' },
                                            { value: 'ffill', label: 'Forward Fill' },
                                        ]}
                                    />
                                )}
                                <Button size="small" icon={<DeleteOutlined />}
                                    onClick={() => removeStep(step.id)} danger />
                            </div>
                        ))}
                        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                            <Button onClick={() => { setSteps([]); setResults(null); }} icon={<CloseCircleOutlined />}>
                                Discard
                            </Button>
                            <Button type="primary" icon={<ThunderboltOutlined />}
                                onClick={simulate} loading={loading}>
                                Simulate on {samplePct}% Sample
                            </Button>
                        </div>
                    </div>
                )}
            </div>

            {/* Results */}
            {results && (
                <div className="glass-panel" style={{ padding: 16 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                        <strong>Simulation Results</strong>
                        <Tag color="blue">Sample: {results.sample_size.toLocaleString()} rows</Tag>
                    </div>

                    {/* Readiness Score */}
                    <Row gutter={16} style={{ marginBottom: 16 }}>
                        <Col span={8}>
                            <Card size="small" style={{ textAlign: 'center', background: 'rgba(0,0,0,0.2)', border: 'none' }}>
                                <Statistic
                                    title="Before"
                                    value={results.readiness_before}
                                    suffix="/100"
                                    valueStyle={{ color: readinessColor(results.readiness_before) }}
                                />
                            </Card>
                        </Col>
                        <Col span={8} style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                            <ArrowRightOutlined style={{ fontSize: 24, color: '#a78bfa' }} />
                        </Col>
                        <Col span={8}>
                            <Card size="small" style={{ textAlign: 'center', background: 'rgba(0,0,0,0.2)', border: 'none' }}>
                                <Statistic
                                    title="After"
                                    value={results.readiness_after}
                                    suffix="/100"
                                    valueStyle={{ color: readinessColor(results.readiness_after) }}
                                />
                            </Card>
                        </Col>
                    </Row>

                    {/* Deltas */}
                    <Divider style={{ margin: '12px 0', borderColor: 'rgba(255,255,255,0.06)' }}>
                        Impact Details
                    </Divider>
                    <Row gutter={[8, 8]}>
                        {Object.entries(results.deltas).map(([key, val]) => (
                            <Col span={6} key={key}>
                                <div style={{
                                    padding: 8, borderRadius: 6,
                                    background: 'rgba(0,0,0,0.15)',
                                    textAlign: 'center', fontSize: 11,
                                }}>
                                    <div style={{ opacity: 0.6, marginBottom: 4 }}>{key}</div>
                                    <div style={{
                                        fontWeight: 700,
                                        color: val > 0 ? '#52c41a' : val < 0 ? '#ff4d4f' : '#8b949e',
                                    }}>
                                        {val > 0 ? '+' : ''}{typeof val === 'number' ? val.toFixed(2) : val}
                                    </div>
                                </div>
                            </Col>
                        ))}
                    </Row>

                    <div style={{ marginTop: 16, display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                        <Button icon={<CloseCircleOutlined />}
                            onClick={() => setResults(null)}>
                            Discard
                        </Button>
                        <Button type="primary" icon={<CheckCircleOutlined />}
                            onClick={commitAll}
                            style={{ background: '#52c41a', borderColor: '#52c41a' }}>
                            Commit to Full Dataset
                        </Button>
                    </div>
                </div>
            )}
        </div>
    );
};

export default WhatIfPanel;
