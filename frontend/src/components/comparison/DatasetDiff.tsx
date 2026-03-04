/**
 * DatasetDiff â€” Upload and compare two datasets (structural + statistical diff).
 */

import React, { useState, useCallback } from 'react';
import { Card, Button, Upload, Space, Tag, Table, Progress, Alert, Divider, Row, Col, Statistic, Empty } from 'antd';
import {
    DiffOutlined, UploadOutlined, SwapOutlined,
    PlusCircleOutlined, MinusCircleOutlined, WarningOutlined,
} from '@ant-design/icons';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

interface DiffResult {
    schema_diff: {
        added_columns: string[];
        removed_columns: string[];
        type_changed: { column: string; from: string; to: string }[];
        common_columns: string[];
    };
    row_diff: {
        added_rows: number;
        removed_rows: number;
        modified_rows: number;
        total_a: number;
        total_b: number;
    };
    column_drift: {
        column: string;
        psi: number;
        ks_statistic: number;
        ks_pvalue: number;
        drift_detected: boolean;
        mean_a: number;
        mean_b: number;
    }[];
}

interface Props {
    fileIdA?: string;
}

const DatasetDiff: React.FC<Props> = ({ fileIdA }) => {
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState<DiffResult | null>(null);
    const [fileB, setFileB] = useState<File | null>(null);

    const runDiff = useCallback(async () => {
        if (!fileIdA || !fileB) return;
        setLoading(true);
        try {
            const formData = new FormData();
            formData.append('file_b', fileB);
            formData.append('file_id_a', fileIdA);

            const res = await fetch(`${API_BASE}/api/comparison/diff`, {
                method: 'POST',
                body: formData,
            });
            const data = await res.json();
            setResult(data);
        } catch (e: any) {
            console.error('Diff error:', e);
        } finally {
            setLoading(false);
        }
    }, [fileIdA, fileB]);

    const driftColumns = [
        {
            title: 'Column',
            dataIndex: 'column',
            render: (v: string, r: any) => (
                <Space>
                    {r.drift_detected && <WarningOutlined style={{ color: '#f59e0b' }} />}
                    <span style={{ fontWeight: 600 }}>{v}</span>
                </Space>
            ),
        },
        {
            title: 'PSI',
            dataIndex: 'psi',
            width: 80,
            render: (v: number) => (
                <Tag color={v > 0.2 ? 'red' : v > 0.1 ? 'orange' : 'green'} style={{ fontSize: 10 }}>
                    {v?.toFixed(3)}
                </Tag>
            ),
        },
        {
            title: 'KS Stat',
            dataIndex: 'ks_statistic',
            width: 80,
            render: (v: number) => <span style={{ fontFamily: 'monospace', fontSize: 11 }}>{v?.toFixed(3)}</span>,
        },
        {
            title: 'p-value',
            dataIndex: 'ks_pvalue',
            width: 80,
            render: (v: number) => (
                <Tag color={v < 0.05 ? 'red' : 'green'} style={{ fontSize: 10 }}>
                    {v?.toFixed(4)}
                </Tag>
            ),
        },
        {
            title: 'Mean A â†’ B',
            render: (_: any, r: any) => (
                <span style={{ fontFamily: 'monospace', fontSize: 11 }}>
                    {r.mean_a?.toFixed(2)} â†’ {r.mean_b?.toFixed(2)}
                </span>
            ),
        },
        {
            title: 'Drift',
            dataIndex: 'drift_detected',
            width: 60,
            render: (v: boolean) => v ? <Tag color="red">YES</Tag> : <Tag color="green">No</Tag>,
        },
    ];

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div className="glass-panel" style={{ padding: 16 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16 }}>
                    <DiffOutlined style={{ color: '#6366f1', fontSize: 18 }} />
                    <strong style={{ fontSize: 15 }}>Dataset Comparison</strong>
                    <Tag color="purple" style={{ fontSize: 10 }}>DIFF</Tag>
                </div>

                <Row gutter={16} align="middle">
                    <Col span={10}>
                        <Card size="small" style={{ textAlign: 'center', background: 'rgba(99,102,241,0.05)' }}>
                            <div style={{ fontSize: 12, opacity: 0.6, marginBottom: 4 }}>Dataset A</div>
                            <Tag color="blue">Current Dataset</Tag>
                        </Card>
                    </Col>
                    <Col span={4} style={{ textAlign: 'center' }}>
                        <SwapOutlined style={{ fontSize: 24, color: '#6366f1' }} />
                    </Col>
                    <Col span={10}>
                        <Upload
                            beforeUpload={file => { setFileB(file); return false; }}
                            maxCount={1}
                            showUploadList={fileB ? true : false}
                        >
                            <Button icon={<UploadOutlined />} block>
                                Upload Dataset B
                            </Button>
                        </Upload>
                    </Col>
                </Row>

                <div style={{ marginTop: 12, textAlign: 'right' }}>
                    <Button type="primary" icon={<DiffOutlined />}
                        onClick={runDiff} loading={loading}
                        disabled={!fileB}>
                        Compare Datasets
                    </Button>
                </div>
            </div>

            {/* Results */}
            {result && (
                <>
                    {/* Schema diff */}
                    <div className="glass-panel" style={{ padding: 12 }}>
                        <strong style={{ fontSize: 13 }}>Schema Changes</strong>
                        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 8 }}>
                            {result.schema_diff.added_columns.map(c => (
                                <Tag key={c} color="green" icon={<PlusCircleOutlined />}>{c}</Tag>
                            ))}
                            {result.schema_diff.removed_columns.map(c => (
                                <Tag key={c} color="red" icon={<MinusCircleOutlined />}>{c}</Tag>
                            ))}
                            {result.schema_diff.type_changed.map(c => (
                                <Tag key={c.column} color="orange" icon={<SwapOutlined />}>
                                    {c.column}: {c.from}â†’{c.to}
                                </Tag>
                            ))}
                            {result.schema_diff.added_columns.length === 0 &&
                                result.schema_diff.removed_columns.length === 0 &&
                                result.schema_diff.type_changed.length === 0 && (
                                    <Tag color="green">Schemas match</Tag>
                                )}
                        </div>
                    </div>

                    {/* Row diff */}
                    <div className="glass-panel" style={{ padding: 12 }}>
                        <Row gutter={16}>
                            <Col span={6}>
                                <Statistic title="Rows in A" value={result.row_diff.total_a} valueStyle={{ fontSize: 16 }} />
                            </Col>
                            <Col span={6}>
                                <Statistic title="Rows in B" value={result.row_diff.total_b} valueStyle={{ fontSize: 16 }} />
                            </Col>
                            <Col span={4}>
                                <Statistic title="Added" value={result.row_diff.added_rows}
                                    valueStyle={{ color: '#52c41a', fontSize: 16 }} prefix="+" />
                            </Col>
                            <Col span={4}>
                                <Statistic title="Removed" value={result.row_diff.removed_rows}
                                    valueStyle={{ color: '#ef4444', fontSize: 16 }} prefix="âˆ’" />
                            </Col>
                            <Col span={4}>
                                <Statistic title="Modified" value={result.row_diff.modified_rows}
                                    valueStyle={{ color: '#f59e0b', fontSize: 16 }} />
                            </Col>
                        </Row>
                    </div>

                    {/* Column drift */}
                    {result.column_drift.length > 0 && (
                        <div className="glass-panel" style={{ padding: 0 }}>
                            <div style={{ padding: '8px 12px', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
                                <strong style={{ fontSize: 13 }}>Column Drift Analysis (PSI + KS Test)</strong>
                            </div>
                            <Table
                                columns={driftColumns}
                                dataSource={result.column_drift}
                                rowKey="column"
                                size="small"
                                pagination={false}
                            />
                        </div>
                    )}
                </>
            )}
        </div>
    );
};

export default DatasetDiff;

