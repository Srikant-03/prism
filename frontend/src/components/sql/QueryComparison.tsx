/**
 * QueryComparison — Run two queries side-by-side and compare results.
 */

import React, { useState, useCallback } from 'react';
import { Input, Button, Tag, Empty, Spin, Table } from 'antd';
import {
    SwapOutlined, PlayCircleOutlined, CheckCircleOutlined,
    CloseCircleOutlined, ColumnWidthOutlined,
} from '@ant-design/icons';
import * as sqlApi from '../../api/sql';
import type { QueryResult } from '../../types/sql';

const { TextArea } = Input;

interface Props {
    initialSQL?: string;
}

const QueryComparison: React.FC<Props> = ({ initialSQL }) => {
    const [sqlA, setSqlA] = useState(initialSQL || '');
    const [sqlB, setSqlB] = useState('');
    const [resultA, setResultA] = useState<QueryResult | null>(null);
    const [resultB, setResultB] = useState<QueryResult | null>(null);
    const [loading, setLoading] = useState(false);

    const runBoth = useCallback(async () => {
        setLoading(true);
        try {
            const [a, b] = await Promise.all([
                sqlApi.executeQuery({ sql: sqlA }),
                sqlApi.executeQuery({ sql: sqlB }),
            ]);
            setResultA(a);
            setResultB(b);
        } catch (e: any) {
            setResultA({ success: false, columns: [], rows: [], row_count: 0, error: e.message });
            setResultB({ success: false, columns: [], rows: [], row_count: 0, error: e.message });
        } finally {
            setLoading(false);
        }
    }, [sqlA, sqlB]);

    const buildColumns = (result: QueryResult) =>
        result.columns.map(col => ({
            title: col, dataIndex: col, key: col, ellipsis: true,
            sorter: (a: any, b: any) => {
                const va = a[col], vb = b[col];
                if (va == null) return -1;
                if (vb == null) return 1;
                return typeof va === 'number' ? va - vb : String(va).localeCompare(String(vb));
            },
            render: (v: any) => v === null ? <span style={{ opacity: 0.2 }}>NULL</span> :
                typeof v === 'number' ? <span style={{ fontFamily: 'monospace' }}>{v.toLocaleString()}</span> : String(v),
        }));

    const renderResult = (result: QueryResult | null, label: string) => (
        <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{
                fontWeight: 600, fontSize: 13, marginBottom: 6,
                display: 'flex', alignItems: 'center', gap: 6,
            }}>
                {result?.success ? <CheckCircleOutlined style={{ color: '#52c41a' }} />
                    : result ? <CloseCircleOutlined style={{ color: '#ff4d4f' }} /> : null}
                {label}
                {result?.success && (
                    <Tag style={{ fontSize: 10 }}>{result.row_count} rows · {
                        (result.execution_time_s || 0) < 1
                            ? `${((result.execution_time_s || 0) * 1000).toFixed(0)}ms`
                            : `${(result.execution_time_s || 0).toFixed(2)}s`
                    }</Tag>
                )}
            </div>
            {result?.success ? (
                <Table columns={buildColumns(result)}
                    dataSource={result.rows.map((r, i) => ({ ...r, __key: i }))}
                    rowKey="__key" size="small" scroll={{ x: true }}
                    pagination={{ pageSize: 10, size: 'small' }}
                    style={{ overflow: 'auto' }} />
            ) : result?.error ? (
                <div style={{ color: '#ff4d4f', fontSize: 12, padding: 8 }}>{result.error}</div>
            ) : (
                <Empty description="Run to see results" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
        </div>
    );

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {/* Header */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: 8,
                padding: '8px 12px',
                background: 'linear-gradient(135deg, rgba(236,72,153,0.06), rgba(139,92,246,0.06))',
                borderRadius: 10, border: '1px solid rgba(236,72,153,0.15)',
            }}>
                <ColumnWidthOutlined style={{ color: '#ec4899', fontSize: 16 }} />
                <span style={{ fontWeight: 700, fontSize: 14 }}>Query Comparison</span>
                <div style={{ marginLeft: 'auto' }}>
                    <Button type="primary" icon={<PlayCircleOutlined />}
                        onClick={runBoth} loading={loading}
                        disabled={!sqlA.trim() || !sqlB.trim()}>
                        Run Both
                    </Button>
                </div>
            </div>

            {/* SQL inputs */}
            <div style={{ display: 'flex', gap: 10 }}>
                <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4, color: '#6366f1' }}>Query A</div>
                    <TextArea value={sqlA} onChange={e => setSqlA(e.target.value)}
                        placeholder="Enter first SQL query..."
                        autoSize={{ minRows: 3, maxRows: 8 }}
                        style={{ fontFamily: 'monospace', fontSize: 12, background: 'rgba(0,0,0,0.2)' }} />
                </div>
                <div style={{ display: 'flex', alignItems: 'center' }}>
                    <Button icon={<SwapOutlined />} size="small"
                        onClick={() => { const t = sqlA; setSqlA(sqlB); setSqlB(t); }} />
                </div>
                <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4, color: '#ec4899' }}>Query B</div>
                    <TextArea value={sqlB} onChange={e => setSqlB(e.target.value)}
                        placeholder="Enter second SQL query..."
                        autoSize={{ minRows: 3, maxRows: 8 }}
                        style={{ fontFamily: 'monospace', fontSize: 12, background: 'rgba(0,0,0,0.2)' }} />
                </div>
            </div>

            {/* Results side-by-side */}
            {loading && <div style={{ textAlign: 'center', padding: 20 }}><Spin size="large" /></div>}
            {!loading && (resultA || resultB) && (
                <div style={{ display: 'flex', gap: 10 }}>
                    {renderResult(resultA, 'Result A')}
                    {renderResult(resultB, 'Result B')}
                </div>
            )}
        </div>
    );
};

export default QueryComparison;

