/**
 * ResultsGrid — Paginated results table with sorting, formatting, and export.
 */

import React, { useMemo, useState } from 'react';
import { Table, Tag, Button, Space, Select, Empty, Alert } from 'antd';
import {
    DownloadOutlined, ClockCircleOutlined, DatabaseOutlined,
    CheckCircleOutlined, CloseCircleOutlined, DashboardOutlined,
} from '@ant-design/icons';
import type { QueryResult } from '../../types/sql';

interface Props {
    result: QueryResult | null;
    onExport: (format: string) => void;
    loading?: boolean;
}

const ResultsGrid: React.FC<Props> = ({ result, onExport, loading }) => {
    const [pageSize, setPageSize] = useState(25);

    const tableColumns = useMemo(() => {
        if (!result?.columns) return [];
        return result.columns.map((col, i) => ({
            title: (
                <div style={{ fontSize: 12 }}>
                    <div>{col}</div>
                    {result.column_types?.[i] && (
                        <Tag style={{ fontSize: 9, marginTop: 2 }}>
                            {result.column_types[i]}
                        </Tag>
                    )}
                </div>
            ),
            dataIndex: col,
            key: col,
            sorter: (a: any, b: any) => {
                const va = a[col];
                const vb = b[col];
                if (va == null && vb == null) return 0;
                if (va == null) return -1;
                if (vb == null) return 1;
                if (typeof va === 'number') return va - vb;
                return String(va).localeCompare(String(vb));
            },
            render: (value: any) => {
                if (value === null || value === undefined) {
                    return <span style={{ color: 'rgba(255,255,255,0.2)', fontStyle: 'italic' }}>NULL</span>;
                }
                if (typeof value === 'boolean') {
                    return <Tag color={value ? 'green' : 'red'}>{value ? 'true' : 'false'}</Tag>;
                }
                if (typeof value === 'number') {
                    return <span style={{ fontFamily: 'monospace' }}>{value.toLocaleString()}</span>;
                }
                const s = String(value);
                if (s.length > 100) {
                    return <span title={s}>{s.slice(0, 100)}…</span>;
                }
                return s;
            },
            ellipsis: true,
        }));
    }, [result]);

    const dataSource = useMemo(() => {
        if (!result?.rows) return [];
        return result.rows.map((row, i) => ({ ...row, __key: i }));
    }, [result]);

    if (!result) {
        return (
            <div className="glass-panel" style={{ padding: 24, textAlign: 'center' }}>
                <Empty
                    description="Run a query to see results"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
            </div>
        );
    }

    if (!result.success) {
        return (
            <div className="glass-panel" style={{ padding: 16 }}>
                <Alert
                    type="error"
                    showIcon
                    icon={<CloseCircleOutlined />}
                    message="Query Error"
                    description={result.error || 'An unknown error occurred.'}
                />
            </div>
        );
    }

    return (
        <div className="glass-panel" style={{ padding: 0 }}>
            {/* Stats bar */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 16px',
                borderBottom: '1px solid rgba(255,255,255,0.06)',
                flexWrap: 'wrap', gap: 8,
            }}>
                <Space size={16}>
                    <Space size={4}>
                        <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 12 }} />
                        <span style={{ fontSize: 12 }}>
                            <strong>{result.row_count.toLocaleString()}</strong> rows
                        </span>
                    </Space>
                    <Space size={4}>
                        <DatabaseOutlined style={{ fontSize: 12, color: 'rgba(255,255,255,0.4)' }} />
                        <span style={{ fontSize: 12, color: 'rgba(255,255,255,0.5)' }}>
                            {result.columns.length} columns
                        </span>
                    </Space>
                    {result.execution_time_s !== undefined && (
                        <Space size={4}>
                            <ClockCircleOutlined style={{ fontSize: 12, color: 'rgba(255,255,255,0.4)' }} />
                            <span style={{ fontSize: 12, color: 'rgba(255,255,255,0.5)' }}>
                                {result.execution_time_s < 1
                                    ? `${(result.execution_time_s * 1000).toFixed(0)}ms`
                                    : `${result.execution_time_s.toFixed(2)}s`
                                }
                            </span>
                        </Space>
                    )}
                    {(result as any).memory_mb !== undefined && (
                        <Space size={4}>
                            <DashboardOutlined style={{ fontSize: 12, color: 'rgba(255,255,255,0.4)' }} />
                            <span style={{ fontSize: 12, color: 'rgba(255,255,255,0.5)' }}>
                                {(result as any).memory_mb.toFixed(1)} MB
                            </span>
                        </Space>
                    )}
                </Space>
                <Space size={4}>
                    <Select
                        size="small"
                        value={pageSize}
                        onChange={setPageSize}
                        style={{ width: 80 }}
                        options={[
                            { value: 10, label: '10' },
                            { value: 25, label: '25' },
                            { value: 50, label: '50' },
                            { value: 100, label: '100' },
                        ]}
                    />
                    <Button size="small" icon={<DownloadOutlined />} onClick={() => onExport('csv')} aria-label="Export CSV">CSV</Button>
                    <Button size="small" icon={<DownloadOutlined />} onClick={() => onExport('json')} aria-label="Export JSON">JSON</Button>
                    <Button size="small" icon={<DownloadOutlined />} onClick={() => onExport('excel')} aria-label="Export Excel">Excel</Button>
                </Space>
            </div>

            {/* Results table */}
            <Table
                columns={tableColumns}
                dataSource={dataSource}
                rowKey="__key"
                size="small"
                scroll={{ x: true }}
                loading={loading}
                pagination={{
                    pageSize,
                    showSizeChanger: false,
                    showTotal: (total) => `${total} rows`,
                    size: 'small',
                }}
                style={{ overflow: 'auto' }}
            />
        </div>
    );
};

export default ResultsGrid;
