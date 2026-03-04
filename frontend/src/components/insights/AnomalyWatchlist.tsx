/**
 * AnomalyWatchlist â€” Persistent panel for curating and investigating suspicious records.
 * Tracks anomalies across profiling/cleaning with investigation workflow.
 */

import React, { useState, useMemo, useCallback } from 'react';
import { Table, Tag, Button, Space, Select, Input, Badge, Tooltip, Modal, Empty, Alert } from 'antd';
import {
    WarningOutlined, EyeOutlined, CheckCircleOutlined,
    EditOutlined, ExclamationCircleOutlined, SearchOutlined, DownloadOutlined
} from '@ant-design/icons';

interface WatchlistItem {
    id: string;
    rowIndex: number;
    column: string;
    value: any;
    severity: 'critical' | 'warning' | 'info';
    reason: string;
    detectedBy: string;
    status: 'unreviewed' | 'investigating' | 'resolved' | 'accepted' | 'fixed';
    note?: string;
}

interface Props {
    items: WatchlistItem[];
    onStatusChange: (id: string, status: WatchlistItem['status']) => void;
    onNoteChange: (id: string, note: string) => void;
    onJumpToRow: (rowIndex: number) => void;
    onBulkAction: (ids: string[], status: WatchlistItem['status']) => void;
}

const severityColors = {
    critical: '#ef4444',
    warning: '#f59e0b',
    info: '#3b82f6',
};

const statusLabels = {
    unreviewed: { color: 'default', text: 'Unreviewed' },
    investigating: { color: 'processing', text: 'Investigating' },
    resolved: { color: 'success', text: 'Resolved' },
    accepted: { color: 'cyan', text: 'Accepted' },
    fixed: { color: 'green', text: 'Fixed' },
};

const AnomalyWatchlist: React.FC<Props> = ({
    items, onStatusChange, onNoteChange, onJumpToRow, onBulkAction,
}) => {
    const [search, setSearch] = useState('');
    const [severityFilter, setSeverityFilter] = useState<string | null>(null);
    const [statusFilter, setStatusFilter] = useState<string | null>(null);
    const [selectedIds, setSelectedIds] = useState<string[]>([]);
    const [noteModal, setNoteModal] = useState<{ visible: boolean; item?: WatchlistItem }>({ visible: false });
    const [noteText, setNoteText] = useState('');

    const filtered = useMemo(() => {
        return items.filter(item => {
            if (severityFilter && item.severity !== severityFilter) return false;
            if (statusFilter && item.status !== statusFilter) return false;
            if (search) {
                const lower = search.toLowerCase();
                return (
                    item.column.toLowerCase().includes(lower) ||
                    item.reason.toLowerCase().includes(lower) ||
                    String(item.value).toLowerCase().includes(lower)
                );
            }
            return true;
        });
    }, [items, search, severityFilter, statusFilter]);

    const counts = useMemo(() => ({
        total: items.length,
        critical: items.filter(i => i.severity === 'critical').length,
        warning: items.filter(i => i.severity === 'warning').length,
        unreviewed: items.filter(i => i.status === 'unreviewed').length,
    }), [items]);

    const handleSaveNote = useCallback(() => {
        if (noteModal.item) {
            onNoteChange(noteModal.item.id, noteText);
        }
        setNoteModal({ visible: false });
        setNoteText('');
    }, [noteModal, noteText, onNoteChange]);

    const exportCSV = useCallback(() => {
        const headers = ['Row', 'Column', 'Value', 'Severity', 'Reason', 'Status', 'Note'];
        const rows = filtered.map(i => [
            i.rowIndex, i.column, i.value, i.severity, i.reason, i.status, i.note || '',
        ]);
        const csv = [headers.join(','), ...rows.map(r => r.map(v => `"${v}"`).join(','))].join('\n');
        const blob = new Blob([csv], { type: 'text/csv' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'anomaly_watchlist.csv';
        a.click();
    }, [filtered]);

    const columns = [
        {
            title: '',
            width: 8,
            render: (_: any, record: WatchlistItem) => (
                <div style={{
                    width: 6, height: 30, borderRadius: 3,
                    background: severityColors[record.severity],
                }} />
            ),
        },
        {
            title: 'Row',
            dataIndex: 'rowIndex',
            width: 60,
            render: (v: number) => (
                <Button type="link" size="small" onClick={() => onJumpToRow(v)}
                    style={{ fontSize: 12, fontFamily: 'monospace' }}>
                    #{v}
                </Button>
            ),
        },
        {
            title: 'Column',
            dataIndex: 'column',
            width: 120,
            render: (v: string) => <Tag style={{ fontSize: 11 }}>{v}</Tag>,
        },
        {
            title: 'Value',
            dataIndex: 'value',
            width: 100,
            ellipsis: true,
            render: (v: any) => (
                <span style={{ fontFamily: 'monospace', fontSize: 11 }}>
                    {v === null ? <i style={{ opacity: 0.3 }}>NULL</i> : String(v).slice(0, 30)}
                </span>
            ),
        },
        {
            title: 'Reason',
            dataIndex: 'reason',
            ellipsis: true,
            render: (v: string, record: WatchlistItem) => (
                <Space size={4}>
                    <Tag color={record.severity === 'critical' ? 'red' : record.severity === 'warning' ? 'orange' : 'blue'}
                        style={{ fontSize: 9 }}>
                        {record.severity}
                    </Tag>
                    <span style={{ fontSize: 12 }}>{v}</span>
                </Space>
            ),
        },
        {
            title: 'Status',
            dataIndex: 'status',
            width: 120,
            render: (status: WatchlistItem['status'], record: WatchlistItem) => (
                <Select
                    size="small"
                    value={status}
                    onChange={v => onStatusChange(record.id, v)}
                    style={{ width: 110 }}
                    options={Object.entries(statusLabels).map(([k, v]) => ({
                        value: k,
                        label: <Tag color={v.color} style={{ margin: 0, fontSize: 10 }}>{v.text}</Tag>,
                    }))}
                />
            ),
        },
        {
            title: '',
            width: 70,
            render: (_: any, record: WatchlistItem) => (
                <Space size={2}>
                    <Tooltip title="Investigate">
                        <Button size="small" icon={<EyeOutlined />}
                            onClick={() => onJumpToRow(record.rowIndex)} />
                    </Tooltip>
                    <Tooltip title="Add note">
                        <Button size="small" icon={<EditOutlined />}
                            onClick={() => {
                                setNoteText(record.note || '');
                                setNoteModal({ visible: true, item: record });
                            }}
                        />
                    </Tooltip>
                </Space>
            ),
        },
    ];

    if (items.length === 0) {
        return (
            <div className="glass-panel" style={{ padding: 32, textAlign: 'center' }}>
                <Empty
                    image={<CheckCircleOutlined style={{ fontSize: 48, color: '#52c41a' }} />}
                    description="No anomalies detected â€” your data looks clean!"
                />
            </div>
        );
    }

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {/* Summary bar */}
            <div className="glass-panel" style={{
                padding: '8px 12px', display: 'flex', justifyContent: 'space-between',
                alignItems: 'center', flexWrap: 'wrap', gap: 8,
            }}>
                <Space size={12}>
                    <Badge count={counts.total} style={{ background: '#6366f1' }}>
                        <Tag color="purple" style={{ fontSize: 12, margin: 0 }}>
                            <WarningOutlined /> Watchlist
                        </Tag>
                    </Badge>
                    <Tag color="red" style={{ fontSize: 11 }}>
                        <ExclamationCircleOutlined /> {counts.critical} critical
                    </Tag>
                    <Tag color="orange" style={{ fontSize: 11 }}>
                        {counts.warning} warnings
                    </Tag>
                    <Tag style={{ fontSize: 11 }}>
                        {counts.unreviewed} unreviewed
                    </Tag>
                </Space>
                <Space size={4}>
                    <Input
                        size="small"
                        prefix={<SearchOutlined />}
                        placeholder="Search..."
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                        style={{ width: 150 }}
                        allowClear
                    />
                    <Select size="small" value={severityFilter} onChange={setSeverityFilter}
                        placeholder="Severity" allowClear style={{ width: 100 }}
                        options={[
                            { value: 'critical', label: 'ðŸ”´ Critical' },
                            { value: 'warning', label: 'ðŸŸ¡ Warning' },
                            { value: 'info', label: 'ðŸ”µ Info' },
                        ]}
                    />
                    <Select size="small" value={statusFilter} onChange={setStatusFilter}
                        placeholder="Status" allowClear style={{ width: 110 }}
                        options={Object.entries(statusLabels).map(([k, v]) => ({
                            value: k, label: v.text,
                        }))}
                    />
                    <Button size="small" icon={<DownloadOutlined />} onClick={exportCSV}>
                        Export
                    </Button>
                </Space>
            </div>

            {/* Bulk actions */}
            {selectedIds.length > 0 && (
                <Alert
                    type="info"
                    showIcon
                    message={`${selectedIds.length} items selected`}
                    action={
                        <Space>
                            <Button size="small" onClick={() => onBulkAction(selectedIds, 'accepted')}>
                                Accept All
                            </Button>
                            <Button size="small" onClick={() => onBulkAction(selectedIds, 'fixed')}>
                                Mark Fixed
                            </Button>
                            <Button size="small" onClick={() => onBulkAction(selectedIds, 'resolved')}>
                                Resolve
                            </Button>
                        </Space>
                    }
                />
            )}

            {/* Table */}
            <div className="glass-panel" style={{ padding: 0 }}>
                <Table
                    columns={columns}
                    dataSource={filtered}
                    rowKey="id"
                    size="small"
                    pagination={{ pageSize: 25, size: 'small', showTotal: t => `${t} items` }}
                    rowSelection={{
                        selectedRowKeys: selectedIds,
                        onChange: keys => setSelectedIds(keys as string[]),
                    }}
                    scroll={{ x: true }}
                />
            </div>

            {/* Note modal */}
            <Modal
                title={`Note â€” Row ${noteModal.item?.rowIndex}, ${noteModal.item?.column}`}
                open={noteModal.visible}
                onOk={handleSaveNote}
                onCancel={() => setNoteModal({ visible: false })}
                width={400}
            >
                <Input.TextArea
                    value={noteText}
                    onChange={e => setNoteText(e.target.value)}
                    rows={3}
                    placeholder="Add investigation notes..."
                />
            </Modal>
        </div>
    );
};

export default AnomalyWatchlist;

