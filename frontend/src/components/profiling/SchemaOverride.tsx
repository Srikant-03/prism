/**
 * SchemaOverride â€” Panel showing type inferences with confidence and override controls.
 * Lets users see and correct type decisions with downstream impact preview.
 */

import React, { useState, useCallback } from 'react';
import { Table, Tag, Select, Button, Space, Progress, Tooltip, Modal, Alert } from 'antd';
import {
    LockOutlined, UnlockOutlined, SwapOutlined,
    CheckCircleOutlined, ExclamationCircleOutlined,
} from '@ant-design/icons';

interface TypeInference {
    column: string;
    inferred_type: string;
    confidence: number;
    evidence: string;
    alternatives: string[];
    locked: boolean;
    conflict: boolean;
}

interface Props {
    inferences: TypeInference[];
    onOverride: (column: string, newType: string) => void;
    onLock: (column: string) => void;
    onBulkOverride: (columns: string[], newType: string) => void;
}

const TYPE_OPTIONS = [
    { value: 'int64', label: 'ðŸ”¢ Integer' },
    { value: 'float64', label: 'ðŸ”¢ Float' },
    { value: 'string', label: 'ðŸ”¤ String' },
    { value: 'datetime', label: 'ðŸ“… DateTime' },
    { value: 'boolean', label: 'âœ… Boolean' },
    { value: 'category', label: 'ðŸ·ï¸ Category' },
];

const SchemaOverride: React.FC<Props> = ({
    inferences, onOverride, onLock, onBulkOverride,
}) => {
    const [selectedRows, setSelectedRows] = useState<string[]>([]);
    const [bulkType, setBulkType] = useState<string | null>(null);
    const [impactModal, setImpactModal] = useState<{
        visible: boolean;
        column: string;
        fromType: string;
        toType: string;
    }>({ visible: false, column: '', fromType: '', toType: '' });

    const conflicts = inferences.filter(i => i.conflict);

    const handleTypeChange = useCallback((column: string, currentType: string, newType: string) => {
        setImpactModal({
            visible: true,
            column,
            fromType: currentType,
            toType: newType,
        });
    }, []);

    const confirmOverride = useCallback(() => {
        onOverride(impactModal.column, impactModal.toType);
        setImpactModal({ visible: false, column: '', fromType: '', toType: '' });
    }, [impactModal, onOverride]);

    const columns = [
        {
            title: 'Column',
            dataIndex: 'column',
            render: (v: string, record: TypeInference) => (
                <Space>
                    {record.conflict && (
                        <Tooltip title="Type conflict â€” multiple valid types detected">
                            <ExclamationCircleOutlined style={{ color: '#faad14' }} />
                        </Tooltip>
                    )}
                    <span style={{ fontWeight: 600 }}>{v}</span>
                </Space>
            ),
        },
        {
            title: 'Inferred Type',
            dataIndex: 'inferred_type',
            width: 130,
            render: (v: string) => {
                const opt = TYPE_OPTIONS.find(t => t.value === v);
                return <Tag color="blue">{opt?.label || v}</Tag>;
            },
        },
        {
            title: 'Confidence',
            dataIndex: 'confidence',
            width: 120,
            render: (v: number) => (
                <Progress
                    percent={Math.round(v * 100)}
                    size="small"
                    strokeColor={v >= 0.9 ? '#52c41a' : v >= 0.7 ? '#1677ff' : '#faad14'}
                    format={p => `${p}%`}
                />
            ),
        },
        {
            title: 'Evidence',
            dataIndex: 'evidence',
            ellipsis: true,
            render: (v: string) => (
                <span style={{ fontSize: 11, opacity: 0.6 }}>{v}</span>
            ),
        },
        {
            title: 'Override',
            width: 160,
            render: (_: any, record: TypeInference) => (
                record.locked ? (
                    <Space>
                        <Tag color="green" icon={<LockOutlined />}>Locked</Tag>
                        <Button size="small" icon={<UnlockOutlined />}
                            onClick={() => onLock(record.column)} />
                    </Space>
                ) : (
                    <Select
                        size="small"
                        value={record.inferred_type}
                        onChange={v => handleTypeChange(record.column, record.inferred_type, v)}
                        style={{ width: 130 }}
                        options={TYPE_OPTIONS}
                    />
                )
            ),
        },
        {
            title: '',
            width: 40,
            render: (_: any, record: TypeInference) => (
                !record.locked && (
                    <Tooltip title="Lock this type">
                        <Button size="small" icon={<LockOutlined />}
                            onClick={() => onLock(record.column)} />
                    </Tooltip>
                )
            ),
        },
    ];

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {/* Conflicts alert */}
            {conflicts.length > 0 && (
                <Alert
                    type="warning"
                    showIcon
                    message={`${conflicts.length} column(s) have ambiguous types â€” please review`}
                    description={conflicts.map(c => c.column).join(', ')}
                />
            )}

            {/* Bulk actions */}
            {selectedRows.length > 0 && (
                <div className="glass-panel" style={{
                    padding: '8px 12px', display: 'flex', alignItems: 'center', gap: 8,
                }}>
                    <Tag color="blue">{selectedRows.length} selected</Tag>
                    <Select size="small" value={bulkType} onChange={setBulkType}
                        placeholder="Set type" style={{ width: 140 }}
                        options={TYPE_OPTIONS} />
                    <Button size="small" type="primary"
                        disabled={!bulkType}
                        onClick={() => {
                            if (bulkType) {
                                onBulkOverride(selectedRows, bulkType);
                                setSelectedRows([]);
                                setBulkType(null);
                            }
                        }}>
                        Apply to All
                    </Button>
                </div>
            )}

            {/* Table */}
            <div className="glass-panel" style={{ padding: 0 }}>
                <Table
                    columns={columns}
                    dataSource={inferences}
                    rowKey="column"
                    size="small"
                    pagination={false}
                    rowSelection={{
                        selectedRowKeys: selectedRows,
                        onChange: keys => setSelectedRows(keys as string[]),
                    }}
                />
            </div>

            {/* Impact Preview Modal */}
            <Modal
                title="Type Override Impact"
                open={impactModal.visible}
                onOk={confirmOverride}
                onCancel={() => setImpactModal({ visible: false, column: '', fromType: '', toType: '' })}
                okText="Confirm Override"
            >
                <Space direction="vertical" style={{ width: '100%' }}>
                    <div>
                        <strong>{impactModal.column}</strong>:{' '}
                        <Tag>{impactModal.fromType}</Tag>
                        <SwapOutlined style={{ margin: '0 8px' }} />
                        <Tag color="blue">{impactModal.toType}</Tag>
                    </div>
                    <Alert
                        type="info"
                        showIcon
                        message="Downstream Impact"
                        description={
                            <ul style={{ margin: 0, paddingLeft: 16, fontSize: 12 }}>
                                <li>Column encoding method will change</li>
                                <li>Profiling statistics will be recomputed</li>
                                <li>Charts and visualizations may update</li>
                                <li>SQL queries referencing this column may need adjustment</li>
                            </ul>
                        }
                    />
                </Space>
            </Modal>
        </div>
    );
};

export default SchemaOverride;

