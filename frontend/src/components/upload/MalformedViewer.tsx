/**
 * MalformedViewer — Side-by-side raw vs parsed data viewer.
 * Highlights affected cells/rows and provides accept/reject actions.
 */

import React, { useState } from 'react';
import {
    Table,
    Tag,
    Typography,
    Button,
    Alert,
    Space,
    Collapse,
    Tooltip,
    Badge,
} from 'antd';
import {
    WarningOutlined,
    CloseCircleOutlined,
    CheckOutlined,
    DeleteOutlined,
    InfoCircleOutlined,
} from '@ant-design/icons';
import type { MalformedReport } from '../../types/ingestion';

const { Text } = Typography;
const { Panel } = Collapse;

interface MalformedViewerProps {
    report: MalformedReport;
    onAcceptBestEffort: () => void;
    onDropMalformed: () => void;
    loading?: boolean;
}

const severityConfig = {
    warning: { color: 'warning', icon: <WarningOutlined />, label: 'Warning' },
    error: { color: 'error', icon: <CloseCircleOutlined />, label: 'Error' },
};

const MalformedViewer: React.FC<MalformedViewerProps> = ({
    report,
    onAcceptBestEffort,
    onDropMalformed,
    loading = false,
}) => {
    const [activeKey, setActiveKey] = useState<string[]>(['summary']);

    const warningCount = report.issues.filter((i) => i.severity === 'warning').length;
    const errorCount = report.issues.filter((i) => i.severity === 'error').length;

    const columns = [
        {
            title: 'Row',
            dataIndex: 'row_number',
            key: 'row_number',
            width: 70,
            render: (val: number) => (val > 0 ? `#${val}` : '—'),
        },
        {
            title: 'Severity',
            dataIndex: 'severity',
            key: 'severity',
            width: 100,
            render: (sev: string) => {
                const config = severityConfig[sev as keyof typeof severityConfig];
                return config ? (
                    <Tag icon={config.icon} color={config.color}>
                        {config.label}
                    </Tag>
                ) : (
                    <Tag>{sev}</Tag>
                );
            },
        },
        {
            title: 'Issue',
            dataIndex: 'issue',
            key: 'issue',
            render: (text: string) => <Text>{text}</Text>,
        },
        {
            title: 'Suggested Fix',
            dataIndex: 'suggested_fix',
            key: 'suggested_fix',
            width: 250,
            render: (text: string | null) =>
                text ? (
                    <Text type="secondary" italic>
                        {text}
                    </Text>
                ) : (
                    '—'
                ),
        },
    ];

    return (
        <div className="malformed-viewer">
            <Alert
                type={errorCount > 0 ? 'error' : 'warning'}
                showIcon
                icon={errorCount > 0 ? <CloseCircleOutlined /> : <WarningOutlined />}
                message="Data Quality Issues Detected"
                description={
                    <Space direction="vertical" size={4}>
                        <Text>{report.summary}</Text>
                        <Space>
                            <Badge count={warningCount} showZero color="#faad14">
                                <Tag color="warning">Warnings</Tag>
                            </Badge>
                            <Badge count={errorCount} showZero color="#ff4d4f">
                                <Tag color="error">Errors</Tag>
                            </Badge>
                            <Tag color="processing">
                                {report.best_effort_rows_parsed} rows recoverable
                            </Tag>
                        </Space>
                    </Space>
                }
                className="malformed-alert"
            />

            <Collapse
                activeKey={activeKey}
                onChange={(keys) => setActiveKey(keys as string[])}
                className="malformed-collapse"
            >
                <Panel
                    header={
                        <Space>
                            <InfoCircleOutlined />
                            <Text strong>Issue Details ({report.total_issues} issues)</Text>
                        </Space>
                    }
                    key="details"
                >
                    <Table
                        dataSource={report.issues.map((issue, idx) => ({ ...issue, key: idx }))}
                        columns={columns}
                        size="small"
                        pagination={{ pageSize: 10, showSizeChanger: true }}
                        scroll={{ x: 800 }}
                        className="malformed-table"
                    />

                    {/* Expandable raw content view */}
                    <Collapse ghost className="raw-content-collapse">
                        {report.issues
                            .filter((i) => i.raw_content)
                            .slice(0, 20)
                            .map((issue, idx) => (
                                <Panel
                                    header={
                                        <Text type="secondary">
                                            Row #{issue.row_number} — Raw Content
                                        </Text>
                                    }
                                    key={`raw-${idx}`}
                                >
                                    <pre className="raw-content-pre">{issue.raw_content}</pre>
                                </Panel>
                            ))}
                    </Collapse>
                </Panel>
            </Collapse>

            <div className="malformed-actions">
                <Space>
                    <Tooltip title="Keep all parseable data as-is, including rows with warnings">
                        <Button
                            type="primary"
                            icon={<CheckOutlined />}
                            onClick={onAcceptBestEffort}
                            loading={loading}
                            size="large"
                        >
                            Accept Best-Effort Parse
                        </Button>
                    </Tooltip>
                    <Tooltip title="Remove rows that have errors and keep only clean data">
                        <Button
                            danger
                            icon={<DeleteOutlined />}
                            onClick={onDropMalformed}
                            loading={loading}
                            size="large"
                        >
                            Drop Malformed Rows ({report.best_effort_rows_dropped})
                        </Button>
                    </Tooltip>
                </Space>
            </div>
        </div>
    );
};

export default MalformedViewer;
