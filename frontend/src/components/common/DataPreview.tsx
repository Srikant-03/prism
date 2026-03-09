/**
 * DataPreview — Rich data table with column types, null counts, and summary stats.
 */

import React from 'react';
import { Table, Typography, Tag, Card, Space, Statistic, Descriptions, Tooltip, Row, Col } from 'antd';
import {
    TableOutlined,
    ColumnWidthOutlined,
    FieldTimeOutlined,
    FileTextOutlined,
    NumberOutlined,
    CalendarOutlined,
    FontSizeOutlined,
    CheckCircleOutlined,
    InfoCircleOutlined,
} from '@ant-design/icons';
import type { IngestionResult, ColumnInfo } from '../../types/ingestion';

const { Text } = Typography;

interface DataPreviewProps {
    result: IngestionResult;
}

const dtypeIcons: Record<string, React.ReactNode> = {
    int64: <NumberOutlined />,
    int32: <NumberOutlined />,
    float64: <NumberOutlined />,
    float32: <NumberOutlined />,
    object: <FontSizeOutlined />,
    string: <FontSizeOutlined />,
    datetime64: <CalendarOutlined />,
    bool: <CheckCircleOutlined />,
    category: <FileTextOutlined />,
};

function getDtypeIcon(dtype: string): React.ReactNode {
    for (const [key, icon] of Object.entries(dtypeIcons)) {
        if (dtype.includes(key)) return icon;
    }
    return <InfoCircleOutlined />;
}

function getDtypeColor(dtype: string): string {
    if (dtype.includes('int') || dtype.includes('float')) return '#1677ff';
    if (dtype.includes('datetime') || dtype.includes('date')) return '#722ed1';
    if (dtype.includes('bool')) return '#52c41a';
    if (dtype.includes('object') || dtype.includes('string')) return '#faad14';
    return '#8c8c8c';
}

function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

const DataPreview: React.FC<DataPreviewProps> = ({ result }) => {
    const { metadata, preview_data, justification } = result;

    // Build table columns from column info
    const tableColumns = metadata.columns.map((col: ColumnInfo) => ({
        title: (
            <Tooltip
                title={
                    <div>
                        <div><b>Type:</b> {col.dtype}</div>
                        <div><b>Non-null:</b> {col.non_null_count.toLocaleString()}</div>
                        <div><b>Null:</b> {col.null_count.toLocaleString()}</div>
                        {col.unique_count != null && (
                            <div><b>Unique:</b> {col.unique_count.toLocaleString()}</div>
                        )}
                    </div>
                }
            >
                <Space orientation="vertical" size={0} align="center">
                    <Text strong style={{ fontSize: 12 }}>{col.name}</Text>
                    <Tag
                        icon={getDtypeIcon(col.dtype)}
                        color={getDtypeColor(col.dtype)}
                        style={{ fontSize: 10, lineHeight: '16px', margin: 0 }}
                    >
                        {col.dtype}
                    </Tag>
                </Space>
            </Tooltip>
        ),
        dataIndex: col.name,
        key: col.name,
        ellipsis: true,
        width: Math.max(100, col.name.length * 10),
        render: (val: unknown) => {
            if (val === null || val === undefined || val === '') {
                return <Text type="secondary" italic>null</Text>;
            }
            return <Text>{String(val).substring(0, 100)}</Text>;
        },
    }));

    return (
        <div className="data-preview">
            {/* Metadata Summary */}
            <Card variant="borderless" className="metadata-card">
                <Row gutter={[24, 16]}>
                    <Col xs={12} sm={6}>
                        <Statistic
                            title="Rows"
                            value={metadata.row_count}
                            prefix={<TableOutlined />}
                            styles={{ content: { color: '#1677ff' } }}
                        />
                    </Col>
                    <Col xs={12} sm={6}>
                        <Statistic
                            title="Columns"
                            value={metadata.col_count}
                            prefix={<ColumnWidthOutlined />}
                            styles={{ content: { color: '#722ed1' } }}
                        />
                    </Col>
                    <Col xs={12} sm={6}>
                        <Statistic
                            title="File Size"
                            value={formatBytes(metadata.file_size_bytes)}
                            prefix={<FileTextOutlined />}
                            styles={{ content: { color: '#13c2c2', fontSize: 20 } }}
                        />
                    </Col>
                    <Col xs={12} sm={6}>
                        <Statistic
                            title="Parse Time"
                            value={`${metadata.ingestion_time_seconds.toFixed(2)}s`}
                            prefix={<FieldTimeOutlined />}
                            styles={{ content: { color: '#52c41a', fontSize: 20 } }}
                        />
                    </Col>
                </Row>

                {/* Detection details */}
                <Descriptions
                    size="small"
                    column={{ xs: 1, sm: 2, md: 3 }}
                    className="detection-details"
                    style={{ marginTop: 16 }}
                >
                    <Descriptions.Item label="Format">
                        <Tag color="processing">{metadata.format.toUpperCase()}</Tag>
                    </Descriptions.Item>
                    {metadata.encoding && (
                        <Descriptions.Item label="Encoding">
                            <Space>
                                <Tag color="default">{metadata.encoding.encoding}</Tag>
                                <Text type="secondary">
                                    {(metadata.encoding.confidence * 100).toFixed(0)}% conf
                                </Text>
                            </Space>
                        </Descriptions.Item>
                    )}
                    {metadata.delimiter && (
                        <Descriptions.Item label="Delimiter">
                            <Space>
                                <Tag color="default">
                                    {metadata.delimiter.delimiter === '\t'
                                        ? 'TAB'
                                        : metadata.delimiter.delimiter === ' '
                                            ? 'SPACE'
                                            : `"${metadata.delimiter.delimiter}"`}
                                </Tag>
                                <Text type="secondary">
                                    {(metadata.delimiter.confidence * 100).toFixed(0)}% conf
                                </Text>
                            </Space>
                        </Descriptions.Item>
                    )}
                </Descriptions>
            </Card>

            {/* AI Justification */}
            <Card variant="borderless" className="justification-card">
                <Space>
                    <InfoCircleOutlined style={{ color: '#1677ff' }} />
                    <Text type="secondary" className="justification-text">
                        {justification}
                    </Text>
                </Space>
            </Card>

            {/* Data Table */}
            <Card
                title={
                    <Space>
                        <TableOutlined />
                        <Text strong>Data Preview</Text>
                        <Text type="secondary">(first {preview_data.length} rows)</Text>
                    </Space>
                }
                variant="borderless"
                className="preview-table-card"
            >
                <Table
                    dataSource={preview_data.map((row, idx) => ({ ...row, key: idx }))}
                    columns={tableColumns}
                    size="small"
                    pagination={{ pageSize: 25, showSizeChanger: true, showTotal: (total) => `${total} rows shown` }}
                    scroll={{ x: 'max-content' }}
                    className="data-table"
                />
            </Card>
        </div>
    );
};

export default DataPreview;

