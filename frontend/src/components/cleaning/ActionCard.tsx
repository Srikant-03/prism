/**
 * ActionCard â€” Displays a single CleaningAction with evidence chain,
 * before/after preview, impact estimate, and action buttons.
 */

import React, { useState } from 'react';
import { Card, Tag, Typography, Table, Space, Button, Select, Tooltip, Collapse } from 'antd';
import {
    CheckCircleOutlined,
    ThunderboltOutlined, EyeOutlined, StopOutlined,
    InfoCircleOutlined, DeleteOutlined, ToolOutlined,
    WarningOutlined, SwapOutlined, FontSizeOutlined,
    TagsOutlined, ClockCircleOutlined, SlidersOutlined, FilterOutlined,
    PieChartOutlined, FormatPainterOutlined, SecurityScanOutlined,
} from '@ant-design/icons';
import type { CleaningAction } from '../../types/cleaning';

const { Text, Paragraph } = Typography;

interface ActionCardProps {
    action: CleaningAction;
    onApply: (actionIndex: number, selectedOption?: string) => void;
    onSkip: (actionIndex: number) => void;
    loading?: boolean;
}

const categoryIcons: Record<string, React.ReactNode> = {
    duplicates: <DeleteOutlined />,
    missing_values: <ToolOutlined />,
    outliers: <WarningOutlined />,
    type_correction: <SwapOutlined />,
    text_preprocessing: <FontSizeOutlined />,
    categorical_encoding: <TagsOutlined />,
    datetime_engineering: <ClockCircleOutlined />,
    feature_scaling: <SlidersOutlined />,
    feature_selection: <FilterOutlined />,
    class_imbalance: <PieChartOutlined />,
    data_standardization: <FormatPainterOutlined />,
    data_leakage: <SecurityScanOutlined />,
    structural: <InfoCircleOutlined />,
};

const categoryColors: Record<string, string> = {
    duplicates: '#ff7875',
    missing_values: '#ffc069',
    outliers: '#91caff',
    type_correction: '#d3adf7',
    text_preprocessing: '#87e8de',
    categorical_encoding: '#ffadd2',
    datetime_engineering: '#b5f5ec',
    feature_scaling: '#ffe58f',
    feature_selection: '#adc6ff',
    class_imbalance: '#f4a8c6',
    data_standardization: '#c3e6cb',
    data_leakage: '#f5c6d0',
    structural: '#b7eb8f',
};

const ActionCard: React.FC<ActionCardProps> = ({ action, onApply, onSkip, loading }) => {
    const [selectedOption, setSelectedOption] = useState<string>(
        action.options.find(o => o.is_default)?.key || action.options[0]?.key || ''
    );
    const [showPreview, setShowPreview] = useState(false);

    const isApplied = action.status === 'applied';
    const isSkipped = action.status === 'skipped';
    const isPending = action.status === 'pending';
    const isDefinitive = action.confidence === 'definitive';

    const cardStyle: React.CSSProperties = {
        opacity: isPending ? 1 : 0.6,
        borderLeft: `4px solid ${isDefinitive ? '#52c41a' : '#faad14'}`,
        transition: 'all 0.3s ease',
    };

    // Build preview table columns from before data
    const previewColumns = action.preview?.columns_before?.map(col => ({
        title: col,
        dataIndex: col,
        key: col,
        ellipsis: true,
    })) || [];

    return (
        <Card
            className="glass-panel action-card"
            style={cardStyle}
            size="small"
            title={
                <Space>
                    {categoryIcons[action.category]}
                    <Text strong style={{ color: categoryColors[action.category] }}>
                        {action.action_type.replace(/_/g, ' ').toUpperCase()}
                    </Text>
                    <Tag color={isDefinitive ? 'success' : 'warning'}>
                        {isDefinitive ? 'âœ“ Definitive' : 'âš¡ Judgment Call'}
                    </Tag>
                    {isApplied && <Tag color="blue" icon={<CheckCircleOutlined />}>Applied</Tag>}
                    {isSkipped && <Tag color="default" icon={<StopOutlined />}>Skipped</Tag>}
                    {action.target_columns.length > 0 && (
                        <Text type="secondary" style={{ fontSize: 12 }}>
                            â†’ {action.target_columns.join(', ')}
                        </Text>
                    )}
                </Space>
            }
            variant="borderless"
        >
            {/* Evidence Chain */}
            <div style={{ marginBottom: 12 }}>
                <div style={{ marginBottom: 6 }}>
                    <Text type="secondary" style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 1 }}>
                        Evidence
                    </Text>
                    <Paragraph style={{ margin: '4px 0 0 0', fontSize: 13 }}>{action.evidence}</Paragraph>
                </div>
                <div style={{ marginBottom: 6 }}>
                    <Text type="secondary" style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 1 }}>
                        Recommendation
                    </Text>
                    <Paragraph style={{ margin: '4px 0 0 0', fontSize: 13 }} strong>{action.recommendation}</Paragraph>
                </div>
                <div>
                    <Text type="secondary" style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 1 }}>
                        Reasoning
                    </Text>
                    <Paragraph style={{ margin: '4px 0 0 0', fontSize: 13 }} type="secondary" italic>
                        {action.reasoning}
                    </Paragraph>
                </div>
            </div>

            {/* Impact Estimate */}
            <div style={{
                background: 'rgba(99, 102, 241, 0.08)',
                borderRadius: 8,
                padding: '8px 12px',
                marginBottom: 12,
            }}>
                <Text style={{ fontSize: 13 }}>
                    ðŸ“Š <Text strong>{action.impact.description}</Text>
                </Text>
                {action.impact.rows_affected > 0 && (
                    <Text type="secondary" style={{ marginLeft: 12, fontSize: 12 }}>
                        ({action.impact.rows_before.toLocaleString()} â†’ {action.impact.rows_after.toLocaleString()} rows)
                    </Text>
                )}
            </div>

            {/* Before/After Preview (collapsible) */}
            {action.preview && action.preview.before.length > 0 && (
                <Collapse
                    ghost
                    activeKey={showPreview ? ['preview'] : []}
                    onChange={() => setShowPreview(!showPreview)}
                    items={[{
                        key: 'preview',
                        label: (
                            <Space>
                                <EyeOutlined />
                                <Text style={{ fontSize: 12 }}>Before/After Preview</Text>
                            </Space>
                        ),
                        children: (
                            <div style={{ display: 'flex', gap: 16 }}>
                                <div style={{ flex: 1 }}>
                                    <Text type="secondary" style={{ fontSize: 11 }}>BEFORE</Text>
                                    <Table
                                        dataSource={action.preview.before}
                                        columns={previewColumns}
                                        size="small"
                                        pagination={false}
                                        rowKey={(_, i) => `before-${i}`}
                                        style={{ marginTop: 4 }}
                                        scroll={{ x: true }}
                                    />
                                </div>
                                {action.preview.after.length > 0 && (
                                    <div style={{ flex: 1 }}>
                                        <Text type="secondary" style={{ fontSize: 11 }}>AFTER</Text>
                                        <Table
                                            dataSource={action.preview.after}
                                            columns={previewColumns}
                                            size="small"
                                            pagination={false}
                                            rowKey={(_, i) => `after-${i}`}
                                            style={{ marginTop: 4 }}
                                            scroll={{ x: true }}
                                        />
                                    </div>
                                )}
                            </div>
                        ),
                    }]}
                />
            )}

            {/* Action Buttons */}
            {isPending && (
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 12 }}>
                    <Space>
                        {action.options.length > 1 && (
                            <Select
                                value={selectedOption}
                                onChange={setSelectedOption}
                                size="small"
                                style={{ minWidth: 180 }}
                                options={action.options.map(o => ({
                                    value: o.key,
                                    label: o.label,
                                }))}
                            />
                        )}
                    </Space>
                    <Space>
                        <Tooltip title="Skip this action">
                            <Button
                                size="small"
                                onClick={() => onSkip(action.index)}
                                icon={<StopOutlined />}
                            >
                                Skip
                            </Button>
                        </Tooltip>
                        <Button
                            type="primary"
                            size="small"
                            loading={loading}
                            onClick={() => onApply(action.index, selectedOption || undefined)}
                            icon={<ThunderboltOutlined />}
                        >
                            Apply
                        </Button>
                    </Space>
                </div>
            )}
        </Card>
    );
};

export default ActionCard;

