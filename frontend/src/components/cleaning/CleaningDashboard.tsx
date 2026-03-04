/**
 * CleaningDashboard â€” Main container for the Data Cleaning Pipeline.
 * Shows summary, filter tabs, and a list of ActionCards.
 */

import React, { useEffect, useState } from 'react';
import {
    Typography, Button, Space, Tag, Tabs, Statistic, Row, Col,
    Alert, Spin, Empty, Badge,
} from 'antd';
import {
    ThunderboltOutlined, ReloadOutlined,
    ExperimentOutlined,
    DeleteOutlined, ToolOutlined, WarningOutlined,
    SwapOutlined, FontSizeOutlined, TagsOutlined,
    ClockCircleOutlined, SlidersOutlined, FilterOutlined,
    PieChartOutlined, FormatPainterOutlined, SecurityScanOutlined,
} from '@ant-design/icons';
import { useCleaning } from '../../hooks/useCleaning';
import ActionCard from './ActionCard';
import type { CleaningAction } from '../../types/cleaning';

const { Title, Paragraph, Text } = Typography;

interface CleaningDashboardProps {
    fileId?: string;
}

const CleaningDashboard: React.FC<CleaningDashboardProps> = ({ fileId }) => {
    const { state, analyze, applyAction, skipAction, applyAllDefinitive } = useCleaning(fileId);
    const [activeTab, setActiveTab] = useState('all');
    const [applyingIndex, setApplyingIndex] = useState<number | null>(null);

    // Auto-analyze on mount
    useEffect(() => {
        if (fileId && state.status === 'idle') {
            analyze();
        }
    }, [fileId, state.status, analyze]);

    const handleApply = async (actionIndex: number, selectedOption?: string) => {
        setApplyingIndex(actionIndex);
        await applyAction(actionIndex, selectedOption);
        setApplyingIndex(null);
    };

    const handleSkip = async (actionIndex: number) => {
        await skipAction(actionIndex);
    };

    // Loading state
    if (state.status === 'analyzing') {
        return (
            <div className="glass-panel" style={{ padding: '3rem', textAlign: 'center' }}>
                <Spin size="large" />
                <Paragraph type="secondary" style={{ marginTop: 16 }}>
                    Analyzing data quality and generating cleaning recommendations...
                </Paragraph>
            </div>
        );
    }

    // Idle state â€” no file
    if (!fileId || state.status === 'idle') {
        return (
            <div className="glass-panel" style={{ padding: '2rem', textAlign: 'center' }}>
                <ExperimentOutlined style={{ fontSize: 48, color: 'rgba(255,255,255,0.2)', marginBottom: 16 }} />
                <Paragraph type="secondary">
                    Upload and profile a dataset to generate intelligent cleaning recommendations.
                </Paragraph>
            </div>
        );
    }

    // Error state
    if (state.status === 'error') {
        return (
            <Alert
                type="error"
                message="Analysis Failed"
                description={state.error}
                showIcon
                action={
                    <Button size="small" onClick={analyze} icon={<ReloadOutlined />}>
                        Retry
                    </Button>
                }
            />
        );
    }

    const plan = state.plan;
    if (!plan) return null;

    // Filter actions by tab
    const filterActions = (actions: CleaningAction[]): CleaningAction[] => {
        if (activeTab === 'all') return actions;
        if (activeTab === 'duplicates') return actions.filter(a => a.category === 'duplicates');
        if (activeTab === 'missing') return actions.filter(a => a.category === 'missing_values');
        if (activeTab === 'outliers') return actions.filter(a => a.category === 'outliers');
        if (activeTab === 'types') return actions.filter(a => a.category === 'type_correction');
        if (activeTab === 'text') return actions.filter(a => a.category === 'text_preprocessing');
        if (activeTab === 'encoding') return actions.filter(a => a.category === 'categorical_encoding');
        if (activeTab === 'datetime') return actions.filter(a => a.category === 'datetime_engineering');
        if (activeTab === 'scaling') return actions.filter(a => a.category === 'feature_scaling');
        if (activeTab === 'selection') return actions.filter(a => a.category === 'feature_selection');
        if (activeTab === 'imbalance') return actions.filter(a => a.category === 'class_imbalance');
        if (activeTab === 'standardize') return actions.filter(a => a.category === 'data_standardization');
        if (activeTab === 'leakage') return actions.filter(a => a.category === 'data_leakage');
        if (activeTab === 'definitive') return actions.filter(a => a.confidence === 'definitive');
        if (activeTab === 'judgment') return actions.filter(a => a.confidence === 'judgment_call');
        return actions;
    };

    const filteredActions = filterActions(plan.actions);
    const pendingDefinitive = plan.actions.filter(
        a => a.confidence === 'definitive' && a.status === 'pending'
    ).length;
    const pendingJudgment = plan.actions.filter(
        a => a.confidence === 'judgment_call' && a.status === 'pending'
    ).length;
    const applied = plan.actions.filter(a => a.status === 'applied').length;
    const dupCount = plan.actions.filter(a => a.category === 'duplicates').length;
    const missCount = plan.actions.filter(a => a.category === 'missing_values').length;
    const outlierCount = plan.actions.filter(a => a.category === 'outliers').length;
    const typeCount = plan.actions.filter(a => a.category === 'type_correction').length;
    const textCount = plan.actions.filter(a => a.category === 'text_preprocessing').length;
    const encodingCount = plan.actions.filter(a => a.category === 'categorical_encoding').length;
    const datetimeCount = plan.actions.filter(a => a.category === 'datetime_engineering').length;
    const scalingCount = plan.actions.filter(a => a.category === 'feature_scaling').length;
    const selectionCount = plan.actions.filter(a => a.category === 'feature_selection').length;
    const imbalanceCount = plan.actions.filter(a => a.category === 'class_imbalance').length;
    const standardizeCount = plan.actions.filter(a => a.category === 'data_standardization').length;
    const leakageCount = plan.actions.filter(a => a.category === 'data_leakage').length;

    return (
        <div className="cleaning-dashboard animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
            {/* Summary Header */}
            <div className="glass-panel" style={{ padding: '20px 24px' }}>
                <Row gutter={[24, 16]} align="middle">
                    <Col flex="auto">
                        <Space direction="vertical" size={4}>
                            <Title level={4} style={{ margin: 0 }}>
                                ðŸ§¹ Intelligent Cleaning Engine
                            </Title>
                            <Text type="secondary">
                                {plan.total_actions} recommendations generated
                                {plan.estimated_rows_affected > 0 &&
                                    ` Â· ~${plan.estimated_rows_affected.toLocaleString()} rows affected (${plan.estimated_rows_affected_pct}%)`
                                }
                            </Text>
                        </Space>
                    </Col>
                    <Col>
                        <Space size={16}>
                            <Statistic
                                title="Safe"
                                value={pendingDefinitive}
                                styles={{ content: { color: '#52c41a', fontSize: 20 } }}
                                prefix={<ThunderboltOutlined />}
                            />
                            <Statistic
                                title="Review"
                                value={pendingJudgment}
                                styles={{ content: { color: '#faad14', fontSize: 20 } }}
                            />
                            <Statistic
                                title="Applied"
                                value={applied}
                                styles={{ content: { color: '#1890ff', fontSize: 20 } }}
                            />
                        </Space>
                    </Col>
                    <Col>
                        {pendingDefinitive > 0 && (
                            <Button
                                type="primary"
                                icon={<ThunderboltOutlined />}
                                onClick={applyAllDefinitive}
                                loading={state.status === 'applying'}
                                style={{ background: '#52c41a', borderColor: '#52c41a' }}
                            >
                                Apply All Safe ({pendingDefinitive})
                            </Button>
                        )}
                    </Col>
                </Row>
            </div>

            {/* Filter Tabs */}
            <Tabs
                activeKey={activeTab}
                onChange={setActiveTab}
                items={[
                    {
                        key: 'all',
                        label: <Badge count={plan.total_actions} size="small" offset={[8, 0]}>All</Badge>,
                    },
                    {
                        key: 'definitive',
                        label: (
                            <Space>
                                <Tag color="success" style={{ margin: 0 }}>âœ“</Tag>
                                Definitive
                            </Space>
                        ),
                    },
                    {
                        key: 'judgment',
                        label: (
                            <Space>
                                <Tag color="warning" style={{ margin: 0 }}>âš¡</Tag>
                                Judgment Calls
                            </Space>
                        ),
                    },
                    {
                        key: 'duplicates',
                        label: (
                            <Badge count={dupCount} size="small" offset={[8, 0]}>
                                <Space><DeleteOutlined /> Duplicates</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'missing',
                        label: (
                            <Badge count={missCount} size="small" offset={[8, 0]}>
                                <Space><ToolOutlined /> Missing</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'outliers',
                        label: (
                            <Badge count={outlierCount} size="small" offset={[8, 0]}>
                                <Space><WarningOutlined /> Outliers</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'types',
                        label: (
                            <Badge count={typeCount} size="small" offset={[8, 0]}>
                                <Space><SwapOutlined /> Types</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'text',
                        label: (
                            <Badge count={textCount} size="small" offset={[8, 0]}>
                                <Space><FontSizeOutlined /> Text</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'encoding',
                        label: (
                            <Badge count={encodingCount} size="small" offset={[8, 0]}>
                                <Space><TagsOutlined /> Encoding</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'datetime',
                        label: (
                            <Badge count={datetimeCount} size="small" offset={[8, 0]}>
                                <Space><ClockCircleOutlined /> Datetime</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'scaling',
                        label: (
                            <Badge count={scalingCount} size="small" offset={[8, 0]}>
                                <Space><SlidersOutlined /> Scaling</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'selection',
                        label: (
                            <Badge count={selectionCount} size="small" offset={[8, 0]}>
                                <Space><FilterOutlined /> Selection</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'imbalance',
                        label: (
                            <Badge count={imbalanceCount} size="small" offset={[8, 0]}>
                                <Space><PieChartOutlined /> Imbalance</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'standardize',
                        label: (
                            <Badge count={standardizeCount} size="small" offset={[8, 0]}>
                                <Space><FormatPainterOutlined /> Standardize</Space>
                            </Badge>
                        ),
                    },
                    {
                        key: 'leakage',
                        label: (
                            <Badge count={leakageCount} size="small" offset={[8, 0]}>
                                <Space><SecurityScanOutlined /> Leakage</Space>
                            </Badge>
                        ),
                    },
                ]}
            />

            {/* Action Cards */}
            {filteredActions.length > 0 ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                    {filteredActions.map(action => (
                        <ActionCard
                            key={action.index}
                            action={action}
                            onApply={handleApply}
                            onSkip={handleSkip}
                            loading={applyingIndex === action.index}
                        />
                    ))}
                </div>
            ) : (
                <Empty
                    description={
                        activeTab === 'all'
                            ? 'No cleaning actions needed â€” your data looks great!'
                            : `No ${activeTab.replace('_', ' ')} actions found.`
                    }
                    style={{ padding: '3rem 0' }}
                />
            )}
        </div>
    );
};

export default CleaningDashboard;

