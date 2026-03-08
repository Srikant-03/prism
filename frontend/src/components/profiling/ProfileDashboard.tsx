/**
 * ProfileDashboard — Main profiling dashboard container.
 * Shows DatasetOverview + searchable/filterable column list with expandable details.
 */

import React, { useState, useMemo } from 'react';
import { Input, Select, Space, Typography, Collapse, Badge, Tag, Empty, Tabs } from 'antd';
import {
    SearchOutlined, DatabaseOutlined, TableOutlined, AppstoreOutlined,
    ExperimentOutlined,
} from '@ant-design/icons';
import DatasetOverview from './DatasetOverview';
import ColumnDetail from './ColumnDetail';
import CrossColumnDashboard from './CrossColumnDashboard';
import CleaningDashboard from '../cleaning/CleaningDashboard';
import SchemaOverride from './SchemaOverride';
import { overrideSchema } from '../../api/ingestion';
import { notification } from 'antd';
import type { DatasetProfile } from '../../types/profiling';

const { Text } = Typography;
// Removed Collapse.Panel destructuring

interface ProfileDashboardProps {
    profile: DatasetProfile;
    fileId?: string;
}

function getQualityColor(score: number): string {
    if (score >= 90) return '#52c41a';
    if (score >= 70) return '#faad14';
    if (score >= 50) return '#fa8c16';
    return '#ff4d4f';
}

const SEMANTIC_TYPE_OPTIONS = [
    { value: '', label: 'All Types' },
    { value: 'numeric_continuous', label: 'Numeric (Continuous)' },
    { value: 'numeric_discrete', label: 'Numeric (Discrete)' },
    { value: 'categorical_nominal', label: 'Categorical (Nominal)' },
    { value: 'categorical_ordinal', label: 'Categorical (Ordinal)' },
    { value: 'boolean', label: 'Boolean' },
    { value: 'datetime', label: 'Datetime' },
    { value: 'free_text', label: 'Free Text' },
    { value: 'id_key', label: 'ID / Key' },
    { value: 'email', label: 'Email' },
    { value: 'url', label: 'URL' },
    { value: 'phone', label: 'Phone' },
    { value: 'currency', label: 'Currency' },
    { value: 'percentage', label: 'Percentage' },
    { value: 'geo_coordinate', label: 'Geo Coordinate' },
    { value: 'hashed', label: 'Hashed' },
];

const ProfileDashboard: React.FC<ProfileDashboardProps> = ({ profile, fileId }) => {
    const [searchTerm, setSearchTerm] = useState('');
    const [typeFilter, setTypeFilter] = useState<string>('');
    const [activeColumns, setActiveColumns] = useState<string[]>([]);

    // Filtered columns
    const filteredColumns = useMemo(() => {
        let cols = profile.columns || [];
        if (searchTerm) {
            const term = searchTerm.toLowerCase();
            cols = cols.filter(c => c.name.toLowerCase().includes(term));
        }
        if (typeFilter) {
            cols = cols.filter(c => c.semantic_type === typeFilter);
        }
        return cols;
    }, [profile.columns, searchTerm, typeFilter]);

    // Type summary for filter badges
    const typeCounts = useMemo(() => {
        const counts: Record<string, number> = {};
        (profile.columns || []).forEach(c => {
            counts[c.semantic_type] = (counts[c.semantic_type] || 0) + 1;
        });
        return counts;
    }, [profile.columns]);

    // Map columns to SchemaOverride format
    const [overrides, setOverrides] = useState<Record<string, string>>({});

    const typeInferences = useMemo(() => {
        return (profile.columns || []).map(col => ({
            column: col.name,
            inferred_type: overrides[col.name] || col.semantic_type,
            confidence: col.semantic_type_confidence,
            evidence: col.quality_justification || 'Inferred from data patterns',
            alternatives: [],
            locked: !!overrides[col.name],
            conflict: false,
        }));
    }, [profile.columns, overrides]);

    const handleOverride = async (column: string, newType: string) => {
        if (!fileId) return;
        try {
            await overrideSchema(fileId, column, newType);
            setOverrides(prev => ({ ...prev, [column]: newType }));
            notification.success({ message: `Schema Overridden`, description: `Column ${column} type updated to ${newType}.` });
        } catch (err: any) {
            notification.error({ message: `Override Failed`, description: err.message });
        }
    };

    const handleBulkOverride = async (columns: string[], newType: string) => {
        if (!fileId) return;
        try {
            await Promise.all(columns.map(c => overrideSchema(fileId!, c, newType)));
            setOverrides(prev => {
                const next = { ...prev };
                columns.forEach(c => next[c] = newType);
                return next;
            });
            notification.success({ message: `Bulk Schema Override`, description: `Updated ${columns.length} columns to ${newType}.` });
        } catch (err: any) {
            notification.error({ message: `Bulk Override Failed`, description: err.message });
        }
    };

    const handleLock = (column: string) => {
        // Toggle lock state locally for demonstration
        setOverrides(prev => {
            const next = { ...prev };
            if (next[column]) delete next[column];
            else next[column] = next[column] || profile.columns?.find(c => c.name === column)?.semantic_type || 'unknown';
            return next;
        });
    };

    return (
        <div className="profile-dashboard">
            <Tabs
                defaultActiveKey="overview"
                type="card"
                items={[
                    {
                        key: 'overview',
                        label: (
                            <Space>
                                <DatabaseOutlined />
                                Dataset Overview
                            </Space>
                        ),
                        children: <DatasetOverview profile={profile} />,
                    },
                    {
                        key: 'columns',
                        label: (
                            <Space>
                                <TableOutlined />
                                Column Profiles ({profile.total_columns})
                            </Space>
                        ),
                        children: (
                            <div className="columns-section">
                                {/* Search & Filter Bar */}
                                <div className="column-filter-bar">
                                    <Space wrap>
                                        <Input
                                            placeholder="Search columns..."
                                            prefix={<SearchOutlined />}
                                            value={searchTerm}
                                            onChange={e => setSearchTerm(e.target.value)}
                                            style={{ width: 250 }}
                                            allowClear
                                        />
                                        <Select
                                            value={typeFilter}
                                            onChange={setTypeFilter}
                                            style={{ width: 200 }}
                                            options={SEMANTIC_TYPE_OPTIONS}
                                            placeholder="Filter by type"
                                        />
                                        <Text type="secondary" style={{ fontSize: 12 }}>
                                            Showing {filteredColumns.length} of {profile.total_columns} columns
                                        </Text>
                                    </Space>
                                    {/* Type Quick Filters */}
                                    <div style={{ marginTop: 8 }}>
                                        <Space wrap size={4}>
                                            {Object.entries(typeCounts)
                                                .sort((a, b) => b[1] - a[1])
                                                .map(([type, count]) => (
                                                    <Tag
                                                        key={type}
                                                        style={{ cursor: 'pointer', opacity: typeFilter === type ? 1 : 0.6 }}
                                                        onClick={() => setTypeFilter(typeFilter === type ? '' : type)}
                                                    >
                                                        {type.replace(/_/g, ' ')} ({count})
                                                    </Tag>
                                                ))
                                            }
                                        </Space>
                                    </div>
                                </div>

                                {/* Column List */}
                                {filteredColumns.length > 0 ? (
                                    <Collapse
                                        activeKey={activeColumns}
                                        onChange={keys => setActiveColumns(keys as string[])}
                                        ghost
                                        className="column-collapse"
                                        items={filteredColumns.map(col => ({
                                            key: col.name,
                                            label: (
                                                <div className="col-panel-header">
                                                    <Space>
                                                        <Text strong>{col.name}</Text>
                                                        <Tag style={{ fontSize: 10 }}>{col.semantic_type.replace(/_/g, ' ')}</Tag>
                                                    </Space>
                                                    <Space>
                                                        <Badge
                                                            count={`${col.quality_score.toFixed(0)}`}
                                                            showZero
                                                            style={{ backgroundColor: getQualityColor(col.quality_score), fontSize: 10 }}
                                                        />
                                                        <Text type="secondary" style={{ fontSize: 11 }}>
                                                            {col.null_percentage > 0 ? `${col.null_percentage.toFixed(1)}% null` : '0% null'}
                                                        </Text>
                                                    </Space>
                                                </div>
                                            ),
                                            children: <ColumnDetail column={col} />
                                        }))}
                                    />
                                ) : (
                                    <Empty description="No columns match your filters" style={{ marginTop: 40 }} />
                                )}
                            </div>
                        ),
                    },
                    {
                        key: 'schema_tools',
                        label: (
                            <Space>
                                <DatabaseOutlined />
                                Schema Overrides
                            </Space>
                        ),
                        children: (
                            <div style={{ padding: 16 }}>
                                <Typography.Title level={5}>Type Inference Settings</Typography.Title>
                                <Typography.Paragraph type="secondary">
                                    Review AI-inferred column types. Override types if incorrect to adjust how the system handles formatting, data cleaning, and future ML feature processing.
                                </Typography.Paragraph>
                                <SchemaOverride
                                    inferences={typeInferences}
                                    onOverride={handleOverride}
                                    onLock={handleLock}
                                    onBulkOverride={handleBulkOverride}
                                />
                            </div>
                        ),
                    },
                    {
                        key: 'cross_column',
                        label: (
                            <Space>
                                <AppstoreOutlined />
                                Cross-Column Analysis
                            </Space>
                        ),
                        children: profile.cross_analysis ? (
                            <CrossColumnDashboard profile={profile.cross_analysis} insights={profile.insights} fileId={fileId} />
                        ) : (
                            <Empty description="No cross-column analysis available" />
                        ),
                    },
                    {
                        key: 'cleaning',
                        label: (
                            <Space>
                                <ExperimentOutlined />
                                🧹 Data Cleaning
                            </Space>
                        ),
                        children: <CleaningDashboard fileId={fileId} />,
                    },
                ]}
            />
        </div>
    );
};

export default ProfileDashboard;

