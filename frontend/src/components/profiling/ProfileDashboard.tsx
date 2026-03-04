/**
 * ProfileDashboard â€” Main profiling dashboard container.
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
import type { DatasetProfile } from '../../types/profiling';

const { Text } = Typography;
const { Panel } = Collapse;

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
        let cols = profile.columns;
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
        profile.columns.forEach(c => {
            counts[c.semantic_type] = (counts[c.semantic_type] || 0) + 1;
        });
        return counts;
    }, [profile.columns]);

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
                                    >
                                        {filteredColumns.map(col => (
                                            <Panel
                                                key={col.name}
                                                header={
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
                                                }
                                            >
                                                <ColumnDetail column={col} />
                                            </Panel>
                                        ))}
                                    </Collapse>
                                ) : (
                                    <Empty description="No columns match your filters" style={{ marginTop: 40 }} />
                                )}
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
                                ðŸ§¹ Data Cleaning
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

