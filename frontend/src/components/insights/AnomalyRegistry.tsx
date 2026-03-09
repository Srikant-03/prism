import React from 'react';
import { Card, Table, Tag, Typography } from 'antd';
import type { AnomalyWarning } from '../../types/insight';

const { Text } = Typography;

interface AnomalyRegistryProps {
    data: AnomalyWarning[];
}

const AnomalyRegistry: React.FC<AnomalyRegistryProps> = ({ data }) => {

    const columns = [
        {
            title: 'Severity',
            dataIndex: 'severity',
            key: 'severity',
            width: 120,
            sorter: (a: AnomalyWarning, b: AnomalyWarning) => {
                const order = { 'Critical': 0, 'High': 1, 'Medium': 2, 'Low': 3, 'Informational': 4 };
                return order[a.severity] - order[b.severity];
            },
            defaultSortOrder: 'ascend' as const,
            render: (severity: string) => {
                let color = 'default';
                switch (severity) {
                    case 'Critical': color = 'error'; break;
                    case 'High': color = 'warning'; break;
                    case 'Medium': color = 'orange'; break;
                    case 'Low': color = 'processing'; break;
                    case 'Informational': color = 'default'; break;
                }
                return <Tag color={color}>{severity}</Tag>;
            }
        },
        {
            title: 'Category',
            dataIndex: 'category',
            key: 'category',
            width: 150,
            render: (text: string) => <Text strong>{text}</Text>,
        },
        {
            title: 'Feature',
            dataIndex: 'feature',
            key: 'feature',
            width: 150,
            render: (text: string | null) => text ? <Tag>{text}</Tag> : <Text type="secondary">Dataset Level</Text>,
        },
        {
            title: 'Description',
            dataIndex: 'description',
            key: 'description',
            render: (text: string, record: AnomalyWarning) => (
                <div>
                    <div>{text}</div>
                    {record.recommendation && (
                        <div style={{ marginTop: 4 }}>
                            <Text type="secondary" italic>Action: {record.recommendation}</Text>
                        </div>
                    )}
                </div>
            )
        }
    ];

    return (
        <Card className="glass-panel" title={`Anomaly & Warning Registry (${data.length} found)`} variant="borderless" style={{ height: '100%' }}>
            <Table
                dataSource={data}
                columns={columns}
                rowKey={(record, i) => `${record.feature}-${record.category}-${i}`}
                pagination={{ pageSize: 5 }}
                size="small"
                scroll={{ y: 400 }}
                style={{ background: 'transparent' }}
            />
        </Card>
    );
};

export default AnomalyRegistry;

