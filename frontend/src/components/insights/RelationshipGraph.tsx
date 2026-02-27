/**
 * RelationshipGraph — Visualizes column relationships using a force-directed graph.
 */

import React, { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import { Empty, Spin, Tag, Space, Slider } from 'antd';
import { ShareAltOutlined, InfoCircleOutlined } from '@ant-design/icons';

interface Node {
    id: string;
    name: string;
    symbolSize: number;
    category: string;
    value: number;
}

interface Link {
    source: string;
    target: string;
    value: number;
}

interface Props {
    data: {
        nodes: Node[];
        links: Link[];
        categories: { name: string }[];
    } | null;
    loading?: boolean;
    onThresholdChange?: (val: number) => void;
    threshold?: number;
}

const RelationshipGraph: React.FC<Props> = ({ data, loading, onThresholdChange, threshold = 0.3 }) => {
    const option = useMemo(() => {
        if (!data) return {};

        return {
            tooltip: {
                trigger: 'item',
                formatter: (params: any) => {
                    if (params.dataType === 'node') {
                        return `Column: <b>${params.name}</b><br/>Type: ${params.data.category}<br/>Rows: ${params.data.value}`;
                    } else {
                        return `Relationship: <b>${params.data.source}</b> ↔ <b>${params.data.target}</b><br/>Strength: ${params.data.value}`;
                    }
                }
            },
            legend: [{
                data: data.categories.map(a => a.name),
                textStyle: { color: '#94a3b8' }
            }],
            series: [{
                name: 'Column Relationships',
                type: 'graph',
                layout: 'force',
                data: data.nodes,
                links: data.links,
                categories: data.categories,
                roam: true,
                label: {
                    position: 'right',
                    formatter: '{b}',
                    color: '#e2e8f0'
                },
                force: {
                    repulsion: 1000,
                    edgeLength: [50, 200]
                },
                lineStyle: {
                    curveness: 0.1
                },
                emphasis: {
                    focus: 'adjacency',
                    lineStyle: { width: 10 }
                }
            }]
        };
    }, [data]);

    if (loading) return <div style={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}><Spin tip="Building graph..." /></div>;
    if (!data || data.nodes.length === 0) return <Empty description="No relationships detected with current threshold" />;

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div className="glass-panel" style={{ padding: '8px 12px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Space>
                    <ShareAltOutlined style={{ color: '#6366f1' }} />
                    <strong style={{ fontSize: 13 }}>Column Relationship Graph</strong>
                    <Tag color="blue">{data.nodes.length} nodes</Tag>
                    <Tag color="purple">{data.links.length} relationships</Tag>
                </Space>
                <Space>
                    <span style={{ fontSize: 11, opacity: 0.6 }}>Correlation threshold:</span>
                    <Slider
                        min={0} max={100} step={5}
                        value={threshold * 100}
                        onChange={v => onThresholdChange?.(v / 100)}
                        style={{ width: 100 }}
                        tooltip={{ formatter: v => `${v}%` }}
                    />
                </Space>
            </div>

            <div className="glass-panel" style={{ height: 500, padding: 0, overflow: 'hidden' }}>
                <ReactECharts
                    option={option}
                    style={{ height: '100%', width: '100%' }}
                    theme="dark"
                />
            </div>

            <div style={{ fontSize: 11, opacity: 0.5, display: 'flex', alignItems: 'center', gap: 4 }}>
                <InfoCircleOutlined />
                <span>Nodes are sized by unique value ratio. Blue lines indicate positive correlation, red lines indicate negative.</span>
            </div>
        </div>
    );
};

export default RelationshipGraph;
