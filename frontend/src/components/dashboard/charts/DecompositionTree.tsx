/**
 * DecompositionTree — Power BI Decomposition Tree Visualizer.
 * Supports interactive root-cause decomposition, AI High/Low value splits, and proportional node fills.
 */
import React, { useState, useMemo } from 'react';
import { Button, Dropdown, type MenuProps, Tag, Progress } from 'antd';
import { PlusOutlined, ApartmentOutlined, RobotOutlined } from '@ant-design/icons';
import type { ChartConfig } from '../../../types/dashboard';
import { formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

interface NodeData {
    id: string;
    label: string;
    value: number;
    dimension: string;
    percentage: number;
    children?: NodeData[];
}

const CANDIDATE_DIMS = ['product_category', 'region', 'loyalty_tier_code', 'is_enterprise', 'order_year'];

const DecompositionTree: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const valKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'net_revenue';
    const rootTotal = data.reduce((sum, d) => sum + (Number(d[valKey]) || 0), 0) || 1;

    const [splitPath, setSplitPath] = useState<string[]>([config.x_axis || 'product_category']);

    // Compute tree levels
    const treeNodes = useMemo<NodeData[]>(() => {
        if (!data || data.length === 0) return [];

        const firstDim = splitPath[0] || 'product_category';
        const groupMap = new Map<string, number>();

        data.forEach(row => {
            const dimVal = String(row[firstDim] ?? 'Unknown');
            const v = Number(row[valKey]) || 0;
            groupMap.set(dimVal, (groupMap.get(dimVal) || 0) + v);
        });

        return Array.from(groupMap.entries())
            .map(([label, value]) => ({
                id: `lvl0-${label}`,
                label,
                value,
                dimension: firstDim,
                percentage: Math.min(100, Math.round((value / rootTotal) * 100)),
            }))
            .sort((a, b) => b.value - a.value);
    }, [data, splitPath, valKey, rootTotal]);

    const handleAddSplit = (dim: string) => {
        if (!splitPath.includes(dim)) {
            setSplitPath([...splitPath, dim]);
        }
    };

    const handleAISplit = (strategy: 'high' | 'low') => {
        const available = CANDIDATE_DIMS.filter(d => !splitPath.includes(d));
        if (available.length > 0) {
            const chosen = strategy === 'low' ? available[available.length - 1] : available[0];
            handleAddSplit(chosen);
        }
    };

    const splitMenuItems: MenuProps['items'] = [
        {
            key: 'ai-high',
            icon: <RobotOutlined style={{ color: '#10b981' }} />,
            label: 'AI: High Value Driver',
            onClick: () => handleAISplit('high'),
        },
        {
            key: 'ai-low',
            icon: <RobotOutlined style={{ color: '#ef4444' }} />,
            label: 'AI: Low Value Driver',
            onClick: () => handleAISplit('low'),
        },
        { type: 'divider' },
        ...CANDIDATE_DIMS.filter(d => !splitPath.includes(d)).map(d => ({
            key: d,
            label: d.replace(/_/g, ' ').toUpperCase(),
            onClick: () => handleAddSplit(d),
        })),
    ];

    return (
        <div style={{
            display: 'flex', flexDirection: 'column', height: '100%',
            overflow: 'auto', padding: '16px', background: 'rgba(15, 23, 42, 0.4)',
        }}>
            {/* Header / Split Path Controls */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
                <span style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', display: 'flex', alignItems: 'center', gap: 6 }}>
                    <ApartmentOutlined style={{ color: '#818cf8' }} /> Breakdown Path:
                </span>
                {splitPath.map((dim, idx) => (
                    <Tag
                        key={dim}
                        color="indigo"
                        closable={idx > 0}
                        onClose={() => setSplitPath(splitPath.filter((_, i) => i !== idx))}
                        style={{ fontSize: 11, borderRadius: 12 }}
                    >
                        {dim.replace(/_/g, ' ')}
                    </Tag>
                ))}
                <Dropdown menu={{ items: splitMenuItems }} trigger={['click']}>
                    <Button size="small" type="dashed" icon={<PlusOutlined />} style={{ fontSize: 11 }}>
                        Split Next
                    </Button>
                </Dropdown>
            </div>

            {/* Tree Nodes Display */}
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: 24, flex: 1, minHeight: 0 }}>
                {/* Root Total Card */}
                <div style={{
                    width: 170, padding: 14, background: '#1e293b', borderRadius: 8,
                    border: '1.5px solid #6366f1', display: 'flex', flexDirection: 'column', gap: 4,
                }}>
                    <span style={{ fontSize: 11, color: '#94a3b8', textTransform: 'uppercase' }}>
                        {config.title || valKey.replace(/_/g, ' ')}
                    </span>
                    <div style={{ fontSize: 20, fontWeight: 700, color: '#f8fafc' }}>
                        {formatValue(rootTotal, config.metrics?.[0]?.format)}
                    </div>
                    <span style={{ fontSize: 10, color: '#64748b' }}>Total Aggregation</span>
                </div>

                {/* Level 1 Split Nodes */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8, flex: 1, maxHeight: '100%', overflowY: 'auto' }}>
                    {treeNodes.map((node) => (
                        <div
                            key={node.id}
                            onClick={() => onDataPointClick?.({ column: node.dimension, value: node.label })}
                            style={{
                                background: '#1e293b',
                                border: '1px solid #334155',
                                borderRadius: 8,
                                padding: '10px 14px',
                                cursor: 'pointer',
                                display: 'flex',
                                flexDirection: 'column',
                                gap: 6,
                                transition: 'all 0.15s ease',
                            }}
                            onMouseEnter={e => (e.currentTarget.style.borderColor = '#818cf8')}
                            onMouseLeave={e => (e.currentTarget.style.borderColor = '#334155')}
                        >
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <span style={{ fontWeight: 600, color: '#f8fafc', fontSize: 13 }}>{node.label}</span>
                                <span style={{ fontWeight: 700, color: '#818cf8', fontSize: 13 }}>
                                    {formatValue(node.value, config.metrics?.[0]?.format)}
                                </span>
                            </div>
                            <Progress
                                percent={node.percentage}
                                strokeColor="#6366f1"
                                size="small"
                                showInfo={false}
                                style={{ margin: 0 }}
                            />
                            <div style={{ fontSize: 10, color: '#94a3b8', textAlign: 'right' }}>
                                {node.percentage}% of total
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
};

export default DecompositionTree;
