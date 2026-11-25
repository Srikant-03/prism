/**
 * KeyInfluencersVisual — Power BI Key Influencers & Top Segments Visualizer.
 * Analyzes which features influence a metric or outcome, showing lift multipliers,
 * comparative segment baselines, and data leakage guards.
 */
import React, { useState, useEffect } from 'react';
import { Tag, Progress, Spin, Empty, Radio } from 'antd';
import { RobotOutlined, SafetyCertificateOutlined, ThunderboltOutlined, TeamOutlined, RiseOutlined } from '@ant-design/icons';
import type { ChartConfig } from '../../../types/dashboard';
import { fetchAuth, API_BASE } from '../../../api/client';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

interface DriverRule {
    feature: string;
    condition: string;
    impact: string;
    multiplier: number;
    segment_size: number;
    target_rate: number;
    baseline_rate: number;
}

interface TopSegment {
    segment_name: string;
    size: number;
    target_rate: number;
    lift: number;
}

const KeyInfluencersVisual: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const targetCol = config.y_axis || 'churn_flag';
    const [selectedTargetVal, setSelectedTargetVal] = useState<number | string>(1);
    const [activeTab, setActiveTab] = useState<'influencers' | 'segments'>('influencers');
    const [selectedDriverIndex, setSelectedDriverIndex] = useState<number>(0);
    const [loading, setLoading] = useState(false);

    const [drivers, setDrivers] = useState<DriverRule[]>([]);
    const [topSegments, setTopSegments] = useState<TopSegment[]>([]);
    const [baselineRate, setBaselineRate] = useState<number>(0.15);
    const [guardedCols, setGuardedCols] = useState<string[]>([]);

    useEffect(() => {
        let isMounted = true;

        const loadKeyInfluencers = async () => {
            if (!data || data.length === 0) return;

            setLoading(true);
            try {
                const res = await fetchAuth(`${API_BASE}/api/dashboard/key-influencers`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        file_id: 'active',
                        target_column: targetCol,
                        target_value: selectedTargetVal,
                    }),
                });

                if (res.ok) {
                    const result = await res.json();
                    if (isMounted) {
                        setDrivers(result.drivers || []);
                        setTopSegments(result.top_segments || []);
                        setBaselineRate(result.baseline_rate ?? 0.15);
                        setGuardedCols(result.guarded_leakage_columns || []);
                        setLoading(false);
                        return;
                    }
                }
            } catch {
                // Fallback to client-side driver calculation if backend route is unavailable
            }

            // Client-side fallback computation
            if (isMounted) {
                const total = data.length;
                const targetMatches = data.filter(d => String(d[targetCol]) === String(selectedTargetVal));
                const computedBase = total > 0 ? targetMatches.length / total : 0.15;
                setBaselineRate(computedBase);

                // Sample candidate categorical features
                const candidateCols = ['product_category', 'region', 'loyalty_tier_code', 'is_enterprise', 'order_year']
                    .filter(c => c in (data[0] || {}) && c !== targetCol);

                const fallbackDrivers: DriverRule[] = [];
                for (const col of candidateCols) {
                    const groups: Record<string, { count: number; targets: number }> = {};
                    data.forEach(r => {
                        const val = String(r[col] ?? 'Unknown');
                        if (!groups[val]) groups[val] = { count: 0, targets: 0 };
                        groups[val].count++;
                        if (String(r[targetCol]) === String(selectedTargetVal)) {
                            groups[val].targets++;
                        }
                    });

                    for (const [val, stat] of Object.entries(groups)) {
                        if (stat.count >= 5) {
                            const segRate = stat.targets / stat.count;
                            const mult = computedBase > 0 ? segRate / computedBase : 1.0;
                            if (Math.abs(mult - 1.0) >= 0.15) {
                                fallbackDrivers.push({
                                    feature: col,
                                    condition: `is '${val}'`,
                                    impact: `${mult.toFixed(1)}x more likely`,
                                    multiplier: Number(mult.toFixed(2)),
                                    segment_size: stat.count,
                                    target_rate: Number(segRate.toFixed(4)),
                                    baseline_rate: Number(computedBase.toFixed(4)),
                                });
                            }
                        }
                    }
                }

                fallbackDrivers.sort((a, b) => Math.abs(b.multiplier - 1.0) - Math.abs(a.multiplier - 1.0));
                setDrivers(fallbackDrivers.slice(0, 8));

                const fallbackSegments = fallbackDrivers.slice(0, 3).map(d => ({
                    segment_name: `Segment where ${d.feature} ${d.condition}`,
                    size: d.segment_size,
                    target_rate: d.target_rate,
                    lift: d.multiplier,
                }));
                setTopSegments(fallbackSegments);

                // Check for guarded columns in data
                const leakCols = Object.keys(data[0] || {}).filter(c =>
                    c.includes('post_churn') || c.includes('refund') || c.includes('uuid')
                );
                setGuardedCols(leakCols);
                setLoading(false);
            }
        };

        loadKeyInfluencers();

        return () => {
            isMounted = false;
        };
    }, [data, targetCol, selectedTargetVal]);

    const activeDriver = drivers[selectedDriverIndex] || drivers[0];

    return (
        <div style={{
            display: 'flex', flexDirection: 'column', height: '100%',
            overflow: 'hidden', padding: '12px 16px', background: 'rgba(15, 23, 42, 0.4)',
        }}>
            {/* Top Bar: Target selector & Tabs */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                borderBottom: '1px solid #334155', paddingBottom: 10, marginBottom: 12, flexWrap: 'wrap', gap: 8,
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <ThunderboltOutlined style={{ color: '#818cf8', fontSize: 16 }} />
                    <span style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8' }}>
                        What influences <Tag color="blue">{targetCol}</Tag> to be:
                    </span>
                    <Radio.Group
                        size="small"
                        value={selectedTargetVal}
                        onChange={e => setSelectedTargetVal(e.target.value)}
                    >
                        <Radio.Button value={1}>1 (High / True)</Radio.Button>
                        <Radio.Button value={0}>0 (Low / False)</Radio.Button>
                    </Radio.Group>
                </div>

                <div style={{ display: 'flex', gap: 6 }}>
                    <button
                        onClick={() => setActiveTab('influencers')}
                        style={{
                            padding: '4px 10px', fontSize: 11, borderRadius: 6, cursor: 'pointer', border: 'none',
                            background: activeTab === 'influencers' ? '#6366f1' : '#1e293b',
                            color: activeTab === 'influencers' ? '#fff' : '#94a3b8',
                            display: 'flex', alignItems: 'center', gap: 4, fontWeight: 600,
                        }}
                    >
                        <RiseOutlined /> Key Influencers
                    </button>
                    <button
                        onClick={() => setActiveTab('segments')}
                        style={{
                            padding: '4px 10px', fontSize: 11, borderRadius: 6, cursor: 'pointer', border: 'none',
                            background: activeTab === 'segments' ? '#6366f1' : '#1e293b',
                            color: activeTab === 'segments' ? '#fff' : '#94a3b8',
                            display: 'flex', alignItems: 'center', gap: 4, fontWeight: 600,
                        }}
                    >
                        <TeamOutlined /> Top Segments
                    </button>
                </div>
            </div>

            {/* Guarded Data Leakage Banner */}
            {guardedCols.length > 0 && (
                <div style={{
                    marginBottom: 10, padding: '4px 10px', background: 'rgba(234, 179, 8, 0.1)',
                    border: '1px solid rgba(234, 179, 8, 0.3)', borderRadius: 6, fontSize: 11, color: '#facc15',
                    display: 'flex', alignItems: 'center', gap: 6,
                }}>
                    <SafetyCertificateOutlined />
                    <span>Leakage Protected: Excluded {guardedCols.slice(0, 3).join(', ')} from analysis.</span>
                </div>
            )}

            {/* Content Body */}
            {loading ? (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1 }}>
                    <Spin indicator={<RobotOutlined style={{ fontSize: 24, color: '#6366f1' }} spin />} />
                </div>
            ) : drivers.length === 0 ? (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1 }}>
                    <Empty description="No statistically significant influencers found" />
                </div>
            ) : activeTab === 'influencers' ? (
                <div style={{ display: 'flex', gap: 16, flex: 1, minHeight: 0 }}>
                    {/* Left: Ranked Influencer Drivers */}
                    <div style={{ flex: 1.2, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 8, paddingRight: 6 }}>
                        {drivers.map((d, idx) => {
                            const isSelected = idx === selectedDriverIndex;
                            const isHigher = d.multiplier >= 1.0;
                            return (
                                <div
                                    key={idx}
                                    onClick={() => {
                                        setSelectedDriverIndex(idx);
                                        onDataPointClick?.({ column: d.feature, value: d.condition.replace(/is '|'/g, '') });
                                    }}
                                    style={{
                                        padding: '10px 12px',
                                        background: isSelected ? 'rgba(99, 102, 241, 0.15)' : '#1e293b',
                                        border: `1px solid ${isSelected ? '#6366f1' : '#334155'}`,
                                        borderRadius: 8,
                                        cursor: 'pointer',
                                        transition: 'all 0.15s ease',
                                    }}
                                >
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                                        <span style={{ fontSize: 12, fontWeight: 600, color: '#f8fafc' }}>
                                            {d.feature.replace(/_/g, ' ')} <span style={{ color: '#818cf8' }}>{d.condition}</span>
                                        </span>
                                        <Tag color={isHigher ? 'green' : 'orange'} style={{ fontWeight: 700, borderRadius: 10 }}>
                                            {d.multiplier}x
                                        </Tag>
                                    </div>
                                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: '#94a3b8' }}>
                                        <span>Segment: {d.segment_size} rows</span>
                                        <span>Target Rate: {(d.target_rate * 100).toFixed(1)}%</span>
                                    </div>
                                </div>
                            );
                        })}
                    </div>

                    {/* Right: Driver Impact Comparison vs Baseline */}
                    {activeDriver && (
                        <div style={{
                            flex: 1, background: '#1e293b', borderRadius: 8, border: '1px solid #334155',
                            padding: 16, display: 'flex', flexDirection: 'column', gap: 14,
                        }}>
                            <div style={{ fontSize: 13, color: '#f8fafc', lineHeight: 1.4 }}>
                                When <strong style={{ color: '#818cf8' }}>{activeDriver.feature.replace(/_/g, ' ')}</strong> {activeDriver.condition}, the likelihood of <strong style={{ color: '#38bdf8' }}>{targetCol}</strong> being {selectedTargetVal} is <strong style={{ color: activeDriver.multiplier >= 1.0 ? '#10b981' : '#f59e0b' }}>{activeDriver.multiplier}x</strong> higher than baseline.
                            </div>

                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                <div>
                                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>
                                        <span>Segment Outcome Rate</span>
                                        <span style={{ fontWeight: 700, color: '#f8fafc' }}>
                                            {(activeDriver.target_rate * 100).toFixed(1)}%
                                        </span>
                                    </div>
                                    <Progress
                                        percent={Math.min(100, Math.round(activeDriver.target_rate * 100))}
                                        strokeColor="#6366f1"
                                        size="small"
                                    />
                                </div>

                                <div>
                                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: '#94a3b8', marginBottom: 4 }}>
                                        <span>Overall Average Baseline</span>
                                        <span style={{ fontWeight: 700, color: '#94a3b8' }}>
                                            {(baselineRate * 100).toFixed(1)}%
                                        </span>
                                    </div>
                                    <Progress
                                        percent={Math.min(100, Math.round(baselineRate * 100))}
                                        strokeColor="#64748b"
                                        size="small"
                                    />
                                </div>
                            </div>

                            <div style={{ marginTop: 'auto', fontSize: 10, color: '#64748b', textAlign: 'center' }}>
                                Click driver to cross-filter dashboard visuals
                            </div>
                        </div>
                    )}
                </div>
            ) : (
                /* Top Segments Tab */
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10, flex: 1, overflowY: 'auto' }}>
                    {topSegments.map((seg, idx) => (
                        <div
                            key={idx}
                            style={{
                                background: '#1e293b', border: '1px solid #334155', borderRadius: 8,
                                padding: '14px 16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                            }}
                        >
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <span style={{
                                        width: 22, height: 22, borderRadius: '50%', background: '#6366f1',
                                        color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center',
                                        fontSize: 11, fontWeight: 700,
                                    }}>
                                        {idx + 1}
                                    </span>
                                    <span style={{ fontSize: 13, fontWeight: 600, color: '#f8fafc' }}>{seg.segment_name}</span>
                                </div>
                                <span style={{ fontSize: 11, color: '#94a3b8', marginLeft: 30 }}>
                                    Population: <strong>{seg.size}</strong> records | Target incidence: <strong>{(seg.target_rate * 100).toFixed(1)}%</strong>
                                </span>
                            </div>
                            <Tag color="cyan" style={{ fontSize: 12, padding: '4px 10px', borderRadius: 12, fontWeight: 700 }}>
                                {seg.lift}x Lift
                            </Tag>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

export default KeyInfluencersVisual;
