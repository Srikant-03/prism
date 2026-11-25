/**
 * SmartNarrativeWidget — Power BI Live Smart Narrative Visualizer.
 * Displays executive summary bullets with dynamically computed KPI statistics,
 * trend summaries, peak performances, and categorical drivers.
 */
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Button, Spin } from 'antd';
import { RobotOutlined, ReloadOutlined } from '@ant-design/icons';
import type { ChartConfig } from '../../../types/dashboard';
import { formatCompactNumber } from '../utils/formatters';
import { fetchAuth, API_BASE } from '../../../api/client';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const computeLocalNarrative = (data: Record<string, any>[], config: ChartConfig) => {
    const totalRows = data ? data.length : 0;
    const hasNet = Boolean(data && data[0] && 'net_revenue' in data[0]);
    const hasGross = Boolean(data && data[0] && 'gross_revenue' in data[0]);
    const localBullets: string[] = [];
    const localKpis: Record<string, any> = { total_rows: totalRows };

    if (hasNet) {
        const totalNet = data.reduce((sum, d) => sum + (Number(d.net_revenue) || 0), 0);
        localKpis.total_net_revenue = totalNet;
        localBullets.push(
            `Total **Net Revenue** stands at **$${formatCompactNumber(totalNet)}** across **${totalRows}** recorded transactions.`
        );

        if (hasGross) {
            const totalGross = data.reduce((sum, d) => sum + (Number(d.gross_revenue) || 0), 0);
            const margin = totalGross > 0 ? (totalNet / totalGross) * 100 : 0;
            localKpis.net_margin_pct = margin;
            localBullets.push(
                `Net realization margin is **${margin.toFixed(1)}%** relative to **$${formatCompactNumber(totalGross)}** in Gross Revenue.`
            );
        }
    }

    if (data && data[0] && 'product_category' in data[0] && hasNet) {
        const catMap: Record<string, number> = {};
        data.forEach(d => {
            const cat = String(d.product_category || 'Unknown');
            catMap[cat] = (catMap[cat] || 0) + (Number(d.net_revenue) || 0);
        });
        const topCat = Object.entries(catMap).sort((a, b) => b[1] - a[1])[0];
        if (topCat) {
            localBullets.push(
                `**${topCat[0]}** is the dominant product line, contributing **$${formatCompactNumber(topCat[1])}**.`
            );
        }
    }

    if (data && data[0] && 'churn_flag' in data[0]) {
        const churnCount = data.filter(d => Number(d.churn_flag) === 1).length;
        const churnRate = totalRows > 0 ? (churnCount / totalRows) * 100 : 0;
        localKpis.churn_rate_pct = churnRate;
        localBullets.push(
            `Overall churn flag rate is **${churnRate.toFixed(1)}%** of active accounts.`
        );
    }

    return {
        title: config.title || 'Executive Intelligence Narrative',
        bullets: localBullets,
        summaryKpis: localKpis,
    };
};

const SmartNarrativeWidget: React.FC<Props> = ({ config, data }) => {
    const local = useMemo(() => computeLocalNarrative(data, config), [data, config]);
    const [loading, setLoading] = useState(false);
    const [remoteNarrative, setRemoteNarrative] = useState<{
        title?: string;
        bullets?: string[];
        summaryKpis?: Record<string, any>;
    } | null>(null);

    const loadNarrative = useCallback(async () => {
        if (!data || data.length === 0) return;

        setLoading(true);
        try {
            const res = await fetchAuth(`${API_BASE}/api/dashboard/narrative`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    file_id: 'active',
                    current_config: config,
                }),
            });

            if (res.ok) {
                const result = await res.json();
                setRemoteNarrative({
                    title: result.title || config.title || 'Executive Intelligence Narrative',
                    bullets: result.bullets || [],
                    summaryKpis: result.summary_kpis || {},
                });
            }
        } catch {
            // Keep computed fallback narrative
        } finally {
            setLoading(false);
        }
    }, [data, config]);

    useEffect(() => {
        loadNarrative();
    }, [loadNarrative]);

    const title = remoteNarrative?.title || local.title;
    const bullets = remoteNarrative?.bullets || local.bullets;
    const summaryKpis = remoteNarrative?.summaryKpis || local.summaryKpis;

    // Parse simple markdown bold syntax (**text**)
    const renderFormattedText = (text: string) => {
        const parts = text.split(/(\*\*.*?\*\*)/g);
        return parts.map((part, i) => {
            if (part.startsWith('**') && part.endsWith('**')) {
                return (
                    <strong key={i} style={{ color: '#818cf8', fontWeight: 700 }}>
                        {part.slice(2, -2)}
                    </strong>
                );
            }
            return <span key={i}>{part}</span>;
        });
    };

    return (
        <div style={{
            display: 'flex', flexDirection: 'column', height: '100%',
            overflow: 'auto', padding: '16px', background: 'rgba(15, 23, 42, 0.4)',
        }}>
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                marginBottom: 12, borderBottom: '1px solid #334155', paddingBottom: 8,
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <div style={{
                        width: 28, height: 28, borderRadius: 6, background: 'rgba(99, 102, 241, 0.2)',
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}>
                        <RobotOutlined style={{ color: '#818cf8', fontSize: 16 }} />
                    </div>
                    <div>
                        <div style={{ fontSize: 13, fontWeight: 700, color: '#f8fafc' }}>{title}</div>
                        <div style={{ fontSize: 10, color: '#64748b' }}>Dynamic, data-grounded AI narrative</div>
                    </div>
                </div>

                <Button
                    size="small"
                    type="text"
                    icon={<ReloadOutlined style={{ color: '#94a3b8' }} />}
                    onClick={loadNarrative}
                    title="Refresh narrative"
                />
            </div>

            {/* KPI Summary Row */}
            {Object.keys(summaryKpis).length > 0 && (
                <div style={{
                    display: 'flex', gap: 8, marginBottom: 14, flexWrap: 'wrap',
                }}>
                    {summaryKpis.total_net_revenue !== undefined && (
                        <div style={{
                            background: '#1e293b', border: '1px solid #334155', borderRadius: 6,
                            padding: '6px 10px', display: 'flex', flexDirection: 'column',
                        }}>
                            <span style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase' }}>Net Revenue</span>
                            <span style={{ fontSize: 13, fontWeight: 700, color: '#38bdf8' }}>
                                ${formatCompactNumber(summaryKpis.total_net_revenue)}
                            </span>
                        </div>
                    )}
                    {summaryKpis.net_margin_pct !== undefined && (
                        <div style={{
                            background: '#1e293b', border: '1px solid #334155', borderRadius: 6,
                            padding: '6px 10px', display: 'flex', flexDirection: 'column',
                        }}>
                            <span style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase' }}>Margin</span>
                            <span style={{ fontSize: 13, fontWeight: 700, color: '#10b981' }}>
                                {Number(summaryKpis.net_margin_pct).toFixed(1)}%
                            </span>
                        </div>
                    )}
                    {summaryKpis.churn_rate_pct !== undefined && (
                        <div style={{
                            background: '#1e293b', border: '1px solid #334155', borderRadius: 6,
                            padding: '6px 10px', display: 'flex', flexDirection: 'column',
                        }}>
                            <span style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase' }}>Churn Rate</span>
                            <span style={{ fontSize: 13, fontWeight: 700, color: '#f59e0b' }}>
                                {Number(summaryKpis.churn_rate_pct).toFixed(1)}%
                            </span>
                        </div>
                    )}
                    {summaryKpis.total_rows !== undefined && (
                        <div style={{
                            background: '#1e293b', border: '1px solid #334155', borderRadius: 6,
                            padding: '6px 10px', display: 'flex', flexDirection: 'column',
                        }}>
                            <span style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase' }}>Records</span>
                            <span style={{ fontSize: 13, fontWeight: 700, color: '#cbd5e1' }}>
                                {summaryKpis.total_rows}
                            </span>
                        </div>
                    )}
                </div>
            )}

            {/* Narrative Bullets */}
            {loading && bullets.length === 0 ? (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1 }}>
                    <Spin indicator={<RobotOutlined style={{ fontSize: 24, color: '#6366f1' }} spin />} />
                </div>
            ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10, flex: 1 }}>
                    {bullets.map((bullet, idx) => (
                        <div
                            key={idx}
                            style={{
                                display: 'flex', alignItems: 'flex-start', gap: 10,
                                background: '#1e293b', padding: '10px 14px', borderRadius: 8,
                                border: '1px solid #334155',
                            }}
                        >
                            <div style={{
                                width: 6, height: 6, borderRadius: '50%', background: '#6366f1',
                                marginTop: 6, flexShrink: 0,
                            }} />
                            <div style={{ fontSize: 12, color: '#e2e8f0', lineHeight: 1.5 }}>
                                {renderFormattedText(bullet)}
                            </div>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};

export default SmartNarrativeWidget;
