/**
 * KPICard — Single metric display with delta badge and sparkline.
 */
import React from 'react';
import { AreaChart, Area, ResponsiveContainer } from 'recharts';
import { ChartConfig, COLOR_PALETTES } from '../../types/dashboard';
import { ArrowUpOutlined, ArrowDownOutlined, MinusOutlined } from '@ant-design/icons';

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const KPICard: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
    const valueKey = config.kpi_value_column || config.y_axis || 'value';

    // Principal value
    const mainValue = data.length === 1
        ? data[0][valueKey] ?? data[0].value ?? 0
        : data.reduce((sum, d) => sum + (Number(d[valueKey]) || 0), 0);

    // Format large numbers
    const formatValue = (v: number) => {
        if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
        if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
        return Number.isInteger(v) ? v.toString() : v.toFixed(2);
    };

    // Delta calculation
    const compKey = config.kpi_comparison_column;
    let delta: number | null = null;
    if (compKey && data.length > 0 && data[0][compKey] !== undefined) {
        const compValue = Number(data[0][compKey]) || 0;
        if (compValue !== 0) delta = ((mainValue - compValue) / Math.abs(compValue)) * 100;
    }

    // Sparkline data (use all rows if multiple)
    const sparkData = data.length > 1 ? data.map(d => ({ v: Number(d[valueKey]) || 0 })) : [];

    return (
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', padding: 24 }}>
            <div style={{ color: '#94a3b8', fontSize: 14, fontWeight: 500, marginBottom: 8, textTransform: 'uppercase', letterSpacing: 1 }}>
                {config.title || valueKey}
            </div>
            <div style={{ fontSize: 48, fontWeight: 700, color: '#f1f5f9', lineHeight: 1.1 }}>
                {formatValue(Number(mainValue))}
            </div>
            {delta !== null && (
                <div style={{
                    display: 'inline-flex', alignItems: 'center', gap: 4, marginTop: 8, padding: '4px 12px',
                    borderRadius: 20, fontSize: 14, fontWeight: 600,
                    background: delta > 0 ? 'rgba(34,197,94,0.15)' : delta < 0 ? 'rgba(239,68,68,0.15)' : 'rgba(148,163,184,0.15)',
                    color: delta > 0 ? '#22c55e' : delta < 0 ? '#ef4444' : '#94a3b8',
                }}>
                    {delta > 0 ? <ArrowUpOutlined /> : delta < 0 ? <ArrowDownOutlined /> : <MinusOutlined />}
                    {Math.abs(delta).toFixed(1)}%
                </div>
            )}
            {sparkData.length > 2 && (
                <div style={{ width: '80%', height: 40, marginTop: 12 }}>
                    <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={sparkData}>
                            <Area type="monotone" dataKey="v" stroke={colors[0]} fill={colors[0]} fillOpacity={0.2} strokeWidth={1.5} dot={false} />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>
            )}
        </div>
    );
};

export default KPICard;
