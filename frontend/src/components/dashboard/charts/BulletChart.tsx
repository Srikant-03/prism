/**
 * BulletChart — Power BI Bullet Graph Visualizer.
 * Renders qualitative performance ranges (poor, satisfactory, good), actual value bar, and target marker line.
 */
import React from 'react';
import type { ChartConfig } from '../../../types/dashboard';
import { formatCompactNumber, formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DashboardBulletChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const dimKey = config.x_axis || 'category';
    const valKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';
    const targetKey = config.kpi_comparison_column;

    const items = (data || []).map((row, idx) => {
        const title = String(row[dimKey] || `Metric ${idx + 1}`);
        const actual = Number(row[valKey]) || 0;
        const target = targetKey ? Number(row[targetKey]) || Math.round(actual * 1.1) : Math.round(actual * 1.1);
        const max = Math.max(actual, target) * 1.25 || 100;
        return {
            title,
            actual,
            target,
            max,
            poor: max * 0.4,
            satisfactory: max * 0.75,
            good: max,
        };
    });

    return (
        <div style={{
            width: '100%', height: '100%', overflowY: 'auto', padding: '16px 20px',
            display: 'flex', flexDirection: 'column', gap: 16, justifyContent: 'center'
        }}>
            {items.map((item, i) => {
                const actualPct = Math.min(100, Math.round((item.actual / item.max) * 100));
                const targetPct = Math.min(100, Math.round((item.target / item.max) * 100));

                return (
                    <div
                        key={i}
                        onClick={() => onDataPointClick?.({ column: dimKey, value: item.title })}
                        style={{ cursor: 'pointer' }}
                    >
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                            <span style={{ fontSize: 13, fontWeight: 600, color: '#f1f5f9' }}>{item.title}</span>
                            <div style={{ fontSize: 12, color: '#94a3b8' }}>
                                Actual: <strong style={{ color: '#60a5fa' }}>{formatValue(item.actual)}</strong>
                                {' '}&bull; Target: <strong style={{ color: '#cbd5e1' }}>{formatCompactNumber(item.target)}</strong>
                            </div>
                        </div>

                        {/* Qualitative background ranges */}
                        <div style={{
                            position: 'relative', height: 24, borderRadius: 4, overflow: 'hidden',
                            background: '#1e293b', display: 'flex',
                        }}>
                            <div style={{ width: '40%', height: '100%', background: 'rgba(239, 68, 68, 0.25)' }} />
                            <div style={{ width: '35%', height: '100%', background: 'rgba(245, 158, 11, 0.25)' }} />
                            <div style={{ width: '25%', height: '100%', background: 'rgba(16, 185, 129, 0.25)' }} />

                            {/* Actual measure bar */}
                            <div
                                style={{
                                    position: 'absolute',
                                    top: 6,
                                    bottom: 6,
                                    left: 0,
                                    width: `${actualPct}%`,
                                    background: '#3b82f6',
                                    borderRadius: '0 3px 3px 0',
                                    transition: 'width 0.3s ease',
                                }}
                            />

                            {/* Target line marker */}
                            <div
                                style={{
                                    position: 'absolute',
                                    top: 2,
                                    bottom: 2,
                                    left: `${targetPct}%`,
                                    width: 3,
                                    background: '#ffffff',
                                    borderRadius: 1,
                                    boxShadow: '0 0 6px rgba(0,0,0,0.8)',
                                }}
                            />
                        </div>
                    </div>
                );
            })}
        </div>
    );
};

export default DashboardBulletChart;
