/**
 * QAVisual — Power BI Natural Language Q&A Interactive Visualizer.
 * Provides an in-canvas Q&A input bar, suggested query chips, and dynamic visual preview.
 */
import React, { useState } from 'react';
import { Input, Button, Tag, Spin } from 'antd';
import { SearchOutlined, QuestionCircleOutlined, ThunderboltOutlined, CheckCircleOutlined } from '@ant-design/icons';
import type { ChartConfig } from '../../../types/dashboard';
import { formatCompactNumber, formatValue } from '../utils/formatters';
import { fetchAuth, API_BASE } from '../../../api/client';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

const DEFAULT_SUGGESTIONS = [
    'Total net revenue by product category',
    'Average discount rate by region',
    'Gross revenue vs net revenue',
    'Churn rate across loyalty tiers',
    'Top 5 transactions by sales',
];

const QAVisual: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const [query, setQuery] = useState('');
    const [loading, setLoading] = useState(false);
    const [answer, setAnswer] = useState<{
        question: string;
        type: 'kpi' | 'table' | 'text';
        title: string;
        value?: any;
        rows?: Record<string, any>[];
    } | null>(null);

    const handleAsk = async (questionText: string) => {
        const q = questionText.trim();
        if (!q) return;

        setLoading(true);
        setQuery(q);

        try {
            // Attempt to interpret using prompt interpreter
            const res = await fetchAuth(`${API_BASE}/api/dashboard/interpret`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ prompt: q }),
            });

            if (res.ok) {
                const interpreted = await res.json();
                if (interpreted.status === 'success' && interpreted.config) {
                    // Execute query if sql returned
                    if (interpreted.config.generated_sql) {
                        const qRes = await fetchAuth(`${API_BASE}/api/dashboard/query`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ sql: interpreted.config.generated_sql }),
                        });
                        if (qRes.ok) {
                            const qData = await qRes.json();
                            const rows = qData.data || [];
                            if (rows.length === 1 && Object.keys(rows[0]).length === 1) {
                                const k = Object.keys(rows[0])[0];
                                setAnswer({
                                    question: q,
                                    type: 'kpi',
                                    title: interpreted.config.title || k,
                                    value: rows[0][k],
                                });
                                setLoading(false);
                                return;
                            } else {
                                setAnswer({
                                    question: q,
                                    type: 'table',
                                    title: interpreted.config.title || q,
                                    rows: rows.slice(0, 5),
                                });
                                setLoading(false);
                                return;
                            }
                        }
                    }
                }
            }
        } catch {
            // Fallback to client-side data inspection
        }

        // Client-side fallback evaluator
        const lowerQ = q.toLowerCase();
        if (lowerQ.includes('net revenue') && lowerQ.includes('product')) {
            const catMap: Record<string, number> = {};
            data.forEach(d => {
                const cat = String(d.product_category || 'Unknown');
                catMap[cat] = (catMap[cat] || 0) + (Number(d.net_revenue) || 0);
            });
            const rows = Object.entries(catMap)
                .map(([cat, rev]) => ({ 'product_category': cat, 'net_revenue': rev }))
                .sort((a, b) => b.net_revenue - a.net_revenue);

            setAnswer({
                question: q,
                type: 'table',
                title: 'Net Revenue by Product Category',
                rows: rows.slice(0, 5),
            });
        } else if (lowerQ.includes('churn') || lowerQ.includes('rate')) {
            const churnCount = data.filter(d => Number(d.churn_flag) === 1).length;
            const rate = data.length > 0 ? (churnCount / data.length) * 100 : 0;
            setAnswer({
                question: q,
                type: 'kpi',
                title: 'Overall Churn Rate',
                value: `${rate.toFixed(1)}%`,
            });
        } else {
            // Default sum of net revenue
            const total = data.reduce((sum, d) => sum + (Number(d.net_revenue) || 0), 0);
            setAnswer({
                question: q,
                type: 'kpi',
                title: config.title || 'Total Net Revenue',
                value: `$${formatCompactNumber(total)}`,
            });
        }

        setLoading(false);
    };

    return (
        <div style={{
            display: 'flex', flexDirection: 'column', height: '100%',
            overflow: 'auto', padding: '16px', background: 'rgba(15, 23, 42, 0.4)',
        }}>
            {/* Search Input Bar */}
            <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
                <Input
                    placeholder="Ask a question about your data (e.g. 'total sales by category')..."
                    value={query}
                    onChange={e => setQuery(e.target.value)}
                    onPressEnter={() => handleAsk(query)}
                    prefix={<SearchOutlined style={{ color: '#818cf8' }} />}
                    style={{
                        background: '#1e293b',
                        borderColor: '#334155',
                        color: '#f8fafc',
                        borderRadius: 6,
                    }}
                />
                <Button
                    type="primary"
                    onClick={() => handleAsk(query)}
                    icon={<ThunderboltOutlined />}
                    style={{ background: '#6366f1', borderColor: '#6366f1' }}
                >
                    Ask
                </Button>
            </div>

            {/* Suggested Question Chips */}
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 14 }}>
                <span style={{ fontSize: 11, color: '#94a3b8', display: 'flex', alignItems: 'center', gap: 4 }}>
                    <QuestionCircleOutlined /> Suggestions:
                </span>
                {DEFAULT_SUGGESTIONS.map((s, idx) => (
                    <Tag
                        key={idx}
                        onClick={() => handleAsk(s)}
                        style={{
                            cursor: 'pointer',
                            background: '#1e293b',
                            borderColor: '#334155',
                            color: '#cbd5e1',
                            fontSize: 11,
                            borderRadius: 12,
                        }}
                    >
                        {s}
                    </Tag>
                ))}
            </div>

            {/* Answer Display */}
            {loading ? (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1 }}>
                    <Spin />
                </div>
            ) : answer ? (
                <div style={{
                    background: '#1e293b', border: '1px solid #334155', borderRadius: 8,
                    padding: 16, display: 'flex', flexDirection: 'column', gap: 12, flex: 1,
                }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', display: 'flex', alignItems: 'center', gap: 6 }}>
                            <CheckCircleOutlined style={{ color: '#10b981' }} /> Q&A Result: {answer.question}
                        </span>
                        <Tag color="geekblue">{answer.type.toUpperCase()}</Tag>
                    </div>

                    {answer.type === 'kpi' ? (
                        <div style={{
                            display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
                            flex: 1, padding: '20px 0',
                        }}>
                            <span style={{ fontSize: 13, color: '#94a3b8', textTransform: 'uppercase' }}>{answer.title}</span>
                            <div style={{ fontSize: 32, fontWeight: 700, color: '#38bdf8', marginTop: 8 }}>
                                {typeof answer.value === 'number' ? formatCompactNumber(answer.value) : answer.value}
                            </div>
                        </div>
                    ) : answer.rows ? (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, flex: 1, overflowY: 'auto' }}>
                            <div style={{ fontSize: 13, fontWeight: 600, color: '#f8fafc', marginBottom: 4 }}>
                                {answer.title}
                            </div>
                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12, color: '#cbd5e1' }}>
                                <thead>
                                    <tr style={{ borderBottom: '1px solid #334155', textAlign: 'left' }}>
                                        {Object.keys(answer.rows[0] || {}).map(col => (
                                            <th key={col} style={{ padding: '6px 8px', color: '#94a3b8', fontWeight: 600 }}>
                                                {col.replace(/_/g, ' ')}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {answer.rows.map((row, rIdx) => (
                                        <tr key={rIdx} style={{ borderBottom: '1px solid rgba(51, 65, 85, 0.4)' }}>
                                            {Object.entries(row).map(([colKey, val], cIdx) => (
                                                <td
                                                    key={cIdx}
                                                    style={{ padding: '6px 8px', cursor: onDataPointClick ? 'pointer' : 'default' }}
                                                    onClick={() => onDataPointClick?.({ column: colKey, value: val })}
                                                >
                                                    {typeof val === 'number' ? formatValue(val) : String(val ?? '—')}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    ) : null}
                </div>
            ) : (
                <div style={{
                    display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
                    flex: 1, color: '#64748b', fontSize: 12, gap: 8,
                }}>
                    <ThunderboltOutlined style={{ fontSize: 24, color: '#475569' }} />
                    <span>Type a natural language question or click a suggestion above to analyze</span>
                </div>
            )}
        </div>
    );
};

export default QAVisual;
