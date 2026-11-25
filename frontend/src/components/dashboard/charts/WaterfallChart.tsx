/**
 * WaterfallChart — Power BI Waterfall / Variance Bridge Visualizer.
 * Displays step-by-step additions, subtractions, and totals leading from starting to ending balance.
 */
import React from 'react';
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid,
    Tooltip, ResponsiveContainer, Cell, ReferenceLine,
} from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { formatValue, formatAxisTick } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

interface WaterfallItem {
    name: string;
    bottom: number;
    amount: number;
    actualValue: number;
    isTotal: boolean;
    fill: string;
}

const DashboardWaterfallChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const dimKey = config.x_axis || 'category';
    const valKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';

    let runningTotal = 0;
    const waterfallData: WaterfallItem[] = [];

    data.forEach((row, idx) => {
        const name = String(row[dimKey] || `Step ${idx + 1}`);
        const val = Number(row[valKey]) || 0;

        if (idx === 0) {
            // Starting base
            waterfallData.push({
                name,
                bottom: 0,
                amount: val,
                actualValue: val,
                isTotal: false,
                fill: '#6366f1',
            });
            runningTotal = val;
        } else {
            const isPos = val >= 0;
            const bottom = isPos ? runningTotal : runningTotal + val;
            waterfallData.push({
                name,
                bottom: Math.max(0, bottom),
                amount: Math.abs(val),
                actualValue: val,
                isTotal: false,
                fill: isPos ? '#10b981' : '#ef4444',
            });
            runningTotal += val;
        }
    });

    // Add final total bar
    if (waterfallData.length > 1) {
        waterfallData.push({
            name: 'Total',
            bottom: 0,
            amount: Math.max(0, runningTotal),
            actualValue: runningTotal,
            isTotal: true,
            fill: '#3b82f6',
        });
    }

    const handleClick = (entry: any) => {
        if (!entry || entry.isTotal) return;
        onDataPointClick?.({ column: dimKey, value: entry.name });
    };

    return (
        <ResponsiveContainer width="100%" height="100%">
            <BarChart data={waterfallData} margin={{ top: 20, right: 30, left: 20, bottom: 25 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />}
                <XAxis
                    dataKey="name"
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    interval={0}
                    angle={waterfallData.length > 5 ? -20 : 0}
                    textAnchor={waterfallData.length > 5 ? 'end' : 'middle'}
                />
                <YAxis
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={{ stroke: '#334155' }}
                    tickFormatter={formatAxisTick}
                />
                <Tooltip
                    contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }}
                    formatter={(_val: any, _name: any, item: any) => {
                        const act = item.payload.actualValue;
                        const sign = act > 0 && !item.payload.isTotal ? '+' : '';
                        return [`${sign}${formatValue(act)}`, item.payload.name];
                    }}
                />
                <ReferenceLine y={0} stroke="#475569" />

                {/* Floating bottom transparent bar */}
                <Bar dataKey="bottom" stackId="waterfall" fill="transparent" />

                {/* Visible delta bar */}
                <Bar dataKey="amount" stackId="waterfall" radius={[4, 4, 0, 0]} onClick={handleClick} cursor="pointer">
                    {waterfallData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                </Bar>
            </BarChart>
        </ResponsiveContainer>
    );
};

export default DashboardWaterfallChart;
