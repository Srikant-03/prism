/**
 * ScatterChart — Recharts scatter with optional regression overlay.
 */
import React from 'react';
import {
    ScatterChart as RechartsScatter, Scatter, XAxis, YAxis, CartesianGrid,
    Tooltip, Legend, ResponsiveContainer, ZAxis,
} from 'recharts';
import { COLOR_PALETTES } from "../../../types/dashboard";
import type { ChartConfig } from "../../../types/dashboard";

interface Props { config: ChartConfig; data: Record<string, any>[]; }

const DashboardScatterChart: React.FC<Props> = ({ config, data }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    return (
        <ResponsiveContainer width="100%" height="100%">
            <RechartsScatter data={data} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
                {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />}
                <XAxis dataKey={config.x_axis || 'x'} name={config.x_axis} tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                <YAxis dataKey={config.y_axis || 'y'} name={config.y_axis} tick={{ fill: '#94a3b8', fontSize: 12 }} axisLine={{ stroke: '#334155' }} />
                {config.size_by && <ZAxis dataKey={config.size_by} range={[20, 400]} />}
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#e2e8f0' }} cursor={{ strokeDasharray: '3 3' }} />
                {config.show_legend && <Legend wrapperStyle={{ color: '#94a3b8' }} />}
                <Scatter name={config.title || 'Data'} fill={colors[0]} />
            </RechartsScatter>
        </ResponsiveContainer>
    );
};

export default DashboardScatterChart;
