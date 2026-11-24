import React from 'react';
import {
  ScatterChart as RechartsScatter, Scatter, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ZAxis,
} from 'recharts';
import { formatCompactNumber } from "../../../types/dashboard";
import type { ChartConfig } from "../../../types/dashboard";

interface Props {
  config: ChartConfig;
  data: Record<string, any>[];
}

const NEBULA_CATEGORY_COLORS: Record<string, string> = {
  'Enterprise Software': '#a855f7',
  'Consulting': '#10b981',
  'Cloud Services': '#f59e0b',
  'Electronics': '#06b6d4',
  'Hardware': '#f43f5e',
};

const DEFAULT_NEBULA_PALETTE = ['#a855f7', '#10b981', '#f59e0b', '#06b6d4', '#f43f5e', '#6366f1'];

export const DashboardScatterChart: React.FC<Props> = ({ config, data }) => {
  const xKey = config.x_axis || 'x';
  const yKey = config.y_axis || 'y';

  // Exclude null / NaN rows
  const validData = (data || []).filter(
    (d) =>
      d[xKey] !== null &&
      d[xKey] !== undefined &&
      !isNaN(Number(d[xKey])) &&
      d[yKey] !== null &&
      d[yKey] !== undefined &&
      !isNaN(Number(d[yKey]))
  );

  const hasGroup = Boolean(config.group_by);
  const groups = hasGroup
    ? [...new Set(validData.map((d) => String(d[config.group_by!] ?? 'Other')))]
    : [];

  const getCategoryColor = (grpName: string, idx: number) => {
    return NEBULA_CATEGORY_COLORS[grpName] || DEFAULT_NEBULA_PALETTE[idx % DEFAULT_NEBULA_PALETTE.length];
  };

  return (
    <ResponsiveContainer width="100%" height="100%">
      <RechartsScatter margin={{ top: 20, right: 30, left: 20, bottom: 15 }}>
        <defs>
          <filter id="nebulaGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {config.show_grid && <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />}

        <XAxis
          type="number"
          dataKey={xKey}
          name={xKey.replace(/_/g, ' ').toUpperCase()}
          domain={['auto', 'auto']}
          tick={{ fill: '#94a3b8', fontSize: 11 }}
          axisLine={{ stroke: '#334155' }}
          tickFormatter={formatCompactNumber}
        />
        <YAxis
          type="number"
          dataKey={yKey}
          name={yKey.replace(/_/g, ' ').toUpperCase()}
          domain={['auto', 'auto']}
          tick={{ fill: '#94a3b8', fontSize: 11 }}
          axisLine={{ stroke: '#334155' }}
          tickFormatter={formatCompactNumber}
        />
        {config.size_by && <ZAxis dataKey={config.size_by} range={[40, 450]} />}

        <Tooltip
          cursor={{ strokeDasharray: '3 3', stroke: 'rgba(255,255,255,0.2)' }}
          content={({ active, payload }) => {
            if (!active || !payload || !payload.length) return null;
            const pt = payload[0].payload;
            const cat = pt[config.group_by || ''] || 'Point';
            const xVal = pt[xKey];
            const yVal = pt[yKey];
            return (
              <div
                style={{
                  background: 'rgba(15, 23, 42, 0.94)',
                  backdropFilter: 'blur(16px)',
                  border: '1px solid rgba(255, 255, 255, 0.16)',
                  borderRadius: 10,
                  padding: '8px 14px',
                  boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
                  color: '#f8fafc',
                  fontSize: 12,
                }}
              >
                <div style={{ fontWeight: 600, color: '#fef08a', marginBottom: 2 }}>
                  {formatCompactNumber(Number(yVal))}
                </div>
                <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.65)' }}>
                  {formatCompactNumber(Number(xVal))} Units • {cat}
                </div>
              </div>
            );
          }}
        />

        {config.show_legend && (
          <Legend
            wrapperStyle={{ color: 'rgba(255, 255, 255, 0.65)', paddingTop: 4, fontSize: 12 }}
          />
        )}

        {hasGroup ? (
          groups.map((grpName, i) => {
            const grpData = validData.filter(
              (d) => String(d[config.group_by!] ?? 'Other') === grpName
            );
            return (
              <Scatter
                key={grpName}
                name={grpName}
                data={grpData}
                fill={getCategoryColor(grpName, i)}
                animationDuration={1400}
                animationEasing="ease-out"
              />
            );
          })
        ) : (
          <Scatter
            name={config.title || 'Data Points'}
            data={validData}
            fill="#a855f7"
            animationDuration={1400}
            animationEasing="ease-out"
          />
        )}
      </RechartsScatter>
    </ResponsiveContainer>
  );
};

export default DashboardScatterChart;
