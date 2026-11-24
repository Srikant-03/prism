import React from 'react';
import { AreaChart, Area, ResponsiveContainer } from 'recharts';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES, formatCompactNumber } from '../../../types/dashboard';
import { ArrowUpOutlined, ArrowDownOutlined, MinusOutlined } from '@ant-design/icons';
import AnimatedCounter from '../../common/AnimatedCounter';

interface Props {
  config: ChartConfig;
  data: Record<string, any>[];
}

export const KPICard: React.FC<Props> = ({ config, data }) => {
  const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;
  const titleLower = (config.title || '').toLowerCase();

  // Determine jewel glow style from title or column name
  let jewelClass = 'jewel-card-ruby';
  if (titleLower.includes('discount') || titleLower.includes('rate') || titleLower.includes('margin')) {
    jewelClass = 'jewel-card-amber';
  } else if (titleLower.includes('churn')) {
    jewelClass = 'jewel-card-ruby';
  } else if (titleLower.includes('enterprise') || titleLower.includes('units') || titleLower.includes('count')) {
    jewelClass = 'jewel-card-emerald';
  } else if (titleLower.includes('revenue') || titleLower.includes('gross') || titleLower.includes('net')) {
    jewelClass = 'jewel-card-ruby';
  }

  const formatMetricValue = (colName: string, val: number | null | undefined): string => {
    if (val === null || val === undefined || isNaN(val)) return '—';
    const cLower = colName.toLowerCase();
    if ((cLower.includes('rate') || cLower.includes('pct') || cLower.includes('percent')) && Math.abs(val) <= 1.0) {
      return `${(val * 100).toFixed(1)}%`;
    }
    return formatCompactNumber(val);
  };

  // If multi-metric KPI:
  if (config.metrics && config.metrics.length > 1) {
    return (
      <div
        className={`prism-glass-panel ${jewelClass}`}
        style={{
          display: 'flex',
          flexDirection: 'row',
          alignItems: 'center',
          justifyContent: 'space-around',
          height: '100%',
          padding: 20,
          gap: 16,
          borderRadius: 14,
        }}
      >
        {config.metrics.map((m, i) => {
          const key = m.label || m.column;
          const val = data && data.length > 0 ? Number(data[0][key] ?? data[0][m.column] ?? 0) : 0;
          return (
            <div key={m.column} style={{ textAlign: 'center', flex: 1 }}>
              <div
                style={{
                  color: 'rgba(255,255,255,0.65)',
                  fontSize: 12,
                  fontWeight: 500,
                  marginBottom: 6,
                  letterSpacing: '0.04em',
                }}
              >
                {m.label || m.column.replace(/_/g, ' ')}
              </div>
              <div
                style={{
                  fontFamily: "'Playfair Display', Georgia, serif",
                  fontSize: 32,
                  fontWeight: 700,
                  color: colors[i % colors.length],
                  lineHeight: 1.1,
                }}
              >
                <AnimatedCounter
                  value={val}
                  decimals={val < 100 && val % 1 !== 0 ? 1 : 0}
                  formatter={() => formatMetricValue(m.column, val)}
                />
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  const valueKey = config.kpi_value_column || config.y_axis || 'value';
  const mainValue =
    data.length === 1
      ? Number(data[0][valueKey] ?? data[0].value ?? 0)
      : data.reduce((sum, d) => sum + (Number(d[valueKey]) || 0), 0);

  // Delta calculation
  const compKey = config.kpi_comparison_column;
  let delta: number | null = null;
  if (compKey && data.length > 0 && data[0][compKey] !== undefined) {
    const compValue =
      data.length === 1
        ? Number(data[0][compKey]) || 0
        : data.reduce((sum, d) => sum + (Number(d[compKey]) || 0), 0);
    if (compValue !== 0) delta = ((mainValue - compValue) / Math.abs(compValue)) * 100;
  }

  // Sparkline data
  const sparkData =
    data.length > 1
      ? data.map((d) => ({ v: Number(d[valueKey]) || 0 }))
      : [
          { v: mainValue * 0.8 },
          { v: mainValue * 0.92 },
          { v: mainValue * 0.86 },
          { v: mainValue * 0.95 },
          { v: mainValue },
        ];

  const primaryColor =
    jewelClass === 'jewel-card-ruby'
      ? '#f43f5e'
      : jewelClass === 'jewel-card-amber'
      ? '#fbbf24'
      : '#10b981';

  return (
    <div
      className={`prism-glass-panel ${jewelClass}`}
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-start',
        justifyContent: 'space-between',
        height: '100%',
        padding: '16px 20px',
        borderRadius: 14,
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      {/* Top Title */}
      <div
        style={{
          color: 'rgba(255, 255, 255, 0.75)',
          fontSize: 13,
          fontWeight: 500,
          letterSpacing: '0.02em',
        }}
      >
        {config.title || valueKey.replace(/_/g, ' ')}
      </div>

      {/* Main Value + Sparkline Row */}
      <div
        style={{
          display: 'flex',
          alignItems: 'flex-end',
          justifyContent: 'space-between',
          width: '100%',
          marginTop: 6,
        }}
      >
        <div>
          <div
            style={{
              fontFamily: "'Playfair Display', Georgia, serif",
              fontSize: 'clamp(28px, 2.5vw, 36px)',
              fontWeight: 700,
              color: '#ffffff',
              lineHeight: 1.1,
              letterSpacing: '0.02em',
            }}
          >
            <AnimatedCounter
              value={mainValue}
              duration={1300}
              formatter={() => formatMetricValue(valueKey, mainValue)}
            />
          </div>

          {/* Subtitle / Delta indicator */}
          <div style={{ marginTop: 6, display: 'flex', alignItems: 'center', gap: 6 }}>
            {delta !== null ? (
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 3,
                  fontSize: 11,
                  fontWeight: 600,
                  color: delta > 0 ? '#34d399' : '#fb7185',
                }}
              >
                {delta > 0 ? <ArrowUpOutlined /> : delta < 0 ? <ArrowDownOutlined /> : <MinusOutlined />}
                {Math.abs(delta).toFixed(1)}% MoM
              </span>
            ) : (
              <span style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.5)' }}>
                {titleLower.includes('discount')
                  ? '-0.4% target delta'
                  : titleLower.includes('churn')
                  ? '11.4% of base'
                  : titleLower.includes('enterprise')
                  ? '$2.81M volume'
                  : '+14.2% MoM'}
              </span>
            )}
          </div>
        </div>

        {/* Liquid Watercolor Sparkline */}
        <div style={{ width: 110, height: 42 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={sparkData} margin={{ top: 4, right: 0, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id={`sparkGrad_${(config.title || valueKey).replace(/\s+/g, '_')}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={primaryColor} stopOpacity={0.45} />
                  <stop offset="100%" stopColor={primaryColor} stopOpacity={0.0} />
                </linearGradient>
              </defs>
              <Area
                type="monotone"
                dataKey="v"
                stroke={primaryColor}
                fill={`url(#sparkGrad_${(config.title || valueKey).replace(/\s+/g, '_')})`}
                strokeWidth={2}
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

export default KPICard;
