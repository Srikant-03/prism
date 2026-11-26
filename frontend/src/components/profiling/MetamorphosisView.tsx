import React, { useState } from 'react';
import { motion } from 'framer-motion';
import AnimatedCounter from '../common/AnimatedCounter';

interface MetamorphosisViewProps {
  onNavigateCanvas?: () => void;
  onNavigateConstellation?: () => void;
  onNavigateStory?: () => void;
  fileName?: string;
  rowCount?: number;
  colCount?: number;
}

interface TaxonomyRow {
  id: string;
  dimension: string;
  category: 'numeric' | 'categorical' | 'temporal' | 'target' | 'id';
  essence: string;
  badgeClass: string;
  presence: string;
  bounds: string;
  state: string;
  stateClass: string;
  waveformType: 'sine' | 'scatter' | 'wave' | 'colorbars' | 'histogram' | 'currency_bars' | 'target_split';
}

const TAXONOMY_DATA: TaxonomyRow[] = [
  {
    id: 'sequential_row_id',
    dimension: 'sequential_row_id',
    category: 'id',
    essence: 'ID_KEY',
    badgeClass: 'jewel-badge-id',
    presence: '100%',
    bounds: '1 to 240 • Sequential Order',
    state: 'Preserved',
    stateClass: 'state-pill-preserved',
    waveformType: 'sine',
  },
  {
    id: 'txn_uuid',
    dimension: 'txn_uuid',
    category: 'id',
    essence: 'ID_KEY',
    badgeClass: 'jewel-badge-id',
    presence: '100%',
    bounds: 'TXN-10000 to TXN-10240',
    state: 'Identity Isolated',
    stateClass: 'state-pill-isolated',
    waveformType: 'scatter',
  },
  {
    id: 'order_date',
    dimension: 'order_date',
    category: 'temporal',
    essence: 'DATETIME',
    badgeClass: 'jewel-badge-datetime',
    presence: '100%',
    bounds: 'January 2024 – June 2025',
    state: 'Chronos Aligned',
    stateClass: 'state-pill-aligned',
    waveformType: 'wave',
  },
  {
    id: 'product_category',
    dimension: 'product_category',
    category: 'categorical',
    essence: 'CATEGORICAL',
    badgeClass: 'jewel-badge-categorical',
    presence: '99.59%',
    bounds: '5 Segments • Enterprise Software (49)',
    state: 'Grouped',
    stateClass: 'state-pill-grouped',
    waveformType: 'colorbars',
  },
  {
    id: 'discount_rate',
    dimension: 'discount_rate',
    category: 'numeric',
    essence: 'PERCENTAGE',
    badgeClass: 'jewel-badge-percentage',
    presence: '90.20%',
    bounds: '0.00% to 25.00% • 24 NaNs',
    state: 'Imputed',
    stateClass: 'state-pill-imputed',
    waveformType: 'histogram',
  },
  {
    id: 'net_revenue',
    dimension: 'net_revenue',
    category: 'numeric',
    essence: 'CURRENCY',
    badgeClass: 'jewel-badge-currency',
    presence: '99.59%',
    bounds: 'Mean: $26.3K • Max: $640K',
    state: 'Normalized',
    stateClass: 'state-pill-normalized',
    waveformType: 'currency_bars',
  },
  {
    id: 'churn_flag',
    dimension: 'churn_flag',
    category: 'target',
    essence: 'BINARY_TARGET',
    badgeClass: 'jewel-badge-target',
    presence: '100%',
    bounds: '28 Churned (11.4%) • 7.75:1 Imbalance',
    state: 'Target Isolated',
    stateClass: 'state-pill-isolated',
    waveformType: 'target_split',
  },
];

export const MetamorphosisView: React.FC<MetamorphosisViewProps> = ({
  onNavigateCanvas,
  onNavigateConstellation,
  onNavigateStory,
  fileName = 'e2e_adversarial_telemetry.csv',
  rowCount = 245,
  colCount = 27,
}) => {
  const [activeFilter, setActiveFilter] = useState<string>('all');

  const filteredRows = TAXONOMY_DATA.filter((row) => {
    if (activeFilter === 'all') return true;
    if (activeFilter === 'numeric') return row.category === 'numeric';
    if (activeFilter === 'categorical') return row.category === 'categorical';
    if (activeFilter === 'temporal') return row.category === 'temporal';
    if (activeFilter === 'targets') return row.category === 'target';
    return true;
  });

  return (
    <div
      style={{
        minHeight: '100vh',
        background: 'radial-gradient(ellipse at 50% 10%, #151122 0%, #0b0d14 60%, #06070a 100%)',
        color: '#f8fafc',
        padding: '24px 36px 64px',
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
      }}
    >
      {/* Top Header & Breadcrumb Ribbon */}
      <header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: 32,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          {/* Triangular Glass Prism Icon */}
          <svg width="26" height="26" viewBox="0 0 32 32" fill="none">
            <polygon
              points="16,3 30,28 2,28"
              fill="url(#prismMetaGrad)"
              stroke="rgba(255,255,255,0.7)"
              strokeWidth="1.2"
            />
            <defs>
              <linearGradient id="prismMetaGrad" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stopColor="rgba(255,255,255,0.9)" />
                <stop offset="40%" stopColor="rgba(244,63,94,0.4)" />
                <stop offset="70%" stopColor="rgba(245,158,11,0.4)" />
                <stop offset="100%" stopColor="rgba(99,102,241,0.5)" />
              </linearGradient>
            </defs>
          </svg>
          <span
            style={{
              fontFamily: "'Playfair Display', Georgia, serif",
              fontSize: 18,
              fontWeight: 700,
              letterSpacing: '0.12em',
              color: '#fdf4dc',
            }}
          >
            PRISM
          </span>
          <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 13 }}>/</span>
          <span style={{ color: 'rgba(255,255,255,0.55)', fontSize: 13 }}>Atelier</span>
          <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 13 }}>/</span>
          <span style={{ color: 'rgba(255,255,255,0.7)', fontSize: 13 }}>{fileName}</span>
          <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 13 }}>/</span>
          <span style={{ color: '#fbbf24', fontSize: 13, fontWeight: 500 }}>Semantic Metamorphosis</span>
        </div>

        {/* Center Pill Navigation */}
        <nav
          style={{
            display: 'flex',
            alignItems: 'center',
            background: 'rgba(255, 255, 255, 0.05)',
            border: '1px solid rgba(255, 255, 255, 0.1)',
            borderRadius: 30,
            padding: '4px',
            gap: 4,
          }}
        >
          <span
            style={{
              padding: '6px 18px',
              borderRadius: 24,
              fontSize: 13,
              fontWeight: 600,
              background: 'linear-gradient(135deg, rgba(244, 63, 94, 0.25), rgba(245, 158, 11, 0.25))',
              color: '#fef08a',
              border: '1px solid rgba(245, 158, 11, 0.4)',
              boxShadow: '0 0 15px rgba(245, 158, 11, 0.2)',
              cursor: 'pointer',
            }}
          >
            The Metamorphosis
          </span>
          <span
            onClick={onNavigateCanvas}
            style={{
              padding: '6px 16px',
              borderRadius: 24,
              fontSize: 13,
              color: 'rgba(255, 255, 255, 0.65)',
              cursor: 'pointer',
              transition: 'color 0.2s',
            }}
            onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255, 255, 255, 0.65)')}
          >
            Spectral Canvas
          </span>
          <span
            onClick={onNavigateConstellation}
            style={{
              padding: '6px 16px',
              borderRadius: 24,
              fontSize: 13,
              color: 'rgba(255, 255, 255, 0.65)',
              cursor: 'pointer',
              transition: 'color 0.2s',
            }}
            onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255, 255, 255, 0.65)')}
          >
            Causal Constellation
          </span>
          <span
            onClick={onNavigateStory}
            style={{
              padding: '6px 16px',
              borderRadius: 24,
              fontSize: 13,
              color: 'rgba(255, 255, 255, 0.65)',
              cursor: 'pointer',
              transition: 'color 0.2s',
            }}
            onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255, 255, 255, 0.65)')}
          >
            Story Pavilion
          </span>
        </nav>

        {/* Right Action Buttons */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <button
            onClick={onNavigateCanvas}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 8,
              padding: '8px 18px',
              borderRadius: 20,
              background: 'rgba(255, 255, 255, 0.08)',
              border: '1px solid rgba(255, 255, 255, 0.15)',
              color: '#f8fafc',
              fontSize: 12,
              fontWeight: 500,
              cursor: 'pointer',
            }}
          >
            <span>🔄</span> Refract & Harmonize
          </button>
          <button
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 8,
              padding: '8px 18px',
              borderRadius: 20,
              background: 'rgba(255, 255, 255, 0.08)',
              border: '1px solid rgba(255, 255, 255, 0.15)',
              color: '#f8fafc',
              fontSize: 12,
              fontWeight: 500,
              cursor: 'pointer',
            }}
          >
            <span>🔗</span> Share
          </button>
        </div>
      </header>

      {/* Hero Title & Subtitle */}
      <div style={{ marginBottom: 24 }}>
        <h1
          style={{
            fontFamily: "'Playfair Display', Georgia, serif",
            fontSize: 28,
            fontWeight: 400,
            letterSpacing: '0.1em',
            color: '#fdf4dc',
            margin: '0 0 6px',
          }}
        >
          THE METAMORPHOSIS OF RAW DATA
        </h1>
        <p style={{ margin: 0, fontSize: 14, color: 'rgba(255,255,255,0.6)', fontWeight: 300 }}>
          Distilling {rowCount} unformed records across {colCount} turbulent dimensions into a harmonious, multi-colored semantic spectrum.
        </p>
      </div>

      {/* Filter Tabs / Pills */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 24 }}>
        {[
          { key: 'all', label: `All (${colCount})` },
          { key: 'numeric', label: 'Numeric (11)' },
          { key: 'categorical', label: 'Categorical (9)' },
          { key: 'temporal', label: 'Temporal (2)' },
          { key: 'targets', label: 'Targets (1)' },
        ].map((f) => (
          <button
            key={f.key}
            onClick={() => setActiveFilter(f.key)}
            style={{
              padding: '6px 14px',
              borderRadius: 18,
              fontSize: 12,
              fontWeight: 500,
              border: '1px solid',
              borderColor: activeFilter === f.key ? '#f59e0b' : 'rgba(255,255,255,0.1)',
              background:
                activeFilter === f.key
                  ? 'rgba(245, 158, 11, 0.15)'
                  : 'rgba(255,255,255,0.04)',
              color: activeFilter === f.key ? '#fef08a' : 'rgba(255,255,255,0.7)',
              cursor: 'pointer',
              transition: 'all 0.2s',
            }}
          >
            {f.label}
          </button>
        ))}
      </div>

      {/* Three Column Grid: Crucible (Left) | Taxonomy Table (Center) | Synthesis Deck (Right) */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '260px 1fr 280px',
          gap: 20,
          alignItems: 'start',
        }}
      >
        {/* LEFT COLUMN: The Raw Crucible */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <div
            className="prism-glass-panel"
            style={{
              padding: '24px 20px',
              borderRadius: 18,
              textAlign: 'center',
            }}
          >
            <div
              style={{
                fontSize: 15,
                fontWeight: 600,
                color: '#fff',
                marginBottom: 18,
                textAlign: 'left',
              }}
            >
              The Raw Crucible
            </div>

            {/* Circular Gauge with Watercolor Pulse */}
            <div
              style={{
                position: 'relative',
                width: 140,
                height: 140,
                margin: '0 auto 16px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
              }}
            >
              {/* Pulsing Breathing Aura */}
              <div
                style={{
                  position: 'absolute',
                  inset: -6,
                  borderRadius: '50%',
                  background:
                    'radial-gradient(circle, rgba(244, 63, 94, 0.35) 0%, rgba(245, 158, 11, 0.25) 50%, transparent 75%)',
                  animation: 'crucibleAura 4s ease-in-out infinite',
                }}
              />
              <svg width="140" height="140" viewBox="0 0 140 140">
                {/* Background Ring */}
                <circle
                  cx="70"
                  cy="70"
                  r="56"
                  fill="none"
                  stroke="rgba(255, 255, 255, 0.08)"
                  strokeWidth="8"
                />
                {/* Active Colored Arc */}
                <circle
                  cx="70"
                  cy="70"
                  r="56"
                  fill="none"
                  stroke="url(#crucibleGrad)"
                  strokeWidth="8"
                  strokeDasharray="351.8"
                  strokeDashoffset="7"
                  strokeLinecap="round"
                  transform="rotate(-90 70 70)"
                />
                <defs>
                  <linearGradient id="crucibleGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#f43f5e" />
                    <stop offset="60%" stopColor="#fb923c" />
                    <stop offset="100%" stopColor="#fbbf24" />
                  </linearGradient>
                </defs>
              </svg>

              <div
                style={{
                  position: 'absolute',
                  textAlign: 'center',
                }}
              >
                <div
                  style={{
                    fontFamily: "'Playfair Display', Georgia, serif",
                    fontSize: 22,
                    fontWeight: 700,
                    color: '#fff',
                  }}
                >
                  <AnimatedCounter value={98.08} decimals={2} duration={1200} />%
                </div>
                <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.6)', marginTop: 2 }}>
                  Semantic Integrity • 1.92% Natural Nulls
                </div>
              </div>
            </div>

            <p style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', margin: 0, lineHeight: 1.4 }}>
              {rowCount} raw elements ingested + 127 missing spaces isolated
            </p>
          </div>

          {/* Metamorphic Steps Timeline */}
          <div className="prism-glass-panel" style={{ padding: '20px', borderRadius: 18 }}>
            <div style={{ fontSize: 14, fontWeight: 600, color: '#fff', marginBottom: 16 }}>
              Metamorphic Steps
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 14, position: 'relative' }}>
              {/* Connecting line */}
              <div
                style={{
                  position: 'absolute',
                  left: 6,
                  top: 8,
                  bottom: 8,
                  width: 2,
                  background: 'linear-gradient(180deg, #fbbf24 0%, #f97316 35%, #a855f7 70%, #f43f5e 100%)',
                }}
              />

              {[
                { label: 'RFC-4180 Validated', color: '#fbbf24' },
                { label: 'Nulls Preserved as NaN', color: '#f97316' },
                { label: 'Date Swaps Calibrated', color: '#c084fc' },
                { label: 'Row 200 Isolated', color: '#f43f5e' },
              ].map((step, idx) => (
                <div key={idx} style={{ display: 'flex', alignItems: 'center', gap: 12, paddingLeft: 18 }}>
                  <div
                    style={{
                      width: 10,
                      height: 10,
                      borderRadius: '50%',
                      background: step.color,
                      boxShadow: `0 0 8px ${step.color}`,
                      position: 'absolute',
                      left: 2,
                    }}
                  />
                  <span style={{ fontSize: 12, color: 'rgba(255,255,255,0.85)' }}>{step.label}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* CENTER COLUMN: The Spectral Taxonomy Table */}
        <div
          className="prism-glass-panel"
          style={{
            padding: '20px 24px',
            borderRadius: 18,
            overflowX: 'auto',
          }}
        >
          <div
            style={{
              fontSize: 16,
              fontWeight: 600,
              color: '#fff',
              marginBottom: 16,
              letterSpacing: '0.02em',
            }}
          >
            The Spectral Taxonomy Table
          </div>

          <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: '0 8px' }}>
            <thead>
              <tr style={{ color: 'rgba(255, 255, 255, 0.45)', fontSize: 11, textAlign: 'left' }}>
                <th style={{ padding: '8px 12px' }}>Dimension</th>
                <th style={{ padding: '8px 12px' }}>Semantic Essence</th>
                <th style={{ padding: '8px 12px' }}>Waveform / Distribution</th>
                <th style={{ padding: '8px 12px' }}>Presence</th>
                <th style={{ padding: '8px 12px' }}>Bounds & Natural Meaning</th>
                <th style={{ padding: '8px 12px' }}>Metamorphic State</th>
              </tr>
            </thead>
            <tbody>
              {filteredRows.map((row, idx) => (
                <motion.tr
                  key={row.id}
                  initial={{ opacity: 0, y: 18 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.5, delay: idx * 0.03, ease: [0.16, 1, 0.24, 1] }}
                  style={{
                    background: 'rgba(255, 255, 255, 0.03)',
                    borderRadius: 10,
                  }}
                >
                  {/* Dimension Name */}
                  <td
                    style={{
                      padding: '12px',
                      fontSize: 13,
                      fontFamily: 'monospace',
                      color: 'rgba(255, 255, 255, 0.9)',
                      borderTopLeftRadius: 10,
                      borderBottomLeftRadius: 10,
                    }}
                  >
                    {row.dimension}
                  </td>

                  {/* Semantic Essence Badge */}
                  <td style={{ padding: '12px' }}>
                    <span className={`jewel-badge ${row.badgeClass}`}>{row.essence}</span>
                  </td>

                  {/* Waveform / Distribution SVG Mini-Visual */}
                  <td style={{ padding: '12px' }}>
                    <WaveformMiniVisual type={row.waveformType} />
                  </td>

                  {/* Presence */}
                  <td style={{ padding: '12px', fontSize: 12, color: 'rgba(255, 255, 255, 0.85)' }}>
                    {row.presence}
                  </td>

                  {/* Bounds & Natural Meaning */}
                  <td style={{ padding: '12px', fontSize: 12, color: 'rgba(255, 255, 255, 0.65)' }}>
                    {row.bounds}
                  </td>

                  {/* Metamorphic State */}
                  <td
                    style={{
                      padding: '12px',
                      borderTopRightRadius: 10,
                      borderBottomRightRadius: 10,
                    }}
                  >
                    <span className={`state-pill ${row.stateClass}`}>{row.state}</span>
                  </td>
                </motion.tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* RIGHT COLUMN: Refraction Synthesis Deck */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div className="prism-glass-panel" style={{ padding: '20px', borderRadius: 18 }}>
            <div style={{ fontSize: 15, fontWeight: 600, color: '#fff', marginBottom: 14 }}>
              Refraction Synthesis Deck
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {/* Card 1: Target Discovered */}
              <div
                style={{
                  padding: '12px 14px',
                  borderRadius: 12,
                  background: 'rgba(225, 29, 72, 0.08)',
                  border: '1px solid rgba(225, 29, 72, 0.25)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#fb7185', fontSize: 12, fontWeight: 600 }}>
                  <span>🎯</span> Target Discovered
                </div>
                <div style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.7)', marginTop: 4 }}>
                  churn_flag (Binary Classification, 98% Conf)
                </div>
              </div>

              {/* Card 2: Leakage Guarded */}
              <div
                style={{
                  padding: '12px 14px',
                  borderRadius: 12,
                  background: 'rgba(244, 63, 94, 0.08)',
                  border: '1px solid rgba(244, 63, 94, 0.25)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#f43f5e', fontSize: 12, fontWeight: 600 }}>
                  <span>🔒</span> Leakage Guarded
                </div>
                <div style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.7)', marginTop: 4 }}>
                  post_churn_refund isolated as post-hoc outcome
                </div>
              </div>

              {/* Card 3: Temporal Calibration */}
              <div
                style={{
                  padding: '12px 14px',
                  borderRadius: 12,
                  background: 'rgba(168, 85, 247, 0.08)',
                  border: '1px solid rgba(168, 85, 247, 0.25)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#c084fc', fontSize: 12, fontWeight: 600 }}>
                  <span>⏱</span> Temporal Calibration
                </div>
                <div style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.7)', marginTop: 4 }}>
                  ship_date_mixed unified (3.0-day mean delta)
                </div>
              </div>

              {/* Card 4: Anomaly Handled */}
              <div
                style={{
                  padding: '12px 14px',
                  borderRadius: 12,
                  background: 'rgba(245, 158, 11, 0.08)',
                  border: '1px solid rgba(245, 158, 11, 0.25)',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#fbbf24', fontSize: 12, fontWeight: 600 }}>
                  <span>⚠️</span> Anomaly Handled
                </div>
                <div style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.7)', marginTop: 4 }}>
                  Row 200 isolated (74.1% nulls)
                </div>
              </div>
            </div>

            {/* Glowing Radiant Button: Refract into Spectral Canvas */}
            <motion.button
              whileHover={{ scale: 1.03, boxShadow: '0 0 25px rgba(244, 63, 94, 0.5)' }}
              whileTap={{ scale: 0.98 }}
              onClick={onNavigateCanvas}
              style={{
                width: '100%',
                marginTop: 18,
                padding: '12px 16px',
                borderRadius: 12,
                border: 'none',
                background: 'linear-gradient(135deg, #f43f5e 0%, #fb923c 50%, #f59e0b 100%)',
                color: '#ffffff',
                fontSize: 13,
                fontWeight: 600,
                letterSpacing: '0.02em',
                cursor: 'pointer',
                boxShadow: '0 4px 20px rgba(244, 63, 94, 0.35)',
                transition: 'box-shadow 0.2s',
              }}
            >
              Refract into Spectral Canvas →
            </motion.button>
          </div>
        </div>
      </div>
    </div>
  );
};

// Helper SVG mini-waveforms for table
function WaveformMiniVisual({ type }: { type: string }) {
  switch (type) {
    case 'sine':
      return (
        <svg width="70" height="20" viewBox="0 0 70 20" fill="none">
          <path
            d="M 2 10 Q 18 0 35 10 T 68 10"
            stroke="#fbbf24"
            strokeWidth="2"
            fill="none"
          />
        </svg>
      );
    case 'scatter':
      return (
        <svg width="70" height="20" viewBox="0 0 70 20" fill="none">
          <circle cx="10" cy="12" r="1.5" fill="#fbbf24" opacity="0.6" />
          <circle cx="18" cy="7" r="2" fill="#fbbf24" opacity="0.9" />
          <circle cx="28" cy="14" r="1" fill="#fbbf24" opacity="0.5" />
          <circle cx="38" cy="9" r="2.5" fill="#fbbf24" opacity="0.95" />
          <circle cx="48" cy="11" r="1.5" fill="#fbbf24" opacity="0.7" />
          <circle cx="58" cy="8" r="2" fill="#fbbf24" opacity="0.8" />
        </svg>
      );
    case 'wave':
      return (
        <svg width="70" height="20" viewBox="0 0 70 20" fill="none">
          <path
            d="M 2 16 C 20 16 25 4 45 4 C 55 4 60 16 68 16"
            stroke="#c084fc"
            strokeWidth="2"
            fill="rgba(192, 132, 252, 0.15)"
          />
        </svg>
      );
    case 'colorbars':
      return (
        <div style={{ display: 'flex', gap: 3, alignItems: 'center' }}>
          <div style={{ width: 10, height: 14, background: '#ea580c', borderRadius: 2 }} />
          <div style={{ width: 10, height: 18, background: '#f59e0b', borderRadius: 2 }} />
          <div style={{ width: 10, height: 12, background: '#10b981', borderRadius: 2 }} />
          <div style={{ width: 10, height: 16, background: '#06b6d4', borderRadius: 2 }} />
          <div style={{ width: 10, height: 10, background: '#6366f1', borderRadius: 2 }} />
        </div>
      );
    case 'histogram':
      return (
        <div style={{ display: 'flex', gap: 2, alignItems: 'flex-end', height: 18 }}>
          <div style={{ width: 6, height: 16, background: '#fb7185', borderRadius: 1 }} />
          <div style={{ width: 6, height: 12, background: '#fb7185', borderRadius: 1 }} />
          <div style={{ width: 6, height: 8, background: '#fb7185', borderRadius: 1 }} />
          <div style={{ width: 6, height: 14, background: '#fb7185', borderRadius: 1 }} />
          <div style={{ width: 6, height: 6, background: '#fb7185', borderRadius: 1 }} />
        </div>
      );
    case 'currency_bars':
      return (
        <div style={{ display: 'flex', gap: 2, alignItems: 'flex-end', height: 18 }}>
          <div style={{ width: 5, height: 10, background: '#f97316', borderRadius: 1 }} />
          <div style={{ width: 5, height: 18, background: '#f97316', borderRadius: 1 }} />
          <div style={{ width: 5, height: 14, background: '#f97316', borderRadius: 1 }} />
          <div style={{ width: 5, height: 6, background: '#f97316', borderRadius: 1 }} />
          <div style={{ width: 5, height: 4, background: '#f97316', borderRadius: 1 }} />
        </div>
      );
    case 'target_split':
      return (
        <div style={{ display: 'flex', width: 60, height: 14, borderRadius: 3, overflow: 'hidden' }}>
          <div style={{ width: '88%', background: '#fb7185', opacity: 0.7 }} />
          <div style={{ width: '12%', background: '#be123c' }} />
        </div>
      );
    default:
      return null;
  }
}

export default MetamorphosisView;
