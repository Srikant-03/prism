import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

interface CelestialNode {
  id: string;
  name: string;
  domain: string;
  color: string;
  glow: string;
  x: number; // percentage in SVG viewBox
  y: number;
  radius: number;
  weight: number;
  isCenter?: boolean;
}

interface CelestialStrand {
  source: string;
  target: string;
  strength: number;
  label: string;
  color: string;
}

interface FeatureRankingItem {
  feature: string;
  score: number;
  subtext: string;
  color: string;
}

const CELESTIAL_NODES: CelestialNode[] = [
  {
    id: 'churn_flag',
    name: 'churn_flag',
    domain: 'Binary Target',
    color: '#f43f5e',
    glow: 'rgba(244, 63, 94, 0.65)',
    x: 350,
    y: 250,
    radius: 36,
    weight: 1.0,
    isCenter: true,
  },
  {
    id: 'customer_notes',
    name: 'customer_notes',
    domain: 'Behavioral',
    color: '#fbbf24',
    glow: 'rgba(251, 191, 36, 0.55)',
    x: 210,
    y: 140,
    radius: 24,
    weight: 0.784,
  },
  {
    id: 'is_enterprise',
    name: 'is_enterprise',
    domain: 'Segment',
    color: '#34d399',
    glow: 'rgba(52, 211, 153, 0.55)',
    x: 510,
    y: 130,
    radius: 22,
    weight: 0.422,
  },
  {
    id: 'discount_rate',
    name: 'discount_rate',
    domain: 'Pricing',
    color: '#22d3ee',
    glow: 'rgba(34, 211, 238, 0.55)',
    x: 550,
    y: 240,
    radius: 20,
    weight: 0.355,
  },
  {
    id: 'net_revenue',
    name: 'net_revenue',
    domain: 'Financial',
    color: '#a855f7',
    glow: 'rgba(168, 85, 247, 0.55)',
    x: 480,
    y: 350,
    radius: 20,
    weight: 0.35,
  },
  {
    id: 'loyalty_tier_code',
    name: 'loyalty_tier_code',
    domain: 'Identity',
    color: '#fb923c',
    glow: 'rgba(251, 146, 60, 0.55)',
    x: 290,
    y: 380,
    radius: 20,
    weight: 0.35,
  },
  {
    id: 'order_month_name',
    name: 'order_month_name',
    domain: 'Temporal',
    color: '#e879f9',
    glow: 'rgba(232, 121, 249, 0.55)',
    x: 160,
    y: 280,
    radius: 17,
    weight: 0.42,
  },
  {
    id: 'order_quarter',
    name: 'order_quarter',
    domain: 'Temporal',
    color: '#e879f9',
    glow: 'rgba(232, 121, 249, 0.55)',
    x: 180,
    y: 360,
    radius: 15,
    weight: 0.78,
  },
  {
    id: 'gross_revenue',
    name: 'gross_revenue',
    domain: 'Financial',
    color: '#818cf8',
    glow: 'rgba(129, 140, 248, 0.55)',
    x: 570,
    y: 320,
    radius: 14,
    weight: 0.35,
  },
  {
    id: 'net_revenue_eur',
    name: 'net_revenue_eur',
    domain: 'Financial',
    color: '#818cf8',
    glow: 'rgba(129, 140, 248, 0.55)',
    x: 530,
    y: 410,
    radius: 12,
    weight: 0.35,
  },
];

const CELESTIAL_STRANDS: CelestialStrand[] = [
  { source: 'churn_flag', target: 'customer_notes', strength: 0.784, label: '+0.78', color: '#fbbf24' },
  { source: 'churn_flag', target: 'is_enterprise', strength: 0.422, label: '+0.42', color: '#34d399' },
  { source: 'churn_flag', target: 'discount_rate', strength: 0.355, label: '+0.35', color: '#22d3ee' },
  { source: 'churn_flag', target: 'net_revenue', strength: 0.35, label: '+0.35', color: '#a855f7' },
  { source: 'churn_flag', target: 'loyalty_tier_code', strength: 0.35, label: '+0.35', color: '#fb923c' },
  { source: 'churn_flag', target: 'order_month_name', strength: 0.42, label: '+0.42', color: '#e879f9' },
  { source: 'order_month_name', target: 'order_quarter', strength: 0.78, label: '+0.78', color: '#e879f9' },
  { source: 'net_revenue', target: 'gross_revenue', strength: 0.65, label: '+0.65', color: '#818cf8' },
  { source: 'net_revenue', target: 'net_revenue_eur', strength: 0.99, label: '+0.99', color: '#818cf8' },
];

const RANKING_DATA: FeatureRankingItem[] = [
  { feature: 'customer_notes', score: 0.784, subtext: 'billing delays / cancellation requests', color: 'linear-gradient(90deg, #fbbf24, #f59e0b)' },
  { feature: 'post_churn_refund', score: 0.641, subtext: 'outcome correlation', color: 'linear-gradient(90deg, #f43f5e, #be123c)' },
  { feature: 'is_enterprise', score: 0.422, subtext: 'segment risk', color: 'linear-gradient(90deg, #34d399, #059669)' },
  { feature: 'discount_rate', score: 0.355, subtext: 'pricing correlation', color: 'linear-gradient(90deg, #22d3ee, #0891b2)' },
  { feature: 'loyalty_tier_code', score: 0.281, subtext: 'identity order code', color: 'linear-gradient(90deg, #fb923c, #ea580c)' },
  { feature: 'net_revenue', score: 0.218, subtext: 'financial volume', color: 'linear-gradient(90deg, #a855f7, #7c3aed)' },
];

export const CausalConstellation: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'network' | 'matrix' | 'drivers'>('network');
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);

  const nodeMap = new Map(CELESTIAL_NODES.map((n) => [n.id, n]));

  // Connected nodes set for hover dimming
  const connectedNodeIds = new Set<string>();
  if (hoveredNode) {
    connectedNodeIds.add(hoveredNode);
    CELESTIAL_STRANDS.forEach((s) => {
      if (s.source === hoveredNode) connectedNodeIds.add(s.target);
      if (s.target === hoveredNode) connectedNodeIds.add(s.source);
    });
  }

  return (
    <div
      style={{
        minHeight: '100vh',
        background: 'radial-gradient(ellipse at 50% 15%, #151122 0%, #0c0e16 65%, #050608 100%)',
        color: '#f8fafc',
        padding: '24px 32px 64px',
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
      }}
    >
      {/* Title & Navigation */}
      <div style={{ marginBottom: 20 }}>
        <h1
          style={{
            fontFamily: "'Playfair Display', Georgia, serif",
            fontSize: 26,
            fontWeight: 400,
            letterSpacing: '0.08em',
            color: '#fdf4dc',
            margin: '0 0 14px',
          }}
        >
          THE CAUSAL CONSTELLATION & FEATURE ASSOCIATION NETWORK
        </h1>

        {/* Tab Filters */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', gap: 6 }}>
            {[
              { key: 'network', label: 'Feature Association Network' },
              { key: 'matrix', label: 'Correlation Matrix' },
              { key: 'drivers', label: 'Target Drivers (churn_flag)' },
            ].map((t) => (
              <button
                key={t.key}
                onClick={() => setActiveTab(t.key as any)}
                style={{
                  padding: '6px 16px',
                  borderRadius: 18,
                  fontSize: 12,
                  fontWeight: 500,
                  border: '1px solid',
                  borderColor: activeTab === t.key ? '#f59e0b' : 'rgba(255,255,255,0.1)',
                  background:
                    activeTab === t.key
                      ? 'rgba(245, 158, 11, 0.15)'
                      : 'rgba(255,255,255,0.04)',
                  color: activeTab === t.key ? '#fef08a' : 'rgba(255,255,255,0.7)',
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                }}
              >
                {t.label}
              </button>
            ))}
          </div>

          <div style={{ display: 'flex', gap: 8, fontSize: 12 }}>
            <span
              style={{
                padding: '4px 12px',
                borderRadius: 14,
                background: 'rgba(255,255,255,0.06)',
                border: '1px solid rgba(255,255,255,0.1)',
                color: 'rgba(255,255,255,0.75)',
              }}
            >
              All Connections
            </span>
            <span
              style={{
                padding: '4px 12px',
                borderRadius: 14,
                background: 'rgba(245,158,11,0.12)',
                border: '1px solid rgba(245,158,11,0.3)',
                color: '#fbbf24',
              }}
            >
              Strong Links (&gt;0.35)
            </span>
            <span
              style={{
                padding: '4px 12px',
                borderRadius: 14,
                background: 'rgba(255,255,255,0.06)',
                border: '1px solid rgba(255,255,255,0.1)',
                color: 'rgba(255,255,255,0.75)',
              }}
            >
              Color by Semantic Domain
            </span>
          </div>
        </div>
      </div>

      {/* 3-Column Layout: Target Drivers (22%) | Celestial Graph (54%) | Matrix & Hypotheses (24%) */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '22% 54% 24%',
          gap: 16,
          alignItems: 'start',
        }}
      >
        {/* LEFT PANEL: Target Drivers (churn_flag) */}
        <div className="prism-glass-panel" style={{ padding: '18px 20px', borderRadius: 16 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
            <span style={{ fontSize: 14, fontWeight: 600, color: '#fff' }}>Target Drivers: churn_flag</span>
            <span style={{ fontSize: 11, color: 'rgba(255,255,255,0.45)' }}>22%</span>
          </div>

          <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', marginBottom: 12 }}>
            Feature Importance Ranking
          </div>

          {/* Ranking Bars */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            {RANKING_DATA.map((item) => (
              <div key={item.feature}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
                  <span style={{ fontFamily: 'monospace', color: '#f8fafc' }}>{item.feature}</span>
                  <span style={{ fontWeight: 600, color: '#fef08a' }}>{item.score.toFixed(3)}</span>
                </div>
                {/* Gradient Fill Bar */}
                <div style={{ height: 6, width: '100%', background: 'rgba(255,255,255,0.08)', borderRadius: 3, overflow: 'hidden' }}>
                  <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: `${item.score * 100}%` }}
                    transition={{ duration: 1.2, ease: [0.16, 1, 0.24, 1] }}
                    style={{ height: '100%', background: item.color, borderRadius: 3 }}
                  />
                </div>
                <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.45)', marginTop: 2 }}>
                  {item.subtext}
                </div>
              </div>
            ))}
          </div>

          {/* Leakage Guard Callout */}
          <div
            style={{
              marginTop: 20,
              padding: '10px 12px',
              borderRadius: 10,
              background: 'rgba(225, 29, 72, 0.1)',
              border: '1px solid rgba(225, 29, 72, 0.25)',
              fontSize: 11,
              color: 'rgba(255,255,255,0.7)',
              lineHeight: 1.4,
            }}
          >
            Binary target: <strong>churn_flag</strong> • 7.75:1 imbalance • Leakage guard active (post_churn_refund isolated)
          </div>
        </div>

        {/* CENTER PANEL: Interactive Feature Association Network Graph */}
        <div
          className="prism-glass-panel"
          style={{
            padding: '16px 20px',
            borderRadius: 16,
            position: 'relative',
            overflow: 'hidden',
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <span style={{ fontSize: 14, fontWeight: 600, color: '#fff' }}>
              Interactive Feature Association Network Graph
            </span>
            <span style={{ fontSize: 11, color: 'rgba(255,255,255,0.45)' }}>54%</span>
          </div>

          {/* Celestial SVG Canvas */}
          <div style={{ width: '100%', height: 490, position: 'relative' }}>
            <svg
              viewBox="0 0 700 500"
              style={{ width: '100%', height: '100%', overflow: 'visible' }}
            >
              <defs>
                {/* Center Sun Glow */}
                <radialGradient id="centerSunGrad" cx="50%" cy="50%" r="50%">
                  <stop offset="0%" stopColor="#f43f5e" />
                  <stop offset="40%" stopColor="#be123c" />
                  <stop offset="80%" stopColor="#881337" />
                  <stop offset="100%" stopColor="transparent" />
                </radialGradient>

                <filter id="celestialGlow" x="-50%" y="-50%" width="200%" height="200%">
                  <feGaussianBlur stdDeviation="5" result="coloredBlur" />
                  <feMerge>
                    <feMergeNode in="coloredBlur" />
                    <feMergeNode in="SourceGraphic" />
                  </feMerge>
                </filter>
              </defs>

              {/* Radiant Light Strands connecting nodes */}
              {CELESTIAL_STRANDS.map((strand, i) => {
                const sNode = nodeMap.get(strand.source);
                const tNode = nodeMap.get(strand.target);
                if (!sNode || !tNode) return null;

                const isDimmed =
                  hoveredNode &&
                  !(
                    (strand.source === hoveredNode && strand.target === hoveredNode) ||
                    strand.source === hoveredNode ||
                    strand.target === hoveredNode
                  );

                // Curved bezier path
                const midX = (sNode.x + tNode.x) / 2 + (sNode.y - tNode.y) * 0.15;
                const midY = (sNode.y + tNode.y) / 2 + (tNode.x - sNode.x) * 0.15;
                const pathD = `M ${sNode.x} ${sNode.y} Q ${midX} ${midY} ${tNode.x} ${tNode.y}`;

                return (
                  <g key={i} opacity={isDimmed ? 0.2 : 0.85} style={{ transition: 'opacity 0.25s' }}>
                    {/* Glowing outer halo line */}
                    <path
                      d={pathD}
                      fill="none"
                      stroke={strand.color}
                      strokeWidth={strand.strength * 4}
                      strokeOpacity="0.4"
                      filter="url(#celestialGlow)"
                    />
                    {/* Animated Dash Core */}
                    <path
                      d={pathD}
                      fill="none"
                      stroke={strand.color}
                      strokeWidth={strand.strength * 2}
                      strokeDasharray="4 6"
                      strokeOpacity="0.9"
                    />
                    {/* Strength label tag */}
                    <text
                      x={midX}
                      y={midY}
                      fill="#fef08a"
                      fontSize="10"
                      textAnchor="middle"
                      dy="-4"
                      style={{ fontFamily: 'monospace', fontWeight: 600 }}
                    >
                      {strand.label}
                    </text>
                  </g>
                );
              })}

              {/* Orbiting Celestial Nodes */}
              {CELESTIAL_NODES.map((node) => {
                const isDimmed = hoveredNode && !connectedNodeIds.has(node.id);
                const isHovered = hoveredNode === node.id;

                return (
                  <g
                    key={node.id}
                    opacity={isDimmed ? 0.25 : 1}
                    style={{
                      cursor: 'pointer',
                      transition: 'opacity 0.25s, transform 0.25s',
                      transformOrigin: `${node.x}px ${node.y}px`,
                      transform: isHovered ? 'scale(1.15)' : 'scale(1)',
                    }}
                    onMouseEnter={() => setHoveredNode(node.id)}
                    onMouseLeave={() => setHoveredNode(null)}
                  >
                    {/* Pulsing Aura for Center Node */}
                    {node.isCenter && (
                      <circle
                        cx={node.x}
                        cy={node.y}
                        r={node.radius + 18}
                        fill="url(#centerSunGrad)"
                        opacity="0.65"
                        filter="url(#celestialGlow)"
                      />
                    )}

                    {/* Outer glow ring */}
                    <circle
                      cx={node.x}
                      cy={node.y}
                      r={node.radius + 4}
                      fill="none"
                      stroke={node.glow}
                      strokeWidth="2"
                    />

                    {/* Main Orb Body */}
                    <circle
                      cx={node.x}
                      cy={node.y}
                      r={node.radius}
                      fill={node.color}
                      filter="url(#celestialGlow)"
                    />

                    {/* Node Labels */}
                    <text
                      x={node.x}
                      y={node.y + node.radius + 14}
                      fill="#ffffff"
                      fontSize="11"
                      textAnchor="middle"
                      fontWeight="500"
                    >
                      {node.name}
                    </text>
                    <text
                      x={node.x}
                      y={node.y + node.radius + 25}
                      fill="rgba(255,255,255,0.45)"
                      fontSize="9"
                      textAnchor="middle"
                    >
                      {node.domain}
                    </text>
                  </g>
                );
              })}
            </svg>

            {/* Interactive Gliding Tooltip */}
            <AnimatePresence>
              {hoveredNode && (
                <motion.div
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0 }}
                  style={{
                    position: 'absolute',
                    top: 16,
                    left: 20,
                    background: 'rgba(15, 23, 42, 0.92)',
                    backdropFilter: 'blur(16px)',
                    border: '1px solid rgba(255, 255, 255, 0.16)',
                    borderRadius: 10,
                    padding: '8px 14px',
                    boxShadow: '0 8px 30px rgba(0,0,0,0.5)',
                    fontSize: 12,
                    color: '#f8fafc',
                    pointerEvents: 'none',
                  }}
                >
                  <span style={{ color: '#fbbf24', fontWeight: 600 }}>
                    churn_flag ↔ {hoveredNode}
                  </span>
                  <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.7)', marginTop: 2 }}>
                    Mutual Info: 0.784 • Key causal driver
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </div>

        {/* RIGHT PANEL: The Refraction Matrix & Hypotheses */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          {/* Correlation Matrix Heatmap */}
          <div className="prism-glass-panel" style={{ padding: '16px 18px', borderRadius: 16 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: '#fff', marginBottom: 10 }}>
              2D Feature Correlation Matrix Heatmap
            </div>

            {/* Mini Heatmap Grid */}
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(5, 1fr)',
                gap: 3,
                marginBottom: 10,
              }}
            >
              {[
                { v: 1.0, c: '#e11d48' }, { v: 0.78, c: '#f59e0b' }, { v: 0.78, c: '#f59e0b' }, { v: 0.42, c: '#10b981' }, { v: 0.1, c: '#6366f1' },
                { v: 0.78, c: '#f59e0b' }, { v: 1.0, c: '#e11d48' }, { v: 0.41, c: '#10b981' }, { v: 0.35, c: '#06b6d4' }, { v: 0.17, c: '#6366f1' },
                { v: 0.78, c: '#f59e0b' }, { v: 0.41, c: '#10b981' }, { v: 1.0, c: '#e11d48' }, { v: 0.35, c: '#06b6d4' }, { v: 0.03, c: '#6366f1' },
                { v: 0.42, c: '#10b981' }, { v: 0.35, c: '#06b6d4' }, { v: 0.35, c: '#06b6d4' }, { v: 1.0, c: '#e11d48' }, { v: 0.21, c: '#6366f1' },
                { v: 0.1, c: '#6366f1' }, { v: 0.17, c: '#6366f1' }, { v: 0.03, c: '#6366f1' }, { v: 0.21, c: '#6366f1' }, { v: 1.0, c: '#e11d48' },
              ].map((cell, idx) => (
                <div
                  key={idx}
                  style={{
                    height: 24,
                    background: cell.c,
                    opacity: 0.85,
                    borderRadius: 3,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: 9,
                    fontWeight: 600,
                    color: '#fff',
                  }}
                >
                  {cell.v === 1 ? '1.0' : cell.v.toFixed(2)}
                </div>
              ))}
            </div>
          </div>

          {/* Automated Causal Hypotheses */}
          <div className="prism-glass-panel" style={{ padding: '16px 18px', borderRadius: 16 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: '#fff', marginBottom: 12 }}>
              Automated Causal Hypotheses
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              {/* Hypothesis 1 */}
              <div
                style={{
                  padding: '10px 12px',
                  borderRadius: 10,
                  background: 'rgba(255, 255, 255, 0.04)',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                }}
              >
                <div style={{ fontSize: 11, fontWeight: 600, color: '#fbbf24', marginBottom: 2 }}>
                  Hypothesis 1: Churn Catalyst
                </div>
                <div style={{ fontSize: 10, color: 'rgba(255, 255, 255, 0.7)', lineHeight: 1.35 }}>
                  Customer notes referencing billing delays show a 78.4% predictive association with churn_flag.
                </div>
              </div>

              {/* Hypothesis 2 */}
              <div
                style={{
                  padding: '10px 12px',
                  borderRadius: 10,
                  background: 'rgba(255, 255, 255, 0.04)',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                }}
              >
                <div style={{ fontSize: 11, fontWeight: 600, color: '#c084fc', marginBottom: 2 }}>
                  Hypothesis 2: Deterministic Derivation
                </div>
                <div style={{ fontSize: 10, color: 'rgba(255, 255, 255, 0.7)', lineHeight: 1.35 }}>
                  net_revenue is derived via gross_revenue x (1 - discount_rate) [R² = 1.000].
                </div>
              </div>

              {/* Hypothesis 3 */}
              <div
                style={{
                  padding: '10px 12px',
                  borderRadius: 10,
                  background: 'rgba(255, 255, 255, 0.04)',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                }}
              >
                <div style={{ fontSize: 11, fontWeight: 600, color: '#34d399', marginBottom: 2 }}>
                  Hypothesis 3: Margin Elasticity
                </div>
                <div style={{ fontSize: 10, color: 'rgba(255, 255, 255, 0.7)', lineHeight: 1.35 }}>
                  Loyalty Tier 1 accounts exhibit 14.8% higher discount sensitivity.
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CausalConstellation;
