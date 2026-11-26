import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';

interface StoryStudioProps {
  onExportPPTX?: () => void;
  onExportPDF?: () => void;
  onShare?: () => void;
  onNavigateMetamorphosis?: () => void;
  onNavigateCanvas?: () => void;
  onNavigateConstellation?: () => void;
}

interface SlideItem {
  id: string;
  num: string;
  title: string;
  icon?: string;
}

const DEFAULT_SLIDES: SlideItem[] = [
  { id: '1', num: '01', title: 'Executive Summary', icon: '💎' },
  { id: '2', num: '02', title: 'Telemetry Composition & Health', icon: '📊' },
  { id: '3', num: '03', title: 'Revenue Composition: Gross vs Net', icon: '📈' },
  { id: '4', num: '04', title: 'Margin Erosion by Segment', icon: '📉' },
  { id: '5', num: '05', title: 'Churn Drivers & Behavioral Context', icon: '🎯' },
  { id: '6', num: '06', title: 'Discount Elasticity Analysis', icon: '🏷️' },
  { id: '7', num: '07', title: 'Temporal Trends & April Surge', icon: '⏱️' },
  { id: '8', num: '08', title: 'Regional Distribution', icon: '🗺️' },
  { id: '9', num: '09', title: 'Strategic Causal Hypotheses', icon: '💡' },
  { id: '10', num: '10', title: 'Recommended Action Plan', icon: '🚀' },
];

const SLIDE_CHART_DATA = [
  { segment: 'Enterprise Software', gross: 3.21, net: 2.81 },
  { segment: 'Consulting', gross: 1.73, net: 1.51 },
  { segment: 'Cloud Services', gross: 1.08, net: 0.97 },
  { segment: 'Electronics', gross: 1.17, net: 0.96 },
  { segment: 'Hardware', gross: 0.134, net: 0.116 },
];

export const StoryStudio: React.FC<StoryStudioProps> = ({
  onExportPPTX,
  onExportPDF,
  onShare,
  onNavigateMetamorphosis,
  onNavigateCanvas,
  onNavigateConstellation,
}) => {
  const [selectedSlideId, setSelectedSlideId] = useState<string>('3');

  const selectedSlide = DEFAULT_SLIDES.find((s) => s.id === selectedSlideId) || DEFAULT_SLIDES[2];

  return (
    <div
      style={{
        minHeight: '100vh',
        background: 'radial-gradient(ellipse at 50% 12%, #141021 0%, #0b0c14 65%, #050608 100%)',
        color: '#f8fafc',
        padding: '24px 32px 64px',
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
      }}
    >
      {/* Top Header & Breadcrumbs */}
      <header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: 24,
          flexWrap: 'wrap',
          gap: 12,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {/* Triangular Glass Prism Icon */}
          <svg width="24" height="24" viewBox="0 0 32 32" fill="none">
            <polygon points="16,3 30,28 2,28" fill="url(#prismStoryGrad)" stroke="rgba(255,255,255,0.7)" strokeWidth="1.2" />
            <defs>
              <linearGradient id="prismStoryGrad" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stopColor="rgba(255,255,255,0.9)" />
                <stop offset="50%" stopColor="rgba(244,63,94,0.4)" />
                <stop offset="100%" stopColor="rgba(99,102,241,0.5)" />
              </linearGradient>
            </defs>
          </svg>
          <span style={{ fontFamily: "'Playfair Display', Georgia, serif", fontSize: 18, fontWeight: 700, color: '#fdf4dc' }}>
            PRISM
          </span>
          <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 13 }}>/</span>
          <span style={{ color: 'rgba(255,255,255,0.55)', fontSize: 13 }}>Atelier</span>
          <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 13 }}>/</span>
          <span style={{ color: 'rgba(255,255,255,0.7)', fontSize: 13 }}>e2e_adversarial_telemetry.csv</span>
          <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 13 }}>/</span>
          <span style={{ color: 'rgba(255,255,255,0.55)', fontSize: 13 }}>Story Studio</span>
          <span style={{ color: 'rgba(255,255,255,0.3)', fontSize: 13 }}>/</span>
          <span
            style={{
              padding: '2px 10px',
              borderRadius: 12,
              background: 'rgba(245, 158, 11, 0.12)',
              border: '1px solid rgba(245, 158, 11, 0.3)',
              color: '#fbbf24',
              fontSize: 12,
              fontWeight: 500,
            }}
          >
            10-Slide Deck Synthesized
          </span>
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
            onClick={onNavigateMetamorphosis}
            style={{ padding: '5px 14px', borderRadius: 20, fontSize: 12, color: 'rgba(255,255,255,0.65)', cursor: 'pointer' }}
          >
            Profiler
          </span>
          <span
            onClick={onNavigateCanvas}
            style={{ padding: '5px 14px', borderRadius: 20, fontSize: 12, color: 'rgba(255,255,255,0.65)', cursor: 'pointer' }}
          >
            AI Canvas
          </span>
          <span
            onClick={onNavigateConstellation}
            style={{ padding: '5px 14px', borderRadius: 20, fontSize: 12, color: 'rgba(255,255,255,0.65)', cursor: 'pointer' }}
          >
            Causal Map
          </span>
          <span
            style={{
              padding: '5px 16px',
              borderRadius: 20,
              fontSize: 12,
              fontWeight: 600,
              background: 'linear-gradient(135deg, rgba(244, 63, 94, 0.25), rgba(245, 158, 11, 0.25))',
              color: '#fef08a',
              border: '1px solid rgba(245, 158, 11, 0.4)',
              boxShadow: '0 0 15px rgba(245, 158, 11, 0.2)',
              cursor: 'pointer',
            }}
          >
            Story Studio
          </span>
        </nav>

        {/* Right Action Buttons */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <button
            onClick={onExportPPTX}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              padding: '7px 16px',
              borderRadius: 18,
              background: 'rgba(255, 255, 255, 0.06)',
              border: '1px solid rgba(255, 255, 255, 0.14)',
              color: '#f8fafc',
              fontSize: 12,
              cursor: 'pointer',
            }}
          >
            <span>📥</span> Export PPTX
          </button>
          <button
            onClick={onExportPDF}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              padding: '7px 16px',
              borderRadius: 18,
              background: 'rgba(255, 255, 255, 0.06)',
              border: '1px solid rgba(255, 255, 255, 0.14)',
              color: '#f8fafc',
              fontSize: 12,
              cursor: 'pointer',
            }}
          >
            <span>📄</span> Export PDF (10 Slides)
          </button>
          <button
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              padding: '7px 16px',
              borderRadius: 18,
              background: 'linear-gradient(135deg, rgba(244, 63, 94, 0.25), rgba(245, 158, 11, 0.25))',
              border: '1px solid rgba(245, 158, 11, 0.35)',
              color: '#fef08a',
              fontSize: 12,
              fontWeight: 500,
              cursor: 'pointer',
            }}
          >
            <span>🚀</span> Publish Live Story
          </button>
          <button
            onClick={onShare}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 6,
              padding: '7px 16px',
              borderRadius: 18,
              background: 'rgba(255, 255, 255, 0.06)',
              border: '1px solid rgba(255, 255, 255, 0.14)',
              color: '#f8fafc',
              fontSize: 12,
              cursor: 'pointer',
            }}
          >
            <span>🔗</span> Share
          </button>
        </div>
      </header>

      {/* 3-Column Layout: Slide Navigator (Left) | Slide Editor & Preview (Center) | AI Storyteller (Right) */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '220px 1fr 280px',
          gap: 20,
          alignItems: 'start',
        }}
      >
        {/* LEFT COLUMN: Slide Navigator (The Narrative Ribbon) */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#fff', marginBottom: 4 }}>
            Slide Navigator
            <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.45)', fontWeight: 400 }}>
              (The Narrative Ribbon)
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 8, maxHeight: 'calc(100vh - 220px)', overflowY: 'auto' }}>
            {DEFAULT_SLIDES.map((slide) => {
              const isSelected = selectedSlideId === slide.id;
              return (
                <div
                  key={slide.id}
                  onClick={() => setSelectedSlideId(slide.id)}
                  style={{
                    position: 'relative',
                    cursor: 'pointer',
                  }}
                >
                  {/* Iridescent active border with layoutId */}
                  {isSelected && (
                    <motion.div
                      layoutId="activeSlideGlow"
                      transition={{ type: 'spring', mass: 0.85, stiffness: 85, damping: 17 }}
                      style={{
                        position: 'absolute',
                        inset: -2,
                        borderRadius: 14,
                        border: '1.5px solid #f43f5e',
                        boxShadow: '0 0 16px rgba(244, 63, 94, 0.45), inset 0 0 8px rgba(245, 158, 11, 0.25)',
                        pointerEvents: 'none',
                        zIndex: 2,
                      }}
                    />
                  )}

                  <div
                    className="prism-glass-panel"
                    style={{
                      padding: '10px 12px',
                      borderRadius: 12,
                      background: isSelected ? 'rgba(244, 63, 94, 0.12)' : 'rgba(15, 23, 42, 0.65)',
                      display: 'flex',
                      alignItems: 'center',
                      gap: 10,
                      position: 'relative',
                      zIndex: 1,
                    }}
                  >
                    <span style={{ fontSize: 14 }}>{slide.icon || '📄'}</span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 10, color: isSelected ? '#fbbf24' : 'rgba(255,255,255,0.45)' }}>
                        {slide.num}:
                      </div>
                      <div
                        style={{
                          fontSize: 12,
                          fontWeight: isSelected ? 600 : 400,
                          color: isSelected ? '#ffffff' : 'rgba(255,255,255,0.8)',
                          whiteSpace: 'nowrap',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                        }}
                      >
                        {slide.title}
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* CENTER COLUMN: Slide Editor & Preview (Slide 03) */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#fff' }}>
            Slide Editor & Preview ({selectedSlide.num})
          </div>

          <motion.div
            key={selectedSlide.id}
            initial={{ opacity: 0, x: 15 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.65, ease: [0.16, 1, 0.24, 1] }}
            className="prism-glass-panel"
            style={{
              padding: '36px 40px',
              borderRadius: 20,
              minHeight: 520,
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'space-between',
              boxShadow: '0 16px 48px rgba(0,0,0,0.5)',
              border: '1px solid rgba(255,255,255,0.12)',
              background: 'linear-gradient(135deg, rgba(20, 24, 38, 0.88) 0%, rgba(10, 14, 24, 0.95) 100%)',
            }}
          >
            {/* Slide Header */}
            <div>
              <h2
                style={{
                  fontFamily: "'Playfair Display', Georgia, serif",
                  fontSize: 24,
                  fontWeight: 600,
                  color: '#ffffff',
                  margin: '0 0 8px',
                  letterSpacing: '0.02em',
                }}
              >
                {selectedSlide.id === '3'
                  ? 'Revenue Composition: Gross vs Net Across Segments'
                  : selectedSlide.title}
              </h2>
              <p style={{ fontSize: 13, color: 'rgba(255,255,255,0.65)', margin: '0 0 24px', fontWeight: 300 }}>
                {selectedSlide.id === '3'
                  ? 'Enterprise Software dominates overall volume at $2.81M net, while Cloud Services retains the highest margin efficiency.'
                  : 'Automated executive narrative distilled from telemetry dimensions.'}
              </p>

              {/* 2-Column Content: Dual Glass Bars (Left) + Executive Takeaways (Right) */}
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: '1.2fr 1fr',
                  gap: 24,
                  alignItems: 'center',
                }}
              >
                {/* Left: Dual Bar Chart */}
                <div style={{ height: 260 }}>
                  <div style={{ fontSize: 12, fontWeight: 500, color: 'rgba(255,255,255,0.7)', marginBottom: 8 }}>
                    Net vs Gross Revenue by Product Category
                  </div>
                  <ResponsiveContainer width="100%" height="90%">
                    <BarChart data={SLIDE_CHART_DATA} margin={{ top: 10, right: 10, left: -10, bottom: 20 }}>
                      <defs>
                        <linearGradient id="storyGrossGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#f97316" />
                          <stop offset="100%" stopColor="#ea580c" />
                        </linearGradient>
                        <linearGradient id="storyNetGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#a855f7" />
                          <stop offset="100%" stopColor="#7c3aed" />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                      <XAxis
                        dataKey="segment"
                        tick={{ fill: '#94a3b8', fontSize: 10 }}
                        axisLine={{ stroke: '#334155' }}
                        interval={0}
                        angle={-15}
                        textAnchor="end"
                      />
                      <YAxis
                        tick={{ fill: '#94a3b8', fontSize: 10 }}
                        axisLine={{ stroke: '#334155' }}
                        tickFormatter={(v) => `$${v}M`}
                      />
                      <Tooltip
                        contentStyle={{
                          background: 'rgba(15, 23, 42, 0.94)',
                          border: '1px solid rgba(255,255,255,0.15)',
                          borderRadius: 8,
                        }}
                      />
                      <Bar dataKey="gross" fill="url(#storyGrossGrad)" radius={[4, 4, 0, 0]} name="Gross Revenue" />
                      <Bar dataKey="net" fill="url(#storyNetGrad)" radius={[4, 4, 0, 0]} name="Net Revenue" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Right: Executive Takeaways */}
                <div
                  style={{
                    padding: '20px',
                    borderRadius: 14,
                    background: 'rgba(255, 255, 255, 0.03)',
                    border: '1px solid rgba(255, 255, 255, 0.08)',
                  }}
                >
                  <div style={{ fontSize: 13, fontWeight: 600, color: '#fef08a', marginBottom: 12 }}>
                    Executive Takeaways
                  </div>
                  <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, color: 'rgba(255,255,255,0.85)', lineHeight: 1.8 }}>
                    <li>
                      <strong>$2.81M Net Revenue</strong> from Enterprise Software represents 43.7% of total portfolio.
                    </li>
                    <li>
                      <strong>Consulting generated $1.51M net</strong> on $1.73M gross, impacted by 12.4% average discounting.
                    </li>
                    <li>
                      <strong>Cloud Services</strong> shows highest conversion efficiency at 90.4% net margin retention.
                    </li>
                    <li>
                      Low marginal contribution from Hardware ($116K net) despite steady transaction volume.
                    </li>
                  </ul>
                </div>
              </div>
            </div>

            {/* Slide Footer */}
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                fontSize: 11,
                color: 'rgba(255, 255, 255, 0.45)',
                borderTop: '1px solid rgba(255, 255, 255, 0.08)',
                paddingTop: 14,
                marginTop: 20,
              }}
            >
              <span>{selectedSlide.num} / 10 • PRISM Executive Data Story • Telemetry Analysis</span>
              <span>Proprietary & Confidential</span>
            </div>
          </motion.div>
        </div>

        {/* RIGHT COLUMN: AI Storyteller & Narrative Director */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div className="prism-glass-panel" style={{ padding: '18px 20px', borderRadius: 16 }}>
            <div style={{ fontSize: 14, fontWeight: 600, color: '#fff', marginBottom: 14 }}>
              AI Storyteller & Narrative Director
            </div>

            {/* Story Context */}
            <div style={{ marginBottom: 14 }}>
              <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', marginBottom: 4 }}>
                Story Context
              </div>
              <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.85)', lineHeight: 1.4 }}>
                Synthesizing financial performance and segmented margin erosion.
              </div>
            </div>

            {/* Narrative Audio Script */}
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', marginBottom: 4 }}>
                Narrative Audio Script / Talking Points
              </div>
              <div
                style={{
                  padding: '10px 12px',
                  borderRadius: 10,
                  background: 'rgba(255, 255, 255, 0.04)',
                  border: '1px solid rgba(255, 255, 255, 0.08)',
                  fontSize: 11,
                  color: 'rgba(255,255,255,0.8)',
                  lineHeight: 1.5,
                }}
              >
                "Enterprise Software remains the anchor of our revenue portfolio, driving 43.7% of net volume ($2.81M). Cloud Services demonstrates exceptional conversion, while Consulting volume carries the heaviest discount burden..."
              </div>
            </div>

            {/* Slide Style Palette */}
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', marginBottom: 6 }}>
                Slide Style Palette
              </div>
              <div
                style={{
                  padding: '8px 12px',
                  borderRadius: 10,
                  background: 'rgba(255, 255, 255, 0.04)',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  fontSize: 12,
                  color: '#f8fafc',
                  marginBottom: 8,
                }}
              >
                Refraction Spectrum Dark (Selected)
              </div>
              {/* Jewel color dots */}
              <div style={{ display: 'flex', gap: 6 }}>
                {['#f43f5e', '#fb923c', '#fbbf24', '#34d399', '#22d3ee', '#818cf8', '#c084fc'].map((col) => (
                  <div
                    key={col}
                    style={{
                      width: 14,
                      height: 14,
                      borderRadius: '50%',
                      background: col,
                      boxShadow: `0 0 6px ${col}`,
                    }}
                  />
                ))}
              </div>
            </div>

            {/* Slide Layout Selector */}
            <div>
              <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', marginBottom: 6 }}>
                Slide Layout Selector
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 6 }}>
                {[1, 2, 3, 4, 5, 6].map((l) => (
                  <div
                    key={l}
                    style={{
                      height: 38,
                      borderRadius: 6,
                      background: l === 1 ? 'rgba(244, 63, 94, 0.2)' : 'rgba(255, 255, 255, 0.04)',
                      border: l === 1 ? '1px solid #f43f5e' : '1px solid rgba(255, 255, 255, 0.08)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      cursor: 'pointer',
                    }}
                  >
                    <span style={{ fontSize: 10, opacity: 0.6 }}>L{l}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default StoryStudio;
