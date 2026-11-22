import React, { useState, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Upload } from 'antd';
import AnimatedCounter from '../common/AnimatedCounter';

const { Dragger } = Upload;

interface SpectralPortalProps {
  onUpload: (files: File[]) => void;
  onEnterWorkspace?: () => void;
  disabled?: boolean;
  fileName?: string;
  rowCount?: number;
}

export const SpectralPortal: React.FC<SpectralPortalProps> = ({
  onUpload,
  onEnterWorkspace,
  disabled = false,
  fileName = 'e2e_adversarial_telemetry.csv',
  rowCount = 245,
}) => {
  const [isDragOver, setIsDragOver] = useState(false);
  const [isDropped, setIsDropped] = useState(false);
  const uploadTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleFiles = (files: File[]) => {
    setIsDropped(true);
    if (uploadTimerRef.current) clearTimeout(uploadTimerRef.current);
    uploadTimerRef.current = setTimeout(() => {
      onUpload(files);
      setIsDropped(false);
    }, 450);
  };

  return (
    <div
      style={{
        minHeight: '100vh',
        width: '100%',
        background: 'radial-gradient(circle at 50% 30%, #161026 0%, #09090f 65%, #050508 100%)',
        color: '#f8fafc',
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
        display: 'flex',
        flexDirection: 'column',
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      {/* Ambient background particles */}
      <div
        style={{
          position: 'absolute',
          inset: 0,
          backgroundImage:
            'radial-gradient(1px 1px at 20px 30px, rgba(251, 191, 36, 0.25), rgba(0,0,0,0)), radial-gradient(1.5px 1.5px at 150px 180px, rgba(244, 63, 94, 0.2), rgba(0,0,0,0)), radial-gradient(1.2px 1.2px at 320px 90px, rgba(34, 211, 238, 0.25), rgba(0,0,0,0))',
          backgroundSize: '350px 350px',
          opacity: 0.7,
          pointerEvents: 'none',
        }}
      />

      {/* Top Header Navigation */}
      <header
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '24px 48px',
          zIndex: 10,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {/* Triangular Glass Prism Icon */}
          <svg width="28" height="28" viewBox="0 0 32 32" fill="none">
            <polygon
              points="16,3 30,28 2,28"
              fill="url(#prismHeaderGrad)"
              stroke="rgba(255,255,255,0.7)"
              strokeWidth="1.2"
            />
            <line x1="16" y1="3" x2="21" y2="28" stroke="rgba(255,255,255,0.5)" strokeWidth="0.8" />
            <line x1="2" y1="28" x2="30" y2="28" stroke="rgba(245,158,11,0.8)" strokeWidth="1" />
            <defs>
              <linearGradient id="prismHeaderGrad" x1="0" y1="0" x2="1" y2="1">
                <stop offset="0%" stopColor="rgba(255,255,255,0.85)" />
                <stop offset="40%" stopColor="rgba(244,63,94,0.4)" />
                <stop offset="70%" stopColor="rgba(245,158,11,0.4)" />
                <stop offset="100%" stopColor="rgba(99,102,241,0.5)" />
              </linearGradient>
            </defs>
          </svg>
          <span
            style={{
              fontFamily: "'Playfair Display', Georgia, serif",
              fontSize: 22,
              fontWeight: 700,
              letterSpacing: '0.12em',
              color: '#fdf4dc',
            }}
          >
            PRISM
          </span>
        </div>

        {/* Center Pill Navigation */}
        <nav
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 28,
            fontSize: 13,
            color: 'rgba(255, 255, 255, 0.65)',
            letterSpacing: '0.02em',
          }}
        >
          <span style={{ color: '#fef08a', cursor: 'pointer', fontWeight: 500 }}>The Refraction</span>
          <span
            style={{ cursor: 'pointer', transition: 'color 0.2s' }}
            onClick={onEnterWorkspace}
            onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255,255,255,0.65)')}
          >
            Spectral Canvas
          </span>
          <span
            style={{ cursor: 'pointer', transition: 'color 0.2s' }}
            onClick={onEnterWorkspace}
            onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255,255,255,0.65)')}
          >
            Constellation of Meaning
          </span>
          <span
            style={{ cursor: 'pointer', transition: 'color 0.2s' }}
            onClick={onEnterWorkspace}
            onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255,255,255,0.65)')}
          >
            The Stories
          </span>
          <span
            style={{ cursor: 'pointer', transition: 'color 0.2s' }}
            onClick={onEnterWorkspace}
            onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255,255,255,0.65)')}
          >
            Atelier Notes
          </span>
        </nav>

        {/* Right CTA */}
        <motion.button
          whileHover={{ scale: 1.04, boxShadow: '0 0 20px rgba(244,63,94,0.3)' }}
          whileTap={{ scale: 0.97 }}
          onClick={onEnterWorkspace}
          style={{
            padding: '9px 22px',
            borderRadius: 24,
            border: '1px solid rgba(255, 255, 255, 0.15)',
            background: 'linear-gradient(135deg, rgba(244, 63, 94, 0.2) 0%, rgba(30, 27, 75, 0.6) 100%)',
            color: '#f8fafc',
            fontSize: 13,
            fontWeight: 500,
            cursor: 'pointer',
            backdropFilter: 'blur(10px)',
          }}
        >
          Enter Workspace
        </motion.button>
      </header>

      {/* Main Hero & Prism Centerpiece */}
      <main
        style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '0 24px 48px',
          zIndex: 5,
          position: 'relative',
        }}
      >
        {/* Hero Title */}
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, ease: [0.16, 1, 0.24, 1] }}
          style={{ textAlign: 'center', marginBottom: 28 }}
        >
          <h1
            style={{
              fontFamily: "'Playfair Display', Georgia, serif",
              fontSize: 'clamp(28px, 4.2vw, 48px)',
              fontWeight: 400,
              letterSpacing: '0.14em',
              color: '#fdf4dc',
              margin: '0 0 12px',
              textShadow: '0 0 35px rgba(253, 244, 220, 0.25)',
            }}
          >
            FROM CHAOS, THE SPECTRUM OF TRUTH.
          </h1>
          <p
            style={{
              fontSize: 'clamp(13px, 1.3vw, 15px)',
              color: 'rgba(255, 255, 255, 0.62)',
              letterSpacing: '0.04em',
              maxWidth: 680,
              margin: '0 auto',
              fontWeight: 300,
            }}
          >
            Refracting raw, unformed data into a fanned, luminescent spectrum of human meaning.
          </p>
        </motion.div>

        {/* The Centerpiece: Prism on Stone with Dust & Spectrum */}
        <div
          style={{
            position: 'relative',
            width: '100%',
            maxWidth: 1060,
            height: 330,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <svg
            viewBox="0 0 1060 330"
            style={{ width: '100%', height: '100%', overflow: 'visible' }}
          >
            <defs>
              {/* Pedestal gradient */}
              <linearGradient id="pedestalTop" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stopColor="#30343f" />
                <stop offset="50%" stopColor="#22252e" />
                <stop offset="100%" stopColor="#14161c" />
              </linearGradient>

              {/* Prism Facet Gradients */}
              <linearGradient id="facetLeft" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.85)" />
                <stop offset="35%" stopColor="rgba(251, 191, 36, 0.45)" />
                <stop offset="100%" stopColor="rgba(244, 63, 94, 0.3)" />
              </linearGradient>

              <linearGradient id="facetRight" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="30%" stopColor="rgba(34, 211, 238, 0.4)" />
                <stop offset="70%" stopColor="rgba(99, 102, 241, 0.5)" />
                <stop offset="100%" stopColor="rgba(168, 85, 247, 0.45)" />
              </linearGradient>

              {/* Spectral Fanned Light Bands */}
              <linearGradient id="bandRed" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="20%" stopColor="rgba(239, 68, 68, 0.85)" />
                <stop offset="100%" stopColor="rgba(185, 28, 28, 0.2)" />
              </linearGradient>

              <linearGradient id="bandOrange" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="20%" stopColor="rgba(249, 115, 22, 0.85)" />
                <stop offset="100%" stopColor="rgba(194, 65, 12, 0.2)" />
              </linearGradient>

              <linearGradient id="bandYellow" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="20%" stopColor="rgba(234, 179, 8, 0.85)" />
                <stop offset="100%" stopColor="rgba(161, 98, 7, 0.2)" />
              </linearGradient>

              <linearGradient id="bandGreen" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="20%" stopColor="rgba(34, 197, 94, 0.85)" />
                <stop offset="100%" stopColor="rgba(21, 128, 61, 0.2)" />
              </linearGradient>

              <linearGradient id="bandCyan" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="20%" stopColor="rgba(6, 182, 212, 0.85)" />
                <stop offset="100%" stopColor="rgba(14, 116, 144, 0.2)" />
              </linearGradient>

              <linearGradient id="bandBlue" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="20%" stopColor="rgba(99, 102, 241, 0.85)" />
                <stop offset="100%" stopColor="rgba(67, 56, 202, 0.2)" />
              </linearGradient>

              <linearGradient id="bandViolet" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="rgba(255, 255, 255, 0.95)" />
                <stop offset="20%" stopColor="rgba(168, 85, 247, 0.85)" />
                <stop offset="100%" stopColor="rgba(126, 34, 206, 0.15)" />
              </linearGradient>

              {/* Caustic filter */}
              <filter id="prismGlow" x="-20%" y="-20%" width="140%" height="140%">
                <feGaussianBlur stdDeviation="6" result="blur" />
                <feComposite in="SourceGraphic" in2="blur" operator="over" />
              </filter>
            </defs>

            {/* LEFT: Raw Data Smoke & Golden Particle Clouds */}
            <g opacity="0.85">
              <path
                d="M 60 170 C 130 110, 240 230, 360 170 C 420 140, 480 160, 520 165"
                fill="none"
                stroke="url(#bandYellow)"
                strokeWidth="14"
                strokeLinecap="round"
                opacity="0.35"
                filter="url(#prismGlow)"
              />
              <path
                d="M 100 185 C 200 130, 290 220, 410 160 C 470 135, 510 160, 520 165"
                fill="none"
                stroke="rgba(251, 191, 36, 0.4)"
                strokeWidth="6"
                strokeDasharray="4 8"
                opacity="0.75"
              />
              <path
                d="M 140 160 C 220 200, 320 120, 440 175 C 480 190, 505 170, 520 165"
                fill="none"
                stroke="rgba(255, 255, 255, 0.55)"
                strokeWidth="2.5"
                opacity="0.6"
              />

              {/* Floating golden dust specks */}
              <circle cx="210" cy="145" r="2.5" fill="#fef08a" opacity="0.8" />
              <circle cx="280" cy="180" r="1.8" fill="#fbbf24" opacity="0.6" />
              <circle cx="340" cy="130" r="3" fill="#fef08a" opacity="0.9" />
              <circle cx="410" cy="165" r="2" fill="#fbbf24" opacity="0.75" />
              <circle cx="460" cy="150" r="2.8" fill="#fef08a" opacity="0.85" />
              <circle cx="490" cy="162" r="1.5" fill="#fff" opacity="0.95" />
            </g>

            {/* PEDESTAL BLOCK */}
            <g id="pedestal">
              {/* Pedestal Top Face */}
              <polygon
                points="420,225 640,225 670,250 390,250"
                fill="url(#pedestalTop)"
                stroke="rgba(255,255,255,0.12)"
                strokeWidth="1"
              />
              {/* Pedestal Front Face */}
              <polygon
                points="390,250 670,250 670,305 390,305"
                fill="#15171f"
                stroke="rgba(255,255,255,0.08)"
                strokeWidth="1"
              />
            </g>

            {/* RIGHT: Fanned Spectral Multi-Colored Light Plates */}
            <g id="spectralPlates" opacity="0.92">
              {/* Red Band */}
              <polygon points="530,165 530,172 1040,110 1040,85" fill="url(#bandRed)" opacity="0.85" />
              {/* Orange Band */}
              <polygon points="530,166 530,174 1040,140 1040,115" fill="url(#bandOrange)" opacity="0.85" />
              {/* Yellow / Saffron Band */}
              <polygon points="530,168 530,176 1040,170 1040,145" fill="url(#bandYellow)" opacity="0.85" />
              {/* Green Band */}
              <polygon points="530,170 530,178 1040,200 1040,175" fill="url(#bandGreen)" opacity="0.85" />
              {/* Cyan Band */}
              <polygon points="530,172 530,180 1040,230 1040,205" fill="url(#bandCyan)" opacity="0.85" />
              {/* Blue Band */}
              <polygon points="530,174 530,182 1040,260 1040,235" fill="url(#bandBlue)" opacity="0.85" />
              {/* Violet Band */}
              <polygon points="530,176 530,184 1040,290 1040,265" fill="url(#bandViolet)" opacity="0.85" />
            </g>

            {/* THE GLASS PRISM (Resting on Pedestal) */}
            <g id="glassPrism" filter="url(#prismGlow)">
              {/* Left Translucent Facet */}
              <polygon
                points="530,80 450,235 530,238"
                fill="url(#facetLeft)"
                stroke="rgba(255,255,255,0.7)"
                strokeWidth="1.4"
              />
              {/* Right Translucent Facet */}
              <polygon
                points="530,80 530,238 610,235"
                fill="url(#facetRight)"
                stroke="rgba(255,255,255,0.7)"
                strokeWidth="1.4"
              />
              {/* Base Refraction Line */}
              <line x1="450" y1="235" x2="610" y2="235" stroke="rgba(255,255,255,0.8)" strokeWidth="1.5" />
              {/* Center Ridge / Apex Specular Highlight */}
              <line x1="530" y1="80" x2="530" y2="238" stroke="rgba(255,255,255,0.95)" strokeWidth="2" />
              {/* Apex Sparkle */}
              <circle cx="530" cy="80" r="4.5" fill="#ffffff" filter="url(#prismGlow)" />
            </g>
          </svg>
        </div>

        {/* Interactive Drop Zone Card (Settle + Golden-Rose Wave) */}
        <div style={{ position: 'relative', width: '100%', maxWidth: 520, marginTop: -15 }}>
          {/* Expanding golden-rose wave on drop */}
          <AnimatePresence>
            {isDropped && (
              <motion.div
                initial={{ scale: 0.95, opacity: 0.8, filter: 'blur(14px)' }}
                animate={{ scale: 1.25, opacity: 0, filter: 'blur(0px)' }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.85, ease: [0.16, 1, 0.24, 1] }}
                style={{
                  position: 'absolute',
                  inset: 0,
                  borderRadius: 24,
                  background: 'radial-gradient(circle, rgba(251, 191, 36, 0.5) 0%, rgba(244, 63, 94, 0.4) 60%, transparent 100%)',
                  pointerEvents: 'none',
                  zIndex: 1,
                }}
              />
            )}
          </AnimatePresence>

          <motion.div
            onDragEnter={() => setIsDragOver(true)}
            onDragLeave={() => setIsDragOver(false)}
            onDrop={() => setIsDragOver(false)}
            animate={{
              scale: isDragOver ? 0.98 : 1,
              y: isDragOver ? 2 : 0,
            }}
            transition={{ duration: 0.22, ease: [0.16, 1, 0.24, 1] }}
            style={{
              borderRadius: 20,
              padding: '2px',
              background: isDragOver
                ? 'linear-gradient(90deg, #f43f5e, #fbbf24, #22d3ee)'
                : 'linear-gradient(135deg, rgba(255,255,255,0.18) 0%, rgba(255,255,255,0.04) 100%)',
              boxShadow: isDragOver
                ? '0 0 35px rgba(251, 191, 36, 0.35)'
                : '0 12px 36px rgba(0,0,0,0.45)',
              backdropFilter: 'blur(16px)',
              cursor: 'pointer',
              zIndex: 2,
            }}
          >
            <Dragger
              name="files"
              multiple={false}
              fileList={[]}
              showUploadList={false}
              disabled={disabled}
              beforeUpload={(_file, files) => {
                handleFiles(files);
                return false;
              }}
              style={{
                background: 'rgba(20, 24, 38, 0.78)',
                border: 'none',
                borderRadius: 18,
                padding: '24px 28px',
                textAlign: 'center',
              }}
            >
              <p
                style={{
                  margin: '0 0 8px',
                  fontSize: 16,
                  fontWeight: 400,
                  color: 'rgba(255, 255, 255, 0.92)',
                  letterSpacing: '0.02em',
                }}
              >
                Gently drop your raw data file here
              </p>
              <div
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 8,
                  padding: '4px 14px',
                  borderRadius: 16,
                  background: 'rgba(255, 255, 255, 0.08)',
                  border: '1px solid rgba(255, 255, 255, 0.1)',
                  fontSize: 12,
                  color: 'rgba(255, 255, 255, 0.65)',
                }}
              >
                <span>{fileName}</span>
                <span>•</span>
                <span>{rowCount} raw elements</span>
              </div>
            </Dragger>
          </motion.div>
        </div>

        {/* 3 Jewel Metric Cards Below Drop Zone */}
        <motion.div
          initial={{ opacity: 0, y: 25 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.9, delay: 0.2, ease: [0.16, 1, 0.24, 1] }}
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(3, 1fr)',
            gap: 16,
            width: '100%',
            maxWidth: 580,
            marginTop: 22,
          }}
        >
          {/* Card 1: Refracted Value (Ruby Glow) */}
          <div
            className="prism-glass-panel jewel-card-ruby"
            style={{
              padding: '14px 18px',
              borderRadius: 14,
              textAlign: 'center',
              backdropFilter: 'blur(14px)',
            }}
          >
            <div
              style={{
                fontFamily: "'Playfair Display', Georgia, serif",
                fontSize: 22,
                fontWeight: 600,
                color: '#fff',
                letterSpacing: '0.03em',
              }}
            >
              $<AnimatedCounter value={6.43} decimals={2} duration={1400} />M
            </div>
            <div style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.65)', marginTop: 2 }}>
              Refracted Value
            </div>
          </div>

          {/* Card 2: Margin Deflection (Amber Glow) */}
          <div
            className="prism-glass-panel jewel-card-amber"
            style={{
              padding: '14px 18px',
              borderRadius: 14,
              textAlign: 'center',
              backdropFilter: 'blur(14px)',
            }}
          >
            <div
              style={{
                fontFamily: "'Playfair Display', Georgia, serif",
                fontSize: 22,
                fontWeight: 600,
                color: '#fff',
                letterSpacing: '0.03em',
              }}
            >
              <AnimatedCounter value={11.67} decimals={2} duration={1400} />%
            </div>
            <div style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.65)', marginTop: 2 }}>
              Margin Deflection
            </div>
          </div>

          {/* Card 3: Elements Transformed (Emerald Glow) */}
          <div
            className="prism-glass-panel jewel-card-emerald"
            style={{
              padding: '14px 18px',
              borderRadius: 14,
              textAlign: 'center',
              backdropFilter: 'blur(14px)',
            }}
          >
            <div
              style={{
                fontFamily: "'Playfair Display', Georgia, serif",
                fontSize: 22,
                fontWeight: 600,
                color: '#fff',
                letterSpacing: '0.03em',
              }}
            >
              <AnimatedCounter value={rowCount} decimals={0} duration={1400} />
            </div>
            <div style={{ fontSize: 11, color: 'rgba(255, 255, 255, 0.65)', marginTop: 2 }}>
              Elements Transformed
            </div>
          </div>
        </motion.div>
      </main>
    </div>
  );
};

export default SpectralPortal;
