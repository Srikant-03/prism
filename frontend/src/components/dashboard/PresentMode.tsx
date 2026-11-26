/**
 * PresentMode — Fullscreen presentation view for PRISM Dashboards.
 * Supports:
 * - Slide-by-slide or Full Canvas Page viewing
 * - Multi-page presentation navigation
 * - Auto-play presentation timer
 * - Keyboard navigation (Left/Right arrows, ESC to exit)
 * - Clean glassmorphic floating control bar
 */
import React, { useEffect, useState, useCallback, useRef } from 'react';
import { Button, Tooltip, Space } from 'antd';
import {
    LeftOutlined, RightOutlined, CloseOutlined,
    PlayCircleOutlined, PauseCircleOutlined,
    FullscreenOutlined,
    FundProjectionScreenOutlined,
} from '@ant-design/icons';
import type { Widget } from '../../types/dashboard';
import ChartWidget from './ChartWidget';

interface Props {
    widgets: Widget[];
    title: string;
    pageTitle?: string;
    onExit: () => void;
}

const PresentMode: React.FC<Props> = ({ widgets, title, pageTitle, onExit }) => {
    const [activeIdx, setActiveIdx] = useState(0);
    const [viewMode, setViewMode] = useState<'slide' | 'canvas'>('slide');
    const [isAutoPlay, setIsAutoPlay] = useState(false);
    const timerRef = useRef<number | null>(null);

    const activeWidgets = widgets.filter(w => !w.hidden);
    const count = activeWidgets.length;

    const handleNext = useCallback(() => {
        setActiveIdx(i => (count > 0 ? (i + 1) % count : 0));
    }, [count]);

    const handlePrev = useCallback(() => {
        setActiveIdx(i => (count > 0 ? (i - 1 + count) % count : 0));
    }, [count]);

    const handleKeyDown = useCallback((e: KeyboardEvent) => {
        if (e.key === 'Escape') {
            onExit();
        } else if (e.key === 'ArrowRight' || e.key === 'Space') {
            e.preventDefault();
            handleNext();
        } else if (e.key === 'ArrowLeft') {
            e.preventDefault();
            handlePrev();
        }
    }, [onExit, handleNext, handlePrev]);

    useEffect(() => {
        document.addEventListener('keydown', handleKeyDown);
        return () => document.removeEventListener('keydown', handleKeyDown);
    }, [handleKeyDown]);

    // Auto-play timer
    useEffect(() => {
        if (isAutoPlay && count > 1) {
            timerRef.current = window.setInterval(() => {
                setActiveIdx(i => (i + 1) % count);
            }, 8000);
        } else {
            if (timerRef.current) clearInterval(timerRef.current);
        }
        return () => {
            if (timerRef.current) clearInterval(timerRef.current);
        };
    }, [isAutoPlay, count]);

    if (count === 0) return null;
    const currentWidget = activeWidgets[activeIdx] || activeWidgets[0];

    return (
        <div
            role="dialog"
            aria-modal="true"
            aria-label="Presentation Mode"
            style={{
                position: 'fixed',
                inset: 0,
                zIndex: 9999,
                background: '#090d16',
                display: 'flex',
                flexDirection: 'column',
                userSelect: 'none',
            }}
        >
            {/* Top Bar */}
            <div style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                padding: '16px 32px',
                background: 'rgba(15, 23, 42, 0.75)',
                borderBottom: '1px solid rgba(51, 65, 85, 0.4)',
                backdropFilter: 'blur(10px)',
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                    <FundProjectionScreenOutlined style={{ fontSize: 20, color: '#818cf8' }} />
                    <span style={{ fontSize: 18, fontWeight: 700, color: '#f8fafc' }}>
                        {title}
                    </span>
                    {pageTitle && (
                        <span style={{ fontSize: 13, color: '#94a3b8', borderLeft: '1px solid #334155', paddingLeft: 12 }}>
                            {pageTitle}
                        </span>
                    )}
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                    <span style={{ fontSize: 12, color: '#64748b' }}>
                        {viewMode === 'slide' ? `Slide ${activeIdx + 1} of ${count}` : `${count} Visuals Grid`}
                    </span>
                    <Button
                        size="small"
                        type="text"
                        icon={<CloseOutlined style={{ color: '#94a3b8' }} />}
                        onClick={onExit}
                        title="Exit Presentation Mode (ESC)"
                    />
                </div>
            </div>

            {/* Main Content Area */}
            <div style={{ flex: 1, padding: '24px 32px', overflow: 'auto', display: 'flex', flexDirection: 'column' }}>
                {viewMode === 'slide' ? (
                    <div style={{
                        flex: 1,
                        background: 'rgba(30, 41, 59, 0.45)',
                        borderRadius: 16,
                        border: '1px solid rgba(99, 102, 241, 0.2)',
                        padding: 24,
                        display: 'flex',
                        flexDirection: 'column',
                        boxShadow: '0 20px 40px rgba(0,0,0,0.4)',
                    }}>
                        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <h2 style={{ fontSize: 20, fontWeight: 700, color: '#f1f5f9', margin: 0 }}>
                                {currentWidget.config.title || 'Visual'}
                            </h2>
                            <span style={{ fontSize: 11, color: '#818cf8', background: 'rgba(99, 102, 241, 0.1)', padding: '2px 8px', borderRadius: 10 }}>
                                {currentWidget.config.chart_type}
                            </span>
                        </div>
                        <div style={{ flex: 1, minHeight: 0 }}>
                            <ChartWidget config={currentWidget.config} data={currentWidget.data || []} />
                        </div>
                    </div>
                ) : (
                    /* Multi-Visual Grid Canvas Mode */
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(420px, 1fr))',
                        gap: 20,
                        flex: 1,
                    }}>
                        {activeWidgets.map(w => (
                            <div
                                key={w.id}
                                style={{
                                    height: 380,
                                    background: 'rgba(30, 41, 59, 0.45)',
                                    borderRadius: 12,
                                    border: '1px solid rgba(51, 65, 85, 0.4)',
                                    padding: 16,
                                    display: 'flex',
                                    flexDirection: 'column',
                                }}
                            >
                                <h3 style={{ fontSize: 14, fontWeight: 600, color: '#f1f5f9', margin: '0 0 10px' }}>
                                    {w.config.title}
                                </h3>
                                <div style={{ flex: 1, minHeight: 0 }}>
                                    <ChartWidget config={w.config} data={w.data || []} />
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Bottom Floating Control Bar */}
            <div style={{
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                padding: '12px 32px',
                background: 'rgba(15, 23, 42, 0.8)',
                borderTop: '1px solid rgba(51, 65, 85, 0.4)',
                backdropFilter: 'blur(12px)',
                gap: 16,
            }}>
                <Space size={8}>
                    <Button
                        size="small"
                        icon={<LeftOutlined />}
                        onClick={handlePrev}
                        disabled={viewMode === 'canvas'}
                        style={{ borderRadius: 6 }}
                    />
                    <Button
                        size="small"
                        icon={<RightOutlined />}
                        onClick={handleNext}
                        disabled={viewMode === 'canvas'}
                        style={{ borderRadius: 6 }}
                    />
                </Space>

                <div style={{
                    display: 'flex',
                    background: '#1e293b',
                    borderRadius: 8,
                    padding: 2,
                    border: '1px solid #334155',
                }}>
                    <button
                        onClick={() => setViewMode('slide')}
                        style={{
                            padding: '4px 10px',
                            fontSize: 11,
                            borderRadius: 6,
                            cursor: 'pointer',
                            border: 'none',
                            background: viewMode === 'slide' ? '#6366f1' : 'transparent',
                            color: viewMode === 'slide' ? '#fff' : '#94a3b8',
                            fontWeight: 600,
                        }}
                    >
                        Single Slide
                    </button>
                    <button
                        onClick={() => setViewMode('canvas')}
                        style={{
                            padding: '4px 10px',
                            fontSize: 11,
                            borderRadius: 6,
                            cursor: 'pointer',
                            border: 'none',
                            background: viewMode === 'canvas' ? '#6366f1' : 'transparent',
                            color: viewMode === 'canvas' ? '#fff' : '#94a3b8',
                            fontWeight: 600,
                        }}
                    >
                        Page Grid
                    </button>
                </div>

                <Tooltip title={isAutoPlay ? 'Pause Auto-Play' : 'Start Auto-Play (8s)'}>
                    <Button
                        size="small"
                        type={isAutoPlay ? 'primary' : 'default'}
                        icon={isAutoPlay ? <PauseCircleOutlined /> : <PlayCircleOutlined />}
                        onClick={() => setIsAutoPlay(!isAutoPlay)}
                        style={{ borderRadius: 6, background: isAutoPlay ? '#10b981' : undefined }}
                    >
                        {isAutoPlay ? 'Auto Playing' : 'Auto Play'}
                    </Button>
                </Tooltip>

                <Tooltip title="Toggle Browser Fullscreen">
                    <Button
                        size="small"
                        icon={<FullscreenOutlined />}
                        onClick={() => {
                            if (!document.fullscreenElement) {
                                document.documentElement.requestFullscreen?.();
                            } else {
                                document.exitFullscreen?.();
                            }
                        }}
                        style={{ borderRadius: 6 }}
                    />
                </Tooltip>

                <Button
                    size="small"
                    danger
                    onClick={onExit}
                    style={{ borderRadius: 6, fontSize: 11 }}
                >
                    Exit (ESC)
                </Button>
            </div>
        </div>
    );
};

export default PresentMode;
