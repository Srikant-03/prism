/**
 * PresentMode — Fullscreen clean view of the dashboard.
 * Hides editing controls, shows only charts and title.
 * Arrow keys to navigate between widgets; Escape to exit.
 */
import React, { useEffect, useState, useCallback } from 'react';
import type { Widget } from '../../types/dashboard';
import ChartWidget from './ChartWidget';

interface Props {
    widgets: Widget[];
    title: string;
    onExit: () => void;
}

const PresentMode: React.FC<Props> = ({ widgets, title, onExit }) => {
    const [activeIdx, setActiveIdx] = useState(0);

    const handleKeyDown = useCallback((e: KeyboardEvent) => {
        if (e.key === 'Escape') onExit();
        else if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
            e.preventDefault();
            setActiveIdx(i => Math.min(i + 1, Math.max(0, widgets.length - 1)));
        }
        else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
            e.preventDefault();
            setActiveIdx(i => Math.max(i - 1, 0));
        }
    }, [onExit, widgets.length]);

    useEffect(() => {
        document.addEventListener('keydown', handleKeyDown);
        return () => document.removeEventListener('keydown', handleKeyDown);
    }, [handleKeyDown]);

    if (widgets.length === 0) return null;
    const w = widgets[activeIdx];
    if (!w) return null;

    return (
        <div
            role="dialog"
            aria-modal="true"
            aria-label="Presentation Mode"
            style={{
                position: 'fixed', inset: 0, zIndex: 9999, background: '#0f172a',
                display: 'flex', flexDirection: 'column',
            }}
        >
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '16px 32px', borderBottom: '1px solid #1e293b',
            }}>
                <h2 style={{ color: '#f1f5f9', margin: 0, fontSize: 20 }}>{title}</h2>
                <div style={{ color: '#64748b', fontSize: 13 }}>
                    {activeIdx + 1} / {widgets.length} • Press ESC to exit
                </div>
            </div>

            {/* Active Chart */}
            <div style={{ flex: 1, padding: 32 }}>
                <div style={{
                    height: '100%', background: 'rgba(15,23,42,0.8)', borderRadius: 16,
                    border: '1px solid rgba(99,102,241,0.1)', padding: 24,
                    display: 'flex', flexDirection: 'column',
                }}>
                    <h3 style={{ color: '#e2e8f0', margin: '0 0 16px', fontSize: 18 }}>
                        {w.config.title}
                    </h3>
                    <div style={{ flex: 1 }}>
                        <ChartWidget config={w.config} data={w.data || []} />
                    </div>
                </div>
            </div>

            {/* Thumbnail Strip */}
            <div style={{
                display: 'flex', gap: 8, padding: '12px 32px',
                borderTop: '1px solid #1e293b', overflowX: 'auto',
            }}>
                {widgets.map((widget, i) => (
                    <div
                        key={widget.id}
                        onClick={() => setActiveIdx(i)}
                        onKeyDown={(e) => e.key === 'Enter' && setActiveIdx(i)}
                        tabIndex={0}
                        role="button"
                        aria-pressed={i === activeIdx}
                        style={{
                            width: 80, height: 50, borderRadius: 8, cursor: 'pointer',
                            background: i === activeIdx ? 'rgba(99,102,241,0.2)' : '#1e293b',
                            border: i === activeIdx ? '2px solid #6366f1' : '1px solid #334155',
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            color: '#94a3b8', fontSize: 10, textAlign: 'center',
                            flexShrink: 0,
                            outline: 'none',
                        }}
                    >
                        {widget.config.chart_type}
                    </div>
                ))}
            </div>
        </div>
    );
};

export default PresentMode;
