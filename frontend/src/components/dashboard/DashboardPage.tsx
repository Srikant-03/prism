/**
 * DashboardPage — Root orchestrator for the AI Dashboard Builder.
 *
 * Manages: widgets[], layout, activeWidgetId, promptHistory[], globalFilters,
 * present mode, save/share/export flows.
 */
import React, { useState, useCallback, useRef } from 'react';
import { message } from 'antd';
import type { Widget, ChartConfig, FilterCondition, InterpretResponse } from '../../types/dashboard';
import DashboardHeader from './DashboardHeader';
import GlobalFilterBar from './GlobalFilterBar';
import DashboardCanvas from './DashboardCanvas';
import PromptBar from './PromptBar';
import PresentMode from './PresentMode';
import { fetchAuth, API_BASE } from '../../api/client';

interface PromptEntry {
    prompt: string;
    timestamp: number;
    chartTypeBefore?: string;
    chartTypeAfter?: string;
    widgetId?: string;
}

interface DashboardState {
    id: string;
    title: string;
    description: string;
    fileId: string;
    widgets: Widget[];
    globalFilters: FilterCondition[];
}

interface Props {
    fileId: string;
    columns?: string[];
}

const DashboardPage: React.FC<Props> = ({ fileId, columns = [] }) => {
    const [state, setState] = useState<DashboardState>({
        id: crypto.randomUUID?.() || Math.random().toString(36).slice(2),
        title: 'Untitled Dashboard',
        description: '',
        fileId,
        widgets: [],
        globalFilters: [],
    });

    const [promptLoading, setPromptLoading] = useState(false);
    const [promptHistory, setPromptHistory] = useState<PromptEntry[]>([]);
    const [suggestions, setSuggestions] = useState<string[]>([]);
    const [clarification, setClarification] = useState<string | undefined>();
    const [activeWidgetId, setActiveWidgetId] = useState<string | null>(null);
    const [presenting, setPresenting] = useState(false);
    const [saving, setSaving] = useState(false);
    const pendingActiveIdRef = useRef<string | null>(null);
    const canvasRef = useRef<HTMLDivElement>(null);

    // Track config history for undo
    const [configSnapshots, setConfigSnapshots] = useState<Widget[][]>([]);

    // ── Prompt Submission ──
    const handlePromptSubmit = useCallback(async (prompt: string) => {
        setPromptLoading(true);
        setClarification(undefined);

        try {
            const activeWidget = activeWidgetId
                ? state.widgets.find(w => w.id === activeWidgetId)
                : undefined;

            const body: any = {
                message: prompt,
                file_id: fileId,
                conversation_history: promptHistory.map(h => h.prompt),
            };
            if (activeWidget) {
                body.current_config = activeWidget.config;
            }

            const resp = await fetchAuth(`${API_BASE}/api/dashboard/interpret`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            if (!resp.ok) throw new Error('Interpretation failed');
            const result: InterpretResponse = await resp.json();

            if (result.clarification) {
                setClarification(result.clarification.clarification);
                setSuggestions(result.clarification.suggestions || []);
                return;
            }

            // Show error from the backend but still create the widget if config was returned
            if (!result.success && result.error && !result.config) {
                message.error(result.error);
                return;
            }

            if (result.config) {
                // Show a warning if SQL execution failed but config was interpreted
                if (!result.success && result.error) {
                    message.warning(`Chart created but query had issues: ${result.error}`);
                }

                pendingActiveIdRef.current = null;

                setState(prev => {
                    // Save snapshot of current widgets before mutating
                    setConfigSnapshots(snaps => [...snaps, prev.widgets.map(w => ({ ...w }))]);

                    const activeWidgetExists = activeWidgetId && prev.widgets.some(w => w.id === activeWidgetId);

                    if (activeWidgetExists) {
                        // Update existing widget
                        return {
                            ...prev,
                            widgets: prev.widgets.map(w =>
                                w.id === activeWidgetId
                                    ? {
                                        ...w,
                                        config: result.config!,
                                        data: result.data || w.data,
                                        sql: result.sql || w.sql,
                                        source_prompt: prompt,
                                        prompt_history: [...w.prompt_history, prompt],
                                        error: (!result.success && result.error) ? result.error : undefined,
                                    }
                                    : w
                            ),
                        };
                    } else {
                        // Create new widget
                        const newWidget: Widget = {
                            id: Math.random().toString(36).slice(2, 10),
                            config: result.config!,
                            source_prompt: prompt,
                            prompt_history: [prompt],
                            layout: {
                                x: (prev.widgets.length * 6) % 12,
                                y: Math.floor(prev.widgets.length / 2) * 4,
                                w: 6,
                                h: 4,
                            },
                            data: result.data || [],
                            sql: result.sql || undefined,
                            error: (!result.success && result.error) ? result.error : undefined,
                        };
                        pendingActiveIdRef.current = newWidget.id;
                        return { ...prev, widgets: [...prev.widgets, newWidget] };
                    }
                });

                // Update active widget ID for newly created widgets
                if (pendingActiveIdRef.current) {
                    setActiveWidgetId(pendingActiveIdRef.current);
                }

                const finalActiveId = pendingActiveIdRef.current || activeWidgetId;
                setPromptHistory(prev => [...prev, {
                    prompt,
                    timestamp: Date.now(),
                    chartTypeBefore: activeWidget?.config.chart_type,
                    chartTypeAfter: result.config!.chart_type,
                    widgetId: finalActiveId || undefined,
                }]);

                // Fetch suggestions for the new config
                fetchSuggestions(result.config);
            }
        } catch (err: any) {
            message.error(err.message || 'Failed to interpret prompt');
        } finally {
            setPromptLoading(false);
        }
    }, [fileId, state.widgets, activeWidgetId, promptHistory]);

    // ── Fetch Follow-up Suggestions ──
    const fetchSuggestions = async (config: ChartConfig) => {
        try {
            const resp = await fetchAuth(`${API_BASE}/api/dashboard/suggest`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, current_config: config }),
            });
            if (resp.ok) {
                const data = await resp.json();
                setSuggestions(data.suggestions || []);
            }
        } catch {
            setSuggestions([]);
        }
    };

    // ── Re-query a widget (e.g. when global filters change) ──
    const reQueryWidget = async (widget: Widget, filters: FilterCondition[]) => {
        try {
            const resp = await fetchAuth(`${API_BASE}/api/dashboard/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ config: widget.config, file_id: fileId, global_filters: filters }),
            });
            if (resp.ok) {
                const data = await resp.json();
                return data.data || [];
            }
            return widget.data || [];
        } catch {
            return widget.data || [];
        }
    };

    // ── Undo ──
    const handleUndo = useCallback(() => {
        if (configSnapshots.length === 0) return;
        const prev = configSnapshots[configSnapshots.length - 1];
        setState(s => ({ ...s, widgets: prev }));
        setConfigSnapshots(ss => ss.slice(0, -1));
        setPromptHistory(ph => ph.slice(0, -1));
    }, [configSnapshots]);

    // ── Layout Change ──
    const handleLayoutChange = useCallback((layout: any[]) => {
        setState(prev => ({
            ...prev,
            widgets: prev.widgets.map(w => {
                const l = layout.find(li => li.i === w.id);
                return l ? { ...w, layout: { x: l.x, y: l.y, w: l.w, h: l.h } } : w;
            }),
        }));
    }, []);

    // ── Widget Actions ──
    const handleEditWidget = (id: string) => setActiveWidgetId(id);

    const handleDuplicateWidget = (id: string) => {
        const w = state.widgets.find(wi => wi.id === id);
        if (!w) return;
        const dup: Widget = {
            ...w,
            id: Math.random().toString(36).slice(2, 10),
            layout: { ...w.layout, x: (w.layout.x + w.layout.w) % 12, y: w.layout.y },
            prompt_history: [...w.prompt_history],
        };
        setState(prev => ({ ...prev, widgets: [...prev.widgets, dup] }));
    };

    const handleDeleteWidget = (id: string) => {
        setState(prev => ({ ...prev, widgets: prev.widgets.filter(w => w.id !== id) }));
        if (activeWidgetId === id) setActiveWidgetId(null);
    };

    const handleFullscreen = (id: string) => {
        const idx = state.widgets.findIndex(w => w.id === id);
        if (idx >= 0) setPresenting(true);
    };

    const handleTitleChange = (id: string, title: string) => {
        setState(prev => ({
            ...prev,
            widgets: prev.widgets.map(w =>
                w.id === id ? { ...w, config: { ...w.config, title } } : w
            ),
        }));
    };

    // ── Global Filters ──
    const handleGlobalFiltersChange = async (filters: FilterCondition[]) => {
        setState(prev => ({ ...prev, globalFilters: filters }));
        // Re-query all widgets
        const updatedWidgets = await Promise.all(
            state.widgets.map(async w => ({
                ...w,
                data: await reQueryWidget(w, filters),
            }))
        );
        setState(prev => ({ ...prev, widgets: updatedWidgets }));
    };

    // ── Save ──
    const handleSave = async () => {
        setSaving(true);
        try {
            await fetchAuth(`${API_BASE}/api/dashboards`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    id: state.id,
                    title: state.title,
                    description: state.description,
                    file_id: fileId,
                    widgets: state.widgets.map(w => ({
                        id: w.id,
                        config: w.config,
                        source_prompt: w.source_prompt,
                        prompt_history: w.prompt_history,
                        layout: w.layout,
                    })),
                    global_filters: state.globalFilters,
                }),
            });
            // Also save to localStorage
            localStorage.setItem(`dashboard_${state.id}`, JSON.stringify(state));
            message.success('Dashboard saved!');
        } catch (err: any) {
            message.error('Failed to save dashboard');
        } finally {
            setSaving(false);
        }
    };

    // ── Share ──
    const handleShare = () => {
        const url = `${window.location.origin}/dashboard/share/${state.id}`;
        navigator.clipboard?.writeText(url);
        message.success('Share link copied to clipboard!');
    };

    // ── Export PNG ──
    const handleExportPng = async () => {
        if (!canvasRef.current) return;
        try {
            const html2canvas = (await import('html2canvas')).default;
            const canvas = await html2canvas(canvasRef.current, { scale: 2, backgroundColor: '#0f172a' });
            const link = document.createElement('a');
            link.download = `${state.title || 'dashboard'}.png`;
            link.href = canvas.toDataURL();
            link.click();
            message.success('PNG exported!');
        } catch {
            message.error('Export failed');
        }
    };

    // ── Export PDF ──
    const handleExportPdf = async () => {
        if (!canvasRef.current) return;
        try {
            const html2canvas = (await import('html2canvas')).default;
            const { jsPDF } = await import('jspdf');
            const canvas = await html2canvas(canvasRef.current, { scale: 2, backgroundColor: '#0f172a' });
            const imgData = canvas.toDataURL('image/png');
            const pdf = new jsPDF({ orientation: 'landscape', unit: 'px', format: [canvas.width, canvas.height] });
            pdf.addImage(imgData, 'PNG', 0, 0, canvas.width, canvas.height);
            pdf.save(`${state.title || 'dashboard'}.pdf`);
            message.success('PDF exported!');
        } catch {
            message.error('Export failed');
        }
    };

    // ── Add Widget (empty) ──
    const handleAddWidget = () => {
        setActiveWidgetId(null);
        message.info('Type a prompt below to create a new chart');
    };

    // Present mode
    if (presenting) {
        return (
            <PresentMode
                widgets={state.widgets}
                title={state.title}
                onExit={() => setPresenting(false)}
            />
        );
    }

    return (
        <div style={{
            minHeight: '100vh', background: '#0f172a',
            display: 'flex', flexDirection: 'column',
            paddingBottom: 120, /* space for prompt bar */
        }}>
            <DashboardHeader
                title={state.title}
                description={state.description}
                onTitleChange={t => setState(s => ({ ...s, title: t }))}
                onSave={handleSave}
                onShare={handleShare}
                onPresent={() => setPresenting(true)}
                onExportPng={handleExportPng}
                onExportPdf={handleExportPdf}
                onAddWidget={handleAddWidget}
                saving={saving}
            />

            <GlobalFilterBar
                filters={state.globalFilters}
                columns={columns}
                onFiltersChange={handleGlobalFiltersChange}
            />

            <div ref={canvasRef} style={{ flex: 1, padding: '16px 24px' }}>
                {state.widgets.length === 0 ? (
                    <div style={{
                        display: 'flex', flexDirection: 'column', alignItems: 'center',
                        justifyContent: 'center', height: 400, color: '#64748b',
                    }}>
                        <div style={{ fontSize: 60, marginBottom: 16 }}>📊</div>
                        <h3 style={{ color: '#94a3b8', marginBottom: 8 }}>No charts yet</h3>
                        <p>Type a prompt below to create your first visualization</p>
                        <p style={{ fontSize: 13, color: '#475569' }}>
                            Try: "Show monthly revenue as a bar chart" or "What's the split of customers by tier?"
                        </p>
                    </div>
                ) : (
                    <DashboardCanvas
                        widgets={state.widgets}
                        onLayoutChange={handleLayoutChange}
                        onEditWidget={handleEditWidget}
                        onDuplicateWidget={handleDuplicateWidget}
                        onDeleteWidget={handleDeleteWidget}
                        onFullscreenWidget={handleFullscreen}
                        onTitleChange={handleTitleChange}
                    />
                )}
            </div>

            <PromptBar
                onSubmit={handlePromptSubmit}
                loading={promptLoading}
                history={promptHistory}
                suggestions={suggestions}
                onUndo={handleUndo}
                canUndo={configSnapshots.length > 0}
                clarification={clarification}
            />
        </div>
    );
};

export default DashboardPage;
