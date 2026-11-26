/**
 * FormatPanel — Power BI-style Visual Formatting and Analytics panel.
 * Controls visual properties, styling, number formatting, reference lines, and forecast/anomaly toggles.
 */
import React from 'react';
import { Input, Switch, Select, Button, InputNumber, Divider } from 'antd';
import { PlusOutlined, DeleteOutlined, LineChartOutlined, AlertOutlined } from '@ant-design/icons';
import type { ChartConfig, ColorScheme, ReferenceLine } from '../../types/dashboard';

interface Props {
    config: ChartConfig;
    onChange: (newConfig: ChartConfig) => void;
    onClose?: () => void;
}

const COLOR_SCHEMES: { value: ColorScheme; label: string }[] = [
    { value: 'default', label: 'Indigo (Default)' },
    { value: 'ocean', label: 'Ocean Blue' },
    { value: 'sunset', label: 'Sunset Glow' },
    { value: 'forest', label: 'Emerald Forest' },
    { value: 'vibrant', label: 'Vibrant Multi' },
    { value: 'pastel', label: 'Soft Pastel' },
    { value: 'dark', label: 'Neon Dark' },
    { value: 'monochrome', label: 'Slate Monochrome' },
];

const FormatPanel: React.FC<Props> = ({ config, onChange, onClose }) => {
    const handleTitleChange = (val: string) => {
        onChange({ ...config, title: val });
    };

    const handleSubtitleChange = (val: string) => {
        onChange({ ...config, subtitle: val || undefined });
    };

    const handleColorSchemeChange = (scheme: ColorScheme) => {
        onChange({ ...config, color_scheme: scheme });
    };

    const handleToggle = (key: keyof ChartConfig, val: boolean) => {
        onChange({ ...config, [key]: val });
    };

    // Reference lines
    const handleAddRefLine = () => {
        const newLine: ReferenceLine = {
            type: 'average',
            label: 'Average',
            color: '#ef4444',
            line_style: 'dashed',
        };
        const currentLines = config.reference_lines || [];
        onChange({ ...config, reference_lines: [...currentLines, newLine] });
    };

    const handleUpdateRefLine = (idx: number, updates: Partial<ReferenceLine>) => {
        const currentLines = [...(config.reference_lines || [])];
        if (currentLines[idx]) {
            currentLines[idx] = { ...currentLines[idx], ...updates };
            onChange({ ...config, reference_lines: currentLines });
        }
    };

    const handleRemoveRefLine = (idx: number) => {
        const currentLines = (config.reference_lines || []).filter((_, i) => i !== idx);
        onChange({ ...config, reference_lines: currentLines });
    };

    // Forecast
    const handleForecastToggle = (enabled: boolean) => {
        onChange({
            ...config,
            forecast_config: {
                enabled,
                periods: config.forecast_config?.periods || 6,
                confidence_interval: config.forecast_config?.confidence_interval || 0.95,
            },
        });
    };

    // Anomaly Detection
    const handleAnomalyToggle = (enabled: boolean) => {
        onChange({
            ...config,
            anomaly_detection: {
                enabled,
                sensitivity: config.anomaly_detection?.sensitivity || 2.5,
            },
        });
    };

    return (
        <div
            style={{
                width: 300,
                background: '#0f172a',
                borderLeft: '1px solid rgba(99, 102, 241, 0.2)',
                display: 'flex',
                flexDirection: 'column',
                height: '100%',
                overflowY: 'auto',
                padding: '16px',
                color: '#e2e8f0',
            }}
        >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                <span style={{ fontWeight: 700, fontSize: 14, color: '#f1f5f9' }}>Format Visual</span>
                {onClose && (
                    <Button size="small" type="text" onClick={onClose} style={{ color: '#64748b' }}>
                        ✕
                    </Button>
                )}
            </div>

            {/* General Settings */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10, marginBottom: 14 }}>
                <span style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.5, color: '#64748b', fontWeight: 600 }}>
                    General
                </span>
                <div>
                    <span style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Title</span>
                    <Input
                        size="small"
                        value={config.title}
                        onChange={(e) => handleTitleChange(e.target.value)}
                        placeholder="Chart Title"
                    />
                </div>
                <div>
                    <span style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Subtitle</span>
                    <Input
                        size="small"
                        value={config.subtitle || ''}
                        onChange={(e) => handleSubtitleChange(e.target.value)}
                        placeholder="Optional subtitle"
                    />
                </div>
            </div>

            <Divider style={{ borderColor: 'rgba(99, 102, 241, 0.15)', margin: '8px 0 12px' }} />

            {/* Visual Styling */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10, marginBottom: 14 }}>
                <span style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.5, color: '#64748b', fontWeight: 600 }}>
                    Visual Styling
                </span>

                <div>
                    <span style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Color Palette</span>
                    <Select
                        size="small"
                        value={config.color_scheme || 'default'}
                        onChange={handleColorSchemeChange}
                        style={{ width: '100%' }}
                        options={COLOR_SCHEMES}
                    />
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 12, color: '#cbd5e1' }}>Data Labels</span>
                    <Switch
                        size="small"
                        checked={config.show_values}
                        onChange={(val) => handleToggle('show_values', val)}
                    />
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 12, color: '#cbd5e1' }}>Legend</span>
                    <Switch
                        size="small"
                        checked={config.show_legend}
                        onChange={(val) => handleToggle('show_legend', val)}
                    />
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 12, color: '#cbd5e1' }}>Gridlines</span>
                    <Switch
                        size="small"
                        checked={config.show_grid}
                        onChange={(val) => handleToggle('show_grid', val)}
                    />
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 12, color: '#cbd5e1' }}>Stacked</span>
                    <Switch
                        size="small"
                        checked={config.stacked}
                        onChange={(val) => handleToggle('stacked', val)}
                    />
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 12, color: '#cbd5e1' }}>Trend Line</span>
                    <Switch
                        size="small"
                        checked={config.trend_line}
                        onChange={(val) => handleToggle('trend_line', val)}
                    />
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 12, color: '#cbd5e1' }}>Smooth Curves</span>
                    <Switch
                        size="small"
                        checked={config.smooth}
                        onChange={(val) => handleToggle('smooth', val)}
                    />
                </div>
            </div>

            <Divider style={{ borderColor: 'rgba(99, 102, 241, 0.15)', margin: '8px 0 12px' }} />

            {/* Reference Lines */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 14 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.5, color: '#64748b', fontWeight: 600 }}>
                        Reference Lines
                    </span>
                    <Button
                        size="small"
                        type="dashed"
                        icon={<PlusOutlined />}
                        onClick={handleAddRefLine}
                        style={{ fontSize: 11, height: 22 }}
                    >
                        Add
                    </Button>
                </div>

                {(config.reference_lines && config.reference_lines.length > 0) ? (
                    config.reference_lines.map((line, idx) => (
                        <div
                            key={idx}
                            style={{
                                background: '#1e293b',
                                padding: 8,
                                borderRadius: 6,
                                border: '1px solid #334155',
                                display: 'flex',
                                flexDirection: 'column',
                                gap: 6,
                            }}
                        >
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <Select
                                    size="small"
                                    value={line.type}
                                    onChange={(t) => handleUpdateRefLine(idx, { type: t, label: t.toUpperCase() })}
                                    style={{ width: 110 }}
                                    options={[
                                        { value: 'average', label: 'Average' },
                                        { value: 'median', label: 'Median' },
                                        { value: 'constant', label: 'Constant' },
                                        { value: 'min', label: 'Min' },
                                        { value: 'max', label: 'Max' },
                                    ]}
                                />
                                <Button
                                    size="small"
                                    type="text"
                                    danger
                                    icon={<DeleteOutlined />}
                                    onClick={() => handleRemoveRefLine(idx)}
                                />
                            </div>

                            {line.type === 'constant' && (
                                <InputNumber
                                    size="small"
                                    value={line.value}
                                    placeholder="Value"
                                    onChange={(v) => handleUpdateRefLine(idx, { value: v ?? undefined })}
                                    style={{ width: '100%' }}
                                />
                            )}
                        </div>
                    ))
                ) : (
                    <span style={{ fontSize: 11, color: '#64748b', fontStyle: 'italic' }}>No reference lines added</span>
                )}
            </div>

            <Divider style={{ borderColor: 'rgba(99, 102, 241, 0.15)', margin: '8px 0 12px' }} />

            {/* AI Analytics: Forecast & Anomaly */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <span style={{ fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.5, color: '#64748b', fontWeight: 600 }}>
                    AI Analytics
                </span>

                {/* Forecast */}
                <div style={{ background: '#1e293b', padding: 8, borderRadius: 6, border: '1px solid #334155' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                        <span style={{ fontSize: 12, fontWeight: 600, color: '#818cf8', display: 'flex', alignItems: 'center', gap: 4 }}>
                            <LineChartOutlined /> Forecast
                        </span>
                        <Switch
                            size="small"
                            checked={Boolean(config.forecast_config?.enabled)}
                            onChange={handleForecastToggle}
                        />
                    </div>
                    {config.forecast_config?.enabled && (
                        <div style={{ marginTop: 6, display: 'flex', flexDirection: 'column', gap: 4 }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <span style={{ fontSize: 11, color: '#94a3b8' }}>Periods</span>
                                <InputNumber
                                    size="small"
                                    min={1}
                                    max={24}
                                    value={config.forecast_config.periods || 6}
                                    onChange={(p) => onChange({ ...config, forecast_config: { ...config.forecast_config!, periods: p || 6 } })}
                                    style={{ width: 64 }}
                                />
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <span style={{ fontSize: 11, color: '#94a3b8' }}>Confidence</span>
                                <Select
                                    size="small"
                                    value={config.forecast_config.confidence_interval || 0.95}
                                    onChange={(ci) => onChange({ ...config, forecast_config: { ...config.forecast_config!, confidence_interval: ci } })}
                                    style={{ width: 80 }}
                                    options={[
                                        { value: 0.80, label: '80%' },
                                        { value: 0.95, label: '95%' },
                                    ]}
                                />
                            </div>
                        </div>
                    )}
                </div>

                {/* Anomaly Detection */}
                <div style={{ background: '#1e293b', padding: 8, borderRadius: 6, border: '1px solid #334155' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ fontSize: 12, fontWeight: 600, color: '#f59e0b', display: 'flex', alignItems: 'center', gap: 4 }}>
                            <AlertOutlined /> Anomaly Detection
                        </span>
                        <Switch
                            size="small"
                            checked={Boolean(config.anomaly_detection?.enabled)}
                            onChange={handleAnomalyToggle}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
};

export default FormatPanel;
