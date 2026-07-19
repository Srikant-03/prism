/**
 * StoryBuilder — Production-grade executive data presentation deck builder & presentation viewer.
 */

import React, { useState, useCallback, useEffect } from 'react';
import { Button, Space, Tag, Input, Select, Modal, Empty, Tooltip, Dropdown, message, Row, Col } from 'antd';
import {
    ReadOutlined, DeleteOutlined, ArrowUpOutlined,
    ArrowDownOutlined, PlayCircleOutlined, DownloadOutlined,
    EditOutlined, CopyOutlined, FilePptOutlined, FileTextOutlined, FilePdfOutlined,
    ThunderboltOutlined
} from '@ant-design/icons';

import { fetchAuth, API_BASE } from '../../api/client';

interface Slide {
    id: string;
    type: 'title' | 'insight' | 'chart' | 'text' | 'kpi' | 'comparison';
    category?: string;
    title: string;
    subtitle?: string;
    content: string;
    bullets?: string[];
    kpiValue?: string | number;
    kpiLabel?: string;
    kpiSubtext?: string;
    badge?: string;
    tags?: string[];
}

interface Props {
    fileId: string;
    insights?: any;
}

const SLIDE_TYPES = [
    { value: 'title', label: '📌 Title Slide', icon: '📌' },
    { value: 'kpi', label: '🎯 Data Health KPI', icon: '🎯' },
    { value: 'insight', label: '💡 Executive Insight', icon: '💡' },
    { value: 'chart', label: '📊 Distribution Chart', icon: '📊' },
    { value: 'comparison', label: '⚖️ Comparison', icon: '⚖️' },
    { value: 'text', label: '📝 Strategic Text', icon: '📝' },
];

const StoryBuilder: React.FC<Props> = ({ fileId }) => {
    const [slides, setSlides] = useState<Slide[]>([{
        id: '0',
        type: 'title',
        category: 'Executive Overview',
        title: 'Executive Intelligence Deck',
        subtitle: 'Dataset Analysis & Strategic Observations',
        content: 'Loading automated story deck...',
        bullets: ['Click "Auto-Generate Executive Deck" to build a 10-slide narrative.'],
        badge: 'EXECUTIVE OVERVIEW',
        tags: ['Dataset', 'Executive Briefing']
    }]);
    const [presenting, setPresenting] = useState(false);
    const [currentSlide, setCurrentSlide] = useState(0);
    const [editModal, setEditModal] = useState<{ visible: boolean; slide?: Slide }>({ visible: false });
    const [generating, setGenerating] = useState(false);
    const [exporting, setExporting] = useState(false);
    const [metaInfo, setMetaInfo] = useState<{ domain?: string; quality_score?: number; quality_grade?: string }>({});

    // Auto-generate story on mount if fileId present
    const autoGenerate = useCallback(async () => {
        setGenerating(true);
        try {
            const res = await fetchAuth(`${API_BASE}/api/story/generate`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId }),
            });
            const data = await res.json();
            if (data.slides && data.slides.length > 0) {
                setSlides(data.slides);
                setMetaInfo({
                    domain: data.domain,
                    quality_score: data.quality_score,
                    quality_grade: data.quality_grade,
                });
                message.success('Auto-generated 10-slide executive story deck!');
            }
        } catch (e: any) {
            console.error('Auto-generate error:', e);
            message.error('Failed to generate story deck');
        } finally {
            setGenerating(false);
        }
    }, [fileId]);

    useEffect(() => {
        if (fileId) {
            autoGenerate();
        }
    }, [fileId]);

    const addSlide = useCallback((type: string) => {
        const typeInfo = SLIDE_TYPES.find(t => t.value === type);
        setSlides(prev => [...prev, {
            id: Date.now().toString(),
            type: type as any,
            category: 'Custom Slide',
            title: `New ${typeInfo?.label || 'Slide'} ${prev.length + 1}`,
            subtitle: 'Slide Subtitle / Focus Area',
            content: 'Add your strategic bullet points here...',
            bullets: ['Key takeaway point 1', 'Key takeaway point 2'],
            badge: 'CUSTOM',
            tags: ['Slide']
        }]);
    }, []);

    const updateSlide = useCallback((id: string, updates: Partial<Slide>) => {
        setSlides(prev => prev.map(s => s.id === id ? { ...s, ...updates } : s));
    }, []);

    const removeSlide = useCallback((id: string) => {
        setSlides(prev => prev.filter(s => s.id !== id));
    }, []);

    const moveSlide = useCallback((id: string, direction: 'up' | 'down') => {
        setSlides(prev => {
            const idx = prev.findIndex(s => s.id === id);
            if (idx < 0) return prev;
            const newIdx = direction === 'up' ? idx - 1 : idx + 1;
            if (newIdx < 0 || newIdx >= prev.length) return prev;
            const re = [...prev];
            [re[idx], re[newIdx]] = [re[newIdx], re[idx]];
            return re;
        });
    }, []);

    const exportStory = useCallback(async (format: 'html' | 'pptx' | 'pdf') => {
        setExporting(true);
        try {
            const res = await fetchAuth(`${API_BASE}/api/story/export`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, format }),
            });
            if (!res.ok) throw new Error('Export request failed');
            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `data_story_${fileId}.${format}`;
            a.click();
            URL.revokeObjectURL(url);
            message.success(`Successfully exported story deck as .${format.toUpperCase()}`);
        } catch (e: any) {
            console.error('Export error:', e);
            message.error(`Export failed: ${e.message}`);
        } finally {
            setExporting(false);
        }
    }, [fileId]);

    const copyMarkdown = useCallback(() => {
        const text = slides.map((s, idx) => (
            `## Slide ${idx + 1}: ${s.title}\n` +
            `*${s.subtitle || s.category || ''}*\n\n` +
            (s.bullets && s.bullets.length > 0
                ? s.bullets.map(b => `- ${b}`).join('\n')
                : s.content) +
            '\n\n---\n'
        )).join('\n');

        navigator.clipboard.writeText(text);
        message.success('Copied full story deck as Markdown!');
    }, [slides]);

    // Keyboard controls in presentation mode
    useEffect(() => {
        if (!presenting) return;
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'ArrowRight' || e.key === ' ') {
                setCurrentSlide(prev => Math.min(prev + 1, slides.length - 1));
            } else if (e.key === 'ArrowLeft') {
                setCurrentSlide(prev => Math.max(prev - 1, 0));
            } else if (e.key === 'Escape') {
                setPresenting(false);
            }
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [presenting, slides.length]);

    const exportMenuItems = [
        {
            key: 'html',
            label: 'Interactive HTML Deck (.html)',
            icon: <FileTextOutlined style={{ color: '#818cf8' }} />,
            onClick: () => exportStory('html')
        },
        {
            key: 'pptx',
            label: 'PowerPoint Presentation (.pptx)',
            icon: <FilePptOutlined style={{ color: '#f43f5e' }} />,
            onClick: () => exportStory('pptx')
        },
        {
            key: 'pdf',
            label: 'PDF Report Deck (.pdf)',
            icon: <FilePdfOutlined style={{ color: '#ef4444' }} />,
            onClick: () => exportStory('pdf')
        },
        {
            type: 'divider' as const
        },
        {
            key: 'md',
            label: 'Copy as Markdown Text',
            icon: <CopyOutlined style={{ color: '#10b981' }} />,
            onClick: copyMarkdown
        }
    ];

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {/* Top Toolbar */}
            <div className="glass-panel" style={{
                padding: '14px 20px',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                flexWrap: 'wrap',
                gap: 12,
                borderRadius: 16,
                border: '1px solid rgba(255, 255, 255, 0.08)'
            }}>
                <Space size="middle">
                    <ReadOutlined style={{ color: '#818cf8', fontSize: 22 }} />
                    <div>
                        <div style={{ fontWeight: 700, fontSize: 16, color: '#f8fafc' }}>
                            Executive Storytelling Engine
                        </div>
                        <div style={{ fontSize: 12, color: '#94a3b8' }}>
                            Auto-compiled 10-slide executive deck from data profiling & insights
                        </div>
                    </div>
                    {metaInfo.domain && (
                        <Tag color="purple" style={{ fontSize: 12, padding: '2px 10px', borderRadius: 12 }}>
                            {metaInfo.domain}
                        </Tag>
                    )}
                    {metaInfo.quality_grade && (
                        <Tag color={metaInfo.quality_grade === 'A' ? 'green' : 'gold'} style={{ fontSize: 12, padding: '2px 10px', borderRadius: 12 }}>
                            Grade {metaInfo.quality_grade} ({metaInfo.quality_score}/100)
                        </Tag>
                    )}
                </Space>

                <Space size="small">
                    <Button
                        type="primary"
                        icon={<ThunderboltOutlined />}
                        onClick={autoGenerate}
                        loading={generating}
                        style={{ background: 'linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)', border: 'none' }}
                    >
                        Auto-Generate Deck
                    </Button>

                    <Select
                        placeholder="+ Add Slide"
                        style={{ width: 150 }}
                        onSelect={(v) => { if (v) addSlide(String(v)); }}
                        value={undefined}
                    >
                        {SLIDE_TYPES.map(t => (
                            <Select.Option key={t.value} value={t.value}>
                                {t.label}
                            </Select.Option>
                        ))}
                    </Select>

                    <Button
                        icon={<PlayCircleOutlined />}
                        onClick={() => { setPresenting(true); setCurrentSlide(0); }}
                        style={{ background: '#1e293b', borderColor: '#334155', color: '#f8fafc' }}
                    >
                        Present Deck
                    </Button>

                    <Dropdown menu={{ items: exportMenuItems }} placement="bottomRight">
                        <Button icon={<DownloadOutlined />} loading={exporting}>
                            Export Deck
                        </Button>
                    </Dropdown>
                </Space>
            </div>

            {/* Slide Cards Grid */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                {slides.map((slide, i) => (
                    <div
                        key={slide.id}
                        className="glass-panel"
                        style={{
                            padding: '16px 20px',
                            display: 'flex',
                            alignItems: 'center',
                            gap: 16,
                            borderRadius: 14,
                            borderLeft: `4px solid ${slide.type === 'title' ? '#818cf8' : slide.type === 'kpi' ? '#10b981' : '#38bdf8'}`,
                            transition: 'all 0.2s ease',
                            background: 'rgba(15, 23, 42, 0.6)'
                        }}
                    >
                        {/* Slide Number Badge */}
                        <div style={{
                            width: 36,
                            height: 36,
                            borderRadius: 10,
                            background: 'rgba(99, 102, 241, 0.15)',
                            color: '#818cf8',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            fontWeight: 800,
                            fontSize: 14
                        }}>
                            {String(i + 1).padStart(2, '0')}
                        </div>

                        {/* Slide Info */}
                        <div style={{ flex: 1 }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 4 }}>
                                <span style={{ fontWeight: 700, fontSize: 15, color: '#f8fafc' }}>
                                    {slide.title}
                                </span>
                                {slide.badge && (
                                    <Tag color="cyan" style={{ fontSize: 10, borderRadius: 10, margin: 0 }}>
                                        {slide.badge}
                                    </Tag>
                                )}
                                {slide.category && (
                                    <span style={{ fontSize: 11, color: '#94a3b8', fontStyle: 'italic' }}>
                                        — {slide.category}
                                    </span>
                                )}
                            </div>

                            {/* Subtitle / Bullets preview */}
                            <div style={{ fontSize: 12, color: '#cbd5e1', lineHeight: 1.5 }}>
                                {slide.subtitle && (
                                    <div style={{ color: '#94a3b8', fontWeight: 500, marginBottom: 2 }}>
                                        {slide.subtitle}
                                    </div>
                                )}
                                {slide.bullets && slide.bullets.length > 0 ? (
                                    <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                                        {slide.bullets.slice(0, 2).map((b, bIdx) => (
                                            <div key={bIdx} style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 750 }}>
                                                <span style={{ color: '#818cf8', marginRight: 6 }}>•</span>
                                                {b}
                                            </div>
                                        ))}
                                        {slide.bullets.length > 2 && (
                                            <div style={{ fontSize: 10, color: '#64748b' }}>
                                                +{slide.bullets.length - 2} more bullet points
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <div style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 750 }}>
                                        {slide.content || 'No content added yet'}
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* Action Controls */}
                        <Space size="small">
                            <Tooltip title="Move Up">
                                <Button size="small" icon={<ArrowUpOutlined />} disabled={i === 0} onClick={() => moveSlide(slide.id, 'up')} />
                            </Tooltip>
                            <Tooltip title="Move Down">
                                <Button size="small" icon={<ArrowDownOutlined />} disabled={i === slides.length - 1} onClick={() => moveSlide(slide.id, 'down')} />
                            </Tooltip>
                            <Tooltip title="Edit Slide">
                                <Button size="small" icon={<EditOutlined />} onClick={() => setEditModal({ visible: true, slide })} />
                            </Tooltip>
                            <Tooltip title="Delete">
                                <Button size="small" icon={<DeleteOutlined />} danger onClick={() => removeSlide(slide.id)} />
                            </Tooltip>
                        </Space>
                    </div>
                ))}
            </div>

            {slides.length === 0 && (
                <Empty description="No slides in story deck. Click 'Auto-Generate Deck' to construct a 10-slide narrative." />
            )}

            {/* Edit Slide Modal */}
            <Modal
                title={`Edit Slide ${editModal.slide ? slides.findIndex(s => s.id === editModal.slide?.id) + 1 : ''} — ${editModal.slide?.type.toUpperCase()}`}
                open={editModal.visible}
                onCancel={() => setEditModal({ visible: false })}
                onOk={() => {
                    if (editModal.slide) {
                        const rawBullets = editModal.slide.content
                            ? editModal.slide.content.split('\n').filter(l => l.trim().length > 0).map(l => l.replace(/^•\s*/, ''))
                            : editModal.slide.bullets;
                        updateSlide(editModal.slide.id, {
                            ...editModal.slide,
                            bullets: rawBullets
                        });
                    }
                    setEditModal({ visible: false });
                }}
                width={650}
            >
                {editModal.slide && (
                    <Space orientation="vertical" size="medium" style={{ width: '100%' }}>
                        <div>
                            <div style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', marginBottom: 4 }}>Slide Category</div>
                            <Input
                                value={editModal.slide.category || ''}
                                onChange={e => setEditModal(prev => ({
                                    ...prev,
                                    slide: prev.slide ? { ...prev.slide, category: e.target.value } : undefined
                                }))}
                                placeholder="Category (e.g. Executive Overview)"
                            />
                        </div>

                        <div>
                            <div style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', marginBottom: 4 }}>Slide Headline / Title</div>
                            <Input
                                value={editModal.slide.title}
                                onChange={e => setEditModal(prev => ({
                                    ...prev,
                                    slide: prev.slide ? { ...prev.slide, title: e.target.value } : undefined
                                }))}
                                placeholder="Slide title"
                            />
                        </div>

                        <div>
                            <div style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', marginBottom: 4 }}>Subtitle / Focal Point</div>
                            <Input
                                value={editModal.slide.subtitle || ''}
                                onChange={e => setEditModal(prev => ({
                                    ...prev,
                                    slide: prev.slide ? { ...prev.slide, subtitle: e.target.value } : undefined
                                }))}
                                placeholder="Subtitle or context description"
                            />
                        </div>

                        {editModal.slide.type === 'kpi' && (
                            <Row gutter={12}>
                                <Col span={12}>
                                    <div style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', marginBottom: 4 }}>KPI Value</div>
                                    <Input
                                        value={String(editModal.slide.kpiValue || '')}
                                        onChange={e => setEditModal(prev => ({
                                            ...prev,
                                            slide: prev.slide ? { ...prev.slide, kpiValue: e.target.value } : undefined
                                        }))}
                                        placeholder="e.g. 95.2/100"
                                    />
                                </Col>
                                <Col span={12}>
                                    <div style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', marginBottom: 4 }}>KPI Label</div>
                                    <Input
                                        value={editModal.slide.kpiLabel || ''}
                                        onChange={e => setEditModal(prev => ({
                                            ...prev,
                                            slide: prev.slide ? { ...prev.slide, kpiLabel: e.target.value } : undefined
                                        }))}
                                        placeholder="e.g. Data Quality Index"
                                    />
                                </Col>
                            </Row>
                        )}

                        <div>
                            <div style={{ fontSize: 12, fontWeight: 600, color: '#94a3b8', marginBottom: 4 }}>
                                Bullet Points (Enter 1 point per line)
                            </div>
                            <Input.TextArea
                                value={
                                    editModal.slide.bullets && editModal.slide.bullets.length > 0
                                        ? editModal.slide.bullets.join('\n')
                                        : editModal.slide.content
                                }
                                onChange={e => {
                                    const val = e.target.value;
                                    const bulletsArr = val.split('\n').filter(l => l.trim().length > 0).map(l => l.replace(/^•\s*/, ''));
                                    setEditModal(prev => ({
                                        ...prev,
                                        slide: prev.slide ? { ...prev.slide, content: val, bullets: bulletsArr } : undefined
                                    }));
                                }}
                                rows={6}
                                placeholder="Bullet point 1&#10;Bullet point 2&#10;Bullet point 3"
                            />
                        </div>
                    </Space>
                )}
            </Modal>

            {/* Cinema Presentation Modal */}
            <Modal
                open={presenting}
                onCancel={() => setPresenting(false)}
                width="92vw"
                footer={null}
                styles={{
                    body: {
                        background: '#090d16',
                        padding: 0,
                        height: '80vh',
                        display: 'flex',
                        flexDirection: 'column',
                        borderRadius: 16,
                        overflow: 'hidden',
                        position: 'relative'
                    }
                }}
            >
                {/* Progress Bar */}
                <div style={{
                    height: 4,
                    background: '#6366f1',
                    width: `${((currentSlide + 1) / slides.length) * 100}%`,
                    transition: 'width 0.3s ease'
                }} />

                {/* Presentation Header */}
                <div style={{
                    padding: '16px 32px',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    borderBottom: '1px solid rgba(255, 255, 255, 0.08)',
                    background: 'rgba(15, 23, 42, 0.8)'
                }}>
                    <Space>
                        <ThunderboltOutlined style={{ color: '#818cf8', fontSize: 18 }} />
                        <span style={{ fontWeight: 700, fontSize: 14, color: '#f8fafc', letterSpacing: 0.5 }}>
                            EXECUTIVE STORY PRESENTATION
                        </span>
                        <Tag color="purple">{slides[currentSlide]?.badge || 'EXECUTIVE'}</Tag>
                    </Space>

                    <div style={{ fontSize: 13, fontWeight: 600, color: '#94a3b8' }}>
                        Slide {currentSlide + 1} of {slides.length}
                    </div>
                </div>

                {/* Main Slide Viewer */}
                <div style={{
                    flex: 1,
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                    padding: '40px 60px',
                    background: '#0f172a'
                }}>
                    {slides[currentSlide] && (
                        <div style={{
                            width: '100%',
                            maxWidth: 900,
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 24,
                            animation: 'fadeIn 0.3s ease-in-out'
                        }}>
                            <div>
                                <div style={{ fontSize: 12, fontWeight: 700, color: '#818cf8', letterSpacing: 1.5, textTransform: 'uppercase', marginBottom: 8 }}>
                                    {slides[currentSlide].category || 'Executive Analysis'}
                                </div>
                                <h1 style={{ fontSize: 32, fontWeight: 800, color: '#ffffff', lineHeight: 1.2, marginBottom: 8 }}>
                                    {slides[currentSlide].title}
                                </h1>
                                {slides[currentSlide].subtitle && (
                                    <div style={{ fontSize: 16, color: '#94a3b8' }}>
                                        {slides[currentSlide].subtitle}
                                    </div>
                                )}
                            </div>

                            {/* KPI Layout */}
                            {slides[currentSlide].type === 'kpi' ? (
                                <Row gutter={32} align="middle">
                                    <Col span={9}>
                                        <div style={{
                                            background: 'rgba(99, 102, 241, 0.12)',
                                            border: '1px solid rgba(99, 102, 241, 0.3)',
                                            borderRadius: 20,
                                            padding: 32,
                                            textAlign: 'center'
                                        }}>
                                            <div style={{ fontSize: 52, fontWeight: 900, color: '#818cf8' }}>
                                                {slides[currentSlide].kpiValue || '100/100'}
                                            </div>
                                            <div style={{ fontSize: 13, color: '#cbd5e1', marginTop: 8, textTransform: 'uppercase', letterSpacing: 1 }}>
                                                {slides[currentSlide].kpiLabel || 'Quality Score'}
                                            </div>
                                            {slides[currentSlide].kpiSubtext && (
                                                <div style={{ fontSize: 14, color: '#10b981', fontWeight: 700, marginTop: 4 }}>
                                                    {slides[currentSlide].kpiSubtext}
                                                </div>
                                            )}
                                        </div>
                                    </Col>
                                    <Col span={15}>
                                        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                                            {(slides[currentSlide].bullets || []).map((b, bIdx) => (
                                                <div key={bIdx} style={{
                                                    display: 'flex',
                                                    alignItems: 'flex-start',
                                                    gap: 12,
                                                    fontSize: 16,
                                                    color: '#e2e8f0',
                                                    background: 'rgba(255, 255, 255, 0.03)',
                                                    padding: '12px 16px',
                                                    borderRadius: 12,
                                                    border: '1px solid rgba(255, 255, 255, 0.05)'
                                                }}>
                                                    <span style={{ color: '#818cf8', fontWeight: 'bold' }}>•</span>
                                                    <span>{b}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </Col>
                                </Row>
                            ) : (
                                /* Standard Bullets Layout */
                                <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
                                    {(slides[currentSlide].bullets && slides[currentSlide].bullets!.length > 0
                                        ? slides[currentSlide].bullets!
                                        : (slides[currentSlide].content ? slides[currentSlide].content.split('\n') : [])
                                    ).map((b, bIdx) => (
                                        b.trim() ? (
                                            <div key={bIdx} style={{
                                                display: 'flex',
                                                alignItems: 'flex-start',
                                                gap: 14,
                                                fontSize: 17,
                                                lineHeight: 1.6,
                                                color: '#e2e8f0',
                                                background: 'rgba(255, 255, 255, 0.03)',
                                                padding: '14px 18px',
                                                borderRadius: 14,
                                                border: '1px solid rgba(255, 255, 255, 0.06)'
                                            }}>
                                                <span style={{ color: '#818cf8', fontWeight: 'bold', fontSize: 18 }}>•</span>
                                                <span>{b.replace(/^•\s*/, '')}</span>
                                            </div>
                                        ) : null
                                    ))}
                                </div>
                            )}
                        </div>
                    )}
                </div>

                {/* Footer Controls */}
                <div style={{
                    padding: '16px 32px',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    borderTop: '1px solid rgba(255, 255, 255, 0.08)',
                    background: 'rgba(15, 23, 42, 0.9)'
                }}>
                    <Button
                        disabled={currentSlide === 0}
                        onClick={() => setCurrentSlide(p => p - 1)}
                        style={{ background: '#1e293b', color: '#fff', borderColor: '#334155' }}
                    >
                        ← Previous
                    </Button>

                    <span style={{ fontSize: 12, color: '#64748b' }}>
                        Use Left / Right Arrow Keys to Navigate Deck
                    </span>

                    <Button
                        type="primary"
                        disabled={currentSlide >= slides.length - 1}
                        onClick={() => setCurrentSlide(p => p + 1)}
                        style={{ background: '#6366f1' }}
                    >
                        Next →
                    </Button>
                </div>
            </Modal>
        </div>
    );
};

export default StoryBuilder;


