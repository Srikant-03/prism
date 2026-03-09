/**
 * WidgetContainer — Wraps each ChartWidget with title bar, source prompt, and action toolbar.
 */
import React, { useState } from 'react';
import { Button, Tooltip, Input } from 'antd';
import {
    EditOutlined, FullscreenOutlined, CopyOutlined,
    DeleteOutlined, DragOutlined,
} from '@ant-design/icons';
import { Widget } from '../../types/dashboard';
import ChartWidget from './ChartWidget';

interface Props {
    widget: Widget;
    onEdit?: (id: string) => void;
    onDuplicate?: (id: string) => void;
    onDelete?: (id: string) => void;
    onFullscreen?: (id: string) => void;
    onTitleChange?: (id: string, title: string) => void;
}

const WidgetContainer: React.FC<Props> = ({
    widget, onEdit, onDuplicate, onDelete, onFullscreen, onTitleChange,
}) => {
    const [editingTitle, setEditingTitle] = useState(false);
    const [titleDraft, setTitleDraft] = useState(widget.config.title);

    return (
        <div style={{
            height: '100%', display: 'flex', flexDirection: 'column',
            background: 'rgba(15,23,42,0.8)', borderRadius: 12,
            border: '1px solid rgba(99,102,241,0.15)',
            backdropFilter: 'blur(12px)', overflow: 'hidden',
        }}>
            {/* Title Bar */}
            <div
                className="widget-drag-handle"
                style={{
                    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    padding: '8px 12px', borderBottom: '1px solid rgba(99,102,241,0.1)',
                    cursor: 'grab', minHeight: 42,
                }}
            >
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 1, minWidth: 0 }}>
                    <DragOutlined style={{ color: '#475569', fontSize: 12 }} />
                    {editingTitle ? (
                        <Input
                            size="small"
                            value={titleDraft}
                            onChange={e => setTitleDraft(e.target.value)}
                            onPressEnter={() => {
                                onTitleChange?.(widget.id, titleDraft);
                                setEditingTitle(false);
                            }}
                            onBlur={() => {
                                onTitleChange?.(widget.id, titleDraft);
                                setEditingTitle(false);
                            }}
                            autoFocus
                            style={{ background: 'transparent', border: 'none', color: '#e2e8f0', padding: 0 }}
                        />
                    ) : (
                        <span
                            onDoubleClick={() => setEditingTitle(true)}
                            style={{
                                color: '#e2e8f0', fontWeight: 600, fontSize: 13,
                                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                            }}
                        >
                            {widget.config.title || 'Untitled Chart'}
                        </span>
                    )}
                </div>
                <div style={{ display: 'flex', gap: 2 }}>
                    <Tooltip title="Edit prompt"><Button type="text" size="small" icon={<EditOutlined />} onClick={() => onEdit?.(widget.id)} style={{ color: '#64748b' }} /></Tooltip>
                    <Tooltip title="Fullscreen"><Button type="text" size="small" icon={<FullscreenOutlined />} onClick={() => onFullscreen?.(widget.id)} style={{ color: '#64748b' }} /></Tooltip>
                    <Tooltip title="Duplicate"><Button type="text" size="small" icon={<CopyOutlined />} onClick={() => onDuplicate?.(widget.id)} style={{ color: '#64748b' }} /></Tooltip>
                    <Tooltip title="Delete"><Button type="text" size="small" icon={<DeleteOutlined />} onClick={() => onDelete?.(widget.id)} style={{ color: '#ef4444' }} /></Tooltip>
                </div>
            </div>

            {/* Source Prompt Subtitle */}
            {widget.source_prompt && (
                <div style={{
                    padding: '4px 12px', color: '#64748b', fontSize: 11,
                    borderBottom: '1px solid rgba(99,102,241,0.05)',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}>
                    💬 {widget.source_prompt}
                </div>
            )}

            {/* Chart Area */}
            <div style={{ flex: 1, padding: 8, minHeight: 0 }}>
                <ChartWidget
                    config={widget.config}
                    data={widget.data || []}
                    loading={widget.loading}
                    error={widget.error}
                />
            </div>
        </div>
    );
};

export default WidgetContainer;
