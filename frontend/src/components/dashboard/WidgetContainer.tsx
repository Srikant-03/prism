/**
 * WidgetContainer — Wraps each ChartWidget with title bar, source prompt, and Power BI action toolbar.
 * Supports drill-up/drill-down, formatting/field well trigger, AI explain trigger, and locking.
 */
import React, { useState } from 'react';
import { Button, Tooltip, Input, Space, message } from 'antd';
import {
    EditOutlined, FullscreenOutlined, CopyOutlined,
    DeleteOutlined, DragOutlined, SettingOutlined,
    ArrowUpOutlined, ArrowDownOutlined, RobotOutlined,
    LockOutlined, DownloadOutlined,
} from '@ant-design/icons';
import type { Widget } from '../../types/dashboard';
import ChartWidget from './ChartWidget';

interface Props {
    widget: Widget;
    isEditing?: boolean;
    isSelected?: boolean;
    onEdit?: (id: string) => void;
    onSelect?: (id: string) => void;
    onDuplicate?: (id: string) => void;
    onDelete?: (id: string) => void;
    onFullscreen?: (id: string) => void;
    onTitleChange?: (id: string, title: string) => void;
    onFormat?: (id: string) => void;
    onDrillUp?: (id: string) => void;
    onDrillDown?: (id: string) => void;
    onExplain?: (widget: Widget) => void;
    onCrossFilter?: (column: string, value: any) => void;
}

const WidgetContainer: React.FC<Props> = ({
    widget,
    isEditing,
    isSelected,
    onEdit,
    onSelect,
    onDuplicate,
    onDelete,
    onFullscreen,
    onTitleChange,
    onFormat,
    onDrillUp,
    onDrillDown,
    onExplain,
    onCrossFilter,
}) => {
    const [editingTitle, setEditingTitle] = useState(false);
    const [titleDraft, setTitleDraft] = useState(widget.config.title);
    const [prevTitle, setPrevTitle] = useState(widget.config.title);

    if (widget.config.title !== prevTitle) {
        setPrevTitle(widget.config.title);
        setTitleDraft(widget.config.title);
    }

    const canDrillDown = widget.config.hierarchy_levels && widget.config.hierarchy_levels.length > 1;
    const isLocked = Boolean(widget.locked);

    const handleExportCSV = (e: React.MouseEvent) => {
        e.stopPropagation();
        if (!widget.data || widget.data.length === 0) {
            message.warning('No data to export');
            return;
        }
        const headers = Object.keys(widget.data[0]);
        const csvRows = [
            headers.join(','),
            ...widget.data.map(row =>
                headers.map(h => {
                    const val = row[h];
                    const str = val === null || val === undefined ? '' : String(val);
                    return str.includes(',') || str.includes('"') || str.includes('\n')
                        ? `"${str.replace(/"/g, '""')}"`
                        : str;
                }).join(',')
            ),
        ];
        const blob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${widget.config.title || 'widget_data'}.csv`;
        link.click();
        URL.revokeObjectURL(url);
        message.success('CSV downloaded!');
    };

    return (
        <div
            onClick={() => onSelect?.(widget.id)}
            style={{
                height: '100%',
                display: 'flex',
                flexDirection: 'column',
                background: 'rgba(15,23,42,0.85)',
                borderRadius: 12,
                border: isEditing
                    ? '2px solid #3b82f6'
                    : isSelected
                        ? '1.5px solid #818cf8'
                        : '1px solid rgba(99,102,241,0.15)',
                boxShadow: isEditing
                    ? '0 0 16px rgba(59,130,246,0.4)'
                    : isSelected
                        ? '0 0 12px rgba(129,140,248,0.25)'
                        : undefined,
                backdropFilter: 'blur(12px)',
                overflow: 'hidden',
                transition: 'border 0.2s, box-shadow 0.2s',
            }}
        >
            {/* Title Bar */}
            <div
                className={isLocked ? undefined : 'widget-drag-handle'}
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '8px 12px',
                    borderBottom: '1px solid rgba(99,102,241,0.1)',
                    cursor: isLocked ? 'default' : 'grab',
                    minHeight: 42,
                }}
            >
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 1, minWidth: 0 }}>
                    {isLocked ? (
                        <LockOutlined style={{ color: '#f59e0b', fontSize: 12 }} />
                    ) : (
                        <DragOutlined style={{ color: '#475569', fontSize: 12 }} />
                    )}

                    {editingTitle ? (
                        <Input
                            size="small"
                            value={titleDraft}
                            onChange={(e) => setTitleDraft(e.target.value)}
                            onPressEnter={() => {
                                onTitleChange?.(widget.id, titleDraft);
                                setEditingTitle(false);
                            }}
                            onBlur={() => {
                                onTitleChange?.(widget.id, titleDraft);
                                setEditingTitle(false);
                            }}
                            onKeyDown={(e) => {
                                if (e.key === 'Escape') {
                                    setTitleDraft(widget.config.title);
                                    setEditingTitle(false);
                                }
                            }}
                            autoFocus
                            style={{ background: 'transparent', border: 'none', color: '#e2e8f0', padding: 0 }}
                        />
                    ) : (
                        <span
                            onDoubleClick={() => setEditingTitle(true)}
                            style={{
                                color: '#e2e8f0',
                                fontWeight: 600,
                                fontSize: 13,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                cursor: 'pointer',
                            }}
                        >
                            {widget.config.title || 'Untitled Chart'}
                        </span>
                    )}

                    {isEditing && (
                        <span style={{ fontSize: 10, background: '#3b82f6', color: '#ffffff', padding: '1px 6px', borderRadius: 4, fontWeight: 600 }}>
                            EDITING
                        </span>
                    )}
                </div>

                <Space size={2}>
                    {/* Drill up / down buttons if hierarchy is present */}
                    {canDrillDown && (
                        <>
                            <Tooltip title="Drill Up">
                                <Button
                                    type="text"
                                    size="small"
                                    icon={<ArrowUpOutlined />}
                                    onClick={() => onDrillUp?.(widget.id)}
                                    style={{ color: '#818cf8' }}
                                />
                            </Tooltip>
                            <Tooltip title="Drill Down">
                                <Button
                                    type="text"
                                    size="small"
                                    icon={<ArrowDownOutlined />}
                                    onClick={() => onDrillDown?.(widget.id)}
                                    style={{ color: '#818cf8' }}
                                />
                            </Tooltip>
                        </>
                    )}

                    {/* AI Explain button */}
                    <Tooltip title="Explain Increase/Decrease (AI)">
                        <Button
                            type="text"
                            size="small"
                            icon={<RobotOutlined />}
                            onClick={() => onExplain?.(widget)}
                            style={{ color: '#a5b4fc' }}
                        />
                    </Tooltip>

                    {/* Format / Field Well */}
                    <Tooltip title="Format & Fields">
                        <Button
                            type="text"
                            size="small"
                            icon={<SettingOutlined />}
                            onClick={() => onFormat?.(widget.id)}
                            style={{ color: '#64748b' }}
                        />
                    </Tooltip>

                    <Tooltip title="Edit Prompt">
                        <Button
                            type="text"
                            size="small"
                            icon={<EditOutlined />}
                            onClick={() => onEdit?.(widget.id)}
                            style={{ color: '#64748b' }}
                        />
                    </Tooltip>
                    <Tooltip title="Export CSV Data">
                        <Button
                            type="text"
                            size="small"
                            icon={<DownloadOutlined />}
                            onClick={handleExportCSV}
                            style={{ color: '#64748b' }}
                        />
                    </Tooltip>
                    <Tooltip title="Fullscreen">
                        <Button
                            type="text"
                            size="small"
                            icon={<FullscreenOutlined />}
                            onClick={() => onFullscreen?.(widget.id)}
                            style={{ color: '#64748b' }}
                        />
                    </Tooltip>
                    <Tooltip title="Duplicate">
                        <Button
                            type="text"
                            size="small"
                            icon={<CopyOutlined />}
                            onClick={() => onDuplicate?.(widget.id)}
                            style={{ color: '#64748b' }}
                        />
                    </Tooltip>
                    <Tooltip title="Delete">
                        <Button
                            type="text"
                            size="small"
                            icon={<DeleteOutlined />}
                            onClick={() => onDelete?.(widget.id)}
                            style={{ color: '#ef4444' }}
                        />
                    </Tooltip>
                </Space>
            </div>

            {/* Source Prompt Subtitle */}
            {widget.source_prompt && (
                <div
                    style={{
                        padding: '4px 12px',
                        color: '#64748b',
                        fontSize: 11,
                        borderBottom: '1px solid rgba(99,102,241,0.05)',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                    }}
                >
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
                    onDataPointClick={(params) => {
                        const col = params.column || widget.config.x_axis || widget.config.group_by;
                        if (col && params.value !== undefined) {
                            onCrossFilter?.(col, params.value);
                        }
                    }}
                />
            </div>
        </div>
    );
};

export default WidgetContainer;
