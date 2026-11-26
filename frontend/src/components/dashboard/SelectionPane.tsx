/**
 * SelectionPane — Power BI-style Selection pane.
 * Controls visual visibility (hide/show), layout locking, z-index ordering, and inline renaming.
 */
import React, { useState } from 'react';
import { Drawer, List, Button, Input, Space, Tooltip } from 'antd';
import {
    EyeOutlined, EyeInvisibleOutlined, LockOutlined, UnlockOutlined,
    ArrowUpOutlined, ArrowDownOutlined, AppstoreOutlined, EditOutlined,
} from '@ant-design/icons';
import type { Widget } from '../../types/dashboard';

interface Props {
    open: boolean;
    widgets: Widget[];
    onClose: () => void;
    onToggleVisibility: (widgetId: string) => void;
    onToggleLock: (widgetId: string) => void;
    onRenameWidget: (widgetId: string, newTitle: string) => void;
    onReorderWidget: (fromIndex: number, toIndex: number) => void;
    onSelectWidget?: (widgetId: string) => void;
}

const SelectionPane: React.FC<Props> = ({
    open,
    widgets,
    onClose,
    onToggleVisibility,
    onToggleLock,
    onRenameWidget,
    onReorderWidget,
    onSelectWidget,
}) => {
    const [editingId, setEditingId] = useState<string | null>(null);
    const [titleDraft, setTitleDraft] = useState('');

    const startEditing = (w: Widget) => {
        setEditingId(w.id);
        setTitleDraft(w.config.title || 'Untitled Visual');
    };

    const saveEditing = (widgetId: string) => {
        if (titleDraft.trim()) {
            onRenameWidget(widgetId, titleDraft.trim());
        }
        setEditingId(null);
    };

    return (
        <Drawer
            title={
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#f8fafc' }}>
                    <AppstoreOutlined style={{ color: '#818cf8' }} />
                    <span>Selection & Layer Order</span>
                </div>
            }
            placement="right"
            open={open}
            onClose={onClose}
            width={320}
            styles={{ body: { padding: '12px', background: '#0f172a' }, header: { background: '#0f172a' } }}
        >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                <span style={{ fontSize: 11, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.5, fontWeight: 600 }}>
                    Visual Layers ({widgets.length})
                </span>

                <List
                    dataSource={widgets}
                    locale={{ emptyText: 'No visuals on this page' }}
                    renderItem={(widget, idx) => {
                        const isHidden = Boolean(widget.hidden);
                        const isLocked = Boolean(widget.locked);
                        const isEditing = editingId === widget.id;

                        return (
                            <List.Item
                                onClick={() => onSelectWidget?.(widget.id)}
                                style={{
                                    background: '#1e293b',
                                    borderRadius: 6,
                                    marginBottom: 6,
                                    padding: '8px 10px',
                                    border: '1px solid #334155',
                                    opacity: isHidden ? 0.5 : 1,
                                    cursor: 'pointer',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'space-between',
                                }}
                            >
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 1, minWidth: 0 }}>
                                    <Tooltip title={isHidden ? 'Show Visual' : 'Hide Visual'}>
                                        <Button
                                            size="small"
                                            type="text"
                                            icon={isHidden ? <EyeInvisibleOutlined style={{ color: '#ef4444' }} /> : <EyeOutlined style={{ color: '#10b981' }} />}
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                onToggleVisibility(widget.id);
                                            }}
                                            style={{ padding: 0, width: 22, height: 22 }}
                                        />
                                    </Tooltip>

                                    <Tooltip title={isLocked ? 'Unlock Position' : 'Lock Position'}>
                                        <Button
                                            size="small"
                                            type="text"
                                            icon={isLocked ? <LockOutlined style={{ color: '#f59e0b' }} /> : <UnlockOutlined style={{ color: '#64748b' }} />}
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                onToggleLock(widget.id);
                                            }}
                                            style={{ padding: 0, width: 22, height: 22 }}
                                        />
                                    </Tooltip>

                                    {isEditing ? (
                                        <Input
                                            size="small"
                                            value={titleDraft}
                                            onChange={(e) => setTitleDraft(e.target.value)}
                                            onPressEnter={() => saveEditing(widget.id)}
                                            onBlur={() => saveEditing(widget.id)}
                                            autoFocus
                                            onClick={(e) => e.stopPropagation()}
                                            style={{ flex: 1, height: 24, fontSize: 12 }}
                                        />
                                    ) : (
                                        <span
                                            onDoubleClick={() => startEditing(widget)}
                                            style={{
                                                fontSize: 12,
                                                color: '#e2e8f0',
                                                fontWeight: 500,
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                whiteSpace: 'nowrap',
                                            }}
                                        >
                                            {widget.config.title || 'Untitled Visual'}
                                        </span>
                                    )}
                                </div>

                                <Space size={2} onClick={(e) => e.stopPropagation()}>
                                    <Button
                                        size="small"
                                        type="text"
                                        icon={<EditOutlined />}
                                        onClick={() => startEditing(widget)}
                                        style={{ color: '#94a3b8', width: 20, height: 20, padding: 0 }}
                                    />
                                    <Button
                                        size="small"
                                        type="text"
                                        icon={<ArrowUpOutlined />}
                                        disabled={idx === 0}
                                        onClick={() => onReorderWidget(idx, idx - 1)}
                                        style={{ color: '#94a3b8', width: 20, height: 20, padding: 0 }}
                                    />
                                    <Button
                                        size="small"
                                        type="text"
                                        icon={<ArrowDownOutlined />}
                                        disabled={idx === widgets.length - 1}
                                        onClick={() => onReorderWidget(idx, idx + 1)}
                                        style={{ color: '#94a3b8', width: 20, height: 20, padding: 0 }}
                                    />
                                </Space>
                            </List.Item>
                        );
                    }}
                />
            </div>
        </Drawer>
    );
};

export default SelectionPane;
