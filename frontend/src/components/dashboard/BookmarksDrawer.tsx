/**
 * BookmarksDrawer — Power BI-style Bookmarks manager.
 * Captures and applies report views (active page, global filters, cross filters, and widget visibility).
 */
import React, { useState } from 'react';
import { Drawer, Button, Input, List, Space, Tag, Popconfirm } from 'antd';
import { BookOutlined, PlusOutlined, PlayCircleOutlined, DeleteOutlined, SyncOutlined, EditOutlined } from '@ant-design/icons';
import type { Bookmark, FilterCondition } from '../../types/dashboard';

interface Props {
    open: boolean;
    bookmarks: Bookmark[];
    currentPageId: string;
    currentFilters: FilterCondition[];
    currentCrossFilters?: Record<string, any>;
    visibleWidgetIds: string[];
    onClose: () => void;
    onApplyBookmark: (bookmark: Bookmark) => void;
    onSaveBookmark: (bookmark: Bookmark) => void;
    onDeleteBookmark: (id: string) => void;
}

const BookmarksDrawer: React.FC<Props> = ({
    open,
    bookmarks,
    currentPageId,
    currentFilters,
    currentCrossFilters,
    visibleWidgetIds,
    onClose,
    onApplyBookmark,
    onSaveBookmark,
    onDeleteBookmark,
}) => {
    const [newBookmarkName, setNewBookmarkName] = useState('');
    const [editingId, setEditingId] = useState<string | null>(null);
    const [editName, setEditName] = useState('');

    const handleCreateBookmark = () => {
        const name = newBookmarkName.trim() || `Bookmark ${bookmarks.length + 1}`;
        const newBm: Bookmark = {
            id: (crypto.randomUUID?.() || Math.random().toString(36).slice(2)),
            name,
            page_id: currentPageId,
            filters: currentFilters,
            cross_filters: currentCrossFilters,
            visible_widget_ids: visibleWidgetIds,
            created_at: Date.now(),
        };
        onSaveBookmark(newBm);
        setNewBookmarkName('');
    };

    const handleUpdateBookmark = (bm: Bookmark) => {
        const updated: Bookmark = {
            ...bm,
            page_id: currentPageId,
            filters: currentFilters,
            cross_filters: currentCrossFilters,
            visible_widget_ids: visibleWidgetIds,
        };
        onSaveBookmark(updated);
    };

    const handleRename = (bm: Bookmark) => {
        if (editName.trim()) {
            onSaveBookmark({ ...bm, name: editName.trim() });
        }
        setEditingId(null);
    };

    return (
        <Drawer
            title={
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#f8fafc' }}>
                    <BookOutlined style={{ color: '#818cf8' }} />
                    <span>Bookmarks & Saved Views</span>
                </div>
            }
            placement="right"
            open={open}
            onClose={onClose}
            width={340}
            styles={{ body: { padding: '16px', background: '#0f172a' }, header: { background: '#0f172a' } }}
        >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
                {/* Create Bookmark Bar */}
                <div style={{ display: 'flex', gap: 8 }}>
                    <Input
                        placeholder="Bookmark name..."
                        value={newBookmarkName}
                        onChange={(e) => setNewBookmarkName(e.target.value)}
                        onPressEnter={handleCreateBookmark}
                    />
                    <Button
                        type="primary"
                        icon={<PlusOutlined />}
                        onClick={handleCreateBookmark}
                        style={{ background: '#6366f1' }}
                    >
                        Add
                    </Button>
                </div>

                {/* Bookmark List */}
                <List
                    dataSource={bookmarks}
                    locale={{ emptyText: 'No bookmarks yet. Capture current filters and page as a view!' }}
                    renderItem={(bm) => {
                        const isEditing = editingId === bm.id;
                        return (
                            <List.Item
                                style={{
                                    background: '#1e293b',
                                    borderRadius: 8,
                                    marginBottom: 8,
                                    padding: '10px 12px',
                                    border: '1px solid #334155',
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'stretch',
                                    gap: 8,
                                }}
                            >
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    {isEditing ? (
                                        <Input
                                            size="small"
                                            value={editName}
                                            onChange={(e) => setEditName(e.target.value)}
                                            onPressEnter={() => handleRename(bm)}
                                            onBlur={() => handleRename(bm)}
                                            autoFocus
                                            style={{ maxWidth: 160 }}
                                        />
                                    ) : (
                                        <span style={{ fontWeight: 600, color: '#f1f5f9', fontSize: 13 }}>
                                            {bm.name}
                                        </span>
                                    )}

                                    <Space size={4}>
                                        <Button
                                            size="small"
                                            type="primary"
                                            icon={<PlayCircleOutlined />}
                                            onClick={() => onApplyBookmark(bm)}
                                            style={{ background: '#10b981', border: 'none', height: 24, fontSize: 11 }}
                                        >
                                            View
                                        </Button>
                                        <Button
                                            size="small"
                                            type="text"
                                            icon={<SyncOutlined />}
                                            title="Update with current view"
                                            onClick={() => handleUpdateBookmark(bm)}
                                            style={{ color: '#818cf8', height: 24 }}
                                        />
                                        <Button
                                            size="small"
                                            type="text"
                                            icon={<EditOutlined />}
                                            onClick={() => {
                                                setEditingId(bm.id);
                                                setEditName(bm.name);
                                            }}
                                            style={{ color: '#94a3b8', height: 24 }}
                                        />
                                        <Popconfirm
                                            title="Delete bookmark?"
                                            onConfirm={() => onDeleteBookmark(bm.id)}
                                            okText="Yes"
                                            cancelText="No"
                                        >
                                            <Button
                                                size="small"
                                                type="text"
                                                danger
                                                icon={<DeleteOutlined />}
                                                style={{ height: 24 }}
                                            />
                                        </Popconfirm>
                                    </Space>
                                </div>

                                <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                                    {bm.filters && bm.filters.length > 0 && (
                                        <Tag style={{ fontSize: 10, background: 'rgba(99,102,241,0.2)', border: 'none', color: '#a5b4fc' }}>
                                            {bm.filters.length} filters
                                        </Tag>
                                    )}
                                    {bm.visible_widget_ids && (
                                        <Tag style={{ fontSize: 10, background: 'rgba(16,185,129,0.2)', border: 'none', color: '#6ee7b7' }}>
                                            {bm.visible_widget_ids.length} visuals
                                        </Tag>
                                    )}
                                </div>
                            </List.Item>
                        );
                    }}
                />
            </div>
        </Drawer>
    );
};

export default BookmarksDrawer;
