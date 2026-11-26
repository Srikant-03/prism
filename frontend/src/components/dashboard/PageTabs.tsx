/**
 * PageTabs — Multi-page navigation bar for Power BI-style dashboard authoring.
 * Supports switching pages, inline renaming, adding, duplicating, and deleting pages.
 */
import React, { useState } from 'react';
import { Button, Input, Dropdown, type MenuProps } from 'antd';
import { PlusOutlined, MoreOutlined, CopyOutlined, DeleteOutlined, EditOutlined, FileTextOutlined } from '@ant-design/icons';
import type { DashboardPage } from '../../types/dashboard';

interface Props {
    pages: DashboardPage[];
    activePageId: string;
    onSelectPage: (id: string) => void;
    onAddPage: () => void;
    onRenamePage: (id: string, newTitle: string) => void;
    onDeletePage: (id: string) => void;
    onDuplicatePage: (id: string) => void;
}

const PageTabs: React.FC<Props> = ({
    pages,
    activePageId,
    onSelectPage,
    onAddPage,
    onRenamePage,
    onDeletePage,
    onDuplicatePage,
}) => {
    const [editingPageId, setEditingPageId] = useState<string | null>(null);
    const [titleDraft, setTitleDraft] = useState('');

    const startEditing = (page: DashboardPage, e: React.MouseEvent) => {
        e.stopPropagation();
        setEditingPageId(page.id);
        setTitleDraft(page.title);
    };

    const saveEditing = (id: string) => {
        if (titleDraft.trim()) {
            onRenamePage(id, titleDraft.trim());
        }
        setEditingPageId(null);
    };

    const getMenuItems = (page: DashboardPage): MenuProps['items'] => [
        {
            key: 'rename',
            icon: <EditOutlined />,
            label: 'Rename',
            onClick: (e) => {
                e.domEvent.stopPropagation();
                setEditingPageId(page.id);
                setTitleDraft(page.title);
            },
        },
        {
            key: 'duplicate',
            icon: <CopyOutlined />,
            label: 'Duplicate Page',
            onClick: (e) => {
                e.domEvent.stopPropagation();
                onDuplicatePage(page.id);
            },
        },
        {
            type: 'divider',
        },
        {
            key: 'delete',
            icon: <DeleteOutlined />,
            label: 'Delete Page',
            danger: true,
            disabled: pages.length <= 1,
            onClick: (e) => {
                e.domEvent.stopPropagation();
                onDeletePage(page.id);
            },
        },
    ];

    return (
        <div
            style={{
                display: 'flex',
                alignItems: 'center',
                gap: 4,
                padding: '6px 16px',
                background: '#0b1120',
                borderTop: '1px solid rgba(99, 102, 241, 0.15)',
                overflowX: 'auto',
                userSelect: 'none',
            }}
        >
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                {pages.map((page) => {
                    const isActive = page.id === activePageId;
                    const isEditing = editingPageId === page.id;

                    return (
                        <div
                            key={page.id}
                            onClick={() => onSelectPage(page.id)}
                            style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: 8,
                                padding: '6px 14px',
                                borderRadius: '6px 6px 0 0',
                                cursor: 'pointer',
                                background: isActive ? '#1e293b' : 'transparent',
                                borderBottom: isActive ? '2px solid #6366f1' : '2px solid transparent',
                                color: isActive ? '#f8fafc' : '#94a3b8',
                                fontWeight: isActive ? 600 : 400,
                                fontSize: 13,
                                transition: 'all 0.15s ease',
                            }}
                            onDoubleClick={(e) => startEditing(page, e)}
                        >
                            <FileTextOutlined style={{ fontSize: 13, color: isActive ? '#818cf8' : '#64748b' }} />

                            {isEditing ? (
                                <Input
                                    size="small"
                                    value={titleDraft}
                                    onChange={(e) => setTitleDraft(e.target.value)}
                                    onBlur={() => saveEditing(page.id)}
                                    onPressEnter={() => saveEditing(page.id)}
                                    autoFocus
                                    onClick={(e) => e.stopPropagation()}
                                    style={{
                                        width: 120,
                                        height: 22,
                                        fontSize: 12,
                                        background: '#0f172a',
                                        color: '#f8fafc',
                                        borderColor: '#6366f1',
                                    }}
                                />
                            ) : (
                                <span>{page.title}</span>
                            )}

                            {page.widgets && page.widgets.length > 0 && (
                                <span
                                    style={{
                                        fontSize: 10,
                                        background: isActive ? 'rgba(99,102,241,0.25)' : 'rgba(148,163,184,0.15)',
                                        color: isActive ? '#a5b4fc' : '#64748b',
                                        padding: '1px 5px',
                                        borderRadius: 10,
                                    }}
                                >
                                    {page.widgets.length}
                                </span>
                            )}

                            <Dropdown menu={{ items: getMenuItems(page) }} trigger={['click']}>
                                <Button
                                    type="text"
                                    size="small"
                                    icon={<MoreOutlined style={{ fontSize: 12 }} />}
                                    onClick={(e) => e.stopPropagation()}
                                    style={{
                                        padding: '0 2px',
                                        height: 18,
                                        color: '#64748b',
                                        marginLeft: 2,
                                    }}
                                />
                            </Dropdown>
                        </div>
                    );
                })}
            </div>

            <Button
                type="text"
                size="small"
                icon={<PlusOutlined />}
                onClick={onAddPage}
                style={{
                    color: '#818cf8',
                    marginLeft: 6,
                    height: 28,
                    borderRadius: 4,
                }}
            >
                New Page
            </Button>
        </div>
    );
};

export default PageTabs;
