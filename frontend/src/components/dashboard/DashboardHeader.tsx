/**
 * DashboardHeader — Title, save, share, present mode, export, bookmarks, selection pane, and measure studio.
 */
import React, { useState } from 'react';
import { Button, Input, Tooltip, Space } from 'antd';
import {
    SaveOutlined, ShareAltOutlined, ExpandOutlined,
    FileImageOutlined, FilePdfOutlined, PlusOutlined,
    BookOutlined, AppstoreOutlined, FunctionOutlined,
} from '@ant-design/icons';

interface Props {
    title: string;
    description: string;
    onTitleChange: (title: string) => void;
    onSave: () => void;
    onShare: () => void;
    onPresent: () => void;
    onExportPng: () => void;
    onExportPdf: () => void;
    onAddWidget: () => void;
    onOpenBookmarks?: () => void;
    onOpenSelection?: () => void;
    onOpenMeasures?: () => void;
    saving?: boolean;
}

const DashboardHeader: React.FC<Props> = ({
    title,
    description,
    onTitleChange,
    onSave,
    onShare,
    onPresent,
    onExportPng,
    onExportPdf,
    onAddWidget,
    onOpenBookmarks,
    onOpenSelection,
    onOpenMeasures,
    saving,
}) => {
    const [editingTitle, setEditingTitle] = useState(false);

    return (
        <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '14px 24px', borderBottom: '1px solid rgba(99,102,241,0.1)',
            background: 'rgba(15, 23, 42, 0.6)',
        }}>
            <div style={{ flex: 1, minWidth: 0, marginRight: 16 }}>
                {editingTitle ? (
                    <Input
                        value={title}
                        onChange={e => onTitleChange(e.target.value)}
                        onBlur={() => setEditingTitle(false)}
                        onPressEnter={() => setEditingTitle(false)}
                        autoFocus
                        style={{
                            fontSize: 22, fontWeight: 700, background: 'transparent',
                            border: 'none', borderBottom: '2px solid #6366f1', color: '#f1f5f9',
                            padding: '4px 0', maxWidth: 400,
                        }}
                    />
                ) : (
                    <h1
                        onDoubleClick={() => setEditingTitle(true)}
                        style={{
                            fontSize: 22, fontWeight: 700, color: '#f1f5f9', margin: 0,
                            cursor: 'pointer', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        }}
                    >
                        {title || 'Untitled Dashboard'}
                    </h1>
                )}
                <p style={{ color: '#64748b', fontSize: 12, margin: '4px 0 0' }}>
                    {description || 'Double-click title to rename • Power BI-style authoring workspace'}
                </p>
            </div>

            <Space size={8} wrap>
                <Button
                    type="primary" icon={<PlusOutlined />}
                    onClick={onAddWidget}
                    style={{ background: '#6366f1', border: 'none', borderRadius: 8 }}
                >
                    Add Visual
                </Button>

                {onOpenMeasures && (
                    <Tooltip title="DAX Measure Studio">
                        <Button icon={<FunctionOutlined />} onClick={onOpenMeasures} style={{ borderRadius: 8 }}>
                            Measures
                        </Button>
                    </Tooltip>
                )}

                {onOpenBookmarks && (
                    <Tooltip title="Bookmarks & Views">
                        <Button icon={<BookOutlined />} onClick={onOpenBookmarks} style={{ borderRadius: 8 }}>
                            Bookmarks
                        </Button>
                    </Tooltip>
                )}

                {onOpenSelection && (
                    <Tooltip title="Selection & Layers">
                        <Button icon={<AppstoreOutlined />} onClick={onOpenSelection} style={{ borderRadius: 8 }}>
                            Layers
                        </Button>
                    </Tooltip>
                )}

                <Tooltip title="Save"><Button icon={<SaveOutlined />} onClick={onSave} loading={saving} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Share"><Button icon={<ShareAltOutlined />} onClick={onShare} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Present Mode"><Button icon={<ExpandOutlined />} onClick={onPresent} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Export PNG"><Button icon={<FileImageOutlined />} onClick={onExportPng} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Export PDF"><Button icon={<FilePdfOutlined />} onClick={onExportPdf} style={{ borderRadius: 8 }} /></Tooltip>
            </Space>
        </div>
    );
};

export default DashboardHeader;
