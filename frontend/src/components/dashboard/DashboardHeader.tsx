/**
 * DashboardHeader — Title, save, share, present mode, export controls.
 */
import React, { useState } from 'react';
import { Button, Input, Tooltip, Space } from 'antd';
import {
    SaveOutlined, ShareAltOutlined, ExpandOutlined,
    FileImageOutlined, FilePdfOutlined, PlusOutlined,
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
    saving?: boolean;
}

const DashboardHeader: React.FC<Props> = ({
    title, description, onTitleChange,
    onSave, onShare, onPresent, onExportPng, onExportPdf, onAddWidget, saving,
}) => {
    const [editingTitle, setEditingTitle] = useState(false);

    return (
        <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '16px 24px', borderBottom: '1px solid rgba(99,102,241,0.1)',
        }}>
            <div style={{ flex: 1 }}>
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
                            cursor: 'pointer',
                        }}
                    >
                        {title || 'Untitled Dashboard'}
                    </h1>
                )}
                <p style={{ color: '#64748b', fontSize: 12, margin: '4px 0 0' }}>
                    {description || 'Double-click title to rename • Type a prompt below to add charts'}
                </p>
            </div>

            <Space size={8}>
                <Button
                    type="primary" icon={<PlusOutlined />}
                    onClick={onAddWidget}
                    style={{ background: '#6366f1', border: 'none', borderRadius: 8 }}
                >
                    Add Widget
                </Button>
                <Tooltip title="Save"><Button icon={<SaveOutlined />} onClick={onSave} loading={saving} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Share"><Button icon={<ShareAltOutlined />} onClick={onShare} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Present"><Button icon={<ExpandOutlined />} onClick={onPresent} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Export PNG"><Button icon={<FileImageOutlined />} onClick={onExportPng} style={{ borderRadius: 8 }} /></Tooltip>
                <Tooltip title="Export PDF"><Button icon={<FilePdfOutlined />} onClick={onExportPdf} style={{ borderRadius: 8 }} /></Tooltip>
            </Space>
        </div>
    );
};

export default DashboardHeader;
