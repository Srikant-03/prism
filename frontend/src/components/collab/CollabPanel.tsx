/**
 * CollabPanel â€” Collaborative annotations with real-time comments and emoji reactions.
 * Uses polling-based approach (upgradeable to WebSocket in production).
 */

import React, { useState, useCallback } from 'react';
import { Input, Button, Space, Tag, Avatar, Tooltip, Popover, Badge, Empty, Drawer } from 'antd';
import {
    SmileOutlined, PushpinOutlined,
    SendOutlined, UserOutlined, TeamOutlined,
    DeleteOutlined,
} from '@ant-design/icons';

interface Annotation {
    id: string;
    author: string;
    text: string;
    timestamp: number;
    target?: { type: 'column' | 'row' | 'cell' | 'general'; column?: string; rowIndex?: number };
    reactions: Record<string, string[]>;
    pinned: boolean;
}

interface Props {
    open: boolean;
    onClose: () => void;
    annotations: Annotation[];
    onAddAnnotation: (text: string, target?: Annotation['target']) => void;
    onAddReaction: (annotationId: string, emoji: string) => void;
    onTogglePin: (annotationId: string) => void;
    onDelete: (annotationId: string) => void;
    currentUser?: string;
}

const REACTIONS = ['ðŸ‘', 'ðŸ‘Ž', 'â“', 'ðŸ’¡', 'ðŸ”¥', 'âœ…', 'âš ï¸', 'ðŸŽ¯'];

const CollabPanel: React.FC<Props> = ({
    open, onClose, annotations, onAddAnnotation, onAddReaction,
    onTogglePin, onDelete, currentUser = 'You',
}) => {
    const [input, setInput] = useState('');
    const [filter, setFilter] = useState<'all' | 'pinned' | 'mine'>('all');

    const filtered = annotations.filter(a => {
        if (filter === 'pinned') return a.pinned;
        if (filter === 'mine') return a.author === currentUser;
        return true;
    }).sort((a, b) => {
        if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
        return b.timestamp - a.timestamp;
    });

    const send = useCallback(() => {
        if (!input.trim()) return;
        onAddAnnotation(input.trim());
        setInput('');
    }, [input, onAddAnnotation]);

    const formatTime = (ts: number) => {
        const d = new Date(ts);
        const now = Date.now();
        const diff = now - ts;
        if (diff < 60000) return 'just now';
        if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
        if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
        return d.toLocaleDateString();
    };

    return (
        <Drawer
            title={
                <Space>
                    <TeamOutlined style={{ color: '#6366f1' }} />
                    <span style={{ fontWeight: 700 }}>Annotations</span>
                    <Badge count={annotations.length} style={{ background: '#6366f1' }} />
                </Space>
            }
            placement="right"
            open={open}
            onClose={onClose}
            width={380}
            styles={{ body: { padding: 0, display: 'flex', flexDirection: 'column', height: '100%' } }}
        >
            {/* Filter bar */}
            <div style={{
                padding: '8px 12px', display: 'flex', gap: 4,
                borderBottom: '1px solid rgba(255,255,255,0.06)',
            }}>
                {(['all', 'pinned', 'mine'] as const).map(f => (
                    <Tag key={f} color={filter === f ? 'blue' : undefined}
                        style={{ cursor: 'pointer', fontSize: 11 }}
                        onClick={() => setFilter(f)}>
                        {f === 'all' ? 'All' : f === 'pinned' ? 'ðŸ“Œ Pinned' : 'ðŸ‘¤ Mine'}
                    </Tag>
                ))}
            </div>

            {/* Messages */}
            <div style={{
                flex: 1, overflow: 'auto', padding: 12,
                display: 'flex', flexDirection: 'column', gap: 8,
            }}>
                {filtered.length === 0 ? (
                    <Empty
                        description="No annotations yet â€” be the first!"
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                    />
                ) : (
                    filtered.map(note => (
                        <div key={note.id} style={{
                            padding: '10px 12px', borderRadius: 8,
                            background: note.pinned ? 'rgba(99,102,241,0.08)' : 'rgba(30,41,59,0.4)',
                            border: `1px solid ${note.pinned ? 'rgba(99,102,241,0.2)' : 'rgba(255,255,255,0.04)'}`,
                        }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                                <Space size={4}>
                                    <Avatar size={18} icon={<UserOutlined />}
                                        style={{ background: '#6366f1' }} />
                                    <span style={{ fontSize: 11, fontWeight: 600 }}>{note.author}</span>
                                    <span style={{ fontSize: 10, opacity: 0.4 }}>
                                        {formatTime(note.timestamp)}
                                    </span>
                                </Space>
                                <Space size={2}>
                                    {note.pinned && <PushpinOutlined style={{ color: '#6366f1', fontSize: 10 }} />}
                                    <Tooltip title={note.pinned ? 'Unpin' : 'Pin'}>
                                        <Button size="small" type="text"
                                            icon={<PushpinOutlined />}
                                            onClick={() => onTogglePin(note.id)}
                                            style={{ fontSize: 10 }} />
                                    </Tooltip>
                                    {note.author === currentUser && (
                                        <Button size="small" type="text" danger
                                            icon={<DeleteOutlined />}
                                            onClick={() => onDelete(note.id)}
                                            style={{ fontSize: 10 }} />
                                    )}
                                </Space>
                            </div>

                            {note.target && (
                                <Tag style={{ fontSize: 9, marginBottom: 4 }}>
                                    {note.target.type === 'column' ? `ðŸ“Š ${note.target.column}` :
                                        note.target.type === 'row' ? `ðŸ”¢ Row ${note.target.rowIndex}` :
                                            note.target.type === 'cell' ? `ðŸ“‹ ${note.target.column}[${note.target.rowIndex}]` :
                                                'ðŸ’¬ General'}
                                </Tag>
                            )}

                            <div style={{ fontSize: 13, lineHeight: 1.5, margin: '4px 0' }}>
                                {note.text}
                            </div>

                            {/* Reactions */}
                            <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginTop: 4 }}>
                                {Object.entries(note.reactions).map(([emoji, users]) => (
                                    <Tooltip key={emoji} title={users.join(', ')}>
                                        <Tag
                                            style={{
                                                cursor: 'pointer', fontSize: 11,
                                                background: users.includes(currentUser)
                                                    ? 'rgba(99,102,241,0.15)' : undefined,
                                            }}
                                            onClick={() => onAddReaction(note.id, emoji)}
                                        >
                                            {emoji} {users.length}
                                        </Tag>
                                    </Tooltip>
                                ))}
                                <Popover
                                    content={
                                        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', width: 160 }}>
                                            {REACTIONS.map(emoji => (
                                                <span key={emoji}
                                                    style={{ cursor: 'pointer', fontSize: 18, padding: 2 }}
                                                    onClick={() => onAddReaction(note.id, emoji)}>
                                                    {emoji}
                                                </span>
                                            ))}
                                        </div>
                                    }
                                    trigger="click"
                                >
                                    <Tag style={{ cursor: 'pointer', fontSize: 10 }}>
                                        <SmileOutlined /> +
                                    </Tag>
                                </Popover>
                            </div>
                        </div>
                    ))
                )}
            </div>

            {/* Input */}
            <div style={{
                padding: '12px', borderTop: '1px solid rgba(255,255,255,0.06)',
                background: 'rgba(15,23,42,0.5)',
            }}>
                <div style={{ display: 'flex', gap: 8 }}>
                    <Input.TextArea
                        value={input}
                        onChange={e => setInput(e.target.value)}
                        placeholder="Add an annotation..."
                        autoSize={{ minRows: 1, maxRows: 3 }}
                        onPressEnter={e => {
                            if (!e.shiftKey) { e.preventDefault(); send(); }
                        }}
                        style={{
                            flex: 1, fontSize: 12,
                            background: 'rgba(0,0,0,0.2)',
                            border: '1px solid rgba(99,102,241,0.15)',
                            borderRadius: 8,
                        }}
                    />
                    <Button type="primary" icon={<SendOutlined />}
                        onClick={send} style={{ borderRadius: 8, alignSelf: 'flex-end' }} />
                </div>
            </div>
        </Drawer>
    );
};

export default CollabPanel;

