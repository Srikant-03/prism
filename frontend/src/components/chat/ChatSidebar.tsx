/**
 * ChatSidebar â€” Persistent AI conversation panel.
 * Multi-turn chat with full dataset context awareness.
 * Responses include clickable actions and proactive insights.
 */

import React, { useState, useCallback, useRef, useEffect } from 'react';
import { Drawer, Input, Button, Avatar, Tag, Space, Tooltip, Spin } from 'antd';
import PrivacyDisclosure from './PrivacyDisclosure';
import {
    RobotOutlined, SendOutlined, UserOutlined, BulbOutlined,
    ThunderboltOutlined, CodeOutlined, TableOutlined,
    ClearOutlined, ExpandOutlined, CompressOutlined,
} from '@ant-design/icons';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

interface ChatAction {
    label: string;
    type: 'sql' | 'grid' | 'fix' | 'navigate';
    payload: string;
}

interface ChatMessage {
    id: string;
    role: 'user' | 'ai';
    content: string;
    actions?: ChatAction[];
    timestamp: number;
    thinking?: boolean;
}

interface Props {
    open: boolean;
    onClose: () => void;
    onAction?: (action: ChatAction) => void;
}

const ChatSidebar: React.FC<Props> = ({ open, onClose, onAction }) => {
    const [messages, setMessages] = useState<ChatMessage[]>([
        {
            id: '0', role: 'ai', content:
                "ðŸ‘‹ Hi! I'm your data assistant. I can see your dataset's schema, profiling results, and any preprocessing steps you've made.\n\nTry asking me things like:\n- \"What columns have the most nulls?\"\n- \"Is this data good enough for ML?\"\n- \"What's suspicious about this dataset?\"\n- \"Show me the top customers by revenue\"",
            timestamp: Date.now(),
        },
    ]);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const [expanded, setExpanded] = useState(false);
    const chatEndRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages]);

    const sendMessage = useCallback(async () => {
        if (!input.trim() || loading) return;
        const userMsg: ChatMessage = {
            id: Date.now().toString(),
            role: 'user',
            content: input.trim(),
            timestamp: Date.now(),
        };
        setMessages(prev => [...prev, userMsg]);
        setInput('');
        setLoading(true);

        // Build conversation history for context
        const history = messages.filter(m => !m.thinking).map(m => ({
            role: m.role, content: m.content,
        }));

        try {
            const res = await fetch(`${API_BASE}/api/chat/message`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: userMsg.content,
                    conversation_history: history.slice(-20),
                }),
            });
            const data = await res.json();
            const aiMsg: ChatMessage = {
                id: (Date.now() + 1).toString(),
                role: 'ai',
                content: data.response || 'I couldn\'t generate a response. Please try again.',
                actions: data.actions || [],
                timestamp: Date.now(),
            };
            setMessages(prev => [...prev, aiMsg]);
        } catch (e: any) {
            setMessages(prev => [...prev, {
                id: (Date.now() + 1).toString(),
                role: 'ai',
                content: `âš ï¸ Error: ${e.message || 'Failed to reach the AI service.'}`,
                timestamp: Date.now(),
            }]);
        } finally {
            setLoading(false);
        }
    }, [input, loading, messages]);

    const clearChat = useCallback(() => {
        setMessages([messages[0]]);
    }, [messages]);

    const actionIcons: Record<string, React.ReactNode> = {
        sql: <CodeOutlined />,
        grid: <TableOutlined />,
        fix: <ThunderboltOutlined />,
        navigate: <BulbOutlined />,
    };

    return (
        <Drawer
            title={
                <Space>
                    <Avatar size="small" style={{ background: 'linear-gradient(135deg, #6366f1, #a78bfa)' }}
                        icon={<RobotOutlined />} />
                    <span style={{ fontWeight: 700 }}>Data Assistant</span>
                    <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>AI</Tag>
                </Space>
            }
            placement="right"
            open={open}
            onClose={onClose}
            width={expanded ? 600 : 400}
            styles={{ body: { padding: 0, display: 'flex', flexDirection: 'column', height: '100%' } }}
            extra={
                <Space>
                    <Tooltip title={expanded ? 'Collapse' : 'Expand'}>
                        <Button size="small" icon={expanded ? <CompressOutlined /> : <ExpandOutlined />}
                            onClick={() => setExpanded(!expanded)} />
                    </Tooltip>
                    <Tooltip title="Clear chat">
                        <Button size="small" icon={<ClearOutlined />} onClick={clearChat} />
                    </Tooltip>
                </Space>
            }
        >
            <PrivacyDisclosure />
            {/* Messages */}
            <div style={{
                flex: 1, overflow: 'auto', padding: 16,
                display: 'flex', flexDirection: 'column', gap: 12,
            }}>
                {messages.map(msg => (
                    <div key={msg.id} style={{
                        display: 'flex', gap: 8,
                        flexDirection: msg.role === 'user' ? 'row-reverse' : 'row',
                    }}>
                        <Avatar size={28}
                            icon={msg.role === 'user' ? <UserOutlined /> : <RobotOutlined />}
                            style={{
                                flexShrink: 0,
                                background: msg.role === 'user'
                                    ? 'rgba(99,102,241,0.2)'
                                    : 'linear-gradient(135deg, #6366f1, #a78bfa)',
                            }}
                        />
                        <div style={{
                            maxWidth: '85%',
                            padding: '10px 14px',
                            borderRadius: msg.role === 'user' ? '16px 16px 4px 16px' : '16px 16px 16px 4px',
                            background: msg.role === 'user'
                                ? 'rgba(99,102,241,0.12)'
                                : 'rgba(30,41,59,0.6)',
                            border: '1px solid ' + (msg.role === 'user'
                                ? 'rgba(99,102,241,0.2)'
                                : 'rgba(255,255,255,0.06)'),
                            fontSize: 13,
                            lineHeight: 1.6,
                            whiteSpace: 'pre-wrap',
                        }}>
                            {msg.content}

                            {/* Clickable actions */}
                            {msg.actions && msg.actions.length > 0 && (
                                <div style={{
                                    display: 'flex', flexWrap: 'wrap', gap: 4,
                                    marginTop: 8, paddingTop: 8,
                                    borderTop: '1px solid rgba(255,255,255,0.06)',
                                }}>
                                    {msg.actions.map((action, i) => (
                                        <Button
                                            key={i}
                                            size="small"
                                            type="dashed"
                                            icon={actionIcons[action.type] || <ThunderboltOutlined />}
                                            onClick={() => onAction?.(action)}
                                            style={{ fontSize: 11, borderRadius: 12 }}
                                        >
                                            {action.label}
                                        </Button>
                                    ))}
                                </div>
                            )}
                        </div>
                    </div>
                ))}
                {loading && (
                    <div style={{ display: 'flex', gap: 8 }}>
                        <Avatar size={28} icon={<RobotOutlined />}
                            style={{ flexShrink: 0, background: 'linear-gradient(135deg, #6366f1, #a78bfa)' }}
                        />
                        <div style={{
                            padding: '12px 16px', borderRadius: '16px 16px 16px 4px',
                            background: 'rgba(30,41,59,0.6)',
                            border: '1px solid rgba(255,255,255,0.06)',
                        }}>
                            <Spin size="small" /> <span style={{ fontSize: 12, marginLeft: 8, opacity: 0.6 }}>Thinking...</span>
                        </div>
                    </div>
                )}
                <div ref={chatEndRef} />
            </div>

            {/* Input area */}
            <div style={{
                padding: '12px 16px',
                borderTop: '1px solid rgba(255,255,255,0.06)',
                background: 'rgba(15,23,42,0.5)',
            }}>
                <div style={{ display: 'flex', gap: 8 }}>
                    <Input.TextArea
                        value={input}
                        onChange={e => setInput(e.target.value)}
                        placeholder="Ask about your data..."
                        autoSize={{ minRows: 1, maxRows: 4 }}
                        onPressEnter={e => {
                            if (!e.shiftKey) { e.preventDefault(); sendMessage(); }
                        }}
                        style={{
                            flex: 1, fontSize: 13,
                            background: 'rgba(0,0,0,0.2)',
                            border: '1px solid rgba(99,102,241,0.15)',
                            borderRadius: 12,
                        }}
                        aria-label="Chat message input"
                    />
                    <Button
                        type="primary"
                        icon={<SendOutlined />}
                        onClick={sendMessage}
                        loading={loading}
                        style={{ borderRadius: 12, alignSelf: 'flex-end' }}
                        aria-label="Send message"
                    />
                </div>
                <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.3)', marginTop: 4 }}>
                    Shift+Enter for new line â€¢ AI has context of your schema, profiling, and cleaning decisions
                </div>
            </div>
        </Drawer>
    );
};

export default ChatSidebar;

