/**
 * PromptBar — Fixed-bottom NL input bar for the AI Dashboard Builder.
 * Includes prompt history, suggested prompts, and undo button.
 */
import React, { useState, useRef, useEffect } from 'react';
import { Input, Button, Tag, Tooltip, Space } from 'antd';
import {
    SendOutlined, UndoOutlined, HistoryOutlined,
    BulbOutlined, LoadingOutlined,
} from '@ant-design/icons';

interface PromptEntry {
    prompt: string;
    timestamp: number;
    chartTypeBefore?: string;
    chartTypeAfter?: string;
}

interface Props {
    onSubmit: (prompt: string) => void;
    loading?: boolean;
    history?: PromptEntry[];
    suggestions?: string[];
    onUndo?: () => void;
    canUndo?: boolean;
    clarification?: string;
}

const PromptBar: React.FC<Props> = ({
    onSubmit, loading, history = [], suggestions = [], onUndo, canUndo, clarification,
}) => {
    const [prompt, setPrompt] = useState('');
    const [showHistory, setShowHistory] = useState(false);
    const inputRef = useRef<any>(null);

    useEffect(() => {
        if (!loading) inputRef.current?.focus();
    }, [loading]);

    const handleSubmit = () => {
        if (!prompt.trim() || loading) return;
        onSubmit(prompt.trim());
        setPrompt('');
    };

    return (
        <div style={{
            position: 'fixed', bottom: 0, left: 0, right: 0, zIndex: 1000,
            background: 'linear-gradient(to top, #0f172a 80%, transparent)',
            padding: '16px 24px 20px',
        }}>
            {/* Clarification banner */}
            {clarification && (
                <div style={{
                    marginBottom: 10, padding: '10px 16px', borderRadius: 10,
                    background: 'rgba(251,191,36,0.1)', border: '1px solid rgba(251,191,36,0.3)',
                    color: '#fbbf24', fontSize: 13,
                }}>
                    🤔 {clarification}
                </div>
            )}

            {/* Suggested prompts */}
            {suggestions.length > 0 && (
                <div style={{ marginBottom: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    <BulbOutlined style={{ color: '#6366f1', marginTop: 4 }} />
                    {suggestions.map((s, i) => (
                        <Tag
                            key={i}
                            onClick={() => { setPrompt(s); inputRef.current?.focus(); }}
                            style={{
                                cursor: 'pointer', background: 'rgba(99,102,241,0.1)',
                                border: '1px solid rgba(99,102,241,0.3)', color: '#a5b4fc',
                                borderRadius: 20, padding: '4px 12px', fontSize: 12,
                            }}
                        >
                            {s}
                        </Tag>
                    ))}
                </div>
            )}

            {/* Prompt History dropdown */}
            {showHistory && history.length > 0 && (
                <div style={{
                    marginBottom: 10, maxHeight: 200, overflowY: 'auto',
                    background: '#1e293b', borderRadius: 10, border: '1px solid #334155',
                    padding: 8,
                }}>
                    {history.slice().reverse().map((entry, i) => (
                        <div
                            key={i}
                            onClick={() => { setPrompt(entry.prompt); setShowHistory(false); }}
                            style={{
                                padding: '6px 12px', cursor: 'pointer', borderRadius: 6,
                                color: '#94a3b8', fontSize: 12,
                                display: 'flex', justifyContent: 'space-between',
                            }}
                            onMouseEnter={e => (e.currentTarget.style.background = 'rgba(99,102,241,0.1)')}
                            onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                        >
                            <span style={{ flex: 1 }}>💬 {entry.prompt}</span>
                            {entry.chartTypeAfter && (
                                <span style={{ color: '#6366f1', fontSize: 11 }}>{entry.chartTypeAfter}</span>
                            )}
                        </div>
                    ))}
                </div>
            )}

            {/* Input Row */}
            <div style={{
                display: 'flex', gap: 8, alignItems: 'center',
                background: '#1e293b', borderRadius: 14,
                border: '1px solid rgba(99,102,241,0.3)',
                padding: '4px 4px 4px 16px',
                boxShadow: '0 -4px 24px rgba(0,0,0,0.4)',
            }}>
                <Space size={4}>
                    <Tooltip title="History">
                        <Button
                            type="text" size="small"
                            icon={<HistoryOutlined />}
                            onClick={() => setShowHistory(!showHistory)}
                            style={{ color: '#64748b' }}
                        />
                    </Tooltip>
                    {canUndo && (
                        <Tooltip title="Undo last prompt">
                            <Button type="text" size="small" icon={<UndoOutlined />} onClick={onUndo} style={{ color: '#fbbf24' }} />
                        </Tooltip>
                    )}
                </Space>

                <Input
                    ref={inputRef}
                    value={prompt}
                    onChange={e => setPrompt(e.target.value)}
                    onPressEnter={handleSubmit}
                    placeholder="Type a prompt... e.g. 'Show monthly revenue as a bar chart'"
                    disabled={loading}
                    variant="borderless"
                    style={{ color: '#e2e8f0', fontSize: 14, flex: 1 }}
                />

                <Button
                    type="primary"
                    size="large"
                    icon={loading ? <LoadingOutlined /> : <SendOutlined />}
                    onClick={handleSubmit}
                    disabled={!prompt.trim() || loading}
                    style={{
                        borderRadius: 10, background: '#6366f1', border: 'none',
                        height: 40, width: 40, display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}
                />
            </div>
        </div>
    );
};

export default PromptBar;
