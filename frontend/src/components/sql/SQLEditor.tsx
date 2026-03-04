/**
 * SQLEditor — Raw SQL editor with syntax highlighting, format/prettify, and auto-generated SQL preview.
 */

import React, { useState } from 'react';
import { Input, Button, Tag, Space, Tooltip } from 'antd';
import { PlayCircleOutlined, CopyOutlined, CodeOutlined, FormatPainterOutlined } from '@ant-design/icons';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';
const { TextArea } = Input;

interface Props {
    rawSQL: string;
    generatedSQL: string;
    onRawSQLChange: (sql: string) => void;
    onExecute: () => void;
    onGenerateSQL: () => void;
    loading?: boolean;
    mode: 'visual' | 'raw';
}

const SQLEditor: React.FC<Props> = ({
    rawSQL, generatedSQL, onRawSQLChange, onExecute, onGenerateSQL, loading, mode,
}) => {
    const displaySQL = mode === 'raw' ? rawSQL : generatedSQL;
    const [formatting, setFormatting] = useState(false);

    const handleCopy = () => {
        navigator.clipboard.writeText(displaySQL);
    };

    const handleFormat = async () => {
        if (!displaySQL.trim()) return;
        setFormatting(true);
        try {
            const res = await fetch(`${API_BASE}/api/sql/format`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql: displaySQL }),
            });
            const data = await res.json();
            if (data.formatted && mode === 'raw') {
                onRawSQLChange(data.formatted);
            }
        } catch { /* format failed silently */ }
        setFormatting(false);
    };

    return (
        <div className="glass-panel" style={{ padding: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                <Space>
                    <CodeOutlined style={{ color: '#6366f1' }} />
                    <span style={{ fontWeight: 600, fontSize: 13 }}>
                        {mode === 'raw' ? 'SQL Editor' : 'Generated SQL'}
                    </span>
                    {mode === 'visual' && (
                        <Tag color="blue" style={{ fontSize: 10 }}>Auto-generated from builder</Tag>
                    )}
                </Space>
                <Space size={4}>
                    {mode === 'visual' && (
                        <Button size="small" onClick={onGenerateSQL} icon={<CodeOutlined />}>
                            Preview SQL
                        </Button>
                    )}
                    {mode === 'raw' && (
                        <Tooltip title="Format / Prettify SQL">
                            <Button
                                size="small"
                                icon={<FormatPainterOutlined />}
                                onClick={handleFormat}
                                loading={formatting}
                                aria-label="Format SQL"
                            >
                                Format
                            </Button>
                        </Tooltip>
                    )}
                    <Tooltip title="Copy to clipboard">
                        <Button size="small" icon={<CopyOutlined />} onClick={handleCopy} aria-label="Copy SQL" />
                    </Tooltip>
                    <Button
                        type="primary"
                        size="small"
                        icon={<PlayCircleOutlined />}
                        onClick={onExecute}
                        loading={loading}
                        aria-label="Execute query"
                    >
                        Run
                    </Button>
                </Space>
            </div>
            <TextArea
                value={displaySQL}
                onChange={e => mode === 'raw' && onRawSQLChange(e.target.value)}
                readOnly={mode === 'visual'}
                rows={Math.min(Math.max(displaySQL.split('\n').length, 3), 12)}
                style={{
                    fontFamily: "'Fira Code', 'Cascadia Code', 'JetBrains Mono', monospace",
                    fontSize: 12,
                    background: 'rgba(0,0,0,0.3)',
                    border: '1px solid rgba(99,102,241,0.15)',
                    color: mode === 'visual' ? 'rgba(165,180,252,0.9)' : 'rgba(255,255,255,0.85)',
                    lineHeight: 1.6,
                }}
                placeholder={mode === 'raw'
                    ? 'SELECT * FROM your_table LIMIT 100'
                    : 'Use the visual builder above, then click "Preview SQL"'
                }
                aria-label="SQL query editor"
            />
        </div>
    );
};

export default SQLEditor;
