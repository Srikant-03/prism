/**
 * NLQueryPanel — Natural Language to SQL query interface.
 * Free-text input → LLM generates SQL → user reviews/edits → execute.
 */

import React, { useState, useCallback } from 'react';
import { Input, Button, Space, Tag, Alert, Tooltip, Spin } from 'antd';
import {
    SendOutlined, EditOutlined, PlayCircleOutlined,
    BulbOutlined, HistoryOutlined,
    ThunderboltOutlined, CheckCircleOutlined, QuestionCircleOutlined,
} from '@ant-design/icons';
import * as sqlApi from '../../api/sql';
import type { NLQueryResult, QueryResult } from '../../types/sql';

const { TextArea } = Input;

const EXAMPLE_QUERIES = [
    "Show me the top 10 rows by the largest numeric column",
    "How many records are there per category?",
    "Find all rows where any column is null",
    "What is the average and median of each numeric column?",
    "Show monthly trends",
    "Find outliers more than 3 standard deviations from the mean",
    "Show the distribution of values in each categorical column",
    "Which records have the most missing fields?",
];

interface Props {
    onResultReady: (result: QueryResult) => void;
    onSQLGenerated?: (sql: string) => void;
}

const NLQueryPanel: React.FC<Props> = ({ onResultReady, onSQLGenerated }) => {
    const [question, setQuestion] = useState('');
    const [nlResult, setNlResult] = useState<NLQueryResult | null>(null);
    const [editedSQL, setEditedSQL] = useState('');
    const [isEditing, setIsEditing] = useState(false);
    const [loading, setLoading] = useState(false);
    const [executing, setExecuting] = useState(false);
    const [refinement, setRefinement] = useState('');
    const [history, setHistory] = useState<Array<{ question: string; sql: string; time: number }>>([]);
    const [showHistory, setShowHistory] = useState(false);

    const confidenceColors = {
        high: '#52c41a',
        medium: '#faad14',
        low: '#ff4d4f',
    };

    const handleAsk = useCallback(async () => {
        if (!question.trim()) return;
        setLoading(true);
        setNlResult(null);
        setIsEditing(false);

        try {
            const result = await sqlApi.nlQuery(question);
            setNlResult(result);
            setEditedSQL(result.sql || '');
            if (result.sql) {
                onSQLGenerated?.(result.sql);
            }
        } catch (e: any) {
            setNlResult({
                sql: '', explanation: '', assumptions: [],
                confidence: 'low', clarification_needed: null,
                schema_context: '', success: false, error: e.message,
            });
        } finally {
            setLoading(false);
        }
    }, [question, onSQLGenerated]);

    const handleRefine = useCallback(async () => {
        if (!refinement.trim() || !nlResult?.sql) return;
        setLoading(true);

        try {
            const result = await sqlApi.nlRefine(question, nlResult.sql, refinement);
            setNlResult(result);
            setEditedSQL(result.sql || '');
            setRefinement('');
            if (result.sql) {
                onSQLGenerated?.(result.sql);
            }
        } catch (e: any) {
            setNlResult(prev => prev ? { ...prev, error: e.message } : null);
        } finally {
            setLoading(false);
        }
    }, [refinement, question, nlResult, onSQLGenerated]);

    const handleExecute = useCallback(async () => {
        const sqlToRun = isEditing ? editedSQL : nlResult?.sql;
        if (!sqlToRun) return;
        setExecuting(true);

        try {
            const result = await sqlApi.executeQuery({ sql: sqlToRun });
            onResultReady(result);

            setHistory(prev => [
                { question, sql: sqlToRun, time: Date.now() },
                ...prev.slice(0, 19),
            ]);
        } catch (e: any) {
            onResultReady({
                success: false, columns: [], rows: [], row_count: 0,
                error: e.message,
            });
        } finally {
            setExecuting(false);
        }
    }, [isEditing, editedSQL, nlResult, question, onResultReady]);

    const handleExampleClick = (example: string) => {
        setQuestion(example);
    };

    const handleHistoryClick = (item: { question: string; sql: string }) => {
        setQuestion(item.question);
        setEditedSQL(item.sql);
        setNlResult({
            sql: item.sql, explanation: 'Loaded from history', assumptions: [],
            confidence: 'high', clarification_needed: null,
            schema_context: '', success: true, error: null,
        });
        setShowHistory(false);
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {/* Privacy Disclosure */}
            <Alert
                type="info"
                showIcon
                closable
                banner
                message="Privacy Notice"
                description="Natural language queries are processed by Google Gemini. Only column names, data types, and a few sample values are sent — never your full dataset rows. All data remains local."
                style={{ borderRadius: 8, fontSize: 12 }}
            />
            {/* Header */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: 8,
                padding: '8px 12px',
                background: 'linear-gradient(135deg, rgba(99,102,241,0.08), rgba(139,92,246,0.08))',
                borderRadius: 10, border: '1px solid rgba(99,102,241,0.15)',
            }}>
                <ThunderboltOutlined style={{ color: '#a78bfa', fontSize: 18 }} />
                <span style={{ fontWeight: 700, fontSize: 14 }}>Ask in Plain English</span>
                <Tag color="purple" style={{ fontSize: 10 }}>Powered by Gemini</Tag>
                <Tooltip title="Query history">
                    <Button
                        size="small"
                        icon={<HistoryOutlined />}
                        onClick={() => setShowHistory(!showHistory)}
                        type={showHistory ? 'primary' : 'default'}
                        style={{ marginLeft: 'auto' }}
                    >
                        {history.length > 0 ? history.length : ''}
                    </Button>
                </Tooltip>
            </div>

            {/* History drawer */}
            {showHistory && history.length > 0 && (
                <div className="glass-panel" style={{ padding: 8, maxHeight: 200, overflow: 'auto' }}>
                    {history.map((h, i) => (
                        <div
                            key={i}
                            onClick={() => handleHistoryClick(h)}
                            style={{
                                padding: '6px 10px', cursor: 'pointer', borderRadius: 6,
                                fontSize: 12, borderBottom: '1px solid rgba(255,255,255,0.04)',
                            }}
                            onMouseEnter={e => (e.currentTarget.style.background = 'rgba(99,102,241,0.08)')}
                            onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                        >
                            <div style={{ fontWeight: 500 }}>{h.question}</div>
                            <div style={{ color: 'rgba(255,255,255,0.3)', fontSize: 10 }}>
                                {new Date(h.time).toLocaleTimeString()}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Input area */}
            <div style={{ position: 'relative' }}>
                <TextArea
                    value={question}
                    onChange={e => setQuestion(e.target.value)}
                    placeholder="Describe what you want to find... e.g., 'Show me the top 10 customers by total spend'"
                    autoSize={{ minRows: 2, maxRows: 5 }}
                    onPressEnter={e => {
                        if (!e.shiftKey) { e.preventDefault(); handleAsk(); }
                    }}
                    style={{
                        fontSize: 14,
                        background: 'rgba(0,0,0,0.2)',
                        border: '1px solid rgba(99,102,241,0.2)',
                        borderRadius: 10,
                        paddingRight: 50,
                    }}
                />
                <Button
                    type="primary"
                    icon={<SendOutlined />}
                    onClick={handleAsk}
                    loading={loading}
                    style={{
                        position: 'absolute', right: 8, bottom: 8,
                        borderRadius: 8,
                    }}
                />
            </div>

            {/* Example queries */}
            {!nlResult && !loading && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                    {EXAMPLE_QUERIES.map((ex, i) => (
                        <Tag
                            key={i}
                            style={{
                                cursor: 'pointer', fontSize: 11, borderRadius: 12,
                                background: 'rgba(99,102,241,0.06)',
                                border: '1px solid rgba(99,102,241,0.12)',
                            }}
                            onClick={() => handleExampleClick(ex)}
                        >
                            {ex}
                        </Tag>
                    ))}
                </div>
            )}

            {/* Loading */}
            {loading && (
                <div style={{ textAlign: 'center', padding: 20 }}>
                    <Spin size="large" />
                    <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 13, marginTop: 8 }}>
                        Analyzing schema and generating SQL...
                    </div>
                </div>
            )}

            {/* Error */}
            {nlResult && !nlResult.success && (
                <Alert
                    type="error"
                    showIcon
                    message="Query Generation Failed"
                    description={nlResult.error || 'An unknown error occurred.'}
                />
            )}

            {/* Result panel */}
            {nlResult && nlResult.success && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                    {/* Explanation */}
                    <div className="glass-panel" style={{ padding: 12 }}>
                        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 8 }}>
                            <BulbOutlined style={{ color: '#faad14', fontSize: 16, marginTop: 2 }} />
                            <div style={{ flex: 1 }}>
                                <div style={{ fontSize: 13, lineHeight: 1.5 }}>
                                    {nlResult.explanation}
                                </div>
                                <div style={{ display: 'flex', gap: 6, marginTop: 6, flexWrap: 'wrap' }}>
                                    <Tag
                                        color={confidenceColors[nlResult.confidence]}
                                        style={{ fontSize: 10 }}
                                    >
                                        <CheckCircleOutlined /> Confidence: {nlResult.confidence}
                                    </Tag>
                                    {nlResult.assumptions.map((a, i) => (
                                        <Tag key={i} style={{ fontSize: 10, opacity: 0.7 }}>
                                            Assumed: {a}
                                        </Tag>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>

                    {/* Clarification needed */}
                    {nlResult.clarification_needed && (
                        <Alert
                            type="warning"
                            showIcon
                            icon={<QuestionCircleOutlined />}
                            message="Clarification Needed"
                            description={nlResult.clarification_needed}
                        />
                    )}

                    {/* Generated SQL */}
                    <div className="glass-panel" style={{ padding: 12 }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
                            <span style={{ fontWeight: 600, fontSize: 13 }}>Generated SQL</span>
                            <Space size={4}>
                                <Button
                                    size="small"
                                    icon={<EditOutlined />}
                                    onClick={() => setIsEditing(!isEditing)}
                                    type={isEditing ? 'primary' : 'default'}
                                >
                                    {isEditing ? 'Editing' : 'Edit'}
                                </Button>
                                <Button
                                    type="primary"
                                    size="small"
                                    icon={<PlayCircleOutlined />}
                                    onClick={handleExecute}
                                    loading={executing}
                                >
                                    Execute
                                </Button>
                            </Space>
                        </div>
                        <TextArea
                            value={isEditing ? editedSQL : nlResult.sql}
                            onChange={e => setEditedSQL(e.target.value)}
                            readOnly={!isEditing}
                            autoSize={{ minRows: 3, maxRows: 15 }}
                            style={{
                                fontFamily: "'Fira Code', 'JetBrains Mono', monospace",
                                fontSize: 12,
                                background: isEditing ? 'rgba(0,0,0,0.3)' : 'rgba(0,0,0,0.2)',
                                border: `1px solid ${isEditing ? 'rgba(99,102,241,0.3)' : 'rgba(255,255,255,0.06)'}`,
                                color: 'rgba(165,180,252,0.9)',
                                lineHeight: 1.6,
                            }}
                        />
                    </div>

                    {/* Refinement input */}
                    <div style={{ display: 'flex', gap: 8 }}>
                        <Input
                            value={refinement}
                            onChange={e => setRefinement(e.target.value)}
                            placeholder="Refine: e.g., 'Also group by month' or 'Show only last 30 days'"
                            onPressEnter={handleRefine}
                            style={{
                                flex: 1, fontSize: 12,
                                background: 'rgba(0,0,0,0.15)',
                                border: '1px solid rgba(139,92,246,0.15)',
                            }}
                        />
                        <Button
                            onClick={handleRefine}
                            icon={<SendOutlined />}
                            loading={loading}
                            disabled={!refinement.trim()}
                        >
                            Refine
                        </Button>
                    </div>
                </div>
            )}
        </div>
    );
};

export default NLQueryPanel;
