/**
 * QueryLibrary â€” Save, load, tag, and share queries.
 * Parameterized queries turn any query into a reusable template.
 */

import React, { useState } from 'react';
import {
    Button, Input, Tag, Space, Empty, Modal, Tooltip,
    Badge, Popconfirm,
} from 'antd';
import {
    SaveOutlined, PlayCircleOutlined, DeleteOutlined,
    ExportOutlined, ImportOutlined, SearchOutlined,
    TagsOutlined, EditOutlined, CopyOutlined,
} from '@ant-design/icons';

const { TextArea } = Input;

interface SavedQuery {
    id: string;
    name: string;
    description: string;
    sql: string;
    tags: string[];
    params: Array<{ name: string; default: string; description: string }>;
    createdAt: number;
    lastUsed: number;
    runCount: number;
}

interface Props {
    currentSQL: string;
    onLoadQuery: (sql: string) => void;
    onExecuteQuery: (sql: string) => void;
}

const STORAGE_KEY = 'sql_query_library';

const QueryLibrary: React.FC<Props> = ({ currentSQL, onLoadQuery, onExecuteQuery }) => {
    const [queries, setQueries] = useState<SavedQuery[]>(() => {
        try {
            return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
        } catch { return []; }
    });
    const [search, setSearch] = useState('');
    const [showSave, setShowSave] = useState(false);
    const [editId, setEditId] = useState<string | null>(null);
    const [form, setForm] = useState({ name: '', description: '', tags: '' as string, sql: '' });
    const [paramValues, setParamValues] = useState<Record<string, string>>({});

    const save = (list: SavedQuery[]) => {
        setQueries(list);
        localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
    };

    const openSaveDialog = () => {
        setForm({
            name: '',
            description: '',
            tags: '',
            sql: currentSQL,
        });
        setEditId(null);
        setShowSave(true);
    };

    const handleSave = () => {
        const paramMatches = form.sql.match(/\{\{(\w+)\}\}/g) || [];
        const paramNames = [...new Set(paramMatches.map(m => m.replace(/\{\{|\}\}/g, '')))];

        const query: SavedQuery = {
            id: editId || Date.now().toString(36),
            name: form.name,
            description: form.description,
            sql: form.sql,
            tags: form.tags.split(',').map(t => t.trim()).filter(Boolean),
            params: paramNames.map(p => ({ name: p, default: '', description: '' })),
            createdAt: editId ? (queries.find(q => q.id === editId)?.createdAt || Date.now()) : Date.now(),
            lastUsed: Date.now(),
            runCount: editId ? (queries.find(q => q.id === editId)?.runCount || 0) : 0,
        };

        const updated = editId
            ? queries.map(q => q.id === editId ? query : q)
            : [query, ...queries];
        save(updated);
        setShowSave(false);
    };

    const handleRun = (query: SavedQuery) => {
        let sql = query.sql;
        for (const p of query.params) {
            const val = paramValues[`${query.id}_${p.name}`] || p.default;
            sql = sql.replaceAll(`{{${p.name}}}`, val);
        }

        const updated = queries.map(q =>
            q.id === query.id ? { ...q, lastUsed: Date.now(), runCount: q.runCount + 1 } : q
        );
        save(updated);
        onExecuteQuery(sql);
    };

    const handleDelete = (id: string) => {
        save(queries.filter(q => q.id !== id));
    };

    const handleExport = () => {
        const blob = new Blob([JSON.stringify(queries, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = 'query_library.json'; a.click();
    };

    const handleImport = () => {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.json';
        input.onchange = async (e: any) => {
            const file = e.target.files?.[0];
            if (!file) return;
            const text = await file.text();
            try {
                const imported = JSON.parse(text);
                save([...imported, ...queries]);
            } catch { /* */ }
        };
        input.click();
    };

    const filtered = queries.filter(q =>
        q.name.toLowerCase().includes(search.toLowerCase()) ||
        q.description.toLowerCase().includes(search.toLowerCase()) ||
        q.tags.some(t => t.toLowerCase().includes(search.toLowerCase()))
    );

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 12px',
                background: 'linear-gradient(135deg, rgba(59,130,246,0.06), rgba(99,102,241,0.06))',
                borderRadius: 10, border: '1px solid rgba(59,130,246,0.15)',
            }}>
                <Space>
                    <SaveOutlined style={{ color: '#3b82f6', fontSize: 16 }} />
                    <span style={{ fontWeight: 700, fontSize: 14 }}>Query Library</span>
                    <Badge count={queries.length}
                        style={{ backgroundColor: 'rgba(59,130,246,0.2)', color: '#93c5fd', fontSize: 10 }} />
                </Space>
                <Space size={4}>
                    <Input prefix={<SearchOutlined style={{ color: 'rgba(255,255,255,0.3)' }} />}
                        placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)}
                        size="small" style={{ width: 150 }} allowClear />
                    <Button size="small" icon={<SaveOutlined />} type="primary" onClick={openSaveDialog}
                        disabled={!currentSQL}>Save Current</Button>
                    <Tooltip title="Export library"><Button size="small" icon={<ExportOutlined />} onClick={handleExport} /></Tooltip>
                    <Tooltip title="Import"><Button size="small" icon={<ImportOutlined />} onClick={handleImport} /></Tooltip>
                </Space>
            </div>

            {/* Saved queries */}
            {filtered.length === 0 ? (
                <Empty description={queries.length ? "No matches" : "No saved queries yet"}
                    image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6, maxHeight: 500, overflow: 'auto' }}>
                    {filtered.map(q => (
                        <div key={q.id} className="glass-panel" style={{ padding: 10 }}>
                            <div style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
                            }}>
                                <div style={{ flex: 1 }}>
                                    <div style={{ fontWeight: 600, fontSize: 13 }}>{q.name}</div>
                                    {q.description && (
                                        <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.4)', marginTop: 2 }}>
                                            {q.description}
                                        </div>
                                    )}
                                    <div style={{ display: 'flex', gap: 4, marginTop: 4, flexWrap: 'wrap' }}>
                                        {q.tags.map(t => <Tag key={t} style={{ fontSize: 10 }}>{t}</Tag>)}
                                        <span style={{ fontSize: 10, color: 'rgba(255,255,255,0.3)' }}>
                                            Used {q.runCount}Ã— Â· {new Date(q.lastUsed).toLocaleDateString()}
                                        </span>
                                    </div>
                                </div>
                                <Space size={4}>
                                    <Tooltip title="Load SQL">
                                        <Button size="small" icon={<CopyOutlined />}
                                            onClick={() => onLoadQuery(q.sql)} />
                                    </Tooltip>
                                    <Tooltip title="Edit">
                                        <Button size="small" icon={<EditOutlined />}
                                            onClick={() => {
                                                setForm({ name: q.name, description: q.description, tags: q.tags.join(', '), sql: q.sql });
                                                setEditId(q.id);
                                                setShowSave(true);
                                            }} />
                                    </Tooltip>
                                    <Button type="primary" size="small" icon={<PlayCircleOutlined />}
                                        onClick={() => handleRun(q)}>Run</Button>
                                    <Popconfirm title="Delete?" onConfirm={() => handleDelete(q.id)}>
                                        <Button size="small" danger icon={<DeleteOutlined />} />
                                    </Popconfirm>
                                </Space>
                            </div>

                            {/* Parameter inputs */}
                            {q.params.length > 0 && (
                                <div style={{
                                    display: 'flex', gap: 8, marginTop: 6,
                                    padding: '6px 0', borderTop: '1px solid rgba(255,255,255,0.04)',
                                    flexWrap: 'wrap',
                                }}>
                                    {q.params.map(p => (
                                        <div key={p.name} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                                            <span style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)' }}>{p.name}:</span>
                                            <Input size="small" style={{ width: 100 }}
                                                value={paramValues[`${q.id}_${p.name}`] || p.default}
                                                onChange={e => setParamValues(prev => ({
                                                    ...prev, [`${q.id}_${p.name}`]: e.target.value,
                                                }))}
                                                placeholder={p.default || p.name}
                                            />
                                        </div>
                                    ))}
                                </div>
                            )}

                            {/* SQL preview */}
                            <div style={{
                                marginTop: 6, padding: 6, fontSize: 10, fontFamily: 'monospace',
                                color: 'rgba(165,180,252,0.6)', background: 'rgba(0,0,0,0.15)',
                                borderRadius: 4, maxHeight: 60, overflow: 'hidden',
                                whiteSpace: 'pre-wrap',
                            }}>
                                {q.sql.slice(0, 200)}{q.sql.length > 200 ? '...' : ''}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Save modal */}
            <Modal title={editId ? "Edit Query" : "Save Query"} open={showSave}
                onOk={handleSave} onCancel={() => setShowSave(false)}
                okText="Save" okButtonProps={{ disabled: !form.name.trim() }}>
                <Space direction="vertical" style={{ width: '100%' }}>
                    <Input placeholder="Query name" value={form.name}
                        onChange={e => setForm(f => ({ ...f, name: e.target.value }))} />
                    <Input placeholder="Description (optional)" value={form.description}
                        onChange={e => setForm(f => ({ ...f, description: e.target.value }))} />
                    <Input placeholder="Tags (comma separated)" value={form.tags}
                        prefix={<TagsOutlined />}
                        onChange={e => setForm(f => ({ ...f, tags: e.target.value }))} />
                    <TextArea value={form.sql} rows={4} style={{ fontFamily: 'monospace', fontSize: 12 }}
                        onChange={e => setForm(f => ({ ...f, sql: e.target.value }))} />
                    <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.4)' }}>
                        ðŸ’¡ Use {"{{param_name}}"} in SQL to create parameterized queries
                    </div>
                </Space>
            </Modal>
        </div>
    );
};

export default QueryLibrary;

