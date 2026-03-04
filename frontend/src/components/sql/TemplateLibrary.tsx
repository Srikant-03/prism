/**
 * TemplateLibrary — Schema-aware, auto-generated query template browser.
 * Organized by category with parameterized SQL and one-click run.
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
    Collapse, Button, Input, InputNumber, Select,
    Space, Badge, Empty, Spin, Tooltip,
} from 'antd';
import {
    PlayCircleOutlined, CodeOutlined, SearchOutlined,
    AuditOutlined, BarChartOutlined, FundOutlined,
    PieChartOutlined, TeamOutlined, ExperimentOutlined,
    SafetyOutlined, NodeIndexOutlined, TrophyOutlined,
    AlertOutlined, CheckCircleOutlined, DatabaseOutlined,
} from '@ant-design/icons';
import * as sqlApi from '../../api/sql';
import type { QueryTemplate, QueryResult } from '../../types/sql';

const CATEGORY_ICONS: Record<string, React.ReactNode> = {
    'Data Audit': <AuditOutlined />,
    'Top-N Analysis': <TrophyOutlined />,
    'Distribution Analysis': <BarChartOutlined />,
    'Completeness': <CheckCircleOutlined />,
    'Trend Analysis': <FundOutlined />,
    'Performance': <PieChartOutlined />,
    'Funnel & Conversion': <NodeIndexOutlined />,
    'Cohort Analysis': <TeamOutlined />,
    'Outlier Detection': <AlertOutlined />,
    'Segmentation': <ExperimentOutlined />,
    'Ranking': <TrophyOutlined />,
    'Relationships': <DatabaseOutlined />,
};

const CATEGORY_COLORS: Record<string, string> = {
    'Data Audit': '#69b1ff',
    'Top-N Analysis': '#ffc069',
    'Distribution Analysis': '#95de64',
    'Completeness': '#5cdbd3',
    'Trend Analysis': '#b37feb',
    'Performance': '#ff9c6e',
    'Funnel & Conversion': '#f759ab',
    'Cohort Analysis': '#597ef7',
    'Outlier Detection': '#ff7875',
    'Segmentation': '#36cfc9',
    'Ranking': '#ffd666',
    'Relationships': '#85a5ff',
};

interface Props {
    tableName: string;
    onResultReady: (result: QueryResult) => void;
    onSQLGenerated?: (sql: string) => void;
}

const TemplateLibrary: React.FC<Props> = ({ tableName, onResultReady, onSQLGenerated }) => {
    const [categories, setCategories] = useState<Record<string, QueryTemplate[]>>({});
    const [templateCount, setTemplateCount] = useState(0);
    const [loading, setLoading] = useState(false);
    const [search, setSearch] = useState('');
    const [paramValues, setParamValues] = useState<Record<string, Record<string, any>>>({});
    const [runningTemplate, setRunningTemplate] = useState<string | null>(null);
    const [previewSQL, setPreviewSQL] = useState<string | null>(null);

    const loadTemplates = useCallback(async () => {
        if (!tableName) return;
        setLoading(true);
        try {
            const res = await sqlApi.fetchTemplates(tableName);
            setCategories(res.categories || {});
            setTemplateCount(res.template_count || 0);

            // Initialize param defaults
            const defaults: Record<string, Record<string, any>> = {};
            for (const cat of Object.values(res.categories || {}) as QueryTemplate[][]) {
                for (const tmpl of cat) {
                    const key = tmpl.title;
                    defaults[key] = {};
                    for (const p of tmpl.params) {
                        defaults[key][p.name] = p.default ?? '';
                    }
                }
            }
            setParamValues(defaults);
        } catch (e) {
            console.error('Failed to load templates', e);
        } finally {
            setLoading(false);
        }
    }, [tableName]);

    useEffect(() => { loadTemplates(); }, [loadTemplates]);

    const setParam = (templateTitle: string, paramName: string, value: any) => {
        setParamValues(prev => ({
            ...prev,
            [templateTitle]: { ...(prev[templateTitle] || {}), [paramName]: value },
        }));
    };

    const getResolvedSQL = (template: QueryTemplate) => {
        let sql = template.sql;
        const vals = paramValues[template.title] || {};
        for (const p of template.params) {
            const placeholder = `{{${p.name}}}`;
            sql = sql.replaceAll(placeholder, String(vals[p.name] ?? p.default ?? ''));
        }
        return sql;
    };

    const handleRun = async (template: QueryTemplate) => {
        const sql = getResolvedSQL(template);
        setRunningTemplate(template.title);
        onSQLGenerated?.(sql);

        try {
            const params = paramValues[template.title] || {};
            const result = await sqlApi.executeTemplate(template.sql, params);
            onResultReady(result);
        } catch (e: any) {
            onResultReady({
                success: false, columns: [], rows: [], row_count: 0,
                error: e.message,
            });
        } finally {
            setRunningTemplate(null);
        }
    };

    const handlePreview = (template: QueryTemplate) => {
        const sql = getResolvedSQL(template);
        setPreviewSQL(previewSQL === sql ? null : sql);
        onSQLGenerated?.(sql);
    };

    // Filter templates by search
    const filteredCategories: Record<string, QueryTemplate[]> = {};
    const searchLower = search.toLowerCase();
    for (const [cat, templates] of Object.entries(categories)) {
        const filtered = templates.filter(t =>
            t.title.toLowerCase().includes(searchLower) ||
            t.description.toLowerCase().includes(searchLower) ||
            cat.toLowerCase().includes(searchLower)
        );
        if (filtered.length > 0) {
            filteredCategories[cat] = filtered;
        }
    }

    const renderParamInput = (template: QueryTemplate, param: QueryTemplate['params'][0]) => {
        const val = paramValues[template.title]?.[param.name] ?? param.default ?? '';

        switch (param.type) {
            case 'number':
                return (
                    <InputNumber
                        size="small"
                        value={val}
                        onChange={v => setParam(template.title, param.name, v)}
                        style={{ width: 80 }}
                    />
                );
            case 'select':
                return (
                    <Select
                        size="small"
                        value={val}
                        onChange={v => setParam(template.title, param.name, v)}
                        style={{ width: 100 }}
                        options={(param.options || []).map(o => ({ value: o, label: o }))}
                    />
                );
            case 'column_value':
                return (
                    <Input
                        size="small"
                        value={val}
                        onChange={e => setParam(template.title, param.name, e.target.value)}
                        placeholder={param.label}
                        style={{ width: 120 }}
                    />
                );
            default:
                return (
                    <Input
                        size="small"
                        value={val}
                        onChange={e => setParam(template.title, param.name, e.target.value)}
                        placeholder={param.label}
                        style={{ width: 120 }}
                    />
                );
        }
    };

    if (loading) {
        return (
            <div style={{ textAlign: 'center', padding: 40 }}>
                <Spin size="large" />
                <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 13, marginTop: 8 }}>
                    Analyzing schema and generating templates...
                </div>
            </div>
        );
    }

    if (!tableName) {
        return (
            <Empty
                description="Select a table to generate query templates"
                image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
        );
    }

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 12px',
                background: 'linear-gradient(135deg, rgba(34,197,94,0.06), rgba(59,130,246,0.06))',
                borderRadius: 10, border: '1px solid rgba(34,197,94,0.15)',
            }}>
                <Space>
                    <SafetyOutlined style={{ color: '#22c55e', fontSize: 18 }} />
                    <span style={{ fontWeight: 700, fontSize: 14 }}>Smart Templates</span>
                    <Badge
                        count={templateCount}
                        style={{ backgroundColor: 'rgba(34,197,94,0.25)', color: '#86efac', fontSize: 10 }}
                    />
                </Space>
                <Input
                    prefix={<SearchOutlined style={{ color: 'rgba(255,255,255,0.3)' }} />}
                    placeholder="Search templates..."
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    allowClear
                    size="small"
                    style={{ width: 200, background: 'rgba(0,0,0,0.15)' }}
                />
            </div>

            {/* Template categories */}
            {Object.keys(filteredCategories).length === 0 ? (
                <Empty description="No templates match your search" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
                <Collapse
                    accordion
                    expandIconPosition="end"
                    style={{ background: 'transparent', border: 'none' }}
                    items={Object.entries(filteredCategories).map(([cat, templates]) => ({
                        key: cat,
                        label: (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                <span style={{ color: CATEGORY_COLORS[cat] || '#fff' }}>
                                    {CATEGORY_ICONS[cat] || <CodeOutlined />}
                                </span>
                                <span style={{ fontWeight: 600, fontSize: 13 }}>{cat}</span>
                                <Badge
                                    count={templates.length}
                                    style={{
                                        backgroundColor: 'rgba(255,255,255,0.08)',
                                        color: 'rgba(255,255,255,0.5)',
                                        fontSize: 10,
                                    }}
                                />
                            </div>
                        ),
                        children: (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                {templates.map((tmpl, i) => (
                                    <div
                                        key={i}
                                        className="glass-panel"
                                        style={{
                                            padding: 12,
                                            border: '1px solid rgba(255,255,255,0.04)',
                                        }}
                                    >
                                        <div style={{
                                            display: 'flex', justifyContent: 'space-between',
                                            alignItems: 'flex-start', marginBottom: 6,
                                        }}>
                                            <div>
                                                <div style={{ fontWeight: 600, fontSize: 13 }}>
                                                    {tmpl.title}
                                                </div>
                                                <div style={{
                                                    fontSize: 12, color: 'rgba(255,255,255,0.5)',
                                                    marginTop: 2,
                                                }}>
                                                    {tmpl.description}
                                                </div>
                                            </div>
                                            <Space size={4}>
                                                <Tooltip title="Preview SQL">
                                                    <Button
                                                        size="small"
                                                        icon={<CodeOutlined />}
                                                        onClick={() => handlePreview(tmpl)}
                                                    />
                                                </Tooltip>
                                                <Button
                                                    type="primary"
                                                    size="small"
                                                    icon={<PlayCircleOutlined />}
                                                    onClick={() => handleRun(tmpl)}
                                                    loading={runningTemplate === tmpl.title}
                                                >
                                                    Run
                                                </Button>
                                            </Space>
                                        </div>

                                        {/* Params */}
                                        {tmpl.params.length > 0 && (
                                            <div style={{
                                                display: 'flex', flexWrap: 'wrap', gap: 8,
                                                padding: '6px 0',
                                                borderTop: '1px solid rgba(255,255,255,0.04)',
                                            }}>
                                                {tmpl.params.map((p, j) => (
                                                    <div key={j} style={{
                                                        display: 'flex', alignItems: 'center', gap: 4,
                                                    }}>
                                                        <span style={{ fontSize: 11, color: 'rgba(255,255,255,0.4)' }}>
                                                            {p.label}:
                                                        </span>
                                                        {renderParamInput(tmpl, p)}
                                                    </div>
                                                ))}
                                            </div>
                                        )}

                                        {/* SQL Preview */}
                                        {previewSQL === getResolvedSQL(tmpl) && (
                                            <div style={{
                                                marginTop: 8, padding: 8,
                                                background: 'rgba(0,0,0,0.2)',
                                                borderRadius: 6, fontSize: 11,
                                                fontFamily: "'Fira Code', monospace",
                                                color: 'rgba(165,180,252,0.8)',
                                                whiteSpace: 'pre-wrap',
                                                lineHeight: 1.5,
                                            }}>
                                                {getResolvedSQL(tmpl)}
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        ),
                        style: {
                            marginBottom: 4,
                            background: 'rgba(255,255,255,0.02)',
                            border: '1px solid rgba(255,255,255,0.04)',
                            borderRadius: 8,
                        },
                    }))}
                />
            )}
        </div>
    );
};

export default TemplateLibrary;
