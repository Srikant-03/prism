/**
 * ReportPanel â€” Full Pillar 4: Report generation, Code export, Data export.
 * Three sections with download controls for all formats.
 */

import React, { useState, useCallback } from 'react';
import { Button, Tag, Alert, Spin, Empty, Switch, Space as AntSpace, Checkbox } from 'antd';
import {
    FileTextOutlined, DownloadOutlined, CodeOutlined, DatabaseOutlined,
    FilePdfOutlined, FileWordOutlined, Html5Outlined, BookOutlined,
    FileExcelOutlined, ThunderboltOutlined,
    BarChartOutlined,
} from '@ant-design/icons';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

interface Props {
    fileId: string | null;
}

interface ReportData {
    title: string;
    generated_at: string;
    sections: Array<{
        title: string;
        content: string;
        tables: Array<{
            title: string;
            headers: string[];
            rows: string[][];
        }>;
    }>;
}

const ReportPanel: React.FC<Props> = ({ fileId }) => {
    const [report, setReport] = useState<ReportData | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [downloading, setDownloading] = useState<string | null>(null);
    const [includeCharts, setIncludeCharts] = useState(true);
    const [includeOutliers, setIncludeOutliers] = useState(true);

    const generateReport = useCallback(async () => {
        if (!fileId) return;
        setLoading(true);
        setError(null);

        try {
            const res = await fetch(`${API_BASE}/api/reporting/generate`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    file_id: fileId,
                    format: 'json',
                    include_charts: includeCharts,
                    include_outliers: includeOutliers
                }),
            });
            const data = await res.json();
            if (data.sections) {
                setReport(data);
            } else {
                setError('Failed to generate report.');
            }
        } catch (e: any) {
            setError(e.message);
        } finally {
            setLoading(false);
        }
    }, [fileId, includeCharts, includeOutliers]);

    const downloadReport = useCallback(async (format: string) => {
        if (!fileId) return;
        setDownloading(format);
        try {
            const res = await fetch(`${API_BASE}/api/reporting/generate`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    file_id: fileId,
                    format,
                    include_charts: includeCharts,
                    include_outliers: includeOutliers
                }),
            });
            const blob = await res.blob();
            const ext = { html: 'html', pdf: 'pdf', docx: 'docx', notebook: 'ipynb' }[format] || format;
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `report.${ext}`;
            a.click();
        } catch (e: any) {
            setError(e.message);
        } finally {
            setDownloading(null);
        }
    }, [fileId, includeCharts, includeOutliers]);

    const downloadCode = useCallback(async (format: string) => {
        if (!fileId) return;
        setDownloading(`code_${format}`);
        try {
            const res = await fetch(`${API_BASE}/api/reporting/export-code`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, format }),
            });
            const blob = await res.blob();
            const extMap: Record<string, string> = { python: 'py', notebook: 'ipynb', json_pipeline: 'json', sql: 'sql' };
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `pipeline.${extMap[format] || format}`;
            a.click();
        } catch (e: any) {
            setError(e.message);
        } finally {
            setDownloading(null);
        }
    }, [fileId]);

    const downloadData = useCallback(async (format: string) => {
        if (!fileId) return;
        setDownloading(`data_${format}`);
        try {
            const res = await fetch(`${API_BASE}/api/reporting/export-data`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, format, use_cleaned: true }),
            });
            const blob = await res.blob();
            const extMap: Record<string, string> = { csv: 'csv', excel: 'xlsx', json: 'json', parquet: 'parquet', feather: 'feather', sql: 'sql' };
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `data.${extMap[format] || format}`;
            a.click();
        } catch (e: any) {
            setError(e.message);
        } finally {
            setDownloading(null);
        }
    }, [fileId]);

    if (!fileId) {
        return (
            <div className="glass-panel" style={{ padding: 24, textAlign: 'center' }}>
                <Empty description="Upload and profile a dataset first" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            </div>
        );
    }

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {error && <Alert type="error" message={error} closable onClose={() => setError(null)} />}

            {/* â”€â”€ 4.1: Full Analysis Report â”€â”€ */}
            <div className="glass-panel" style={{ padding: 16 }}>
                <div style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    marginBottom: 16, borderBottom: '1px solid rgba(255,255,255,0.06)',
                    paddingBottom: 12
                }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <FileTextOutlined style={{ color: '#6366f1', fontSize: 18 }} />
                        <span style={{ fontWeight: 700, fontSize: 16 }}>Full Analysis Report</span>
                    </div>
                    <AntSpace>
                        <AntSpace size={16} style={{ marginRight: 24 }}>
                            <AntSpace size={4}>
                                <BarChartOutlined style={{ fontSize: 12, opacity: 0.6 }} />
                                <span style={{ fontSize: 11 }}>Include Charts</span>
                                <Switch size="small" checked={includeCharts} onChange={setIncludeCharts} />
                            </AntSpace>
                            <Checkbox checked={includeOutliers} onChange={e => setIncludeOutliers(e.target.checked)}>
                                <span style={{ fontSize: 11 }}>Anomaly Deep-dive</span>
                            </Checkbox>
                        </AntSpace>
                        <Button type="primary" icon={<ThunderboltOutlined />}
                            onClick={generateReport} loading={loading}>
                            Generate Report
                        </Button>
                    </AntSpace>
                </div>

                {loading && <div style={{ textAlign: 'center', padding: 20 }}><Spin size="large" /></div>}

                {report && !loading && (
                    <>
                        {/* Report preview */}
                        <div style={{
                            maxHeight: 400, overflow: 'auto', marginBottom: 12,
                            padding: 12, background: 'rgba(0,0,0,0.15)', borderRadius: 8,
                        }}>
                            <h3 style={{ color: '#a5b4fc', margin: '0 0 8px 0', fontSize: 14 }}>
                                {report.title}
                            </h3>
                            <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.3)', marginBottom: 12 }}>
                                Generated: {new Date(report.generated_at).toLocaleString()}
                            </div>

                            {report.sections.map((section, i) => (
                                <div key={i} style={{ marginBottom: 16 }}>
                                    <h4 style={{ color: '#79c0ff', margin: '0 0 4px 0', fontSize: 13 }}>
                                        {i + 1}. {section.title}
                                    </h4>
                                    <p style={{
                                        fontSize: 12, color: 'rgba(255,255,255,0.7)',
                                        lineHeight: 1.6, margin: '0 0 8px 0',
                                        whiteSpace: 'pre-wrap',
                                    }}>
                                        {section.content}
                                    </p>

                                    {section.tables?.map((table, j) => (
                                        <div key={j} style={{ marginTop: 6 }}>
                                            <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 4 }}>
                                                {table.title}
                                            </div>
                                            <div style={{ overflow: 'auto' }}>
                                                <table style={{
                                                    borderCollapse: 'collapse', fontSize: 11, width: '100%',
                                                }}>
                                                    <thead>
                                                        <tr>
                                                            {table.headers.map(h => (
                                                                <th key={h} style={{
                                                                    padding: '4px 8px',
                                                                    borderBottom: '1px solid rgba(255,255,255,0.1)',
                                                                    textAlign: 'left', fontWeight: 600,
                                                                    color: '#a5b4fc', fontSize: 10,
                                                                }}>{h}</th>
                                                            ))}
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {table.rows.slice(0, 20).map((row, ri) => (
                                                            <tr key={ri}>
                                                                {row.map((cell, ci) => (
                                                                    <td key={ci} style={{
                                                                        padding: '3px 8px',
                                                                        borderBottom: '1px solid rgba(255,255,255,0.04)',
                                                                        fontSize: 10,
                                                                    }}>{cell}</td>
                                                                ))}
                                                            </tr>
                                                        ))}
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            ))}
                        </div>

                        {/* Export buttons */}
                        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                            <Button icon={<Html5Outlined />} onClick={() => downloadReport('html')}
                                loading={downloading === 'html'}>HTML</Button>
                            <Button icon={<FilePdfOutlined />} onClick={() => downloadReport('pdf')}
                                loading={downloading === 'pdf'}>PDF</Button>
                            <Button icon={<FileWordOutlined />} onClick={() => downloadReport('docx')}
                                loading={downloading === 'docx'}>DOCX</Button>
                            <Button icon={<BookOutlined />} onClick={() => downloadReport('notebook')}
                                loading={downloading === 'notebook'}>Notebook</Button>
                        </div>
                    </>
                )}
            </div>

            {/* â”€â”€ 4.2: Code Export â”€â”€ */}
            <div className="glass-panel" style={{ padding: 16 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                    <CodeOutlined style={{ color: '#34d399', fontSize: 18 }} />
                    <span style={{ fontWeight: 700, fontSize: 16 }}>Code Export</span>
                    <Tag color="green" style={{ fontSize: 10 }}>Pipeline as Code</Tag>
                </div>

                <p style={{ fontSize: 12, color: 'rgba(255,255,255,0.5)', margin: '0 0 12px 0' }}>
                    Export the preprocessing pipeline as production-ready, reusable code.
                </p>

                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    <Button icon={<CodeOutlined />} onClick={() => downloadCode('python')}
                        loading={downloading === 'code_python'}>
                        ðŸ Python Script
                    </Button>
                    <Button icon={<BookOutlined />} onClick={() => downloadCode('notebook')}
                        loading={downloading === 'code_notebook'}>
                        ðŸ““ Jupyter Notebook
                    </Button>
                    <Button icon={<DatabaseOutlined />} onClick={() => downloadCode('json_pipeline')}
                        loading={downloading === 'code_json_pipeline'}>
                        ðŸ“‹ JSON Pipeline
                    </Button>
                    <Button icon={<CodeOutlined />} onClick={() => downloadCode('sql')}
                        loading={downloading === 'code_sql'}>
                        ðŸ—„ï¸ SQL Queries
                    </Button>
                </div>
            </div>

            {/* â”€â”€ 4.3: Data Export â”€â”€ */}
            <div className="glass-panel" style={{ padding: 16 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                    <DatabaseOutlined style={{ color: '#f59e0b', fontSize: 18 }} />
                    <span style={{ fontWeight: 700, fontSize: 16 }}>Data Export</span>
                    <Tag color="orange" style={{ fontSize: 10 }}>Cleaned Dataset</Tag>
                </div>

                <p style={{ fontSize: 12, color: 'rgba(255,255,255,0.5)', margin: '0 0 12px 0' }}>
                    Export the cleaned (or raw) dataset in the format you need.
                </p>

                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    <Button icon={<DownloadOutlined />} onClick={() => downloadData('csv')}
                        loading={downloading === 'data_csv'}>CSV</Button>
                    <Button icon={<FileExcelOutlined />} onClick={() => downloadData('excel')}
                        loading={downloading === 'data_excel'}>Excel</Button>
                    <Button icon={<DownloadOutlined />} onClick={() => downloadData('json')}
                        loading={downloading === 'data_json'}>JSON</Button>
                    <Button icon={<DownloadOutlined />} onClick={() => downloadData('parquet')}
                        loading={downloading === 'data_parquet'}>Parquet</Button>
                    <Button icon={<DownloadOutlined />} onClick={() => downloadData('feather')}
                        loading={downloading === 'data_feather'}>Feather</Button>
                    <Button icon={<DatabaseOutlined />} onClick={() => downloadData('sql')}
                        loading={downloading === 'data_sql'}>SQL INSERT</Button>
                </div>
            </div>
        </div>
    );
};

export default ReportPanel;

