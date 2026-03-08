import React, { useState } from 'react';
import { Card, Typography, Divider, Button, Space, message } from 'antd';
import { FilePdfOutlined, FileWordOutlined } from '@ant-design/icons';
import type { AnalystBriefing } from '../../types/insight';
import { API_BASE } from '../../api/ingestion';

const { Title, Paragraph, Text } = Typography;

interface AnalystBriefingProps {
    data: AnalystBriefing;
    fileId: string;
}

const AnalystBriefingPanel: React.FC<AnalystBriefingProps> = ({ data, fileId }) => {
    const [downloadingPdf, setDownloadingPdf] = useState(false);
    const [downloadingDocx, setDownloadingDocx] = useState(false);

    const handleDownload = async (format: 'pdf' | 'docx') => {
        const isPdf = format === 'pdf';
        isPdf ? setDownloadingPdf(true) : setDownloadingDocx(true);

        try {
            // Using window.location trick or fetch to download blob
            const response = await fetch(`${API_BASE}/api/insights/${fileId}/${format}`);
            if (!response.ok) throw new Error('Export Generation Failed');

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `Data_Intelligence_Briefing_${fileId}.${format}`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();

            message.success(`Successfully downloaded ${format.toUpperCase()} report.`);
        } catch (error) {
            message.error(`Failed to download ${format.toUpperCase()} report.`);
            console.error(error);
        } finally {
            isPdf ? setDownloadingPdf(false) : setDownloadingDocx(false);
        }
    };

    return (
        <Card
            className="glass-panel"
            title="Auto-Generated Analyst Briefing"
            variant="borderless"
            style={{ height: '100%', display: 'flex', flexDirection: 'column' }}
            extra={
                <Space>
                    <Button
                        type="primary"
                        icon={<FilePdfOutlined />}
                        onClick={() => handleDownload('pdf')}
                        loading={downloadingPdf}
                    >
                        PDF
                    </Button>
                    <Button
                        icon={<FileWordOutlined />}
                        onClick={() => handleDownload('docx')}
                        loading={downloadingDocx}
                    >
                        DOCX
                    </Button>
                </Space>
            }
        >
            <div style={{ flex: 1, overflowY: 'auto', paddingRight: '10px' }}>
                <Title level={5}>Executive Summary</Title>
                <Paragraph>{data.executive_summary}</Paragraph>

                <Title level={5}>Dataset Characteristics</Title>
                <Paragraph>{data.dataset_characteristics}</Paragraph>

                <Title level={5}>Quality Assessment</Title>
                <Paragraph>{data.quality_assessment}</Paragraph>

                <Divider style={{ borderColor: 'rgba(255,255,255,0.1)' }} />

                <Title level={5}>Key Findings</Title>
                <ul>
                    {data.key_findings.map((finding, idx) => (
                        <li key={idx} style={{ marginBottom: '8px' }}>
                            <Text>{finding}</Text>
                        </li>
                    ))}
                </ul>

                <Title level={5}>Recommended Actions</Title>
                <ul>
                    {data.recommended_actions.map((action, idx) => (
                        <li key={idx} style={{ marginBottom: '8px' }}>
                            <Text strong>{action}</Text>
                        </li>
                    ))}
                </ul>

                {data.column_deep_dives && data.column_deep_dives.length > 0 && (
                    <>
                        <Divider style={{ borderColor: 'rgba(255,255,255,0.1)' }} />
                        <Title level={5}>Column Deep-Dive (Layman Explanations)</Title>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', marginTop: '16px' }}>
                            {data.column_deep_dives.map((col, idx) => (
                                <Card key={idx} size="small" variant="borderless" style={{ background: 'rgba(255,255,255,0.02)' }}>
                                    <Text strong style={{ color: '#177ddc', fontSize: '16px' }}>{col.column_name}</Text>
                                    <Paragraph style={{ marginTop: '8px', marginBottom: '8px' }}>
                                        <Text type="secondary">Mathematical Summary: </Text><br />
                                        <Text style={{ fontFamily: 'monospace', fontSize: '12px', color: '#8c8c8c' }}>{col.mathematical_summary}</Text>
                                    </Paragraph>
                                    <Paragraph style={{ margin: 0 }}>
                                        <Text type="secondary">Layman Explanation: </Text><br />
                                        <Text style={{ fontSize: '14px', lineHeight: '1.5' }}>{col.layman_explanation}</Text>
                                    </Paragraph>
                                </Card>
                            ))}
                        </div>
                    </>
                )}
            </div>
        </Card>
    );
};

export default AnalystBriefingPanel;

