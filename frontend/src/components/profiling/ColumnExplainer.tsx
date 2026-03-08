/**
 * ColumnExplainer — One-click deep-dive narrative for any column.
 * Generates a 3-5 paragraph analysis using AI.
 */

import React, { useState, useCallback } from 'react';
import { Modal, Button, Spin, Tag, Space, Empty } from 'antd';
import { FileSearchOutlined, RobotOutlined, CopyOutlined } from '@ant-design/icons';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

interface Props {
    column: string;
    fileId: string;
    trigger?: React.ReactNode;
}

const ColumnExplainer: React.FC<Props> = ({ column, fileId, trigger }) => {
    const [open, setOpen] = useState(false);
    const [loading, setLoading] = useState(false);
    const [explanation, setExplanation] = useState<string | null>(null);

    const generate = useCallback(async () => {
        setOpen(true);
        setLoading(true);
        try {
            const res = await fetch(`${API_BASE}/api/explain/column`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, column }),
            });
            const data = await res.json();
            setExplanation(data.explanation || 'Unable to generate explanation.');
        } catch (e: any) {
            setExplanation(`Error: ${e.message}`);
        } finally {
            setLoading(false);
        }
    }, [column, fileId]);

    const copyToClipboard = useCallback(() => {
        if (explanation) navigator.clipboard.writeText(explanation);
    }, [explanation]);

    return (
        <>
            {trigger ? (
                <div onClick={generate} style={{ cursor: 'pointer' }}>{trigger}</div>
            ) : (
                <Button size="small" icon={<FileSearchOutlined />} onClick={generate}>
                    Explain "{column}"
                </Button>
            )}

            <Modal
                title={
                    <Space>
                        <RobotOutlined style={{ color: '#6366f1' }} />
                        <span>Column Deep Dive: <strong>{column}</strong></span>
                        <Tag color="purple" style={{ fontSize: 10 }}>AI</Tag>
                    </Space>
                }
                open={open}
                onCancel={() => setOpen(false)}
                width={600}
                footer={[
                    <Button key="copy" icon={<CopyOutlined />} onClick={copyToClipboard}>
                        Copy
                    </Button>,
                    <Button key="close" type="primary" onClick={() => setOpen(false)}>
                        Close
                    </Button>,
                ]}
            >
                {loading ? (
                    <div style={{ textAlign: 'center', padding: 40 }}>
                        <Spin size="large" />
                        <div style={{ marginTop: 12, opacity: 0.6 }}>Analyzing column...</div>
                    </div>
                ) : explanation ? (
                    <div style={{
                        lineHeight: 1.8, fontSize: 14,
                        whiteSpace: 'pre-wrap',
                    }}>
                        {explanation}
                    </div>
                ) : (
                    <Empty description="No analysis available" />
                )}
            </Modal>
        </>
    );
};

export default ColumnExplainer;

