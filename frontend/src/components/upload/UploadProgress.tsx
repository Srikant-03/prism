/**
 * UploadProgress â€” Animated progress bar with ETA, memory usage, and stage label.
 */

import React from 'react';
import { Progress, Typography, Space, Tag, Card } from 'antd';
import {
    CloudUploadOutlined,
    SearchOutlined,
    ThunderboltOutlined,
    SafetyCertificateOutlined,
    CheckCircleOutlined,
    ExclamationCircleOutlined,
    LoadingOutlined,
} from '@ant-design/icons';
import type { IngestionStage } from '../../types/ingestion';

const { Text } = Typography;

interface UploadProgressProps {
    uploadPct: number;
    stage?: IngestionStage;
    progressPct?: number;
    etaSeconds?: number | null;
    memoryMb?: number;
    message?: string;
    bytesRead?: number;
    totalBytes?: number;
}

const STAGE_CONFIG: Record<IngestionStage, { icon: React.ReactNode; label: string; color: string }> = {
    uploading: { icon: <CloudUploadOutlined spin />, label: 'Uploading', color: '#1677ff' },
    detecting_format: { icon: <SearchOutlined />, label: 'Detecting Format', color: '#722ed1' },
    detecting_encoding: { icon: <SearchOutlined />, label: 'Detecting Encoding', color: '#722ed1' },
    decompressing: { icon: <ThunderboltOutlined />, label: 'Decompressing', color: '#fa541c' },
    parsing: { icon: <LoadingOutlined spin />, label: 'Parsing Data', color: '#faad14' },
    validating: { icon: <SafetyCertificateOutlined />, label: 'Validating', color: '#13c2c2' },
    checking_malformed: { icon: <ExclamationCircleOutlined />, label: 'Checking Quality', color: '#eb2f96' },
    complete: { icon: <CheckCircleOutlined />, label: 'Complete', color: '#52c41a' },
    error: { icon: <ExclamationCircleOutlined />, label: 'Error', color: '#ff4d4f' },
};

function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatEta(seconds: number): string {
    if (seconds < 60) return `${Math.ceil(seconds)}s`;
    const mins = Math.floor(seconds / 60);
    const secs = Math.ceil(seconds % 60);
    return `${mins}m ${secs}s`;
}

const UploadProgress: React.FC<UploadProgressProps> = ({
    uploadPct,
    stage = 'uploading',
    progressPct,
    etaSeconds,
    memoryMb,
    message: statusMessage,
    bytesRead,
    totalBytes,
}) => {
    const stageConfig = STAGE_CONFIG[stage] || STAGE_CONFIG.uploading;
    const displayPct = stage === 'uploading' ? uploadPct : (progressPct ?? uploadPct);
    const isComplete = stage === 'complete';
    const isError = stage === 'error';

    return (
        <Card className="upload-progress-card" variant="borderless">
            <div className="progress-header">
                <Space>
                    <span className="stage-icon" style={{ color: stageConfig.color }}>
                        {stageConfig.icon}
                    </span>
                    <Text strong className="stage-label" style={{ color: stageConfig.color }}>
                        {stageConfig.label}
                    </Text>
                </Space>
                {etaSeconds != null && etaSeconds > 0 && (
                    <Tag color="default" className="eta-tag">
                        ETA: {formatEta(etaSeconds)}
                    </Tag>
                )}
            </div>

            <Progress
                percent={Math.round(displayPct)}
                status={isComplete ? 'success' : isError ? 'exception' : 'active'}
                strokeColor={
                    isComplete
                        ? '#52c41a'
                        : {
                            '0%': stageConfig.color,
                            '100%': '#52c41a',
                        }
                }
                trailColor="rgba(255,255,255,0.08)"
                strokeWidth={10}
                className="main-progress"
            />

            <div className="progress-details">
                {statusMessage && (
                    <Text type="secondary" className="status-message">
                        {statusMessage}
                    </Text>
                )}
                <Space className="progress-stats" split={<span className="stat-divider">â€¢</span>}>
                    {bytesRead != null && totalBytes != null && totalBytes > 0 && (
                        <Text type="secondary" className="stat-item">
                            {formatBytes(bytesRead)} / {formatBytes(totalBytes)}
                        </Text>
                    )}
                    {memoryMb != null && memoryMb > 0 && (
                        <Text type="secondary" className="stat-item">
                            Memory: {memoryMb.toFixed(1)} MB
                        </Text>
                    )}
                </Space>
            </div>
        </Card>
    );
};

export default UploadProgress;

