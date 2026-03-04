/**
 * FileUploader â€” Drag & drop file upload with animated visual feedback.
 * Supports multi-file upload, shows accepted format badges.
 */

import React, { useState } from 'react';
import { Upload, Typography, Tag, Space } from 'antd';
import {
    InboxOutlined,
    FileExcelOutlined,
    FileTextOutlined,
    FileZipOutlined,
    DatabaseOutlined,
    CodeOutlined,
} from '@ant-design/icons';
import type { UploadFile } from 'antd/es/upload/interface';

const { Dragger } = Upload;
const { Title, Text } = Typography;

interface FileUploaderProps {
    onUpload: (files: File[]) => void;
    disabled?: boolean;
}

const ACCEPTED_FORMATS = [
    { ext: '.csv', label: 'CSV', color: '#52c41a', icon: <FileTextOutlined /> },
    { ext: '.xlsx', label: 'Excel', color: '#1677ff', icon: <FileExcelOutlined /> },
    { ext: '.xls', label: 'XLS', color: '#1677ff', icon: <FileExcelOutlined /> },
    { ext: '.xlsm', label: 'XLSM', color: '#1677ff', icon: <FileExcelOutlined /> },
    { ext: '.json', label: 'JSON', color: '#faad14', icon: <CodeOutlined /> },
    { ext: '.parquet', label: 'Parquet', color: '#722ed1', icon: <DatabaseOutlined /> },
    { ext: '.feather', label: 'Feather', color: '#722ed1', icon: <DatabaseOutlined /> },
    { ext: '.xml', label: 'XML', color: '#eb2f96', icon: <CodeOutlined /> },
    { ext: '.sql', label: 'SQL', color: '#13c2c2', icon: <DatabaseOutlined /> },
    { ext: '.tsv', label: 'TSV', color: '#52c41a', icon: <FileTextOutlined /> },
    { ext: '.txt', label: 'TXT', color: '#8c8c8c', icon: <FileTextOutlined /> },
    { ext: '.zip', label: 'ZIP', color: '#fa541c', icon: <FileZipOutlined /> },
    { ext: '.gz', label: 'GZIP', color: '#fa541c', icon: <FileZipOutlined /> },
];

const FileUploader: React.FC<FileUploaderProps> = ({ onUpload, disabled = false }) => {
    const [isDragOver, setIsDragOver] = useState(false);

    const draggerProps = {
        name: 'files',
        multiple: true,
        fileList: [] as UploadFile[],
        beforeUpload: (_file: File, files: File[]) => {
            // Small delay to allow all files to be added
            setTimeout(() => {
                onUpload(files);
            }, 100);
            return false; // Prevent default upload behavior
        },
        onDragEnter: () => setIsDragOver(true),
        onDragLeave: () => setIsDragOver(false),
        onDrop: () => setIsDragOver(false),
        disabled,
        showUploadList: false,
    };

    return (
        <div className={`file-uploader ${isDragOver ? 'drag-over' : ''}`}>
            <Dragger {...draggerProps} className="upload-dragger">
                <div className="upload-icon-wrapper">
                    <InboxOutlined className="upload-icon" />
                </div>
                <Title level={3} className="upload-title">
                    Drop your data files here
                </Title>
                <Text className="upload-subtitle">
                    or click to browse - supports any data format
                </Text>
                <div className="format-badges">
                    <Space wrap size={[6, 6]} style={{ justifyContent: 'center', marginTop: 16 }}>
                        {ACCEPTED_FORMATS.map((fmt) => (
                            <Tag
                                key={fmt.ext}
                                icon={fmt.icon}
                                color={fmt.color}
                                className="format-tag"
                            >
                                {fmt.label}
                            </Tag>
                        ))}
                    </Space>
                </div>
            </Dragger>
        </div>
    );
};

export default FileUploader;

