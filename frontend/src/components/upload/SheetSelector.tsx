/**
 * SheetSelector â€” Excel sheet picker with previews and metadata.
 */

import React, { useState } from 'react';
import { Card, Checkbox, Button, Typography, Tag, Space, Tooltip } from 'antd';
import {
    FileExcelOutlined,
    MergeCellsOutlined,
    TableOutlined,
} from '@ant-design/icons';
import type { SheetInfo } from '../../types/ingestion';

const { Title, Text } = Typography;

interface SheetSelectorProps {
    sheets: SheetInfo[];
    onSelect: (selectedIndices: number[]) => void;
    loading?: boolean;
}

const SheetSelector: React.FC<SheetSelectorProps> = ({ sheets, onSelect, loading = false }) => {
    const [selected, setSelected] = useState<number[]>([0]); // Default to first sheet

    const handleToggle = (index: number) => {
        setSelected((prev) =>
            prev.includes(index) ? prev.filter((i) => i !== index) : [...prev, index],
        );
    };

    const handleSelectAll = () => {
        setSelected(sheets.map((_, i) => i));
    };

    const handleConfirm = () => {
        if (selected.length > 0) {
            onSelect(selected);
        }
    };

    return (
        <div className="sheet-selector">
            <div className="sheet-selector-header">
                <Space>
                    <FileExcelOutlined style={{ fontSize: 24, color: '#1677ff' }} />
                    <div>
                        <Title level={4} style={{ margin: 0 }}>Select Sheets to Analyze</Title>
                        <Text type="secondary">
                            This Excel file contains {sheets.length} sheet{sheets.length > 1 ? 's' : ''}.
                            Choose which to include.
                        </Text>
                    </div>
                </Space>
            </div>

            <div className="sheet-grid">
                {sheets.map((sheet) => {
                    const isSelected = selected.includes(sheet.index);
                    return (
                        <Card
                            key={sheet.index}
                            className={`sheet-card ${isSelected ? 'selected' : ''}`}
                            hoverable
                            onClick={() => handleToggle(sheet.index)}
                            size="small"
                        >
                            <div className="sheet-card-header">
                                <Space>
                                    <Checkbox checked={isSelected} onChange={() => handleToggle(sheet.index)} />
                                    <Text strong>{sheet.name}</Text>
                                </Space>
                                {sheet.has_merged_cells && (
                                    <Tooltip title="Contains merged cells â€” will be unmerged automatically">
                                        <Tag icon={<MergeCellsOutlined />} color="warning">
                                            Merged
                                        </Tag>
                                    </Tooltip>
                                )}
                            </div>

                            <div className="sheet-card-stats">
                                <Tag icon={<TableOutlined />} color="processing">
                                    {sheet.row_count.toLocaleString()} rows Ã— {sheet.col_count} cols
                                </Tag>
                            </div>

                            {sheet.preview.length > 0 && (
                                <div className="sheet-preview">
                                    <table className="mini-preview-table">
                                        <tbody>
                                            {sheet.preview.slice(0, 3).map((row, ri) => (
                                                <tr key={ri}>
                                                    {row.slice(0, 5).map((cell, ci) => (
                                                        <td key={ci} className={ri === 0 ? 'header-cell' : ''}>
                                                            {cell != null ? String(cell).substring(0, 20) : 'â€”'}
                                                        </td>
                                                    ))}
                                                    {row.length > 5 && <td className="more-cell">â€¦</td>}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </Card>
                    );
                })}
            </div>

            <div className="sheet-selector-actions">
                <Space>
                    <Button onClick={handleSelectAll}>Select All</Button>
                    <Button
                        type="primary"
                        onClick={handleConfirm}
                        disabled={selected.length === 0}
                        loading={loading}
                    >
                        Analyze {selected.length} Sheet{selected.length !== 1 ? 's' : ''}
                    </Button>
                </Space>
            </div>
        </div>
    );
};

export default SheetSelector;

