/**
 * TableExplorer â€” Tree view of all available tables with expandable column lists.
 */

import React, { useState } from 'react';
import { Tree, Tag, Input, Badge, Tooltip } from 'antd';
import {
    TableOutlined, NumberOutlined, FontSizeOutlined,
    CalendarOutlined, CheckSquareOutlined, TagOutlined,
    QuestionOutlined, DatabaseOutlined,
} from '@ant-design/icons';
import type { TableInfo, ColumnInfo } from '../../types/sql';

const { Search } = Input;

const typeIcons: Record<string, React.ReactNode> = {
    integer: <NumberOutlined style={{ color: '#69b1ff' }} />,
    float: <NumberOutlined style={{ color: '#95de64' }} />,
    text: <FontSizeOutlined style={{ color: '#ff9c6e' }} />,
    datetime: <CalendarOutlined style={{ color: '#b37feb' }} />,
    boolean: <CheckSquareOutlined style={{ color: '#5cdbd3' }} />,
    categorical: <TagOutlined style={{ color: '#ffc069' }} />,
    other: <QuestionOutlined style={{ color: '#888' }} />,
};

const typeColors: Record<string, string> = {
    integer: 'blue', float: 'green', text: 'orange',
    datetime: 'purple', boolean: 'cyan', categorical: 'gold', other: 'default',
};

interface Props {
    tables: TableInfo[];
    columnsCache: Record<string, ColumnInfo[]>;
    onLoadColumns: (table: string) => Promise<ColumnInfo[]>;
    onSelectTable: (table: string) => void;
    selectedTable?: string;
}

const TableExplorer: React.FC<Props> = ({
    tables, columnsCache, onLoadColumns, onSelectTable, selectedTable,
}) => {
    const [search, setSearch] = useState('');
    const [expandedKeys, setExpandedKeys] = useState<string[]>([]);

    const handleExpand = async (keys: React.Key[]) => {
        const newKeys = keys.filter(k => !expandedKeys.includes(k as string));
        for (const key of newKeys) {
            const tableName = (key as string).replace('table-', '');
            if (!columnsCache[tableName]) {
                await onLoadColumns(tableName);
            }
        }
        setExpandedKeys(keys as string[]);
    };

    const filteredTables = tables.filter(t =>
        t.name.toLowerCase().includes(search.toLowerCase()) ||
        t.original_name.toLowerCase().includes(search.toLowerCase())
    );

    const treeData = filteredTables.map(table => ({
        key: `table-${table.name}`,
        title: (
            <div
                style={{
                    display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer',
                    color: selectedTable === table.name ? '#6366f1' : undefined,
                    fontWeight: selectedTable === table.name ? 600 : 400,
                }}
                onClick={() => onSelectTable(table.name)}
            >
                <TableOutlined />
                <span>{table.name}</span>
                <Tag color={table.source === 'raw' ? 'default' : 'green'} style={{ fontSize: 10 }}>
                    {table.source}
                </Tag>
                <span style={{ color: 'rgba(255,255,255,0.35)', fontSize: 11, marginLeft: 'auto' }}>
                    {table.n_rows.toLocaleString()} rows
                </span>
            </div>
        ),
        children: (columnsCache[table.name] || []).map(col => ({
            key: `col-${table.name}-${col.name}`,
            title: (
                <Tooltip title={`${col.dtype} | ${col.unique_count} unique | ${col.null_pct}% null`}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12 }}>
                        {typeIcons[col.ui_type] || typeIcons.other}
                        <span style={{ color: 'rgba(255,255,255,0.75)' }}>{col.name}</span>
                        <Tag color={typeColors[col.ui_type] || 'default'} style={{ fontSize: 9, lineHeight: '14px', padding: '0 4px' }}>
                            {col.ui_type}
                        </Tag>
                    </div>
                </Tooltip>
            ),
            isLeaf: true,
        })),
    }));

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
            <div style={{ padding: '12px 12px 8px', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                    <DatabaseOutlined style={{ color: '#6366f1' }} />
                    <span style={{ fontWeight: 600, fontSize: 13 }}>Tables</span>
                    <Badge count={tables.length} style={{ backgroundColor: 'rgba(99,102,241,0.3)', color: '#a5b4fc', fontSize: 10 }} />
                </div>
                <Search
                    placeholder="Search tables..."
                    size="small"
                    value={search}
                    onChange={e => setSearch(e.target.value)}
                    allowClear
                    style={{ marginBottom: 4 }}
                />
            </div>
            <div style={{ flex: 1, overflow: 'auto', padding: '4px 8px' }}>
                {filteredTables.length === 0 ? (
                    <div style={{ padding: 20, textAlign: 'center', color: 'rgba(255,255,255,0.3)', fontSize: 12 }}>
                        No tables loaded yet. Upload a file to get started.
                    </div>
                ) : (
                    <Tree
                        treeData={treeData}
                        expandedKeys={expandedKeys}
                        onExpand={handleExpand}
                        showLine={{ showLeafIcon: false }}
                        style={{ background: 'transparent', color: 'rgba(255,255,255,0.8)' }}
                    />
                )}
            </div>
        </div>
    );
};

export default TableExplorer;

