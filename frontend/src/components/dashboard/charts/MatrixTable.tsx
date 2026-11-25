/**
 * MatrixTable — Power BI-style Matrix / Hierarchical Pivot Grid.
 * Displays nested collapsible rows with subtotals, grand totals, and number formatting.
 */
import React from 'react';
import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { ChartConfig } from '../../../types/dashboard';
import { formatValue } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

interface MatrixRow {
    key: string;
    [key: string]: any;
    children?: MatrixRow[];
}

const DashboardMatrixTable: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const rowDim = config.x_axis || 'category';
    const subDim = config.group_by;
    const metrics = config.metrics && config.metrics.length > 0
        ? config.metrics
        : [{ column: config.y_axis || 'value', aggregation: config.aggregation, label: config.y_axis || 'Value' }];

    // Build hierarchy if secondary grouping exists
    const matrixRows: MatrixRow[] = [];

    if (subDim) {
        const parentMap = new Map<string, { parent: MatrixRow; children: MatrixRow[] }>();

        data.forEach((row, i) => {
            const pKey = String(row[rowDim] || 'Other');
            const cKey = String(row[subDim] || 'Other');

            if (!parentMap.has(pKey)) {
                const parentRow: MatrixRow = {
                    key: `parent-${pKey}`,
                    [rowDim]: pKey,
                    isParent: true,
                };
                metrics.forEach(m => {
                    const k = m.label || m.column;
                    parentRow[k] = 0;
                });
                parentMap.set(pKey, { parent: parentRow, children: [] });
            }

            const { parent, children } = parentMap.get(pKey)!;
            const childRow: MatrixRow = {
                key: `child-${pKey}-${cKey}-${i}`,
                [rowDim]: cKey,
                isChild: true,
            };

            metrics.forEach(m => {
                const k = m.label || m.column;
                const v = Number(row[k]) || 0;
                childRow[k] = v;
                parent[k] = (Number(parent[k]) || 0) + v;
            });

            children.push(childRow);
        });

        parentMap.forEach(({ parent, children }) => {
            matrixRows.push({
                ...parent,
                children: children.length > 0 ? children : undefined,
            });
        });
    } else {
        data.forEach((row, i) => {
            matrixRows.push({
                key: `row-${i}`,
                ...row,
            });
        });
    }

    const columns: ColumnsType<MatrixRow> = [
        {
            title: rowDim.replace(/_/g, ' ').toUpperCase(),
            dataIndex: rowDim,
            key: rowDim,
            render: (text: string, record: MatrixRow) => (
                <span
                    style={{
                        fontWeight: record.isParent ? 700 : 400,
                        color: record.isParent ? '#f8fafc' : '#cbd5e1',
                        cursor: 'pointer',
                    }}
                    onClick={() => onDataPointClick?.({ column: rowDim, value: text })}
                >
                    {text}
                </span>
            ),
        },
        ...metrics.map(m => {
            const k = m.label || m.column;
            return {
                title: (m.label || m.column).replace(/_/g, ' ').toUpperCase(),
                dataIndex: k,
                key: k,
                align: 'right' as const,
                sorter: (a: MatrixRow, b: MatrixRow) => (Number(a[k]) || 0) - (Number(b[k]) || 0),
                render: (val: any, record: MatrixRow) => (
                    <span style={{ fontWeight: record.isParent ? 700 : 400, color: '#f1f5f9' }}>
                        {formatValue(val, m.format)}
                    </span>
                ),
            };
        }),
    ];

    return (
        <div style={{ width: '100%', height: '100%', overflow: 'auto' }}>
            <Table
                dataSource={matrixRows}
                columns={columns}
                size="small"
                pagination={false}
                style={{ background: 'transparent' }}
            />
        </div>
    );
};

export default DashboardMatrixTable;
