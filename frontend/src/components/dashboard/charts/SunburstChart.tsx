/**
 * SunburstChart — Apache ECharts Multi-Level Radial Sunburst Visualizer.
 * Renders hierarchical breakdown across nested dimensions with interactive drill-down.
 */
import React from 'react';
import ReactECharts from 'echarts-for-react';
import type { ChartConfig } from '../../../types/dashboard';
import { COLOR_PALETTES } from '../../../types/dashboard';
import { formatCompactNumber } from '../utils/formatters';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    onDataPointClick?: (params: { column?: string; value: any }) => void;
}

interface TreeNode {
    name: string;
    value?: number;
    children?: TreeNode[];
    itemStyle?: { color?: string };
}

const DashboardSunburstChart: React.FC<Props> = ({ config, data, onDataPointClick }) => {
    const colors = COLOR_PALETTES[config.color_scheme] || COLOR_PALETTES.default;

    const valueKey = config.metrics?.[0]?.label || config.metrics?.[0]?.column || config.y_axis || 'value';
    const primaryDim = config.x_axis || 'category';
    const secondaryDim = config.group_by;

    // Build hierarchical tree
    const treeMap = new Map<string, TreeNode>();

    data.forEach((row) => {
        const pVal = String(row[primaryDim] || 'Other');
        const v = Number(row[valueKey]) || 0;

        if (!treeMap.has(pVal)) {
            treeMap.set(pVal, {
                name: pVal,
                children: [],
                value: 0,
            });
        }

        const parentNode = treeMap.get(pVal)!;
        parentNode.value = (parentNode.value || 0) + v;

        if (secondaryDim && row[secondaryDim]) {
            const sVal = String(row[secondaryDim]);
            const existingChild = parentNode.children?.find((c) => c.name === sVal);
            if (existingChild) {
                existingChild.value = (existingChild.value || 0) + v;
            } else {
                parentNode.children?.push({
                    name: sVal,
                    value: v,
                });
            }
        }
    });

    const treeData: TreeNode[] = Array.from(treeMap.values()).map((node, i) => ({
        ...node,
        itemStyle: { color: colors[i % colors.length] },
        children: node.children && node.children.length > 0 ? node.children : undefined,
    }));

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'item',
            formatter: (params: any) => {
                const val = params.value ? formatCompactNumber(params.value) : '';
                return `<strong>${params.name}</strong><br/>${val}`;
            },
        },
        series: {
            type: 'sunburst',
            data: treeData,
            radius: [0, '90%'],
            sort: undefined,
            emphasis: {
                focus: 'ancestor',
            },
            levels: [
                {},
                {
                    r0: '15%',
                    r: '45%',
                    itemStyle: {
                        borderWidth: 2,
                        borderColor: '#0f172a',
                    },
                    label: {
                        rotate: 'tangential',
                        color: '#f8fafc',
                        fontSize: 11,
                    },
                },
                {
                    r0: '45%',
                    r: '85%',
                    label: {
                        align: 'right',
                        color: '#cbd5e1',
                        fontSize: 10,
                    },
                },
            ],
        },
    };

    const onEvents = {
        click: (params: any) => {
            if (params.name) {
                onDataPointClick?.({ column: primaryDim, value: params.name });
            }
        },
    };

    return (
        <div style={{ width: '100%', height: '100%' }}>
            <ReactECharts
                option={option}
                style={{ height: '100%', width: '100%' }}
                onEvents={onEvents}
            />
        </div>
    );
};

export default DashboardSunburstChart;
