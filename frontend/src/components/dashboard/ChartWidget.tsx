/**
 * ChartWidget — Dispatcher that renders the appropriate chart based on config.chartType.
 * Contains loading state, error boundary, and empty-state display.
 */
import React from 'react';
import { Spin, Empty, Alert } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import type { ChartConfig } from '../../types/dashboard';

// Lazy-load chart components
import DashboardBarChart from './charts/BarChart';
import DashboardLineChart from './charts/LineChart';
import DashboardAreaChart from './charts/AreaChart';
import DashboardPieChart from './charts/PieChart';
import DashboardScatterChart from './charts/ScatterChart';
import DashboardHeatmapChart from './charts/HeatmapChart';
import DashboardTreemapChart from './charts/TreemapChart';
import DashboardFunnelChart from './charts/FunnelChart';
import KPICard from './charts/KPICard';
import DashboardDataTable from './charts/DataTable';

interface Props {
    config: ChartConfig;
    data: Record<string, any>[];
    loading?: boolean;
    error?: string;
}

const CHART_MAP: Record<string, React.FC<{ config: ChartConfig; data: Record<string, any>[] }>> = {
    bar: DashboardBarChart,
    line: DashboardLineChart,
    area: DashboardAreaChart,
    pie: DashboardPieChart,
    donut: DashboardPieChart,
    scatter: DashboardScatterChart,
    heatmap: DashboardHeatmapChart,
    treemap: DashboardTreemapChart,
    funnel: DashboardFunnelChart,
    kpi: KPICard,
    table: DashboardDataTable,
};

const ChartWidget: React.FC<Props> = ({ config, data, loading, error }) => {
    if (loading) {
        return (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
                <Spin indicator={<LoadingOutlined style={{ fontSize: 32, color: '#6366f1' }} />} />
            </div>
        );
    }

    if (error) {
        return (
            <div style={{ padding: 16, height: '100%', display: 'flex', alignItems: 'center' }}>
                <Alert type="error" message="Chart Error" description={error} showIcon style={{ width: '100%' }} />
            </div>
        );
    }

    if (!data || data.length === 0) {
        return (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
                <Empty description="No data available" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            </div>
        );
    }

    const ChartComponent = CHART_MAP[config.chart_type];
    if (!ChartComponent) {
        return (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#94a3b8' }}>
                Unsupported chart type: {config.chart_type}
            </div>
        );
    }

    return <ChartComponent config={config} data={data} />;
};

export default ChartWidget;
