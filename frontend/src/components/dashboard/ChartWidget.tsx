/**
 * ChartWidget — Dispatcher that renders the appropriate chart based on config.chart_type.
 * Supports all Power BI visuals, statistical charts, KPI/gauges, maps, loading states, and cross-filter callbacks.
 */
import React from 'react';
import { Spin, Empty, Alert } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import type { ChartConfig } from '../../types/dashboard';

// Import visual components
import DashboardBarChart from './charts/BarChart';
import DashboardLineChart from './charts/LineChart';
import DashboardAreaChart from './charts/AreaChart';
import DashboardComboChart from './charts/ComboChart';
import DashboardRibbonChart from './charts/RibbonChart';
import DashboardPieChart from './charts/PieChart';
import DashboardTreemapChart from './charts/TreemapChart';
import DashboardSunburstChart from './charts/SunburstChart';
import DashboardFunnelChart from './charts/FunnelChart';
import DashboardWaterfallChart from './charts/WaterfallChart';
import DashboardScatterChart from './charts/ScatterChart';
import DashboardHeatmapChart from './charts/HeatmapChart';
import DashboardGaugeChart from './charts/GaugeChart';
import DashboardBulletChart from './charts/BulletChart';
import DashboardHistogramChart from './charts/HistogramChart';
import DashboardBoxPlotChart from './charts/BoxPlotChart';
import DashboardMatrixTable from './charts/MatrixTable';
import DashboardGeoMapChart from './charts/GeoMapChart';
import SlicerWidget from './charts/SlicerWidget';
import KPICard from './charts/KPICard';
import DashboardDataTable from './charts/DataTable';
import DecompositionTree from './charts/DecompositionTree';
import KeyInfluencersVisual from './charts/KeyInfluencersVisual';
import SmartNarrativeWidget from './charts/SmartNarrativeWidget';
import QAVisual from './charts/QAVisual';

interface Props {
    config: ChartConfig;
    data: Record<string, unknown>[];
    loading?: boolean;
    error?: string;
    onDataPointClick?: (params: { column?: string; value: unknown }) => void;
}

type ChartComponentProps = {
    config: ChartConfig;
    data: Record<string, unknown>[];
    onDataPointClick?: (params: { column?: string; value: unknown }) => void;
};

const CHART_MAP: Record<string, React.ComponentType<ChartComponentProps>> = {
    // Bars & Columns
    bar: DashboardBarChart,
    stacked_bar: DashboardBarChart,
    stacked_bar_100: DashboardBarChart,
    column: DashboardBarChart,
    stacked_column: DashboardBarChart,
    stacked_column_100: DashboardBarChart,

    // Lines & Areas
    line: DashboardLineChart,
    area: DashboardAreaChart,
    stacked_area: DashboardAreaChart,
    ribbon: DashboardRibbonChart,
    combo_bar_line: DashboardComboChart,
    combo_stacked_line: DashboardComboChart,

    // Composition & Hierarchical
    pie: DashboardPieChart,
    donut: DashboardPieChart,
    treemap: DashboardTreemapChart,
    sunburst: DashboardSunburstChart,
    funnel: DashboardFunnelChart,
    waterfall: DashboardWaterfallChart,

    // Distribution & Statistical
    scatter: DashboardScatterChart,
    bubble: DashboardScatterChart,
    histogram: DashboardHistogramChart,
    boxplot: DashboardBoxPlotChart,
    heatmap: DashboardHeatmapChart,

    // KPI & Gauges
    kpi: KPICard,
    multi_kpi: KPICard,
    gauge: DashboardGaugeChart,
    bullet: DashboardBulletChart,

    // Tables, Matrix & Slicers
    table: DashboardDataTable,
    matrix: DashboardMatrixTable,
    slicer: SlicerWidget,

    // Geospatial
    bubble_map: DashboardGeoMapChart,
    choropleth_map: DashboardGeoMapChart,

    // AI-Native Super-Visuals
    decomposition_tree: DecompositionTree,
    key_influencers: KeyInfluencersVisual,
    smart_narrative: SmartNarrativeWidget,
    narrative: SmartNarrativeWidget,
    qa_visual: QAVisual,
    qa: QAVisual,
};

const ChartWidget: React.FC<Props> = ({ config, data, loading, error, onDataPointClick }) => {
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
                <Alert type="error" title="Chart Error" description={error} showIcon style={{ width: '100%' }} />
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
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#94a3b8', fontSize: 13, flexDirection: 'column', gap: 6 }}>
                <span>Power BI visual type: <strong>{config.chart_type}</strong></span>
                <span style={{ fontSize: 11, color: '#64748b' }}>Rendering default table view</span>
                <DashboardDataTable config={config} data={data} />
            </div>
        );
    }

    return <ChartComponent config={config} data={data} onDataPointClick={onDataPointClick} />;
};

export default ChartWidget;
