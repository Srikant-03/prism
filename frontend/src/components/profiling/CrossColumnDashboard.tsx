import React from 'react';
import { Tabs } from 'antd';
import type { CrossColumnProfile } from '../../types/profiling';
import type { DatasetInsights } from '../../types/insight';
import CorrelationPanel from './CorrelationPanel';
import TargetAnalysisPanel from './TargetAnalysisPanel';
import TemporalPatternsPanel from './TemporalPatternsPanel';
import GeoPatternsPanel from './GeoPatternsPanel';
import InsightDashboard from '../insights/InsightDashboard';


interface CrossColumnDashboardProps {
    profile: CrossColumnProfile;
    insights?: DatasetInsights;
    fileId?: string;
}

const CrossColumnDashboard: React.FC<CrossColumnDashboardProps> = ({ profile, insights, fileId }) => {
    const items = [
        {
            key: 'insights',
            label: 'Automated Insights',
            children: <InsightDashboard insights={insights} fileId={fileId} />,
        },
        {
            key: 'target',
            label: 'Target Analysis',
            children: <TargetAnalysisPanel data={profile.target} />,
        },
        {
            key: 'correlation',
            label: 'Correlations & Associations',
            children: <CorrelationPanel data={profile.correlations} />,
        },
        {
            key: 'temporal',
            label: 'Temporal Patterns',
            children: <TemporalPatternsPanel data={profile.temporal} />,
        },
        {
            key: 'geo',
            label: 'Geospatial Distribution',
            children: <GeoPatternsPanel data={profile.geo} />,
        },
    ];

    const activeKey = 'insights';

    return (
        <div className="cross-column-dashboard animate-fade-in">
            <Tabs defaultActiveKey={activeKey} items={items} className="cross-column-tabs glass-panel" />
        </div>
    );
};

export default CrossColumnDashboard;

