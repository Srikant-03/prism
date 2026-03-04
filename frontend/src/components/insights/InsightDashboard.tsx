import React from 'react';
import { Row, Col, Typography } from 'antd';
import type { DatasetInsights } from '../../types/insight';
import QualityReportCard from './QualityReportCard';
import AnomalyRegistry from './AnomalyRegistry';
import FeatureRanking from './FeatureRanking';
import AnalystBriefingPanel from './AnalystBriefing';

const { Paragraph } = Typography;

interface InsightDashboardProps {
    insights?: DatasetInsights;
    fileId?: string;
}

const InsightDashboard: React.FC<InsightDashboardProps> = ({ insights, fileId }) => {
    if (!insights) {
        return (
            <div className="glass-panel" style={{ padding: '2rem', textAlign: 'center' }}>
                <Paragraph type="secondary">Waiting for Autonomous Insights generation...</Paragraph>
            </div>
        );
    }

    return (
        <div className="insight-dashboard animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
            <Row gutter={[24, 24]}>
                {/* 1. Quality Report Card */}
                <Col xs={24} lg={8}>
                    <QualityReportCard data={insights.quality_score} />
                </Col>

                {/* 2. Anomaly Registry */}
                <Col xs={24} lg={16}>
                    <AnomalyRegistry data={insights.anomalies} />
                </Col>
            </Row>

            <Row gutter={[24, 24]}>
                {/* 3. Feature Ranking */}
                <Col xs={24} lg={12}>
                    <FeatureRanking data={insights.feature_ranking} />
                </Col>

                {/* 4. Analyst Briefing with Export */}
                <Col xs={24} lg={12}>
                    <AnalystBriefingPanel data={insights.analyst_briefing} fileId={fileId || 'unknown'} />
                </Col>
            </Row>
        </div>
    );
};

export default InsightDashboard;

