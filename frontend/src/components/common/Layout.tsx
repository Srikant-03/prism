/**
 * Layout — App shell with animated background, header, and theme/help controls.
 */

import React, { useState } from 'react';
import { Layout as AntLayout, Typography, Space, Button, Tooltip, Tour } from 'antd';
import type { TourProps } from 'antd';
import {
    ThunderboltOutlined, BulbOutlined, BulbFilled,
    QuestionCircleOutlined,
} from '@ant-design/icons';
import { useTheme } from '../../hooks/useTheme';

const { Header, Content } = AntLayout;
const { Title, Text } = Typography;

interface LayoutProps {
    children: React.ReactNode;
}

const ONBOARDING_KEY = 'dip_onboarding_done';

const Layout: React.FC<LayoutProps> = ({ children }) => {
    const { isDark, toggle } = useTheme();
    const [tourOpen, setTourOpen] = useState(() => {
        return !localStorage.getItem(ONBOARDING_KEY);
    });

    const tourSteps: TourProps['steps'] = [
        {
            title: '👋 Welcome to Data Intelligence Platform',
            description: 'Upload any dataset — CSV, Excel, JSON, Parquet, XML, or SQL dump. The system handles everything automatically.',
            target: null,
        },
        {
            title: '📊 Pillar 1: Smart Profiling',
            description: 'After upload, every column is deeply profiled — types, distributions, quality, correlations, anomalies.',
            target: null,
        },
        {
            title: '🧹 Pillar 2: Auto Preprocessing',
            description: 'The AI Decision Engine recommends cleaning steps — duplicates, missing values, outliers, encoding, scaling. You approve or let it run autonomously.',
            target: null,
        },
        {
            title: '🗃️ Pillar 3: SQL Query Engine',
            description: 'Query your data with a visual builder, raw SQL, or plain English. Auto-visualization suggests the best chart for every result.',
            target: null,
        },
        {
            title: '📄 Pillar 4: Reporting & Export',
            description: 'Export full analysis reports (PDF, DOCX, HTML, Notebook), pipeline code, and data in 6+ formats.',
            target: null,
        },
        {
            title: '🌗 Theme & Settings',
            description: 'Toggle dark/light mode anytime. Your settings, queries, and pipeline state persist across sessions.',
            target: null,
        },
    ];

    const handleTourClose = () => {
        setTourOpen(false);
        localStorage.setItem(ONBOARDING_KEY, 'true');
    };

    return (
        <AntLayout className="app-layout" data-theme={isDark ? 'dark' : 'light'}>
            {/* Animated Background */}
            <div className="animated-bg">
                <div className="bg-gradient-1" />
                <div className="bg-gradient-2" />
                <div className="bg-gradient-3" />
                <div className="grid-overlay" />
            </div>

            <Header className="app-header" role="banner">
                <div className="header-content">
                    <Space align="center" size={12}>
                        <div className="logo-icon" aria-label="Data Intelligence Platform logo">
                            <ThunderboltOutlined />
                        </div>
                        <div>
                            <Title level={4} className="header-title">
                                Data Intelligence Platform
                            </Title>
                            <Text className="header-subtitle">
                                Autonomous Data Analysis Engine
                            </Text>
                        </div>
                    </Space>
                    <Space size={8}>
                        <Tooltip title={isDark ? 'Switch to light mode' : 'Switch to dark mode'}>
                            <Button
                                type="text"
                                icon={isDark ? <BulbOutlined /> : <BulbFilled />}
                                onClick={toggle}
                                aria-label="Toggle dark/light mode"
                                style={{ color: isDark ? 'rgba(255,255,255,0.7)' : 'rgba(0,0,0,0.7)' }}
                            />
                        </Tooltip>
                        <Tooltip title="Guided walkthrough">
                            <Button
                                type="text"
                                icon={<QuestionCircleOutlined />}
                                onClick={() => setTourOpen(true)}
                                aria-label="Open guided walkthrough"
                                style={{ color: isDark ? 'rgba(255,255,255,0.7)' : 'rgba(0,0,0,0.7)' }}
                            />
                        </Tooltip>
                        <div className="header-badge">
                            <Text className="badge-text">v1.0</Text>
                        </div>
                    </Space>
                </div>
            </Header>

            <Content className="app-content" role="main">
                <div className="content-wrapper">{children}</div>
            </Content>

            <Tour
                open={tourOpen}
                onClose={handleTourClose}
                steps={tourSteps}
            />
        </AntLayout>
    );
};

export default Layout;
