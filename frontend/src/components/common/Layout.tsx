/**
 * Layout — App shell with animated background, header, and theme/help controls.
 */

import React, { useState } from 'react';
import { Layout as AntLayout, Typography, Space, Button, Tooltip, Tour } from 'antd';
import type { TourProps } from 'antd';
import {
    BulbOutlined, BulbFilled,
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
            title: 'Welcome to Data Intelligence Platform',
            description: 'Upload any dataset — CSV, Excel, JSON, Parquet, XML, or SQL dump. The system handles everything automatically.',
            target: null,
        },
        {
            title: 'Pillar 1: Smart Profiling',
            description: 'After upload, every column is deeply profiled — types, distributions, quality, correlations, anomalies.',
            target: null,
        },
        {
            title: 'Pillar 2: Auto Preprocessing',
            description: 'The AI Decision Engine recommends cleaning steps — duplicates, missing values, outliers, encoding, scaling. You approve or let it run autonomously.',
            target: null,
        },
        {
            title: 'Pillar 3: SQL Query Engine',
            description: 'Query your data with a visual builder, raw SQL, or plain English. Auto-visualization suggests the best chart for every result.',
            target: null,
        },
        {
            title: 'Pillar 4: Reporting & Export',
            description: 'Export full analysis reports (PDF, DOCX, HTML, Notebook), pipeline code, and data in 6+ formats.',
            target: null,
        },
        {
            title: 'Theme & Settings',
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
                        <div
                            style={{
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                width: 34,
                                height: 34,
                                borderRadius: 10,
                                background: 'linear-gradient(135deg, rgba(244,63,94,0.3) 0%, rgba(245,158,11,0.2) 100%)',
                                border: '1px solid rgba(255,255,255,0.15)',
                            }}
                            aria-label="PRISM logo"
                        >
                            <svg width="22" height="22" viewBox="0 0 32 32" fill="none">
                                <polygon
                                    points="16,3 30,28 2,28"
                                    fill="url(#layoutPrismGrad)"
                                    stroke="rgba(255,255,255,0.7)"
                                    strokeWidth="1.2"
                                />
                                <defs>
                                    <linearGradient id="layoutPrismGrad" x1="0" y1="0" x2="1" y2="1">
                                        <stop offset="0%" stopColor="rgba(255,255,255,0.9)" />
                                        <stop offset="40%" stopColor="rgba(244,63,94,0.5)" />
                                        <stop offset="70%" stopColor="rgba(245,158,11,0.5)" />
                                        <stop offset="100%" stopColor="rgba(99,102,241,0.5)" />
                                    </linearGradient>
                                </defs>
                            </svg>
                        </div>
                        <div>
                            <Title
                                level={4}
                                className="header-title"
                                style={{
                                    fontFamily: "'Playfair Display', Georgia, serif",
                                    letterSpacing: '0.12em',
                                    color: '#fdf4dc',
                                    fontWeight: 700,
                                    margin: 0,
                                }}
                            >
                                PRISM
                            </Title>
                            <Text
                                className="header-subtitle"
                                style={{ fontSize: 10, letterSpacing: '0.08em', color: 'rgba(255,255,255,0.5)' }}
                            >
                                Spectral Data Metamorphosis
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
                        <div
                            style={{
                                padding: '2px 10px',
                                borderRadius: 12,
                                background: 'linear-gradient(135deg, rgba(244,63,94,0.2) 0%, rgba(245,158,11,0.2) 100%)',
                                border: '1px solid rgba(245,158,11,0.35)',
                            }}
                        >
                            <Text style={{ fontSize: 11, color: '#fef08a', fontWeight: 600 }}>v2.0 • Spectral</Text>
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

