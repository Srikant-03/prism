/**
 * OnboardingWalkthrough.tsx â€” Step-by-step interactive guide for first-time users.
 */

import React, { useState } from 'react';
import { Modal, Button, Steps, Typography } from 'antd';
import { RocketOutlined, BarChartOutlined, BulbOutlined, SafetyOutlined } from '@ant-design/icons';

const { Title, Paragraph } = Typography;

const OnboardingWalkthrough: React.FC = () => {
    const [open, setOpen] = useState(!localStorage.getItem('onboarding_complete'));
    const [current, setCurrent] = useState(0);

    const steps = [
        {
            title: 'Welcome',
            icon: <RocketOutlined />,
            content: (
                <div>
                    <Title level={4}>Data Intelligence Platform v2.0</Title>
                    <Paragraph>
                        Welcome to the future of data engineering. This platform automates the entire
                        pipeline from ingestion to insight. Let's take a quick tour.
                    </Paragraph>
                </div>
            )
        },
        {
            title: 'Profiling',
            icon: <BarChartOutlined />,
            content: (
                <div>
                    <Title level={4}>Automated Profiling</Title>
                    <Paragraph>
                        Upload any file (CSV, Parquet, Excel). We'll instantly detect types,
                        anomalies, and quality scores without you writing a single line of code.
                    </Paragraph>
                </div>
            )
        },
        {
            title: 'AI Analyst',
            icon: <BulbOutlined />,
            content: (
                <div>
                    <Title level={4}>Your AI Partner</Title>
                    <Paragraph>
                        Use the Chat Sidebar to ask questions about your data in plain English.
                        The AI understands your full dataset context and can even suggest fixes.
                    </Paragraph>
                </div>
            )
        },
        {
            title: 'Reporting',
            icon: <SafetyOutlined />,
            content: (
                <div>
                    <Title level={4}>Certified Clean</Title>
                    <Paragraph>
                        Generate executive reports, statistical deep-dives, and data stories.
                        Export your work as PDF, HTML, or production-ready Python code.
                    </Paragraph>
                </div>
            )
        }
    ];

    const handleFinish = () => {
        localStorage.setItem('onboarding_complete', 'true');
        setOpen(false);
    };

    return (
        <Modal
            open={open}
            onCancel={() => setOpen(false)}
            footer={
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Button type="link" onClick={handleFinish}>Skip Tour</Button>
                    <div>
                        {current > 0 && <Button onClick={() => setCurrent(p => p - 1)} style={{ marginRight: 8 }}>Previous</Button>}
                        {current < steps.length - 1 ? (
                            <Button type="primary" onClick={() => setCurrent(p => p + 1)}>Next</Button>
                        ) : (
                            <Button type="primary" onClick={handleFinish}>Get Started</Button>
                        )}
                    </div>
                </div>
            }
            width={600}
            centered
        >
            <Steps current={current} items={steps.map(s => ({ title: s.title, icon: s.icon }))} size="small" style={{ marginBottom: 24 }} />
            <div style={{ minHeight: 180, padding: '0 24px' }}>
                {steps[current].content}
            </div>
        </Modal>
    );
};

export default OnboardingWalkthrough;

