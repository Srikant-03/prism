/**
 * StatTestPanel â€” Built-in statistical testing suite with auto-suggestions.
 */

import React, { useState, useCallback } from 'react';
import { Select, Button, Space, Tag, Alert, Empty, Row, Col, Statistic } from 'antd';
import {
    ExperimentOutlined, CheckCircleOutlined, CloseCircleOutlined,
    BulbOutlined, PlayCircleOutlined,
} from '@ant-design/icons';

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

interface TestResult {
    test: string;
    statistic: number;
    p_value: number;
    significant: boolean;
    interpretation: string;
    degrees_of_freedom?: number;
}

interface Props {
    columns: { name: string; dtype: string }[];
    fileId: string;
}

const AVAILABLE_TESTS = [
    { value: 'shapiro_wilk', label: 'Shapiro-Wilk (Normality)', needs: ['column'] },
    { value: 'ks_test', label: 'Kolmogorov-Smirnov (Normality)', needs: ['column'] },
    { value: 't_test', label: 'Independent t-test', needs: ['column', 'groupBy'] },
    { value: 'mann_whitney', label: 'Mann-Whitney U', needs: ['column', 'groupBy'] },
    { value: 'anova', label: 'One-Way ANOVA', needs: ['column', 'groupBy'] },
    { value: 'chi_squared', label: 'Chi-Squared', needs: ['column', 'column2'] },
    { value: 'pearson_correlation', label: 'Pearson Correlation', needs: ['column', 'column2'] },
    { value: 'spearman_correlation', label: 'Spearman Correlation', needs: ['column', 'column2'] },
    { value: 'levene', label: "Levene's Variance Test", needs: ['column', 'groupBy'] },
];

const StatTestPanel: React.FC<Props> = ({ columns, fileId }) => {
    const [selectedTest, setSelectedTest] = useState<string | null>(null);
    const [column1, setColumn1] = useState<string | null>(null);
    const [column2, setColumn2] = useState<string | null>(null);
    const [results, setResults] = useState<TestResult[]>([]);
    const [loading, setLoading] = useState(false);

    const runTest = useCallback(async () => {
        if (!selectedTest || !column1) return;
        setLoading(true);
        try {
            const res = await fetch(`${API_BASE}/api/stats/test`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    test: selectedTest,
                    file_id: fileId,
                    column: column1,
                    column2: column2,
                }),
            });
            const data = await res.json();
            setResults(prev => [data, ...prev]);
        } catch (e: any) {
            console.error('Test error:', e);
        } finally {
            setLoading(false);
        }
    }, [selectedTest, column1, column2, fileId]);

    const testConfig = AVAILABLE_TESTS.find(t => t.value === selectedTest);
    const catCols = columns.filter(c => ['object', 'string', 'category'].includes(c.dtype));

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div className="glass-panel" style={{ padding: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                    <ExperimentOutlined style={{ color: '#6366f1', fontSize: 18 }} />
                    <strong style={{ fontSize: 15 }}>Statistical Testing Suite</strong>
                </div>

                <Space wrap>
                    <Select
                        value={selectedTest}
                        onChange={setSelectedTest}
                        placeholder="Select a test"
                        style={{ width: 250 }}
                        options={AVAILABLE_TESTS.map(t => ({ value: t.value, label: t.label }))}
                    />
                    <Select
                        value={column1}
                        onChange={setColumn1}
                        placeholder="Column"
                        style={{ width: 160 }}
                        showSearch
                        options={columns.map(c => ({ value: c.name, label: `${c.name} (${c.dtype})` }))}
                    />
                    {testConfig?.needs.includes('column2') && (
                        <Select
                            value={column2}
                            onChange={setColumn2}
                            placeholder="Second column"
                            style={{ width: 160 }}
                            showSearch
                            options={columns.map(c => ({ value: c.name, label: `${c.name} (${c.dtype})` }))}
                        />
                    )}
                    {testConfig?.needs.includes('groupBy') && (
                        <Select
                            value={column2}
                            onChange={setColumn2}
                            placeholder="Group by"
                            style={{ width: 160 }}
                            showSearch
                            options={catCols.map(c => ({ value: c.name, label: c.name }))}
                        />
                    )}
                    <Button type="primary" icon={<PlayCircleOutlined />}
                        onClick={runTest} loading={loading}
                        disabled={!selectedTest || !column1}>
                        Run Test
                    </Button>
                </Space>
            </div>

            {/* Results */}
            {results.map((result, i) => (
                <div key={i} className="glass-panel" style={{
                    padding: 16,
                    borderLeft: `3px solid ${result.significant ? '#f59e0b' : '#52c41a'}`,
                }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                        <strong>{result.test}</strong>
                        <Tag color={result.significant ? 'orange' : 'green'}
                            icon={result.significant ? <CheckCircleOutlined /> : <CloseCircleOutlined />}>
                            {result.significant ? 'Significant' : 'Not Significant'}
                        </Tag>
                    </div>

                    <Row gutter={16} style={{ marginBottom: 12 }}>
                        <Col span={8}>
                            <Statistic
                                title="Test Statistic"
                                value={result.statistic}
                                precision={4}
                                styles={{ content: { fontSize: 16 } }}
                            />
                        </Col>
                        <Col span={8}>
                            <Statistic
                                title="P-Value"
                                value={result.p_value}
                                precision={6}
                                styles={{
                                    content: {
                                        fontSize: 16,
                                        color: result.p_value < 0.05 ? '#f59e0b' : '#52c41a',
                                    }
                                }}
                            />
                        </Col>
                        {result.degrees_of_freedom !== undefined && (
                            <Col span={8}>
                                <Statistic
                                    title="Degrees of Freedom"
                                    value={result.degrees_of_freedom}
                                    styles={{ content: { fontSize: 16 } }}
                                />
                            </Col>
                        )}
                    </Row>

                    <Alert
                        type="info"
                        showIcon
                        icon={<BulbOutlined />}
                        title="Interpretation"
                        description={result.interpretation}
                        style={{ background: 'rgba(99,102,241,0.05)', border: '1px solid rgba(99,102,241,0.1)' }}
                    />
                </div>
            ))}

            {results.length === 0 && !loading && (
                <Empty
                    description="Select a test and column(s) above to run statistical analysis"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
            )}
        </div>
    );
};

export default StatTestPanel;

