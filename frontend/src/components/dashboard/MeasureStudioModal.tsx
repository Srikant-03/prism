/**
 * MeasureStudioModal — Power BI-style DAX / Measure creation and validation studio.
 * Supports manual DAX syntax editing, AI formula generation, and DuckDB validation.
 */
import React, { useState } from 'react';
import { Modal, Input, Select, Button, Alert, Typography } from 'antd';
import { FunctionOutlined, RobotOutlined, CheckCircleOutlined } from '@ant-design/icons';
import type { MeasureDefinition } from '../../types/dashboard';
import { fetchAuth, API_BASE } from '../../api/client';

const { TextArea } = Input;
const { Text } = Typography;

interface Props {
    open: boolean;
    fileId: string;
    onClose: () => void;
    onSave: (measure: MeasureDefinition) => void;
    initialMeasure?: MeasureDefinition;
}

const FORMAT_OPTIONS = [
    { value: 'currency', label: 'Currency ($#,##0.00)' },
    { value: 'percentage', label: 'Percentage (0.0%)' },
    { value: 'compact', label: 'Compact Number (1.2M)' },
    { value: 'integer', label: 'Integer (#,##0)' },
    { value: 'decimal', label: 'Decimal (0.00)' },
];

const MeasureStudioModal: React.FC<Props> = ({
    open,
    fileId,
    onClose,
    onSave,
    initialMeasure,
}) => {
    const [name, setName] = useState(initialMeasure?.name || '');
    const [expression, setExpression] = useState(initialMeasure?.expression || '');
    const [format, setFormat] = useState(initialMeasure?.format || 'currency');
    const [description, setDescription] = useState(initialMeasure?.description || '');

    // AI prompt states
    const [aiPrompt, setAiPrompt] = useState('');
    const [aiLoading, setAiLoading] = useState(false);

    // Validation states
    const [validating, setValidating] = useState(false);
    const [validationResult, setValidationResult] = useState<{
        valid: boolean;
        sampleValue?: any;
        error?: string;
    } | null>(null);

    const handleAIGenerate = async () => {
        if (!aiPrompt.trim()) return;
        setAiLoading(true);
        setValidationResult(null);

        try {
            const resp = await fetchAuth(`${API_BASE}/api/dashboard/measure/create`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, prompt: aiPrompt.trim() }),
            });
            if (resp.ok) {
                const data = await resp.json();
                if (data.measure) {
                    setName(data.measure.name || name);
                    setExpression(data.measure.expression || expression);
                    if (data.measure.format) setFormat(data.measure.format);
                    if (data.measure.description) setDescription(data.measure.description);
                }
            }
        } catch (err: any) {
            setValidationResult({ valid: false, error: err.message || 'AI Generation failed' });
        } finally {
            setAiLoading(false);
        }
    };

    const handleValidate = async () => {
        if (!expression.trim()) return;
        setValidating(true);
        setValidationResult(null);

        try {
            const resp = await fetchAuth(`${API_BASE}/api/dashboard/measure/validate`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_id: fileId, expression: expression.trim() }),
            });
            const data = await resp.json();
            if (resp.ok && data.valid) {
                setValidationResult({ valid: true, sampleValue: data.sample_value });
            } else {
                setValidationResult({ valid: false, error: data.error || 'Invalid formula expression' });
            }
        } catch (err: any) {
            setValidationResult({ valid: false, error: err.message || 'Validation request failed' });
        } finally {
            setValidating(false);
        }
    };

    const handleSave = () => {
        if (!name.trim() || !expression.trim()) return;
        onSave({
            name: name.trim(),
            expression: expression.trim(),
            format,
            description: description.trim() || undefined,
        });
        onClose();
    };

    return (
        <Modal
            title={
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#f8fafc' }}>
                    <FunctionOutlined style={{ color: '#818cf8', fontSize: 18 }} />
                    <span>DAX Measure Studio</span>
                </div>
            }
            open={open}
            onCancel={onClose}
            onOk={handleSave}
            okText="Save Measure"
            okButtonProps={{ disabled: !name.trim() || !expression.trim() }}
            width={620}
            styles={{ body: { padding: '16px 0' } }}
        >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
                {/* AI Assistant Banner */}
                <div
                    style={{
                        background: 'rgba(99, 102, 241, 0.1)',
                        border: '1px solid rgba(99, 102, 241, 0.3)',
                        borderRadius: 8,
                        padding: 12,
                    }}
                >
                    <div style={{ fontSize: 12, fontWeight: 600, color: '#a5b4fc', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 6 }}>
                        <RobotOutlined /> AI Formula Copilot
                    </div>
                    <div style={{ display: 'flex', gap: 8 }}>
                        <Input
                            size="small"
                            placeholder="e.g. 'Calculate gross profit margin percentage' or 'Running total of net revenue'"
                            value={aiPrompt}
                            onChange={(e) => setAiPrompt(e.target.value)}
                            onPressEnter={handleAIGenerate}
                            style={{ flex: 1 }}
                        />
                        <Button
                            size="small"
                            type="primary"
                            icon={<RobotOutlined />}
                            loading={aiLoading}
                            onClick={handleAIGenerate}
                            style={{ background: '#6366f1' }}
                        >
                            Generate
                        </Button>
                    </div>
                </div>

                {/* Measure Name & Format */}
                <div style={{ display: 'flex', gap: 12 }}>
                    <div style={{ flex: 1 }}>
                        <span style={{ fontSize: 12, color: '#cbd5e1', fontWeight: 600, display: 'block', marginBottom: 4 }}>
                            Measure Name
                        </span>
                        <Input
                            placeholder="e.g. Profit Margin %, Churned Revenue"
                            value={name}
                            onChange={(e) => setName(e.target.value)}
                        />
                    </div>
                    <div style={{ width: 200 }}>
                        <span style={{ fontSize: 12, color: '#cbd5e1', fontWeight: 600, display: 'block', marginBottom: 4 }}>
                            Format
                        </span>
                        <Select
                            value={format}
                            onChange={(f) => setFormat(f)}
                            style={{ width: '100%' }}
                            options={FORMAT_OPTIONS}
                        />
                    </div>
                </div>

                {/* DAX Formula Expression */}
                <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                        <span style={{ fontSize: 12, color: '#cbd5e1', fontWeight: 600 }}>
                            Formula Expression (DAX / DuckDB)
                        </span>
                        <Button
                            size="small"
                            icon={<CheckCircleOutlined />}
                            loading={validating}
                            onClick={handleValidate}
                            style={{ fontSize: 11, height: 22 }}
                        >
                            Test Formula
                        </Button>
                    </div>
                    <TextArea
                        rows={4}
                        placeholder="DIVIDE(SUM(net_revenue), SUM(gross_revenue), 0)"
                        value={expression}
                        onChange={(e) => {
                            setExpression(e.target.value);
                            setValidationResult(null);
                        }}
                        style={{ fontFamily: 'monospace', fontSize: 13 }}
                    />
                    <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>
                        Supported: <Text code>SUM</Text>, <Text code>AVG</Text>, <Text code>DIVIDE(a, b, fallback)</Text>, <Text code>CALCULATE(expr, cond)</Text>, <Text code>SHAREOFTOTAL</Text>, <Text code>YTD</Text>
                    </div>
                </div>

                {/* Validation Status */}
                {validationResult && (
                    <Alert
                        type={validationResult.valid ? 'success' : 'error'}
                        showIcon
                        message={
                            validationResult.valid
                                ? `Formula Valid! Sample Evaluation: ${validationResult.sampleValue}`
                                : `Syntax/Execution Error: ${validationResult.error}`
                        }
                    />
                )}

                {/* Description */}
                <div>
                    <span style={{ fontSize: 12, color: '#cbd5e1', fontWeight: 600, display: 'block', marginBottom: 4 }}>
                        Description (Optional)
                    </span>
                    <Input
                        placeholder="Short business description for tooltips"
                        value={description}
                        onChange={(e) => setDescription(e.target.value)}
                    />
                </div>
            </div>
        </Modal>
    );
};

export default MeasureStudioModal;
