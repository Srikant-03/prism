/**
 * QualityScoreWidget — Persistent floating widget showing data quality score.
 * Gamified: animated score, dimension breakdown, next-best-action, badge system.
 */

import React, { useState, useMemo, useEffect } from 'react';
import { Progress, Tag, Space, Tooltip, Button, Drawer } from 'antd';
import {
    TrophyOutlined, ArrowUpOutlined, CheckCircleOutlined,
    CloseOutlined, ExpandOutlined,
} from '@ant-design/icons';

interface QualityDimension {
    name: string;
    score: number;
    maxScore: number;
    suggestion?: string;
}

interface Props {
    dimensions: QualityDimension[];
    nextAction?: { label: string; impact: number; onApply?: () => void };
    scoreHistory?: number[];
}

const QualityScoreWidget: React.FC<Props> = ({ dimensions, nextAction, scoreHistory = [] }) => {
    const [expanded, setExpanded] = useState(false);
    const [animatedScore, setAnimatedScore] = useState(0);

    const totalScore = useMemo(() => {
        const sum = dimensions.reduce((a, d) => a + d.score, 0);
        const max = dimensions.reduce((a, d) => a + d.maxScore, 0);
        return max > 0 ? Math.round((sum / max) * 100) : 0;
    }, [dimensions]);

    // Animate score
    useEffect(() => {
        const start = animatedScore;
        const end = totalScore;
        const duration = 800;
        const startTime = Date.now();
        const animate = () => {
            const elapsed = Date.now() - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            setAnimatedScore(Math.round(start + (end - start) * eased));
            if (progress < 1) requestAnimationFrame(animate);
        };
        requestAnimationFrame(animate);
    }, [totalScore]);

    const scoreColor = animatedScore >= 90 ? '#52c41a' : animatedScore >= 70 ? '#1677ff' :
        animatedScore >= 50 ? '#faad14' : '#ff4d4f';

    const isCertified = animatedScore >= 90;

    return (
        <>
            {/* Floating Widget */}
            <div
                onClick={() => setExpanded(true)}
                style={{
                    position: 'fixed',
                    bottom: 24,
                    left: 24,
                    width: 64,
                    height: 64,
                    borderRadius: '50%',
                    background: `conic-gradient(${scoreColor} ${animatedScore * 3.6}deg, rgba(255,255,255,0.05) 0deg)`,
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    boxShadow: `0 0 20px ${scoreColor}40`,
                    transition: 'all 0.3s ease',
                    zIndex: 100,
                }}
                role="button"
                aria-label={`Data quality score: ${animatedScore}/100`}
            >
                <div style={{
                    width: 52, height: 52, borderRadius: '50%',
                    background: 'rgba(15,23,42,0.9)',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    flexDirection: 'column',
                }}>
                    <span style={{ fontSize: 16, fontWeight: 800, color: scoreColor, lineHeight: 1 }}>
                        {animatedScore}
                    </span>
                    {isCertified && (
                        <TrophyOutlined style={{ fontSize: 10, color: '#fbbf24', marginTop: 2 }} />
                    )}
                </div>
            </div>

            {/* Expanded Drawer */}
            <Drawer
                title={
                    <Space>
                        <Progress type="circle" percent={animatedScore} size={32}
                            strokeColor={scoreColor} format={p => p} />
                        <span>Data Quality Score</span>
                        {isCertified && (
                            <Tag color="gold" icon={<TrophyOutlined />} style={{ fontSize: 10 }}>
                                CERTIFIED CLEAN
                            </Tag>
                        )}
                    </Space>
                }
                placement="left"
                width={340}
                open={expanded}
                onClose={() => setExpanded(false)}
                styles={{ body: { padding: 16 } }}
            >
                {/* Dimensions */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                    {dimensions.map(dim => {
                        const pct = dim.maxScore > 0 ? Math.round((dim.score / dim.maxScore) * 100) : 0;
                        return (
                            <div key={dim.name}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
                                    <span>{dim.name}</span>
                                    <span style={{ fontWeight: 600 }}>{dim.score}/{dim.maxScore}</span>
                                </div>
                                <Progress
                                    percent={pct}
                                    size="small"
                                    strokeColor={pct >= 80 ? '#52c41a' : pct >= 60 ? '#1677ff' : pct >= 40 ? '#faad14' : '#ff4d4f'}
                                    format={() => ''}
                                />
                                {dim.suggestion && (
                                    <div style={{ fontSize: 10, opacity: 0.5, marginTop: 2 }}>
                                        💡 {dim.suggestion}
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>

                {/* Next Best Action */}
                {nextAction && (
                    <div style={{
                        marginTop: 16, padding: 12, borderRadius: 8,
                        background: 'rgba(99,102,241,0.08)',
                        border: '1px solid rgba(99,102,241,0.15)',
                    }}>
                        <div style={{ fontSize: 11, opacity: 0.6, marginBottom: 4 }}>
                            🎯 Next Best Action
                        </div>
                        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
                            {nextAction.label}
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <Tag color="green" icon={<ArrowUpOutlined />} style={{ fontSize: 10 }}>
                                +{nextAction.impact} points
                            </Tag>
                            {nextAction.onApply && (
                                <Button size="small" type="primary" onClick={nextAction.onApply}>
                                    Apply
                                </Button>
                            )}
                        </div>
                    </div>
                )}

                {/* Score History (sparkline) */}
                {scoreHistory.length > 1 && (
                    <div style={{ marginTop: 16 }}>
                        <div style={{ fontSize: 11, opacity: 0.6, marginBottom: 8 }}>Score History</div>
                        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 2, height: 40 }}>
                            {scoreHistory.map((s, i) => (
                                <Tooltip key={i} title={`Step ${i + 1}: ${s}`}>
                                    <div style={{
                                        flex: 1, minWidth: 4,
                                        height: `${s * 0.4}px`,
                                        background: s >= 90 ? '#52c41a' : s >= 70 ? '#1677ff' : s >= 50 ? '#faad14' : '#ff4d4f',
                                        borderRadius: 2,
                                        transition: 'height 0.3s',
                                    }} />
                                </Tooltip>
                            ))}
                        </div>
                    </div>
                )}
            </Drawer>
        </>
    );
};

export default QualityScoreWidget;
