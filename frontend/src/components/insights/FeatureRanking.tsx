import React, { useMemo } from 'react';
import { Card } from 'antd';
import ReactECharts from 'echarts-for-react';
import type { FeatureImportance } from '../../types/insight';

interface FeatureRankingProps {
    data: FeatureImportance[];
}

const FeatureRanking: React.FC<FeatureRankingProps> = ({ data }) => {

    // Reverse data so top feature is at the top of the horizontal bar chart
    const chartData = useMemo(() => {
        return [...data].reverse().slice(-15); // Show top 15 max
    }, [data]);

    const option = useMemo(() => {
        return {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' },
                formatter: (params: any) => {
                    const featureData = chartData[params[0].dataIndex];
                    return `
                        <b>${featureData.feature}</b><br/>
                        Score: ${featureData.importance_score.toFixed(1)}/100<br/>
                        <span style="font-size: 0.9em; color: #888;">
                            Variance: ${featureData.variance_score.toFixed(2)}<br/>
                            ${featureData.correlation_to_target ? `Target Corr: ${featureData.correlation_to_target.toFixed(2)}<br/>` : ''}
                            ${featureData.mutual_information ? `Avg MI: ${featureData.mutual_information.toFixed(2)}<br/>` : ''}
                            <i>${featureData.reasoning}</i>
                        </span>
                    `;
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: {
                type: 'value',
                max: 100,
                axisLabel: { color: 'rgba(255, 255, 255, 0.6)' },
                splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.1)' } }
            },
            yAxis: {
                type: 'category',
                data: chartData.map(d => d.feature),
                axisLabel: { color: 'rgba(255, 255, 255, 0.8)' },
                axisLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.2)' } }
            },
            series: [
                {
                    name: 'Importance Score',
                    type: 'bar',
                    data: chartData.map(d => d.importance_score),
                    itemStyle: {
                        color: {
                            type: 'linear',
                            x: 0, y: 0, x2: 1, y2: 0,
                            colorStops: [{
                                offset: 0, color: 'rgba(24, 144, 255, 0.3)'
                            }, {
                                offset: 1, color: 'rgba(24, 144, 255, 1)'
                            }]
                        },
                        borderRadius: [0, 4, 4, 0]
                    }
                }
            ]
        };
    }, [chartData]);

    return (
        <Card className="glass-panel" title="Feature Significance Ranking" variant="borderless" style={{ height: '100%' }}>
            <div style={{ height: '400px', width: '100%' }}>
                <ReactECharts
                    option={option}
                    style={{ height: '100%', width: '100%' }}
                />
            </div>
        </Card>
    );
};

export default FeatureRanking;

