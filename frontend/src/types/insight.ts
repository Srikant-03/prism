export interface DataQualityScore {
    completeness: number;
    uniqueness: number;
    validity: number;
    consistency: number;
    timeliness: number | null;
    overall_score: number;
    grade: string;
}

export interface AnomalyWarning {
    feature: string | null;
    severity: 'Critical' | 'High' | 'Medium' | 'Low' | 'Informational';
    category: string;
    description: string;
    recommendation: string | null;
}

export interface FeatureImportance {
    feature: string;
    importance_score: number;
    variance_score: number;
    correlation_to_target: number | null;
    mutual_information: number | null;
    reasoning: string;
}

export interface AnalystBriefing {
    executive_summary: string;
    dataset_characteristics: string;
    quality_assessment: string;
    key_findings: string[];
    recommended_actions: string[];
}

export interface DatasetInsights {
    quality_score: DataQualityScore;
    anomalies: AnomalyWarning[];
    feature_ranking: FeatureImportance[];
    analyst_briefing: AnalystBriefing;
}
