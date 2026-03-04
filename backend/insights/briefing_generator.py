from typing import List
from profiling.profiling_models import DatasetProfile
from insights.insight_models import AnalystBriefing, DataQualityScore, FeatureImportance, AnomalyWarning, AnomalySeverity

class BriefingGenerator:
    """
    Constructs a plain-English briefing without LLM calls.
    Uses templated heuristic string interpolation to simulate a Data Analyst report.
    """

    @staticmethod
    def generate(profile: DatasetProfile, quality: DataQualityScore, anomalies: List[AnomalyWarning], rankings: List[FeatureImportance]) -> AnalystBriefing:
        
        # 1. Executive Summary
        domain_str = f"appears to represent an {profile.estimated_domain} dataset" if profile.estimated_domain != "Unknown" else "is an unclassified dataset"
        target_str = ""
        if profile.cross_analysis and profile.cross_analysis.target_analysis:
            target = profile.cross_analysis.target_analysis
            target_str = f" A preliminary Machine Learning setup suggests '{target.target_column}' is the likely target for {target.problem_type.replace('_', ' ')}."
        
        quality_adj = "exceptional" if quality.overall_score >= 90 else "reasonable" if quality.overall_score >= 70 else "poor"
        
        exec_summary = (
            f"This {quality_adj} quality dataset {domain_str}, consisting of {profile.total_rows:,} records "
            f"across {profile.total_columns} attributes.{target_str} It achieved an overall quality grade of '{quality.grade}' ({quality.overall_score}/100)."
        )

        # 2. Dataset Characteristics
        memory_mb = profile.memory_size_bytes / (1024 * 1024)
        date_cols = [c.name for c in profile.columns if c.semantic_type == 'datetime']
        date_str = f" Temporal coverage spans {len(date_cols)} datetime columns." if date_cols else ""
        
        char_summary = (
            f"The dataset shape is ({profile.total_rows:,}, {profile.total_columns}) consuming approximately {memory_mb:.2f} MB of memory. "
            f"It boasts a structural completeness of {100 - (sum(c.null_percentage for c in profile.columns)/max(1, profile.total_columns)):.1f}%."
            f"{date_str}"
        )

        # 3. Quality Assessment
        qual_summary = (
            f"Completeness is rated at {quality.completeness}/100. "
            f"Uniqueness scores {quality.uniqueness}/100, while Validity and Consistency track at {quality.validity}/100 and {quality.consistency}/100 respectively. "
        )
        if quality.timeliness:
            qual_summary += f"Timeliness decay metrics score {quality.timeliness}/100."

        # 4. Key Findings
        findings = []
        if rankings:
            findings.append(f"Top predictive feature identified as '{rankings[0].feature}' (Score: {rankings[0].importance_score}/100).")
            if len(rankings) > 1:
                findings.append(f"Second strongest driver is '{rankings[1].feature}'.")
        
        critical_anomalies = [a for a in anomalies if a.severity == AnomalySeverity.CRITICAL]
        if critical_anomalies:
            findings.append(f"CRITICAL RISK: {critical_anomalies[0].description} ({critical_anomalies[0].feature})")
            
        high_anomalies = [a for a in anomalies if a.severity == getattr(AnomalySeverity, 'HIGH', 'High')]
        if high_anomalies:
            findings.append(f"High Severity Warning: {high_anomalies[0].description} ({high_anomalies[0].feature})")

        if not findings:
            findings.append("No critical statistical deviations detected.")

        # 5. Recommended Actions
        actions = []
        if critical_anomalies:
            actions.append(f"Immediate Action Required: {critical_anomalies[0].recommendation}")
        
        null_cols = [c.name for c in profile.columns if c.null_percentage > 20]
        if null_cols:
            actions.append(f"Impute or drop columns with >20% missing data: {', '.join(null_cols[:3])}{'...' if len(null_cols) > 3 else ''}")
        
        if profile.cross_analysis and profile.cross_analysis.multicollinearity.has_multicollinearity:
            actions.append("Address multicollinearity by dropping redundant numerical features or applying PCA.")
            
        if not actions:
            actions.append("Dataset is clean. Proceed directly to feature engineering or downstream analytics.")

        return AnalystBriefing(
            executive_summary=exec_summary,
            dataset_characteristics=char_summary,
            quality_assessment=qual_summary,
            key_findings=findings,
            recommended_actions=actions
        )
