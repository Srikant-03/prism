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
        if profile.cross_analysis and profile.cross_analysis.get("target"):
            target = profile.cross_analysis["target"]
            target_col = target.get("target_column")
            problem_type = (target.get("problem_type") or "").replace('_', ' ')
            if target_col and problem_type:
                target_str = f" A preliminary Machine Learning setup suggests '{target_col}' is the likely target for {problem_type}."
        
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
        
        if profile.cross_analysis and profile.cross_analysis.get("correlations"):
            collinearity = profile.cross_analysis["correlations"].get("multicollinearity", {})
            if collinearity and collinearity.get("has_multicollinearity"):
                actions.append("Address multicollinearity by dropping redundant numerical features or applying PCA.")
            
        if not actions:
            actions.append("Dataset is clean. Proceed directly to feature engineering or downstream analytics.")

        # 6. Column Deep Dives (Layman + Math)
        deep_dives = []
        # Sort by quality score if available, otherwise just take all cols
        sorted_cols = sorted(profile.columns, key=lambda c: getattr(c, 'quality_score', 0), reverse=True)[:15]
        for col in sorted_cols:
            math_summary = f"Type: {col.semantic_type.value if hasattr(col.semantic_type, 'value') else str(col.semantic_type)}. Nulls: {col.null_percentage:.1f}%. Distinct: {col.distinct_count}."
            if col.numeric:
                math_summary += f" Mean: {col.numeric.mean:.2f}, StdDev: {col.numeric.std_dev:.2f}, Min/Max: {col.numeric.min:.2f} / {col.numeric.max:.2f}."
            elif col.categorical:
                top_cat = col.categorical.top_values[0] if col.categorical.top_values else None
                if top_cat:
                    math_summary += f" Top value: '{top_cat.value}' ({top_cat.count} times)."

            # Layman explanation
            sem = col.semantic_type.value if hasattr(col.semantic_type, 'value') else str(col.semantic_type)
            if 'id' in sem or 'hash' in sem:
                layman = f"This looks like a unique identifier or tracking code. It doesn't hold mathematical weight but is crucial for linking data."
            elif col.numeric:
                layman = f"This is numerical data. On average, the value is around {col.numeric.mean:.2f}. "
                if col.numeric.std_dev and col.numeric.std_dev > abs(col.numeric.mean):
                    layman += "The values vary wildly, meaning you have some extremely high or low outliers."
                else:
                    layman += "Most of the numbers are clustered reliably around the average."
            elif col.categorical:
                layman = "This contains text categories or groups. "
                if col.categorical.top_values:
                    layman += f"The most common group is '{col.categorical.top_values[0].value}', showing it's the dominant category here."
            elif col.datetime:
                layman = "This column tracks dates or timestamps. It helps us understand the timeline of events and find chronological trends."
            else:
                layman = "This column holds general text attributes."
                
            if col.null_percentage > 10:
                layman += f" Note: Over {col.null_percentage:.0f}% of this information is missing, so handle it with care!"

            deep_dives.append({
                "column_name": col.name,
                "mathematical_summary": math_summary,
                "layman_explanation": layman
            })

        return AnalystBriefing(
            executive_summary=exec_summary,
            dataset_characteristics=char_summary,
            quality_assessment=qual_summary,
            key_findings=findings,
            recommended_actions=actions,
            column_deep_dives=deep_dives
        )
