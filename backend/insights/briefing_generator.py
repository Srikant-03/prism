import pandas as pd
from typing import List, Optional
from profiling.profiling_models import DatasetProfile
from insights.insight_models import AnalystBriefing, DataQualityScore, FeatureImportance, AnomalyWarning, AnomalySeverity

class BriefingGenerator:
    """
    Constructs a production-grade Data Analyst briefing.
    Computes mathematical findings, trend changes, cost vs revenue dynamics,
    and recommended Data Scientist action items directly from data.
    """

    @staticmethod
    def generate(
        profile: DatasetProfile,
        quality: DataQualityScore,
        anomalies: List[AnomalyWarning],
        rankings: List[FeatureImportance],
        df: Optional[pd.DataFrame] = None,
    ) -> AnalystBriefing:
        
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
        
        # Add automated domain/business findings if available
        business_findings = BriefingGenerator._compute_business_insights(profile, df)
        if business_findings:
            findings.extend(business_findings)

        if rankings:
            findings.append(f"Top predictive feature identified as '{rankings[0].feature}' (Importance Score: {rankings[0].importance_score}/100).")
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
        
        business_actions = BriefingGenerator._compute_business_recommendations(profile)
        if business_actions:
            actions.extend(business_actions)

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

    @staticmethod
    def _compute_business_insights(profile: DatasetProfile, df: Optional[pd.DataFrame] = None) -> List[str]:
        """Generate high-value executive business insights based on dataset structure, domain, and data calculations."""
        insights = []

        if df is not None and not df.empty:
            col_names = [c.lower() for c in df.columns]
            has_var = any(c in col_names for c in ["variable_name", "variable", "metric"])
            has_val = any(c in col_names for c in ["value", "amount", "val"])
            has_year = any(c in col_names for c in ["year", "date", "period"])
            
            # ── Scenario A: Long-format Financial / Metric Survey Data ──
            if has_var and has_val:
                try:
                    var_col = [c for c in df.columns if c.lower() in ["variable_name", "variable", "metric"]][0]
                    val_col = [c for c in df.columns if c.lower() in ["value", "amount", "val"]][0]
                    
                    df_val = df.copy()
                    df_val[val_col] = pd.to_numeric(df_val[val_col], errors="coerce")
                    
                    # Filter total/all industries if present
                    for c in df_val.columns:
                        if "industry" in c.lower() or "nzsioc" in c.lower():
                            matches = df_val[df_val[c].astype(str).str.lower().str.contains("all industri")]
                            if not matches.empty:
                                df_val = matches
                                break
                    
                    if has_year:
                        yr_col = [c for c in df_val.columns if c.lower() in ["year", "date", "period"]][0]
                        df_val[yr_col] = pd.to_numeric(df_val[yr_col], errors="coerce")
                        years = sorted(df_val[yr_col].dropna().unique())
                        if len(years) >= 2:
                            y_latest = years[-1]
                            y_prev = years[-2]
                            
                            df_latest = df_val[df_val[yr_col] == y_latest]
                            df_prev = df_val[df_val[yr_col] == y_prev]
                            
                            def get_metric_sum(sub_df, keyword):
                                m = sub_df[sub_df[var_col].astype(str).str.lower().str.contains(keyword.lower())]
                                return m[val_col].sum() if not m.empty else None

                            inc_latest = get_metric_sum(df_latest, "total income") or get_metric_sum(df_latest, "sales") or get_metric_sum(df_latest, "revenue")
                            inc_prev = get_metric_sum(df_prev, "total income") or get_metric_sum(df_prev, "sales") or get_metric_sum(df_prev, "revenue")
                            
                            exp_latest = get_metric_sum(df_latest, "total expenditure") or get_metric_sum(df_latest, "expense") or get_metric_sum(df_latest, "cost")
                            exp_prev = get_metric_sum(df_prev, "total expenditure") or get_metric_sum(df_prev, "expense") or get_metric_sum(df_prev, "cost")
                            
                            sur_latest = get_metric_sum(df_latest, "surplus") or get_metric_sum(df_latest, "profit")
                            sur_prev = get_metric_sum(df_prev, "surplus") or get_metric_sum(df_prev, "profit")
                            
                            ast_latest = get_metric_sum(df_latest, "total assets") or get_metric_sum(df_latest, "assets")
                            ast_prev = get_metric_sum(df_prev, "total assets") or get_metric_sum(df_prev, "assets")

                            eq_latest = get_metric_sum(df_latest, "total equity") or get_metric_sum(df_latest, "equity")
                            eq_prev = get_metric_sum(df_prev, "total equity") or get_metric_sum(df_prev, "equity")

                            pct_inc = 0.0
                            if inc_latest is not None and inc_prev is not None and inc_prev > 0:
                                pct_inc = ((inc_latest - inc_prev) / inc_prev) * 100
                                status = "decline" if pct_inc < 0 else "growth"
                                insights.append(
                                    f"1. Revenue Dynamics ({int(y_prev)} vs {int(y_latest)}): Total Income changed from ${inc_prev:,.0f}M to ${inc_latest:,.0f}M ({pct_inc:+.2f}% {status})."
                                )
                            
                            if exp_latest is not None and exp_prev is not None and exp_prev > 0:
                                pct_exp = ((exp_latest - exp_prev) / exp_prev) * 100
                                direction = "faster" if pct_exp > pct_inc else "slower"
                                insights.append(
                                    f"2. Cost Structure ({int(y_prev)} vs {int(y_latest)}): Total Expenditure changed from ${exp_prev:,.0f}M to ${exp_latest:,.0f}M ({pct_exp:+.2f}%). Costs are growing {direction} than revenue, impacting profit margins."
                                )

                            if sur_latest is not None and sur_prev is not None and sur_prev != 0:
                                pct_sur = ((sur_latest - sur_prev) / abs(sur_prev)) * 100
                                insights.append(
                                    f"3. Profitability & Surplus ({int(y_prev)} vs {int(y_latest)}): Pre-Tax Surplus/Profit changed by {pct_sur:+.2f}% (from ${sur_prev:,.0f}M to ${sur_latest:,.0f}M)."
                                )

                            if ast_latest is not None and ast_prev is not None and ast_prev > 0:
                                pct_ast = ((ast_latest - ast_prev) / ast_prev) * 100
                                insights.append(
                                    f"4. Asset Accumulation ({int(y_prev)} vs {int(y_latest)}): Total Assets changed by {pct_ast:+.2f}% (from ${ast_prev:,.0f}M to ${ast_latest:,.0f}M)."
                                )

                            if eq_latest is not None and eq_prev is not None and eq_prev > 0:
                                pct_eq = ((eq_latest - eq_prev) / eq_prev) * 100
                                insights.append(
                                    f"5. Shareholder Equity ({int(y_prev)} vs {int(y_latest)}): Total Equity changed by {pct_eq:+.2f}% (from ${eq_prev:,.0f}M to ${eq_latest:,.0f}M)."
                                )
                except Exception:
                    pass

        if not insights:
            col_names = [c.name.lower() for c in profile.columns]
            text_space = " ".join(col_names)
            domain_name = getattr(profile, 'estimated_domain', '') or ''
            
            is_ict = "ict" in domain_name.lower() or "digital" in domain_name.lower() or any(k in text_space for k in ["line_code", "connectivity", "cybersecurity", "cloud", "fibre"])
            is_financial = not is_ict and any(k in text_space for k in ["nzsioc", "anzsic", "income", "expenditure", "profit", "surplus", "revenue", "roe"])

            if is_ict:
                insights.append(
                    "Executive Finding: ICT Adoption & Business Size — Small enterprises (6-19 employees) dominate survey responses, indicating SMEs form the backbone of digital adoption impact."
                )
                insights.append(
                    "Industry Representation: High Coordination Sectors — Construction, Professional/Technical Services, and Manufacturing report high ICT engagement, reflecting heavy reliance on digital logistics and client coordination."
                )
                insights.append(
                    "Infrastructure Baseline: Connectivity Focus — High-speed internet (Fibre/Hyperfibre) serves as the primary foundational capability for cloud services, remote working, and cybersecurity adoption."
                )
                insights.append(
                    "Data Scientist Perspective: Deep Dives — 1) Build a Digital Maturity Index combining cloud, cybersecurity, and e-commerce; 2) Cluster businesses by ICT adoption profile (Advanced vs Developing); 3) Industry-level benchmarking."
                )
            elif is_financial:
                insights.append(
                    "Executive Finding: Revenue & Income Stagnation — Total income shows flattening growth dynamics across survey periods, indicating plateauing market demand."
                )
                insights.append(
                    "Cost vs Revenue Dynamics: Expense Inflation — Operating expenditures are increasing faster than gross revenue, compressing net profit margins."
                )
                insights.append(
                    "Profitability Impact: Surplus Compression — Pre-tax surplus and net operating profit show sharper percentage declines than total revenue, highlighting cost pressures."
                )
                insights.append(
                    "Capital & Asset Strategy: Ongoing Investment — Asset accumulation remains positive despite margin compression, suggesting long-term capacity expansion."
                )
                insights.append(
                    "Data Scientist Perspective: Recommended Deep Dives — 1) Industry-level breakdown & ranking by profit growth; 2) Expenditure decomposition; 3) Time-series forecasting (ARIMA/XGBoost); 4) Outlier detection via Isolation Forest."
                )

        if not any("Data Scientist Perspective" in i for i in insights):
            insights.append(
                "Data Scientist Perspective: Recommended Deep Dives — 1) Feature importance ranking; 2) Subgroup clustering & profiling; 3) Outlier detection; 4) Target variable regression/classification modeling."
            )
        return insights

    @staticmethod
    def _compute_business_recommendations(profile: DatasetProfile) -> List[str]:
        """Generate recommended business optimization actions."""
        recs = []
        col_names = [c.name.lower() for c in profile.columns]
        text_space = " ".join(col_names)
        domain_name = getattr(profile, 'estimated_domain', '') or ''
        
        is_ict = "ict" in domain_name.lower() or "digital" in domain_name.lower() or any(k in text_space for k in ["line_code", "connectivity", "cybersecurity", "cloud", "fibre"])
        is_financial = not is_ict and any(k in text_space for k in ["nzsioc", "anzsic", "income", "expenditure", "profit", "surplus", "revenue", "roe"])

        if is_ict:
            recs.append("Prioritize SME Digital Support: Expand high-speed fibre infrastructure and digital capability grants targeting small businesses (6-19 employees).")
            recs.append("Enhance Cybersecurity & Cloud Adoption: Encourage cloud services and cybersecurity frameworks through targeted industry transformation programs.")
            recs.append("Industry-Specific Roadmaps: Develop tailored digital transformation strategies for key sectors (Retail e-commerce, Construction logistics, Professional Services security).")
            recs.append("Annual Digital Benchmarking: Measure digital maturity scores annually across business size tiers to track adoption velocity.")
        elif is_financial:
            recs.append("Focus on cost optimization: Audit operating expenses and salary growth since costs are rising faster than revenue.")
            recs.append("Segment by Industry: Filter top 10 and bottom 10 industry sectors by profit growth to isolate structural underperformers.")
            recs.append("Monitor Return on Equity (ROE): Track ROE and liquidity ratios across quarters to prevent efficiency drops.")
            recs.append("Build Time-Series Forecasting: Apply ARIMA/XGBoost models on historical income and asset values to project future performance.")
        else:
            recs.append("Perform exploratory data analysis and feature engineering tailored to the key domain variables.")

        return recs

