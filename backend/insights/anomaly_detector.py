from typing import List
from profiling.profiling_models import DatasetProfile
from insights.insight_models import AnomalyWarning, AnomalySeverity

class AnomalyDetector:
    """
    Scans a DatasetProfile to generate a categorized, severity-ranked list of business/data anomalies.
    """

    @staticmethod
    def detect(profile: DatasetProfile) -> List[AnomalyWarning]:
        warnings: List[AnomalyWarning] = []

        # 1. Dataset Level Anomalies
        if profile.total_rows == 0:
            warnings.append(AnomalyWarning(
                severity=AnomalySeverity.CRITICAL,
                category="Structural",
                description="The dataset contains zero rows.",
                recommendation="Verify ingestion source and file integrity."
            ))
            return warnings

        # 2. Column Nullability
        for col in profile.columns:
            if col.null_percentage > 95:
                warnings.append(AnomalyWarning(
                    feature=col.name,
                    severity=AnomalySeverity.HIGH,
                    category="Missingness",
                    description=f"Column is {col.null_percentage:.1f}% null.",
                    recommendation="Consider dropping this feature as it provides minimal variance."
                ))
            elif col.null_percentage > 50:
                warnings.append(AnomalyWarning(
                    feature=col.name,
                    severity=AnomalySeverity.MEDIUM,
                    category="Missingness",
                    description=f"Column is {col.null_percentage:.1f}% null.",
                    recommendation="Imputation may introduce significant bias. Proceed with caution."
                ))

        # 3. Categorical Issues (Constant, Imbalance, Inconsistencies)
        for col in profile.columns:
            if col.distinct_count == 1 and col.non_null_count > 0:
                warnings.append(AnomalyWarning(
                    feature=col.name,
                    severity=AnomalySeverity.MEDIUM,
                    category="Variance",
                    description="Column contains only a single constant value.",
                    recommendation="Drop this column as it carries zero information gain."
                ))
            
            if col.categorical:
                inconsistencies = col.categorical.case_inconsistencies or []
                if len(inconsistencies) > 0:
                    warnings.append(AnomalyWarning(
                        feature=col.name,
                        severity=AnomalySeverity.LOW,
                        category="Formatting",
                        description=f"Found {len(inconsistencies)} casing inconsistencies (e.g. 'Apple' vs 'apple').",
                        recommendation="Apply a lowercase/uppercase transform to standardize."
                    ))

        # 4. Numeric Outliers
        for col in profile.columns:
            if col.numeric:
                outliers = col.numeric.box_outliers or []
                if len(outliers) > (profile.total_rows * 0.05):
                    warnings.append(AnomalyWarning(
                        feature=col.name,
                        severity=AnomalySeverity.INFO,
                        category="Distribution",
                        description=f"More than 5% of records are statistical outliers.",
                        recommendation="Consider robust scaling or capping outliers before modeling."
                    ))

        # 5. Cross-Column Anomalies
        if profile.cross_analysis:
            cross = profile.cross_analysis
            correlations = cross.get("correlations", {})
            target_analysis = cross.get("target", {})
            
            # Multicollinearity
            if correlations.get("multicollinearity", {}).get("has_multicollinearity"):
                warnings.append(AnomalyWarning(
                    severity=AnomalySeverity.HIGH,
                    category="Correlation",
                    description="High Multicollinearity detected among numerical features.",
                    recommendation="Apply PCA or drop redundant columns (check Matrix for VIF scores)."
                ))
            
            # Perfect Correlations (Leakage)
            for pair in correlations.get("strongest_pairs", []):
                score = pair.get("score", 0.0)
                col1 = pair.get("col1")
                col2 = pair.get("col2")
                if col1 is not None and col2 is not None and abs(score) >= 0.99 and col1 != col2:
                    warnings.append(AnomalyWarning(
                        feature=col1,
                        severity=AnomalySeverity.CRITICAL,
                        category="Data Leakage",
                        description=f"Nearly perfect correlation (r={score:.2f}) with '{col2}'.",
                        recommendation="One of these is likely a duplicate or directly derived from the other. Drop one."
                    ))

            # Target Imbalance
            problem_type = target_analysis.get("problem_type")
            class_dist = target_analysis.get("class_distribution", {})
            if problem_type == 'binary_classification' and class_dist:
                dist = list(class_dist.values())
                if len(dist) == 2 and (min(dist) < 0.1):
                    warnings.append(AnomalyWarning(
                        feature=target_analysis.get("target_column"),
                        severity=AnomalySeverity.HIGH,
                        category="Target Imbalance",
                        description=f"Highly imbalanced target distribution ({min(dist)*100:.1f}% vs {max(dist)*100:.1f}%).",
                        recommendation="Use SMOTE, class weighting, or stratified sampling."
                    ))
                    
        # 6. PII Risk
        for col in profile.columns:
            if col.text and col.text.has_pii_risk:
                risks = col.text.pii_risks or []
                for risk in risks:
                    warnings.append(AnomalyWarning(
                        feature=col.name,
                        severity=AnomalySeverity.CRITICAL,
                        category="Security",
                        description=f"Detected potential {risk['type']} PII footprint.",
                        recommendation="Anonymize, hash, or drop this column before downstream storage."
                    ))

        # Sort by severity
        severity_map = {
            AnomalySeverity.CRITICAL: 0,
            AnomalySeverity.HIGH: 1,
            AnomalySeverity.MEDIUM: 2,
            AnomalySeverity.LOW: 3,
            AnomalySeverity.INFO: 4
        }
        warnings.sort(key=lambda w: severity_map.get(w.severity, 5))
        return warnings
