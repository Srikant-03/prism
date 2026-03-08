"""
Malformed data detection and reporting engine.
Produces structured reports for the side-by-side UI viewer.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from config import IngestionConfig
from models.schemas import MalformedReport, MalformedRow, MalformedSeverity


class MalformedHandler:
    """
    Post-parse malformed data analysis.
    
    While parsers detect format-specific issues during parsing,
    this handler performs additional cross-cutting checks after
    a DataFrame has been produced:
    - Entirely empty columns
    - Duplicate column names
    - Excessive null ratios
    - Mixed types within columns
    - Encoding artifacts in string data
    """

    @staticmethod
    def analyze(
        df: pd.DataFrame,
        existing_report: Optional[MalformedReport] = None,
    ) -> MalformedReport:
        """
        Analyze a parsed DataFrame for additional data quality issues.
        Merges findings with any existing malformed report from the parser.
        """
        config = IngestionConfig()
        issues: list[MalformedRow] = []

        if existing_report and existing_report.issues:
            issues.extend(existing_report.issues)

        if df.empty:
            return MalformedReport(
                has_issues=len(issues) > 0,
                total_issues=len(issues),
                issues=issues,
                summary="DataFrame is empty. " + (existing_report.summary if existing_report else ""),
                best_effort_rows_parsed=0,
            )

        # Check for duplicate column names
        dup_cols = df.columns[df.columns.duplicated()].tolist()
        if dup_cols:
            issues.append(MalformedRow(
                row_number=0,
                raw_content=f"Columns: {dup_cols}",
                issue=f"Duplicate column names detected: {dup_cols}. Values may be overwritten.",
                severity=MalformedSeverity.WARNING,
                affected_columns=dup_cols,
                suggested_fix="Columns will be suffixed with _1, _2, etc. to make them unique.",
            ))

        # Check for entirely empty columns
        empty_cols = [col for col in df.columns if df[col].isna().all() or (df[col].astype(str).str.strip() == "").all()]
        if empty_cols:
            issues.append(MalformedRow(
                row_number=0,
                raw_content=f"Columns: {empty_cols}",
                issue=f"{len(empty_cols)} column(s) are entirely empty: {empty_cols[:10]}",
                severity=MalformedSeverity.WARNING,
                affected_columns=empty_cols,
                suggested_fix="These columns contain no data and could be dropped.",
            ))

        # Check for excessively null columns (>90% null)
        null_threshold = 0.9
        for col in df.columns:
            if col in empty_cols:
                continue
            null_ratio = df[col].isna().sum() / len(df)
            if null_ratio > null_threshold:
                issues.append(MalformedRow(
                    row_number=0,
                    raw_content=f"Column '{col}': {null_ratio*100:.1f}% null",
                    issue=f"Column '{col}' is {null_ratio*100:.1f}% null ({df[col].isna().sum()}/{len(df)} values missing).",
                    severity=MalformedSeverity.WARNING,
                    affected_columns=[col],
                    suggested_fix="Consider whether this column is relevant for analysis.",
                ))

        # Check for encoding artifacts in string columns (common: Ã, Â, â€, é)
        encoding_artifacts = ["Ã", "Â", "â€", "é", "è", "ü", "\ufffd"]
        for col in df.select_dtypes(include=["object"]).columns:
            sample = df[col].dropna().head(1000).astype(str)
            for artifact in encoding_artifacts:
                count = sample.str.contains(artifact, na=False).sum()
                if count > 0:
                    issues.append(MalformedRow(
                        row_number=0,
                        raw_content=f"Column '{col}': {count} occurrences of '{artifact}'",
                        issue=f"Column '{col}' contains encoding artifacts ('{artifact}' found {count} times). "
                              f"This may indicate incorrect encoding detection.",
                        severity=MalformedSeverity.WARNING,
                        affected_columns=[col],
                        suggested_fix="Try re-reading the file with a different encoding.",
                    ))
                    break  # One encoding warning per column is enough

        # Cap total issues
        issues = issues[:config.MAX_MALFORMED_REPORT_ROWS]

        best_effort_rows = len(df)
        if existing_report:
            best_effort_rows = existing_report.best_effort_rows_parsed or len(df)

        summary_parts = []
        if existing_report and existing_report.summary:
            summary_parts.append(existing_report.summary)

        new_issue_count = len(issues) - (len(existing_report.issues) if existing_report else 0)
        if new_issue_count > 0:
            summary_parts.append(
                f"Post-parse analysis found {new_issue_count} additional data quality issue(s)."
            )

        return MalformedReport(
            has_issues=len(issues) > 0,
            total_issues=len(issues),
            issues=issues,
            summary=" ".join(summary_parts) if summary_parts else "No data quality issues detected.",
            best_effort_rows_parsed=best_effort_rows,
            best_effort_rows_dropped=(
                existing_report.best_effort_rows_dropped if existing_report else 0
            ),
        )

    @staticmethod
    def generate_side_by_side(
        df: pd.DataFrame,
        malformed_report: MalformedReport,
        max_rows: int = 20,
    ) -> list[dict]:
        """
        Generate side-by-side data for the UI malformed viewer.
        Returns a list of row comparisons showing raw vs cleaned data.
        """
        comparisons = []

        for issue in malformed_report.issues[:max_rows]:
            if issue.row_number <= 0:
                # Column-level issues don't have row comparisons
                comparisons.append({
                    "type": "column_issue",
                    "issue": issue.issue,
                    "severity": issue.severity.value,
                    "suggested_fix": issue.suggested_fix,
                    "affected_columns": issue.affected_columns,
                })
            else:
                # Row-level issues — show the raw vs parsed
                row_idx = issue.row_number - 2  # Adjust for 0-index and header
                parsed_values = {}

                if 0 <= row_idx < len(df):
                    parsed_values = df.iloc[row_idx].to_dict()
                    # Convert to string representations for display
                    parsed_values = {
                        str(k): str(v) if v is not None else "<null>"
                        for k, v in parsed_values.items()
                    }

                comparisons.append({
                    "type": "row_issue",
                    "row_number": issue.row_number,
                    "raw": issue.raw_content,
                    "parsed": parsed_values,
                    "issue": issue.issue,
                    "severity": issue.severity.value,
                    "suggested_fix": issue.suggested_fix,
                })

        return comparisons
