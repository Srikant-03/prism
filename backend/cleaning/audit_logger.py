"""
Audit Logger — Pipeline control, versioning, and audit trail.
Supports undo/redo, named snapshots, reproducibility export (Python / JSON),
before/after comparison, and full audit log.
"""

from __future__ import annotations

import copy
import csv
import io
import json
import time
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import numpy as np


class AuditEntry:
    """Single audit log entry."""

    def __init__(
        self, step_name: str, action_type: str, trigger_reason: str,
        rows_before: int, rows_after: int,
        cols_before: int, cols_after: int,
        columns_affected: list[str],
        before_stats: dict[str, Any], after_stats: dict[str, Any],
        status: str = "applied",
    ):
        self.timestamp = datetime.utcnow().isoformat()
        self.step_name = step_name
        self.action_type = action_type
        self.trigger_reason = trigger_reason
        self.rows_before = rows_before
        self.rows_after = rows_after
        self.cols_before = cols_before
        self.cols_after = cols_after
        self.columns_affected = columns_affected
        self.before_stats = before_stats
        self.after_stats = after_stats
        self.status = status  # applied, skipped, user_overridden

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "step_name": self.step_name,
            "action_type": self.action_type,
            "trigger_reason": self.trigger_reason,
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "cols_before": self.cols_before,
            "cols_after": self.cols_after,
            "columns_affected": self.columns_affected,
            "status": self.status,
            "before_stats": self.before_stats,
            "after_stats": self.after_stats,
        }


class PipelineSnapshot:
    """Named snapshot of pipeline state."""

    def __init__(self, name: str, df: pd.DataFrame, audit_log: list[AuditEntry]):
        self.name = name
        self.created_at = datetime.utcnow().isoformat()
        self.df = df.copy()
        self.audit_log = list(audit_log)
        self.n_rows = len(df)
        self.n_cols = len(df.columns)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "created_at": self.created_at,
            "n_rows": self.n_rows,
            "n_cols": self.n_cols,
            "steps_applied": len(self.audit_log),
        }


class AuditLogger:
    """
    Full pipeline audit and control system.
    Wraps a DataFrame and tracks all transformations with undo/redo,
    snapshots, and reproducibility export.
    """

    def __init__(self, original_df: pd.DataFrame, file_id: str):
        self.file_id = file_id
        self.original_df = original_df.copy()
        self.current_df = original_df.copy()

        # Undo/Redo stacks
        self._undo_stack: list[tuple[pd.DataFrame, AuditEntry]] = []
        self._redo_stack: list[tuple[pd.DataFrame, AuditEntry]] = []

        # Audit log
        self.audit_log: list[AuditEntry] = []

        # Snapshots
        self.snapshots: dict[str, PipelineSnapshot] = {}

        # Pipeline steps for reproducibility
        self._pipeline_steps: list[dict] = []

        # Auto-save initial snapshot
        self.save_snapshot("__original__")

    # ── Core operations ───────────────────────────────────────────────

    def record_step(
        self,
        step_name: str,
        action_type: str,
        trigger_reason: str,
        new_df: pd.DataFrame,
        columns_affected: list[str],
        pipeline_code: Optional[str] = None,
        status: str = "applied",
    ) -> AuditEntry:
        """Record a preprocessing step with before/after comparison."""
        before_stats = self._compute_stats(self.current_df, columns_affected)
        after_stats = self._compute_stats(new_df, columns_affected)

        entry = AuditEntry(
            step_name=step_name,
            action_type=action_type,
            trigger_reason=trigger_reason,
            rows_before=len(self.current_df),
            rows_after=len(new_df),
            cols_before=len(self.current_df.columns),
            cols_after=len(new_df.columns),
            columns_affected=columns_affected,
            before_stats=before_stats,
            after_stats=after_stats,
            status=status,
        )

        # Push to undo stack
        self._undo_stack.append((self.current_df.copy(), entry))
        self._redo_stack.clear()

        # Update current
        self.current_df = new_df.copy()
        self.audit_log.append(entry)

        # Record pipeline code for reproducibility
        if pipeline_code:
            self._pipeline_steps.append({
                "step_name": step_name,
                "action_type": action_type,
                "code": pipeline_code,
            })

        return entry

    def undo(self) -> Optional[AuditEntry]:
        """Undo the last applied step."""
        if not self._undo_stack:
            return None

        prev_df, entry = self._undo_stack.pop()
        self._redo_stack.append((self.current_df.copy(), entry))

        self.current_df = prev_df
        entry.status = "undone"
        return entry

    def redo(self) -> Optional[AuditEntry]:
        """Redo the last undone step."""
        if not self._redo_stack:
            return None

        next_df, entry = self._redo_stack.pop()
        self._undo_stack.append((self.current_df.copy(), entry))

        self.current_df = next_df
        entry.status = "applied"
        return entry

    # ── Snapshots ─────────────────────────────────────────────────────

    def save_snapshot(self, name: str) -> PipelineSnapshot:
        """Save a named snapshot of the current state."""
        snapshot = PipelineSnapshot(name, self.current_df, self.audit_log)
        self.snapshots[name] = snapshot
        return snapshot

    def load_snapshot(self, name: str) -> bool:
        """Restore a previously saved snapshot."""
        if name not in self.snapshots:
            return False

        snapshot = self.snapshots[name]
        self.current_df = snapshot.df.copy()
        return True

    def list_snapshots(self) -> list[dict]:
        """List all saved snapshots."""
        return [s.to_dict() for s in self.snapshots.values()]

    # ── Comparison ────────────────────────────────────────────────────

    def compare_with_original(self) -> dict:
        """Generate a comprehensive diff between original and current dataset."""
        orig = self.original_df
        curr = self.current_df

        # Column changes
        orig_cols = set(orig.columns)
        curr_cols = set(curr.columns)
        added_cols = list(curr_cols - orig_cols)
        removed_cols = list(orig_cols - curr_cols)
        common_cols = list(orig_cols & curr_cols)

        # Row changes
        rows_removed = len(orig) - len(curr) if len(curr) < len(orig) else 0
        rows_added = len(curr) - len(orig) if len(curr) > len(orig) else 0

        # Value changes in common columns
        values_changed = 0
        type_changes: list[dict] = []
        for col in common_cols:
            if col in orig.columns and col in curr.columns:
                if str(orig[col].dtype) != str(curr[col].dtype):
                    type_changes.append({
                        "column": col,
                        "before": str(orig[col].dtype),
                        "after": str(curr[col].dtype),
                    })

                try:
                    min_len = min(len(orig), len(curr))
                    diff_mask = orig[col].iloc[:min_len].astype(str) != curr[col].iloc[:min_len].astype(str)
                    values_changed += int(diff_mask.sum())
                except Exception:
                    pass

        # Missing value changes
        orig_missing = int(orig[common_cols].isnull().sum().sum()) if common_cols else 0
        curr_missing = int(curr[list(curr_cols & set(common_cols))].isnull().sum().sum()) if common_cols else 0

        return {
            "original_shape": {"rows": len(orig), "columns": len(orig.columns)},
            "current_shape": {"rows": len(curr), "columns": len(curr.columns)},
            "rows_added": rows_added,
            "rows_removed": rows_removed,
            "columns_added": added_cols,
            "columns_removed": removed_cols,
            "type_changes": type_changes,
            "values_changed": values_changed,
            "missing_values_before": orig_missing,
            "missing_values_after": curr_missing,
            "total_steps_applied": len([e for e in self.audit_log if e.status == "applied"]),
        }

    def step_comparison(self, step_index: int) -> Optional[dict]:
        """Get before/after comparison for a specific step."""
        if step_index < 0 or step_index >= len(self.audit_log):
            return None

        entry = self.audit_log[step_index]
        return {
            "step_index": step_index,
            "step_name": entry.step_name,
            "action_type": entry.action_type,
            "rows": {"before": entry.rows_before, "after": entry.rows_after},
            "columns": {"before": entry.cols_before, "after": entry.cols_after},
            "columns_affected": entry.columns_affected,
            "before_stats": entry.before_stats,
            "after_stats": entry.after_stats,
            "status": entry.status,
        }

    # ── Export ─────────────────────────────────────────────────────────

    def export_audit_log_json(self) -> str:
        """Export audit log as JSON."""
        return json.dumps(
            [entry.to_dict() for entry in self.audit_log],
            indent=2, default=str,
        )

    def export_audit_log_csv(self) -> str:
        """Export audit log as CSV."""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            "timestamp", "step_name", "action_type", "trigger_reason",
            "rows_before", "rows_after", "cols_before", "cols_after",
            "columns_affected", "status",
        ])
        writer.writeheader()
        for entry in self.audit_log:
            row = entry.to_dict()
            row["columns_affected"] = ", ".join(row["columns_affected"])
            row.pop("before_stats", None)
            row.pop("after_stats", None)
            writer.writerow(row)
        return output.getvalue()

    def export_pipeline_python(self) -> str:
        """Export reproducible Python script."""
        lines = [
            '"""',
            f'Reproducible preprocessing pipeline for file: {self.file_id}',
            f'Generated: {datetime.utcnow().isoformat()}',
            f'Steps: {len(self._pipeline_steps)}',
            '"""',
            '',
            'import pandas as pd',
            'import numpy as np',
            '',
            '',
            'def preprocess(df: pd.DataFrame) -> pd.DataFrame:',
            '    """Apply the full preprocessing pipeline."""',
            '    df = df.copy()',
            '',
        ]

        for i, step in enumerate(self._pipeline_steps):
            lines.append(f'    # Step {i + 1}: {step["step_name"]} ({step["action_type"]})')
            for code_line in step["code"].split("\n"):
                lines.append(f'    {code_line}')
            lines.append('')

        lines.extend([
            '    return df',
            '',
            '',
            'if __name__ == "__main__":',
            '    import sys',
            '    input_file = sys.argv[1] if len(sys.argv) > 1 else "input.csv"',
            '    output_file = sys.argv[2] if len(sys.argv) > 2 else "cleaned.csv"',
            '',
            '    df = pd.read_csv(input_file)',
            '    print(f"Input: {len(df)} rows, {len(df.columns)} columns")',
            '',
            '    df_clean = preprocess(df)',
            '    print(f"Output: {len(df_clean)} rows, {len(df_clean.columns)} columns")',
            '',
            '    df_clean.to_csv(output_file, index=False)',
            '    print(f"Saved to {output_file}")',
        ])

        return "\n".join(lines)

    def export_pipeline_json(self) -> str:
        """Export pipeline as JSON specification."""
        steps = []
        for i, entry in enumerate(self.audit_log):
            if entry.status != "applied":
                continue
            steps.append({
                "order": i + 1,
                "step_name": entry.step_name,
                "action_type": entry.action_type,
                "trigger_reason": entry.trigger_reason,
                "columns_affected": entry.columns_affected,
                "rows_before": entry.rows_before,
                "rows_after": entry.rows_after,
            })

        spec = {
            "file_id": self.file_id,
            "generated_at": datetime.utcnow().isoformat(),
            "original_shape": {
                "rows": len(self.original_df),
                "columns": len(self.original_df.columns),
            },
            "final_shape": {
                "rows": len(self.current_df),
                "columns": len(self.current_df.columns),
            },
            "steps": steps,
            "comparison": self.compare_with_original(),
        }

        return json.dumps(spec, indent=2, default=str)

    # ── Pipeline state for UI ─────────────────────────────────────────

    def get_pipeline_state(self) -> dict:
        """Get the current state of the pipeline for frontend rendering."""
        steps = []
        for i, entry in enumerate(self.audit_log):
            steps.append({
                "index": i,
                "step_name": entry.step_name,
                "action_type": entry.action_type,
                "status": entry.status,
                "trigger_reason": entry.trigger_reason,
                "rows_delta": entry.rows_after - entry.rows_before,
                "cols_delta": entry.cols_after - entry.cols_before,
                "timestamp": entry.timestamp,
                "enabled": entry.status == "applied",
            })

        return {
            "steps": steps,
            "total_steps": len(steps),
            "applied_steps": len([s for s in steps if s["status"] == "applied"]),
            "can_undo": len(self._undo_stack) > 0,
            "can_redo": len(self._redo_stack) > 0,
            "snapshots": self.list_snapshots(),
            "comparison": self.compare_with_original(),
        }

    # ── Private helpers ───────────────────────────────────────────────

    @staticmethod
    def _compute_stats(df: pd.DataFrame, columns: list[str]) -> dict:
        """Compute summary statistics for affected columns."""
        stats: dict[str, Any] = {}
        for col in columns:
            if col not in df.columns:
                continue
            s = df[col]
            col_stats: dict[str, Any] = {
                "dtype": str(s.dtype),
                "null_count": int(s.isnull().sum()),
                "unique_count": int(s.nunique()),
            }
            if pd.api.types.is_numeric_dtype(s):
                non_null = s.dropna()
                if len(non_null) > 0:
                    col_stats.update({
                        "mean": round(float(non_null.mean()), 4),
                        "std": round(float(non_null.std()), 4),
                        "min": round(float(non_null.min()), 4),
                        "max": round(float(non_null.max()), 4),
                    })
            stats[col] = col_stats
        return stats

    def generate_pipeline_code(self, action_type: str, columns: list[str],
                                option: str = "", metadata: dict = None) -> str:
        """Generate reproducible Python code for a pipeline step."""
        if metadata is None:
            metadata = {}
        col = columns[0] if columns else "col"

        code_map = {
            "remove_exact_duplicates": "df = df.drop_duplicates()",
            "drop_column": f"df = df.drop(columns={columns})",
            "drop_rows": f"df = df.dropna(subset={columns})",
            "impute_mean": f"df['{col}'] = df['{col}'].fillna(df['{col}'].mean())",
            "impute_median": f"df['{col}'] = df['{col}'].fillna(df['{col}'].median())",
            "impute_mode": f"df['{col}'] = df['{col}'].fillna(df['{col}'].mode().iloc[0])",
            "standard_scale": f"df['{col}'] = (df['{col}'] - df['{col}'].mean()) / df['{col}'].std()",
            "minmax_scale": f"df['{col}'] = (df['{col}'] - df['{col}'].min()) / (df['{col}'].max() - df['{col}'].min())",
            "log1p_transform": f"df['{col}'] = np.log1p(df['{col}'])",
            "standardize_casing": f"df['{col}'] = df['{col}'].str.{option or 'lower'}()",
            "standardize_whitespace": f"df['{col}'] = df['{col}'].str.strip().str.replace(r'\\s+', ' ', regex=True)",
            "parse_dates": f"df['{col}'] = pd.to_datetime(df['{col}'], errors='coerce')",
            "label_encode": f"df['{col}'] = df['{col}'].astype('category').cat.codes",
            "one_hot_encode": f"df = pd.get_dummies(df, columns=['{col}'], dtype=int)",
        }

        return code_map.get(action_type, f"# {action_type} applied to {columns}")
