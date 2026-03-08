"""
Simulate API — What-If sandbox for testing preprocessing steps.

Handlers only do request/response marshalling. All business logic
is delegated to cleaning.simulation_utils.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/simulate", tags=["simulate"])


class SimStep(BaseModel):
    action: str
    column: Optional[str] = None
    params: dict = {}


class SimRequest(BaseModel):
    file_id: str
    steps: list[SimStep]
    sample_pct: int = 10


@router.post("/chain")
async def simulate_chain(request: SimRequest):
    """Simulate a chain of preprocessing steps on a sample."""
    try:
        from ingestion.orchestrator import get_stored_dataframe
        from cleaning.simulation_utils import apply_step, compute_stats, readiness_score

        df = get_stored_dataframe(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Sample
        n_sample = max(1, int(len(df) * request.sample_pct / 100))
        n_sample = min(n_sample, 10000, len(df))
        sample = df.sample(n=n_sample, random_state=42).copy()

        # Compute before stats
        before = compute_stats(sample)
        readiness_before = readiness_score(sample)

        # Apply steps
        for step in request.steps:
            sample = apply_step(sample, step.action, step.column, step.params)

        # Compute after stats
        after = compute_stats(sample)
        readiness_after = readiness_score(sample)

        # Compute deltas
        deltas = {}
        for key in before:
            if isinstance(before[key], (int, float)) and isinstance(after.get(key), (int, float)):
                deltas[key] = round(after[key] - before[key], 4)

        return {
            "before": before,
            "after": after,
            "deltas": deltas,
            "readiness_before": readiness_before,
            "readiness_after": readiness_after,
            "rows_affected": abs(len(df) - len(sample)),
            "sample_size": n_sample,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/commit")
async def commit_steps(request: SimRequest):
    """Apply steps to the full dataset (commit simulation)."""
    try:
        from ingestion.orchestrator import get_stored_dataframe, update_stored_dataframe
        from cleaning.simulation_utils import apply_step

        df = get_stored_dataframe(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found")

        df = df.copy()
        for step in request.steps:
            df = apply_step(df, step.action, step.column, step.params)
        update_stored_dataframe(request.file_id, df)
        return {"committed": True, "new_row_count": len(df)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
