"""
Cleaning API endpoints.
Exposes the Decision Engine via REST for frontend interaction.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from cleaning.decision_engine import DecisionEngine
from cleaning.cleaning_models import CleaningPlan, ActionResult, ActionStatus
from ingestion.orchestrator import get_stored_dataframe, get_stored_data
from api.profiling import _profile_store

router = APIRouter(prefix="/api/cleaning", tags=["cleaning"])

# In-memory store for cleaning state
_cleaning_store: dict[str, dict] = {}


def _get_engine(file_id: str) -> DecisionEngine:
    """Create or retrieve a DecisionEngine for a file_id."""
    df = get_stored_dataframe(file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="File not found. Please ingest the file first.")

    # Get profile (for feature importances and semantic types)
    profile = None
    if file_id in _profile_store:
        profile = _profile_store[file_id].profile

    return DecisionEngine(df.copy(), file_id, profile)


@router.get("/{file_id}/analyze")
async def analyze_cleaning(file_id: str):
    """
    Run the Decision Engine and return a full CleaningPlan.
    Each action includes evidence, reasoning, preview, and impact.
    """
    engine = _get_engine(file_id)
    plan = engine.analyze()

    # Store engine + plan for future apply calls
    _cleaning_store[file_id] = {
        "engine": engine,
        "plan": plan,
    }

    return plan.model_dump()


@router.post("/{file_id}/apply/{action_index}")
async def apply_action(file_id: str, action_index: int, selected_option: str | None = None):
    """Apply a single cleaning action by its index."""
    if file_id not in _cleaning_store:
        raise HTTPException(status_code=400, detail="Run analyze first.")

    store = _cleaning_store[file_id]
    engine: DecisionEngine = store["engine"]
    plan: CleaningPlan = store["plan"]

    if action_index < 0 or action_index >= len(plan.actions):
        raise HTTPException(status_code=400, detail=f"Invalid action index: {action_index}")

    action = plan.actions[action_index]
    if action.status != ActionStatus.PENDING:
        raise HTTPException(status_code=400, detail=f"Action already {action.status.value}.")

    new_df, result = engine.apply_action(action, selected_option)
    action.status = ActionStatus.APPLIED

    # Update stored DataFrame so subsequent actions operate on the cleaned data
    from ingestion.orchestrator import update_stored_dataframe
    update_stored_dataframe(file_id, new_df)

    return result.model_dump()


@router.post("/{file_id}/apply-all-definitive")
async def apply_all_definitive(file_id: str):
    """Auto-apply all definitive (safe) actions in one pass."""
    if file_id not in _cleaning_store:
        # Run analysis first
        engine = _get_engine(file_id)
        plan = engine.analyze()
        _cleaning_store[file_id] = {"engine": engine, "plan": plan}

    store = _cleaning_store[file_id]
    engine: DecisionEngine = store["engine"]
    plan: CleaningPlan = store["plan"]

    new_df, results = engine.apply_all_definitive(plan)

    # Update stored DataFrame
    from ingestion.orchestrator import update_stored_dataframe
    update_stored_dataframe(file_id, new_df)

    return {
        "results": [r.model_dump() for r in results],
        "total_applied": len(results),
        "rows_after": len(new_df),
        "columns_after": len(new_df.columns),
    }


@router.post("/{file_id}/skip/{action_index}")
async def skip_action(file_id: str, action_index: int):
    """Mark an action as skipped."""
    if file_id not in _cleaning_store:
        raise HTTPException(status_code=400, detail="Run analyze first.")

    plan: CleaningPlan = _cleaning_store[file_id]["plan"]
    if action_index < 0 or action_index >= len(plan.actions):
        raise HTTPException(status_code=400, detail=f"Invalid action index: {action_index}")

    plan.actions[action_index].status = ActionStatus.SKIPPED
    return {"status": "skipped", "action_index": action_index}


@router.get("/{file_id}/preview/{action_index}")
async def preview_action(file_id: str, action_index: int):
    """Get the before/after preview for a specific action."""
    if file_id not in _cleaning_store:
        raise HTTPException(status_code=400, detail="Run analyze first.")

    plan: CleaningPlan = _cleaning_store[file_id]["plan"]
    if action_index < 0 or action_index >= len(plan.actions):
        raise HTTPException(status_code=400, detail=f"Invalid action index: {action_index}")

    action = plan.actions[action_index]
    return {
        "action_index": action_index,
        "action_type": action.action_type.value,
        "preview": action.preview.model_dump() if action.preview else None,
        "impact": action.impact.model_dump(),
    }
