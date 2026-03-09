"""
Recipe API — Reusable data processing recipe endpoints.
Save, search, apply, and map cleaning pipelines across datasets.
"""

from __future__ import annotations

import time
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from state import recipe_store as _recipe_store
from api.profiling import get_stored_profile

router = APIRouter(prefix="/api/recipe", tags=["recipe"])


# ── Action → Category mapping ────────────────────────────────────────

_ACTION_TO_CATEGORY = {
    "remove_exact_duplicates": "duplicates",
    "remove_subset_duplicates": "duplicates",
    "merge_near_duplicates": "duplicates",
    "drop_duplicate_column": "duplicates",
    "drop_derived_column": "duplicates",
    "drop_column": "missing_values",
    "drop_rows": "missing_values",
    "impute_": "missing_values",
    "ffill": "missing_values",
    "bfill": "missing_values",
    "interpolate": "missing_values",
    "add_indicator": "missing_values",
    "flag_only": "missing_values",
    "cap_outliers": "outliers",
    "winsorize": "outliers",
    "log_transform": "outliers",
    "replace_boundary": "outliers",
    "remove_outlier_rows": "outliers",
    "flag_outlier": "outliers",
    "convert_type": "type_correction",
    "parse_dates": "type_correction",
    "parse_currency": "type_correction",
    "standardize_booleans": "type_correction",
    "replace_pseudo_nulls": "type_correction",
    "normalize_text": "text_preprocessing",
    "extract_text_features": "text_preprocessing",
    "tfidf_vectorize": "text_preprocessing",
    "label_encode": "categorical_encoding",
    "one_hot_encode": "categorical_encoding",
    "ordinal_encode": "categorical_encoding",
    "standard_scale": "feature_scaling",
    "minmax_scale": "feature_scaling",
    "robust_scale": "feature_scaling",
    "drop_zero_variance": "feature_selection",
    "drop_near_zero_variance": "feature_selection",
    "drop_high_correlation": "feature_selection",
    "standardize_casing": "data_standardization",
    "standardize_whitespace": "data_standardization",
}


def _guess_category(action_value: str) -> str:
    for key, category in _ACTION_TO_CATEGORY.items():
        if action_value.startswith(key) or action_value == key:
            return category
    return "structural"


# ── Models ────────────────────────────────────────────────────────────

class RecipeStep(BaseModel):
    action: str
    column: Optional[str] = None
    columns: list[str] = []
    params: dict = {}


class Recipe(BaseModel):
    name: str
    description: str = ""
    tags: list[str] = []
    steps: list[RecipeStep] = []


class ApplyRecipeRequest(BaseModel):
    file_id: str
    recipe: Recipe


class ApplyMappedRequest(BaseModel):
    file_id: str
    recipe_name: str
    column_mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Maps original column names → new dataset column names",
    )


class SaveFromSessionRequest(BaseModel):
    file_id: str
    recipe_name: str
    description: str = ""
    tags: list[str] = []


# ── CRUD endpoints ───────────────────────────────────────────────────

@router.post("/save")
async def save_recipe(recipe: Recipe):
    """Save a recipe for later reuse."""
    data = recipe.model_dump()
    data["created_at"] = time.time()
    _recipe_store[recipe.name] = data
    return {"saved": True, "name": recipe.name, "steps_count": len(recipe.steps)}


@router.get("/list")
async def list_recipes():
    """List all saved recipes."""
    return {"recipes": list(_recipe_store.values()), "count": len(_recipe_store)}


@router.get("/search")
async def search_recipes(q: str = "", tag: str = ""):
    """Search recipes by name/description text or tag."""
    results = []
    q_lower = q.lower()
    tag_lower = tag.lower()

    for recipe_data in _recipe_store.values():
        if not isinstance(recipe_data, dict):
            continue

        # Text match on name + description
        if q_lower:
            name = (recipe_data.get("name") or "").lower()
            desc = (recipe_data.get("description") or "").lower()
            if q_lower not in name and q_lower not in desc:
                continue

        # Tag match
        if tag_lower:
            tags = [t.lower() for t in (recipe_data.get("tags") or [])]
            if tag_lower not in tags:
                continue

        results.append(recipe_data)

    return {"recipes": results, "count": len(results)}


@router.get("/{name}")
async def get_recipe(name: str):
    """Get a single recipe by name."""
    data = _recipe_store.get(name)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Recipe '{name}' not found")
    return data


@router.delete("/{name}")
async def delete_recipe(name: str):
    """Delete a saved recipe."""
    if name not in _recipe_store:
        raise HTTPException(status_code=404, detail=f"Recipe '{name}' not found")
    del _recipe_store[name]
    return {"deleted": True, "name": name}


# ── Apply recipe (direct) ────────────────────────────────────────────

@router.post("/apply")
async def apply_recipe(request: ApplyRecipeRequest):
    """Apply a sequence of cleaning steps to the dataset."""
    return await _execute_recipe(
        file_id=request.file_id,
        recipe_name=request.recipe.name,
        steps=request.recipe.steps,
        column_mapping={},
    )


# ── Apply recipe with column mapping ─────────────────────────────────

@router.post("/apply-mapped")
async def apply_mapped(request: ApplyMappedRequest):
    """Apply a saved recipe to a new dataset with column name mapping."""
    recipe_data = _recipe_store.get(request.recipe_name)
    if recipe_data is None:
        raise HTTPException(status_code=404, detail=f"Recipe '{request.recipe_name}' not found")

    steps = [RecipeStep(**s) for s in recipe_data.get("steps", [])]
    return await _execute_recipe(
        file_id=request.file_id,
        recipe_name=request.recipe_name,
        steps=steps,
        column_mapping=request.column_mapping,
    )


# ── Save from current cleaning session ───────────────────────────────

@router.post("/save-from-session")
async def save_from_session(request: SaveFromSessionRequest):
    """Capture the current cleaning session's applied steps as a named recipe."""
    from state import get_cleaning_state

    state = get_cleaning_state(request.file_id)
    if state is None:
        raise HTTPException(status_code=404, detail="No cleaning session found for this file. Run cleaning first.")

    engine = state.get("engine")
    if engine is None:
        raise HTTPException(status_code=404, detail="Cleaning engine not found.")

    # Extract the audit log from the engine
    audit_log = getattr(engine, "audit_log", None) or []
    if not audit_log:
        raise HTTPException(status_code=400, detail="No actions applied in the current session.")

    steps = []
    for entry in audit_log:
        if isinstance(entry, dict):
            action = entry.get("action_type", entry.get("action", "unknown"))
            cols = entry.get("target_columns", [])
            column = cols[0] if cols else None
            params = entry.get("metadata", entry.get("params", {}))
        else:
            action = getattr(entry, "action_type", "unknown")
            if hasattr(action, "value"):
                action = action.value
            cols = getattr(entry, "target_columns", [])
            column = cols[0] if cols else None
            params = getattr(entry, "metadata", {})

        steps.append(RecipeStep(action=str(action), column=column, columns=list(cols), params=params or {}))

    recipe_data = {
        "name": request.recipe_name,
        "description": request.description or f"Auto-captured from session on {request.file_id}",
        "tags": request.tags,
        "steps": [s.model_dump() for s in steps],
        "source_file_id": request.file_id,
        "created_at": time.time(),
    }
    _recipe_store[request.recipe_name] = recipe_data

    return {
        "saved": True,
        "name": request.recipe_name,
        "steps_count": len(steps),
        "steps": [s.model_dump() for s in steps],
    }


# ── Shared execution logic ───────────────────────────────────────────

async def _execute_recipe(
    file_id: str,
    recipe_name: str,
    steps: list[RecipeStep],
    column_mapping: dict[str, str],
):
    """Execute recipe steps through DecisionEngine with optional column remapping."""
    try:
        from cleaning.decision_engine import DecisionEngine
        from cleaning.cleaning_models import (
            CleaningAction, ActionType, ActionCategory, ActionConfidence,
        )
        from state import get_df, set_df

        df = get_df(file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found. Upload a file first.")

        profile = None
        stored_profile = get_stored_profile(file_id)
        if stored_profile is not None:
            profile = stored_profile.profile if hasattr(stored_profile, "profile") else stored_profile.get("profile") if isinstance(stored_profile, dict) else None

        engine = DecisionEngine(df.copy(), file_id, profile)
        rows_before = len(engine.df)
        cols_before = len(engine.df.columns)
        results = []

        for i, step in enumerate(steps):
            # Apply column mapping
            mapped_col = step.column
            if mapped_col and column_mapping:
                mapped_col = column_mapping.get(mapped_col, mapped_col)

            mapped_cols = []
            if getattr(step, "columns", None):
                mapped_cols = [column_mapping.get(c, c) for c in step.columns]
            elif mapped_col:
                mapped_cols = [mapped_col]

            # Resolve ActionType
            try:
                action_type = ActionType(step.action)
            except ValueError:
                action_type = None
                for at in ActionType:
                    if at.value.lower() == step.action.lower():
                        action_type = at
                        break
                if action_type is None:
                    results.append({
                        "step": i, "action": step.action,
                        "success": False, "error": f"Unknown action: '{step.action}'",
                    })
                    continue

            cat_value = _guess_category(action_type.value)
            try:
                category = ActionCategory(cat_value)
            except ValueError:
                category = ActionCategory.STRUCTURAL

            # Remap target_columns in metadata if needed
            params = dict(step.params)
            if column_mapping:
                for key in ("target_column", "column", "columns"):
                    if key in params:
                        val = params[key]
                        if isinstance(val, str):
                            params[key] = column_mapping.get(val, val)
                        elif isinstance(val, list):
                            params[key] = [column_mapping.get(v, v) for v in val]

            action = CleaningAction(
                index=i,
                category=category,
                action_type=action_type,
                confidence=ActionConfidence.DEFINITIVE,
                evidence=f"Recipe '{recipe_name}' step {i + 1}",
                recommendation=f"Apply {action_type.value}",
                reasoning=f"From recipe '{recipe_name}'",
                target_columns=mapped_cols,
                metadata=params,
            )

            _, result = engine.apply_action(action)
            results.append({
                "step": i,
                "action": step.action,
                "column": mapped_col,
                "success": result.success,
                "description": result.description,
                "rows_after": result.rows_after,
                "columns_after": result.columns_after,
            })

        set_df(file_id, engine.df)

        return {
            "status": "completed",
            "recipe_name": recipe_name,
            "steps_executed": len(results),
            "rows_before": rows_before,
            "rows_after": len(engine.df),
            "columns_before": cols_before,
            "columns_after": len(engine.df.columns),
            "column_mapping_applied": bool(column_mapping),
            "results": results,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
