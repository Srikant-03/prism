"""
Recipe API — Reusable data processing recipe endpoints.
Applies saved cleaning pipelines through the DecisionEngine.
"""

from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.profiling import get_stored_profile

router = APIRouter(prefix="/api/recipe", tags=["recipe"])

# In-memory recipe library
_recipe_store: dict[str, dict] = {}

# Map action prefixes to ActionCategory for automatic categorization
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
    """Map an action type string to an ActionCategory value."""
    for key, category in _ACTION_TO_CATEGORY.items():
        if action_value.startswith(key) or action_value == key:
            return category
    return "structural"


class RecipeStep(BaseModel):
    action: str
    column: Optional[str] = None
    params: dict = {}


class Recipe(BaseModel):
    name: str
    steps: list[RecipeStep]


class ApplyRecipeRequest(BaseModel):
    file_id: str
    recipe: Recipe


@router.post("/save")
async def save_recipe(recipe: Recipe):
    """Save a recipe for later reuse."""
    _recipe_store[recipe.name] = recipe.model_dump()
    return {"saved": True, "name": recipe.name, "steps_count": len(recipe.steps)}


@router.get("/list")
async def list_recipes():
    """List all saved recipes."""
    return {"recipes": list(_recipe_store.values()), "count": len(_recipe_store)}


@router.delete("/{name}")
async def delete_recipe(name: str):
    """Delete a saved recipe."""
    if name not in _recipe_store:
        raise HTTPException(status_code=404, detail=f"Recipe '{name}' not found")
    del _recipe_store[name]
    return {"deleted": True, "name": name}


@router.post("/apply")
async def apply_recipe(request: ApplyRecipeRequest):
    """Apply a sequence of cleaning steps to the dataset using the DecisionEngine."""
    try:
        from cleaning.decision_engine import DecisionEngine
        from cleaning.cleaning_models import (
            CleaningAction, ActionType, ActionCategory, ActionConfidence,
        )
        from ingestion.orchestrator import get_stored_dataframe
        from api.upload import _storage

        df = get_stored_dataframe(request.file_id)
        if df is None:
            raise HTTPException(status_code=404, detail="Dataset not found. Upload a file first.")

        # Get profile for the engine (optional but improves analysis)
        profile = None
        stored_profile = get_stored_profile(request.file_id)
        if stored_profile is not None:
            profile = stored_profile.profile

        engine = DecisionEngine(df.copy(), request.file_id, profile)

        rows_before = len(engine.df)
        cols_before = len(engine.df.columns)
        results = []

        for i, step in enumerate(request.recipe.steps):
            # Resolve ActionType enum
            try:
                action_type = ActionType(step.action)
            except ValueError:
                # Fallback: case-insensitive search
                action_type = None
                for at in ActionType:
                    if at.value.lower() == step.action.lower():
                        action_type = at
                        break
                if action_type is None:
                    results.append({
                        "step": i,
                        "action": step.action,
                        "success": False,
                        "error": f"Unknown action type: '{step.action}'",
                    })
                    continue

            # Resolve ActionCategory
            cat_value = _guess_category(action_type.value)
            try:
                category = ActionCategory(cat_value)
            except ValueError:
                category = ActionCategory.STRUCTURAL

            # Build the full CleaningAction with all required fields
            action = CleaningAction(
                index=i,
                category=category,
                action_type=action_type,
                confidence=ActionConfidence.DEFINITIVE,
                evidence=f"Recipe '{request.recipe.name}' step {i + 1}",
                recommendation=f"Apply {action_type.value}",
                reasoning=f"Included in saved recipe '{request.recipe.name}'",
                target_columns=[step.column] if step.column else [],
                metadata=step.params,
            )

            # Execute via the decision engine
            _, result = engine.apply_action(action)

            results.append({
                "step": i,
                "action": step.action,
                "column": step.column,
                "success": result.success,
                "description": result.description,
                "rows_after": result.rows_after,
                "columns_after": result.columns_after,
            })

        # Persist the transformed DataFrame back to the storage
        if request.file_id in _storage:
            _storage[request.file_id]["df"] = engine.df

        return {
            "status": "completed",
            "recipe_name": request.recipe.name,
            "steps_executed": len(results),
            "rows_before": rows_before,
            "rows_after": len(engine.df),
            "columns_before": cols_before,
            "columns_after": len(engine.df.columns),
            "results": results,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
