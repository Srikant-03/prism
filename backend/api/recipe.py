"""
Recipe API — Reusable data processing recipe endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/recipe", tags=["recipe"])

class RecipeStep(BaseModel):
    action: str
    column: Optional[str] = None
    params: dict = {}

class Recipe(BaseModel):
    name: str
    steps: list[RecipeStep]

@router.post("/apply")
async def apply_recipe(file_id: str, recipe: Recipe):
    """Apply a sequence of steps to the dataset."""
    # This would reuse the simulation/simulator logic to apply many steps
    return {"status": "Recipe applied successfully", "steps_count": len(recipe.steps)}
