"""
Collab API — Annotation and collaborative session endpoints.
"""

from __future__ import annotations

import uuid
import time
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/collab", tags=["collab"])

# In-memory store for annotations (per-session)
_annotations = {}

class Annotation(BaseModel):
    author: str
    text: str
    target: Optional[dict] = None

@router.get("/{file_id}")
async def get_annotations(file_id: str):
    """Get all annotations for a dataset."""
    return {"annotations": _annotations.get(file_id, [])}

@router.post("/{file_id}")
async def add_annotation(file_id: str, note: Annotation):
    """Add a new annotation."""
    if file_id not in _annotations:
        _annotations[file_id] = []
    
    new_note = {
        "id": str(uuid.uuid4())[:8],
        "author": note.author,
        "text": note.text,
        "timestamp": int(time.time() * 1000),
        "target": note.target,
        "reactions": {},
        "pinned": False
    }
    _annotations[file_id].append(new_note)
    return new_note
