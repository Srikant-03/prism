"""
Collab API — Annotation and collaborative session endpoints.
Supports creating, listing, editing, and deleting annotations per dataset.
"""

from __future__ import annotations

import uuid
import time
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/collab", tags=["collab"])

# In-memory store for annotations (per-session)
_annotations: dict[str, list[dict]] = {}


class Annotation(BaseModel):
    author: str
    text: str
    target: Optional[dict] = None


class AnnotationUpdate(BaseModel):
    text: Optional[str] = None
    pinned: Optional[bool] = None


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
        "pinned": False,
    }
    _annotations[file_id].append(new_note)
    return new_note


@router.patch("/{file_id}/{annotation_id}")
async def update_annotation(file_id: str, annotation_id: str, update: AnnotationUpdate):
    """Edit an existing annotation (text and/or pinned status)."""
    notes = _annotations.get(file_id, [])
    for note in notes:
        if note["id"] == annotation_id:
            if update.text is not None:
                note["text"] = update.text
                note["edited_at"] = int(time.time() * 1000)
            if update.pinned is not None:
                note["pinned"] = update.pinned
            return note

    raise HTTPException(status_code=404, detail=f"Annotation '{annotation_id}' not found")


@router.delete("/{file_id}/{annotation_id}")
async def delete_annotation(file_id: str, annotation_id: str):
    """Delete an annotation by ID."""
    notes = _annotations.get(file_id, [])
    for i, note in enumerate(notes):
        if note["id"] == annotation_id:
            notes.pop(i)
            return {"deleted": True, "id": annotation_id}

    raise HTTPException(status_code=404, detail=f"Annotation '{annotation_id}' not found")
