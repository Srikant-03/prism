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

from state import annotation_store as _annotations


class Annotation(BaseModel):
    author: str
    text: str
    target: Optional[dict] = None


class AnnotationUpdate(BaseModel):
    text: Optional[str] = None
    pinned: Optional[bool] = None


class ReactionRequest(BaseModel):
    emoji: str
    author: str


@router.get("/{file_id}")
async def get_annotations(file_id: str):
    """Get all annotations for a dataset."""
    from state import get_stored_data
    if not get_stored_data(file_id):
        raise HTTPException(status_code=404, detail=f"Dataset '{file_id}' not found. Upload a file first.")
    return {"annotations": _annotations.get(file_id, [])}


@router.post("/{file_id}")
async def add_annotation(file_id: str, note: Annotation):
    """Add a new annotation."""
    from state import get_stored_data
    if not get_stored_data(file_id):
        raise HTTPException(status_code=404, detail=f"Dataset '{file_id}' not found. Upload a file first.")
    notes = _annotations.get(file_id, [])
    
    new_note = {
        "id": str(uuid.uuid4())[:8],
        "author": note.author,
        "text": note.text,
        "timestamp": int(time.time() * 1000),
        "target": note.target,
        "reactions": {},
        "pinned": False,
    }
    notes.append(new_note)
    _annotations[file_id] = notes
    return new_note


@router.patch("/{file_id}/{annotation_id}")
async def update_annotation(file_id: str, annotation_id: str, update: AnnotationUpdate):
    """Edit an existing annotation (text and/or pinned status)."""
    from state import get_stored_data
    if not get_stored_data(file_id):
        raise HTTPException(status_code=404, detail=f"Dataset '{file_id}' not found. Upload a file first.")
    notes = _annotations.get(file_id, [])
    for note in notes:
        if note["id"] == annotation_id:
            if update.text is not None:
                note["text"] = update.text
                note["edited_at"] = int(time.time() * 1000)
            if update.pinned is not None:
                note["pinned"] = update.pinned
            _annotations[file_id] = notes
            return note

    raise HTTPException(status_code=404, detail=f"Annotation '{annotation_id}' not found")


@router.delete("/{file_id}/{annotation_id}")
async def delete_annotation(file_id: str, annotation_id: str):
    """Delete an annotation by ID."""
    from state import get_stored_data
    if not get_stored_data(file_id):
        raise HTTPException(status_code=404, detail=f"Dataset '{file_id}' not found. Upload a file first.")
    notes = _annotations.get(file_id, [])
    for i, note in enumerate(notes):
        if note["id"] == annotation_id:
            notes.pop(i)
            _annotations[file_id] = notes
            return {"deleted": True, "id": annotation_id}

    raise HTTPException(status_code=404, detail=f"Annotation '{annotation_id}' not found")


@router.post("/{file_id}/{annotation_id}/react")
async def add_reaction(file_id: str, annotation_id: str, reaction: ReactionRequest):
    """Add or toggle a reaction emoji on an annotation."""
    from state import get_stored_data
    if not get_stored_data(file_id):
        raise HTTPException(status_code=404, detail=f"Dataset '{file_id}' not found. Upload a file first.")
    notes = _annotations.get(file_id, [])
    for note in notes:
        if note["id"] == annotation_id:
            reactions = note.get("reactions", {})
            emoji = reaction.emoji
            if emoji not in reactions:
                reactions[emoji] = []
            if reaction.author in reactions[emoji]:
                reactions[emoji].remove(reaction.author)  # Toggle off
            else:
                reactions[emoji].append(reaction.author)  # Toggle on
            note["reactions"] = reactions
            _annotations[file_id] = notes
            return note
    raise HTTPException(status_code=404, detail=f"Annotation '{annotation_id}' not found")
