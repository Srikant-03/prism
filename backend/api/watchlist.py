"""
Watchlist API — Anomaly watchlist CRUD and auto-population.
"""

from __future__ import annotations

import uuid
import time
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from state import watchlist_store

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

# Capped watchlist (max 500 items — evicts oldest on overflow)
_MAX_WATCHLIST = 500


def _get_list() -> list[dict]:
    return watchlist_store.get("global_list", [])


def _save_list(lst: list[dict]):
    watchlist_store["global_list"] = lst


def _append_bounded(entry: dict):
    """Append to watchlist with capacity enforcement."""
    lst = _get_list()
    lst.append(entry)
    while len(lst) > _MAX_WATCHLIST:
        lst.pop(0)
    _save_list(lst)


class WatchlistItem(BaseModel):
    row_index: int
    column: str
    value: Optional[str] = None
    severity: str = "warning"
    reason: str = ""
    detected_by: str = "manual"


class StatusUpdate(BaseModel):
    status: str


class NoteUpdate(BaseModel):
    note: str


class BulkUpdate(BaseModel):
    ids: list[str]
    status: str


@router.get("/")
async def list_watchlist():
    """Get all watchlist items."""
    lst = _get_list()
    return {"items": lst, "count": len(lst)}


@router.post("/")
async def add_item(item: WatchlistItem):
    """Add an item to the watchlist."""
    entry = {
        "id": str(uuid.uuid4())[:8],
        "rowIndex": item.row_index,
        "column": item.column,
        "value": item.value,
        "severity": item.severity,
        "reason": item.reason,
        "detectedBy": item.detected_by,
        "status": "unreviewed",
        "note": "",
        "created_at": time.time(),
    }
    _append_bounded(entry)
    return entry


@router.post("/bulk-add")
async def bulk_add(items: list[WatchlistItem]):
    """Bulk add items (used by profiling auto-populate)."""
    lst = _get_list()
    added = []
    for item in items:
        entry = {
            "id": str(uuid.uuid4())[:8],
            "rowIndex": item.row_index,
            "column": item.column,
            "value": item.value,
            "severity": item.severity,
            "reason": item.reason,
            "detectedBy": item.detected_by,
            "status": "unreviewed",
            "note": "",
            "created_at": time.time(),
        }
        lst.append(entry)
        added.append(entry)
        
    while len(lst) > _MAX_WATCHLIST:
        lst.pop(0)
    _save_list(lst)
    
    return {"added": len(added), "total": len(lst)}


@router.put("/{item_id}/status")
async def update_status(item_id: str, update: StatusUpdate):
    """Update an item's status."""
    lst = _get_list()
    for item in lst:
        if item["id"] == item_id:
            item["status"] = update.status
            _save_list(lst)
            return item
    raise HTTPException(status_code=404, detail="Item not found")


@router.put("/{item_id}/note")
async def update_note(item_id: str, update: NoteUpdate):
    """Update an item's note."""
    lst = _get_list()
    for item in lst:
        if item["id"] == item_id:
            item["note"] = update.note
            _save_list(lst)
            return item
    raise HTTPException(status_code=404, detail="Item not found")


@router.post("/bulk-status")
async def bulk_status(update: BulkUpdate):
    """Bulk update status for multiple items."""
    lst = _get_list()
    updated = 0
    for item in lst:
        if item["id"] in update.ids:
            item["status"] = update.status
            updated += 1
    _save_list(lst)
    return {"updated": updated}


@router.delete("/{item_id}")
async def delete_item(item_id: str):
    """Remove an item from the watchlist."""
    lst = _get_list()
    before = len(lst)
    lst = [i for i in lst if i["id"] != item_id]
    if len(lst) == before:
        raise HTTPException(status_code=404, detail="Item not found")
    _save_list(lst)
    return {"deleted": True}


@router.delete("/")
async def clear_watchlist():
    """Clear all watchlist items."""
    lst = _get_list()
    count = len(lst)
    _save_list([])
    return {"cleared": count}
