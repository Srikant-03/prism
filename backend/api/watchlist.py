"""
Watchlist API — Anomaly watchlist CRUD and auto-population.
"""

from __future__ import annotations

import uuid
import time
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

# Capped in-memory watchlist (max 500 items — evicts oldest on overflow)
_MAX_WATCHLIST = 500
_watchlist: list[dict] = []


def _append_bounded(entry: dict):
    """Append to _watchlist with capacity enforcement."""
    _watchlist.append(entry)
    while len(_watchlist) > _MAX_WATCHLIST:
        _watchlist.pop(0)


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
    return {"items": _watchlist, "count": len(_watchlist)}


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
        _append_bounded(entry)
        added.append(entry)
    return {"added": len(added), "total": len(_watchlist)}


@router.put("/{item_id}/status")
async def update_status(item_id: str, update: StatusUpdate):
    """Update an item's status."""
    for item in _watchlist:
        if item["id"] == item_id:
            item["status"] = update.status
            return item
    raise HTTPException(status_code=404, detail="Item not found")


@router.put("/{item_id}/note")
async def update_note(item_id: str, update: NoteUpdate):
    """Update an item's note."""
    for item in _watchlist:
        if item["id"] == item_id:
            item["note"] = update.note
            return item
    raise HTTPException(status_code=404, detail="Item not found")


@router.post("/bulk-status")
async def bulk_status(update: BulkUpdate):
    """Bulk update status for multiple items."""
    updated = 0
    for item in _watchlist:
        if item["id"] in update.ids:
            item["status"] = update.status
            updated += 1
    return {"updated": updated}


@router.delete("/{item_id}")
async def delete_item(item_id: str):
    """Remove an item from the watchlist."""
    global _watchlist
    before = len(_watchlist)
    _watchlist = [i for i in _watchlist if i["id"] != item_id]
    if len(_watchlist) == before:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"deleted": True}


@router.delete("/")
async def clear_watchlist():
    """Clear all watchlist items."""
    global _watchlist
    count = len(_watchlist)
    _watchlist = []
    return {"cleared": count}
