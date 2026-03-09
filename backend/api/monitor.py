"""
Data Freshness Monitor API.
Allows scheduling recurring jobs to poll an origin endpoint/file, check for
schema drift or quality drops, and record a history of these metrics.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import pandas as pd

router = APIRouter(prefix="/api/monitor", tags=["monitor"])

# In-memory store for monitor jobs and histories (replace with Redis in production)
_monitors = {}
_history = {}


class MonitorRequest(BaseModel):
    file_id: str
    origin_url: str
    cron_expr: str = "0 * * * *"  # e.g., hourly


@router.post("/schedule")
async def schedule_monitor(request: MonitorRequest, background_tasks: BackgroundTasks):
    """Schedule a recurring freshness check for a dataset."""
    from state import get_df

    baseline = get_df(request.file_id)
    if baseline is None:
        raise HTTPException(status_code=404, detail="Baseline dataset not found")

    job_id = str(uuid.uuid4())[:8]
    _monitors[job_id] = {
        "file_id": request.file_id,
        "origin_url": request.origin_url,
        "cron_expr": request.cron_expr,
        "status": "active",
        "last_run": None,
        "next_run": "scheduled",
        "created_at": time.time(),
    }
    _history[job_id] = []

    # Kick off an immediate async check as a background task
    background_tasks.add_task(_run_monitor_job, job_id)

    return {"job_id": job_id, "status": "scheduled", "details": _monitors[job_id]}


@router.get("/{job_id}")
async def get_monitor_status(job_id: str):
    """Get the current status and history of a monitor."""
    if job_id not in _monitors:
        raise HTTPException(status_code=404, detail="Monitor not found")
    return {
        "monitor": _monitors[job_id],
        "history": _history.get(job_id, []),
    }


@router.delete("/{job_id}")
async def delete_monitor(job_id: str):
    """Delete a scheduled monitor."""
    if job_id not in _monitors:
        raise HTTPException(status_code=404, detail="Monitor not found")
    del _monitors[job_id]
    if job_id in _history:
        del _history[job_id]
    return {"deleted": True, "job_id": job_id}


async def _run_monitor_job(job_id: str):
    """Background task to fetch new data, profile it, and compare to baseline."""
    monitor = _monitors.get(job_id)
    if not monitor or monitor["status"] != "active":
        return

    file_id = monitor["file_id"]
    origin_url = monitor["origin_url"]

    from state import get_df, get_profile
    base_df = get_df(file_id)
    base_profile = get_profile(file_id)

    if base_df is None or base_profile is None:
        monitor["status"] = "error"
        monitor["last_error"] = "Baseline data disappeared."
        return

    try:
        # Simulate fetching new data from origin
        # In a real system, you'd use httpx or read_csv from the origin URL
        # For demonstration purposes, we'll pretend we fetched slightly worse data
        if "http" in origin_url:
            raise NotImplementedError("HTTP fetching not fully implemented in demo mode.")
        elif origin_url.endswith(".csv"):
            new_df = pd.read_csv(origin_url)
        else:
            # Fallback: just add some noise and nulls to the baseline to simulate drift
            new_df = base_df.copy()
            if not new_df.empty:
                random_col = new_df.columns[0]
                new_df.loc[new_df.sample(frac=0.05).index, random_col] = None

        # 1. Run profile on new data (fast path)
        from profiling.engine import DataProfiler
        profiler = DataProfiler(new_df)
        new_profile = profiler.run()

        # 2. Compare using Schema Drift API logic (embedded here for BG task)
        from api.drift import _extract_quality

        q_base = _extract_quality(base_profile)
        q_new = _extract_quality(new_profile)

        score_drop = 0
        if q_base and q_new:
            score_drop = (q_base.get("overall_score") or 0) - (q_new.get("overall_score") or 0)

        schema_changed = set(base_df.columns) != set(new_df.columns)

        # Record history
        record = {
            "timestamp": time.time(),
            "rows": len(new_df),
            "columns": len(new_df.columns),
            "quality_score": q_new.get("overall_score") if q_new else None,
            "schema_changed": schema_changed,
            "quality_drop": round(score_drop, 2),
            "status": "ok" if score_drop < 10 and not schema_changed else "warning",
            "anomalies": [],
        }

        # Add to watchlist if significant drop or schema change
        if record["status"] == "warning":
            from api.watchlist import _append_bounded
            import uuid
            msg = "Schema drift detected" if schema_changed else f"Quality dropped by {score_drop:.1f} points"
            _append_bounded({
                "id": str(uuid.uuid4())[:8],
                "rowIndex": -1,
                "column": "DATASET_LEVEL",
                "value": "N/A",
                "severity": "high",
                "reason": f"Monitor {job_id}: {msg}. Origin: {origin_url}",
                "detectedBy": "monitor",
                "status": "unreviewed",
                "note": "",
                "created_at": time.time(),
            })

        _history[job_id].append(record)
        monitor["last_run"] = time.time()

    except Exception as e:
        monitor["last_error"] = str(e)
        monitor["status"] = "error"
