"""
Upload & ingestion API endpoints.
Handles file upload, WebSocket progress streaming, and user interaction flows.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, UploadFile, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse

from config import AppConfig, IngestionConfig
from models.schemas import (
    IngestionResult,
    MalformedConfirmRequest,
    MultiFileResolveRequest,
    ProgressUpdate,
    SheetSelectionRequest,
)
from ingestion.orchestrator import IngestionOrchestrator, get_stored_data, get_stored_dataframe
from ingestion.malformed_handler import MalformedHandler
from ingestion.schema_comparator import SchemaComparator
from api.profiling import auto_profile

router = APIRouter(prefix="/api", tags=["ingestion"])

# Active WebSocket connections per file_id
_ws_connections: dict[str, WebSocket] = {}


@router.post("/upload")
async def upload_file(files: list[UploadFile] = File(...)):
    """
    Upload one or more files for ingestion.
    
    Returns:
    - Single file: IngestionResult with preview, metadata, malformed report + auto-profile
    - Multiple files: Individual results + schema comparison
    """
    config = IngestionConfig()
    config.ensure_dirs()

    saved_files: list[tuple[Path, str]] = []

    for upload_file in files:
        # Generate unique filename to prevent collisions
        file_id = str(uuid.uuid4())
        ext = Path(upload_file.filename or "unknown").suffix
        save_path = config.UPLOAD_DIR / f"{file_id}{ext}"

        # Save uploaded file to disk
        try:
            with open(str(save_path), "wb") as f:
                content = await upload_file.read()
                f.write(content)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

        saved_files.append((save_path, upload_file.filename or "unknown"))

    # Create orchestrator with optional WebSocket progress
    orchestrator = IngestionOrchestrator()

    if len(saved_files) == 1:
        # Single file ingestion
        file_path, original_name = saved_files[0]
        result = await orchestrator.ingest_file(file_path, original_name)
        response = result.model_dump()

        # Auto-profile if ingestion was successful and no user interaction needed
        if (
            result.success
            and not result.requires_sheet_selection
            and not (result.malformed_report and result.malformed_report.has_issues)
        ):
            try:
                profile_result = auto_profile(result.file_id)
                if profile_result and profile_result.success:
                    response["profile"] = profile_result.model_dump()
            except Exception:
                pass  # Profiling failure shouldn't block ingestion

        return response
    else:
        # Multi-file ingestion with schema comparison
        multi_result = await orchestrator.ingest_multiple_files(saved_files)
        
        serialized_results = {}
        for fid, res in multi_result["results"].items():
            serialized_results[fid] = res.model_dump()

        return {
            "results": serialized_results,
            "schema_comparison": (
                multi_result["schema_comparison"].model_dump()
                if multi_result["schema_comparison"] else None
            ),
            "requires_schema_decision": multi_result["requires_schema_decision"],
        }


@router.post("/upload/select-sheet")
async def select_sheet(request: SheetSelectionRequest):
    """Handle user's sheet selection for Excel files."""
    orchestrator = IngestionOrchestrator()
    result = await orchestrator.select_sheets(request.file_id, request.selected_sheets)
    return result.model_dump()


@router.post("/upload/confirm-malformed")
async def confirm_malformed(request: MalformedConfirmRequest):
    """Handle user's decision on malformed data."""
    stored = get_stored_data(request.file_id)
    if not stored:
        raise HTTPException(status_code=404, detail="File session not found. Please re-upload.")

    df = stored.get("dataframe")
    if df is None:
        raise HTTPException(status_code=400, detail="No parsed data available for this file.")

    if request.drop_malformed_rows:
        malformed_report = stored.get("malformed_report")
        if malformed_report and malformed_report.issues:
            # Drop rows that were flagged as errors
            error_rows = {
                issue.row_number - 2  # Adjust for 0-index and header
                for issue in malformed_report.issues
                if issue.severity.value == "error" and issue.row_number > 0
            }
            if error_rows:
                valid_indices = [i for i in range(len(df)) if i not in error_rows]
                df = df.iloc[valid_indices].reset_index(drop=True)

    config = IngestionConfig()
    preview_data = df.head(config.PREVIEW_ROWS).fillna("").to_dict(orient="records")

    return {
        "success": True,
        "file_id": request.file_id,
        "action": "accepted_best_effort" if request.accept_best_effort else "dropped_malformed",
        "row_count": len(df),
        "preview_data": preview_data,
    }


@router.post("/upload/resolve-multi")
async def resolve_multi_file(request: MultiFileResolveRequest):
    """Handle user's decision on multi-file schema conflicts."""
    dataframes = {}
    for fid in request.file_ids:
        df = get_stored_dataframe(fid)
        if df is not None:
            stored = get_stored_data(fid)
            fname = stored.get("metadata", {})
            if hasattr(fname, "original_filename"):
                dataframes[fname.original_filename] = df
            else:
                dataframes[fid] = df

    if request.action == "merge":
        merged_df = SchemaComparator.merge_dataframes(dataframes)
        config = IngestionConfig()
        preview = merged_df.head(config.PREVIEW_ROWS).fillna("").to_dict(orient="records")

        return {
            "success": True,
            "action": "merged",
            "row_count": len(merged_df),
            "col_count": len(merged_df.columns),
            "preview_data": preview,
        }
    elif request.action == "separate":
        return {
            "success": True,
            "action": "separate",
            "file_count": len(dataframes),
            "message": "Files will be treated as separate datasets for analysis.",
        }
    else:
        raise HTTPException(status_code=400, detail=f"Unknown action: {request.action}")


@router.get("/upload/{file_id}/malformed-comparison")
async def get_malformed_comparison(file_id: str):
    """Get side-by-side malformed data comparison for the viewer UI."""
    stored = get_stored_data(file_id)
    if not stored:
        raise HTTPException(status_code=404, detail="File session not found.")

    df = stored.get("dataframe")
    report = stored.get("malformed_report")

    if df is None or report is None:
        raise HTTPException(status_code=400, detail="No malformed data available.")

    comparisons = MalformedHandler.generate_side_by_side(df, report)
    return {
        "file_id": file_id,
        "comparisons": comparisons,
        "total_issues": report.total_issues,
        "summary": report.summary,
    }


@router.websocket("/ws/progress")
async def websocket_progress(websocket: WebSocket):
    """
    WebSocket endpoint for real-time progress streaming.
    Client connects and receives ProgressUpdate messages during ingestion.
    """
    await websocket.accept()
    connection_id = str(uuid.uuid4())

    try:
        # Wait for file_id from client
        data = await websocket.receive_text()
        msg = json.loads(data)
        file_id = msg.get("file_id", connection_id)

        _ws_connections[file_id] = websocket

        # Keep connection alive until client disconnects
        while True:
            try:
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
    except WebSocketDisconnect:
        pass
    finally:
        # Clean up
        for fid, ws in list(_ws_connections.items()):
            if ws == websocket:
                del _ws_connections[fid]
                break
