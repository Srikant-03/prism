"""
Upload & ingestion API endpoints.
Handles file upload, WebSocket progress streaming, and user interaction flows.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, File, Request, UploadFile, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse

from slowapi import Limiter
from slowapi.util import get_remote_address

from config import AppConfig, IngestionConfig

logger = logging.getLogger(__name__)
from models.schemas import (
    IngestionResult,
    MalformedConfirmRequest,
    MultiFileResolveRequest,
    ProgressUpdate,
    SheetSelectionRequest,
)
from ingestion.orchestrator import IngestionOrchestrator
from state import get_stored_data, get_stored_dataframe
from ingestion.malformed_handler import MalformedHandler
from ingestion.schema_comparator import SchemaComparator
from api.profiling import auto_profile
from api.sql import register_table_from_upload

router = APIRouter(prefix="/api", tags=["ingestion"])
limiter = Limiter(key_func=get_remote_address)

# Active WebSocket connections per file_id
_ws_connections: dict[str, WebSocket] = {}


@limiter.limit(AppConfig.RATE_LIMIT_UPLOAD)
@router.post("/upload", response_model=None)
async def upload_file(request: Request, files: List[UploadFile] = File(...)) -> dict:
    """
    Upload one or more files for ingestion.
    
    Returns:
    - Single file: IngestionResult with preview, metadata, malformed report + auto-profile
    - Multiple files: Individual results + schema comparison
    """
    config = IngestionConfig()
    config.ensure_dirs()

    saved_files: list[tuple[Path, str]] = []
    file_id = str(uuid.uuid4())  # Default ID; overwritten per-file inside loop

    for upload_file in files:
        # ── Validate file extension ──────────────────────────────────
        ext = Path(upload_file.filename or "unknown").suffix.lower()
        if ext not in config.ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"File type '{ext}' is not allowed. Accepted: {', '.join(sorted(config.ALLOWED_EXTENSIONS))}",
            )

        # ── Validate MIME type ───────────────────────────────────────
        content_type = (upload_file.content_type or "").lower()
        if content_type and content_type not in config.ALLOWED_MIME_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"MIME type '{content_type}' is not allowed.",
            )

        # ── Read content & enforce file-size limit ───────────────────
        content = await upload_file.read()
        if config.MAX_FILE_SIZE and len(content) > config.MAX_FILE_SIZE:
            size_mb = len(content) / (1024 * 1024)
            limit_mb = config.MAX_FILE_SIZE / (1024 * 1024)
            raise HTTPException(
                status_code=400,
                detail=f"File too large ({size_mb:.1f} MB). Maximum allowed: {limit_mb:.0f} MB.",
            )

        # ── Verify magic bytes (defense against spoofed Content-Type) ─
        try:
            import magic
            detected_mime = magic.from_buffer(content[:2048], mime=True)
            if detected_mime and detected_mime not in config.ALLOWED_MIME_TYPES:
                logger.warning(
                    "Magic-byte MIME mismatch for %s: header=%s, detected=%s",
                    upload_file.filename, content_type, detected_mime,
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"File content does not match an allowed type (detected: {detected_mime}).",
                )
        except ImportError:
            pass  # python-magic not installed — skip magic-byte check

        # Generate unique filename to prevent collisions
        file_id = str(uuid.uuid4())
        save_path = config.UPLOAD_DIR / f"{file_id}{ext}"

        # Save uploaded file to disk
        try:
            with open(str(save_path), "wb") as f:
                f.write(content)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

        saved_files.append((save_path, upload_file.filename or "unknown"))

    # Build a progress callback that pushes updates to any connected WebSocket
    def _make_ws_callback(fid: str):
        """Return a callback that sends ProgressUpdate JSON over the WebSocket for fid."""
        import asyncio

        def callback(update):
            ws = _ws_connections.get(fid)
            if ws:
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(ws.send_json(update.model_dump()))
                    else:
                        loop.run_until_complete(ws.send_json(update.model_dump()))
                except Exception:
                    pass  # Connection closed or broken — silently degrade
        return callback

    # Create orchestrator with WebSocket progress callback
    progress_cb = _make_ws_callback(file_id) if saved_files else None
    orchestrator = IngestionOrchestrator(progress_callback=progress_cb)

    if len(saved_files) == 1:
        # Single file ingestion
        file_path, original_name = saved_files[0]
        result = await orchestrator.ingest_file(file_path, original_name, file_id=file_id)
        response = result.model_dump()

        # Auto-profile if ingestion was successful and no user interaction needed
        if (
            result.success
            and not result.requires_sheet_selection
            and not (result.malformed_report and result.malformed_report.has_issues)
        ):
            try:
                profile_result = await auto_profile(result.file_id)
                if profile_result and profile_result.success:
                    response["profile"] = profile_result.profile.model_dump()
            except Exception as e:
                logger.error("Auto-profile failed: %s", e)

            # Register with SQL engine
            df = get_stored_dataframe(result.file_id)
            if df is not None:
                try:
                    register_table_from_upload(
                        df, original_name, "raw", result.file_id
                    )
                except Exception as e:
                    logger.error("Failed to auto-register SQL table: %s", e)

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
    
    # Register with SQL engine if no malformed issues (if there are, confirm-malformed will handle it)
    if result.success and not (result.malformed_report and result.malformed_report.has_issues):
        df = get_stored_dataframe(request.file_id)
        if df is not None:
            try:
                stored_data = get_stored_data(request.file_id)
                original_name = "unknown"
                if stored_data and "metadata" in stored_data:
                    original_name = getattr(stored_data["metadata"], "original_filename", "unknown")
                register_table_from_upload(df, original_name, "raw", request.file_id)
            except Exception as e:
                logger.error("Failed to auto-register SQL table: %s", e)
                
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
            # row_number is 1-indexed with the header as row 1, so data
            # starts at row 2.  DataFrame uses 0-indexed rows, hence row - 2.
            # Guard with max(..., 0) to prevent negative indices for edge cases.
            error_rows = {
                max(issue.row_number - 2, 0)
                for issue in malformed_report.issues
                if issue.severity.value == "error" and issue.row_number > 1
            }
            if error_rows:
                valid_indices = [i for i in range(len(df)) if i not in error_rows]
                df = df.iloc[valid_indices].reset_index(drop=True)

    # Update stored dataframe so profiling/cleaning use the corrected data
    from ingestion.orchestrator import update_stored_dataframe
    update_stored_dataframe(request.file_id, df)

    config = IngestionConfig()
    preview_data = df.head(config.PREVIEW_ROWS).fillna("").to_dict(orient="records")

    # Register in SQL engine
    try:
        original_name = "unknown"
        if stored and "metadata" in stored:
            original_name = getattr(stored["metadata"], "original_filename", "unknown")
        status = "cleaned" if request.drop_malformed_rows else "raw"
        register_table_from_upload(df, original_name, status, request.file_id)
    except Exception as e:
        logger.error("Failed to update SQL table after malformed conf: %s", e)

    # Auto-profile (generates insights)
    profile_data = None
    try:
        profile_result = await auto_profile(request.file_id)
        if profile_result and profile_result.success:
            profile_data = profile_result.profile.model_dump()
    except Exception as e:
        logger.error("Auto-profile after malformed confirmation failed: %s", e)

    return {
        "success": True,
        "file_id": request.file_id,
        "action": "accepted_best_effort" if request.accept_best_effort else "dropped_malformed",
        "row_count": len(df),
        "preview_data": preview_data,
        "profile": profile_data,
    }


@router.post("/upload/resolve-multi")
async def resolve_multi_file(request: MultiFileResolveRequest):
    """Handle user's decision on multi-file schema conflicts."""
    dataframes = {}
    fname_to_fid: dict[str, str] = {}  # filename → actual file_id (UUID)
    for fid in request.file_ids:
        df = get_stored_dataframe(fid)
        if df is not None:
            stored = get_stored_data(fid)
            fname = stored.get("metadata", {})
            if hasattr(fname, "original_filename"):
                key = fname.original_filename
                dataframes[key] = df
                fname_to_fid[key] = fid
            else:
                dataframes[fid] = df
                fname_to_fid[fid] = fid

    if request.action == "merge":
        merged_df = SchemaComparator.merge_dataframes(dataframes)
        config = IngestionConfig()
        preview = merged_df.head(config.PREVIEW_ROWS).fillna("").to_dict(orient="records")

        # Register merged result
        try:
            table_name = f"merged_{request.file_ids[0][:8]}"
            register_table_from_upload(merged_df, table_name, "merged", request.file_ids[0])
        except Exception as e:
            logger.error("Failed to auto-register merged dataset: %s", e)

        return {
            "success": True,
            "action": "merged",
            "row_count": len(merged_df),
            "col_count": len(merged_df.columns),
            "preview_data": preview,
        }
    elif request.action == "separate":
        # Register them separately using the correct file_id for each
        for fname, df in dataframes.items():
            try:
                actual_fid = fname_to_fid.get(fname, fname)
                register_table_from_upload(df, fname, "raw", actual_fid)
            except Exception as e:
                logger.error("Failed to register separated table %s: %s", fname, e)

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
