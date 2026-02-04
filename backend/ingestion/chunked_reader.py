"""
Large file chunked reading engine.
Manages memory-aware reading with progress tracking.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Callable, Optional

import psutil

from config import IngestionConfig
from models.schemas import ProgressUpdate, IngestionStage


class ChunkedReader:
    """
    Manages chunked reading of large files with:
    - Progress tracking (bytes read, ETA)
    - Memory usage monitoring via psutil
    - Adaptive chunk sizing based on available memory
    """

    def __init__(self, file_path: Path, file_id: str = ""):
        self.file_path = file_path
        self.file_id = file_id
        self.total_bytes = file_path.stat().st_size
        self.bytes_read = 0
        self.start_time: Optional[float] = None
        self._config = IngestionConfig()

    @property
    def is_large_file(self) -> bool:
        """Check if the file exceeds the large file threshold."""
        return self.total_bytes > self._config.LARGE_FILE_THRESHOLD

    def get_optimal_chunk_size(self) -> int:
        """
        Calculate optimal chunk size based on available memory.
        Uses at most 25% of available memory per chunk.
        """
        try:
            available_mb = psutil.virtual_memory().available / (1024 * 1024)
            # Use at most 25% of available memory, but cap at 100MB chunks
            max_chunk_mb = min(available_mb * 0.25, 100)
            chunk_bytes = int(max_chunk_mb * 1024 * 1024)
            # But at least 1MB
            return max(chunk_bytes, 1024 * 1024)
        except Exception:
            return self._config.CHUNK_SIZE

    def get_progress(self) -> ProgressUpdate:
        """Get current progress snapshot."""
        elapsed = time.time() - self.start_time if self.start_time else 0
        pct = (self.bytes_read / self.total_bytes * 100) if self.total_bytes > 0 else 0

        # ETA calculation
        if self.bytes_read > 0 and elapsed > 0:
            bytes_per_sec = self.bytes_read / elapsed
            remaining_bytes = self.total_bytes - self.bytes_read
            eta = remaining_bytes / bytes_per_sec if bytes_per_sec > 0 else None
        else:
            eta = None

        # Memory usage
        try:
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / (1024 * 1024)
        except Exception:
            memory_mb = 0.0

        return ProgressUpdate(
            file_id=self.file_id,
            stage=IngestionStage.PARSING,
            progress_pct=round(pct, 2),
            bytes_read=self.bytes_read,
            total_bytes=self.total_bytes,
            eta_seconds=round(eta, 1) if eta else None,
            memory_usage_mb=round(memory_mb, 1),
            message=f"Processing: {pct:.1f}% ({self._format_bytes(self.bytes_read)} / {self._format_bytes(self.total_bytes)})",
        )

    def start(self):
        """Mark the start of chunked reading."""
        self.start_time = time.time()
        self.bytes_read = 0

    def update(self, bytes_read: int):
        """Update bytes read counter."""
        self.bytes_read = bytes_read

    def make_progress_callback(
        self, ws_callback: Optional[Callable[[ProgressUpdate], None]] = None
    ) -> Callable[[float, int, int], None]:
        """
        Create a progress callback compatible with parser.parse_chunked().
        Optionally wraps a WebSocket send function.
        """
        def callback(pct: float, bytes_read: int, total_bytes: int):
            self.bytes_read = bytes_read
            progress = self.get_progress()
            progress.progress_pct = round(pct, 2)
            if ws_callback:
                ws_callback(progress)

        return callback

    @staticmethod
    def _format_bytes(b: int) -> str:
        """Human-readable byte size."""
        for unit in ["B", "KB", "MB", "GB"]:
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"
