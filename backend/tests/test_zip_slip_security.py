"""
Tests for Zip Slip path traversal security and tempfile cleanup in CompressedParser.
"""

import io
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
import pytest

from ingestion.parsers.compressed import CompressedParser
from config import IngestionConfig


def test_zip_slip_path_traversal_blocked(tmp_path):
    """Ensure that an archive containing '../' traversal entries is blocked and cannot write outside temp dir."""
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w", zipfile.ZIP_DEFLATED) as zf:
        # Malicious filename attempting to escape extraction directory
        zf.writestr("../../evil_escape.txt", "MALICIOUS CONTENT")
        zf.writestr("safe.csv", "id,val\n1,10\n")

    zip_file = tmp_path / "malicious.zip"
    zip_file.write_bytes(zip_bytes.getvalue())

    parser = CompressedParser(zip_file)
    result = parser.parse(parser_factory=None)

    # Verification: Must fail or warn with path traversal security warning
    assert any("traversal" in w.lower() or "security" in w.lower() or "failed" in w.lower() for w in result.warnings)
    # Target escape file must NOT exist on filesystem
    assert not Path(tempfile.gettempdir() / Path("evil_escape.txt")).exists()


def test_temp_dir_cleaned_on_error(tmp_path):
    """Ensure that failed or corrupted decompression does not leak temp directories in TEMP_DIR."""
    corrupted_zip = tmp_path / "corrupted.zip"
    corrupted_zip.write_bytes(b"NOT A VALID ZIP FILE HEADER")

    config = IngestionConfig()
    config.ensure_dirs()
    temp_dir_before = set(config.TEMP_DIR.iterdir())

    parser = CompressedParser(corrupted_zip)
    result = parser.parse(parser_factory=None)

    temp_dir_after = set(config.TEMP_DIR.iterdir())
    new_leaked_dirs = [p for p in temp_dir_after - temp_dir_before if p.is_dir() and "tmp" in p.name]

    assert len(new_leaked_dirs) == 0, f"Leaked temporary directories: {new_leaked_dirs}"
