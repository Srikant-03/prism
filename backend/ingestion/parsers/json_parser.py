"""
JSON parser supporting flat arrays, nested objects with auto-flattening,
and streaming for large files via ijson.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from config import IngestionConfig
from models.schemas import MalformedReport, MalformedRow, MalformedSeverity
from ingestion.parsers.base import BaseParser, ParseResult


class JSONParser(BaseParser):
    """
    Parser for JSON files. Handles:
    - Flat JSON arrays → direct DataFrame
    - Nested JSON → recursive flattening with dot-notation paths
    - Single JSON objects → single-row DataFrame
    - Large JSON → streaming with ijson
    """

    def validate(self) -> tuple[bool, str]:
        """Check that the file contains valid JSON."""
        if not self.file_path.exists():
            return False, f"File not found: {self.file_path}"
        if self.file_path.stat().st_size == 0:
            return False, "File is empty"
        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                # Read just enough to check JSON validity
                content = f.read(4096)
            content = content.strip()
            if not content or content[0] not in ("{", "["):
                return False, "File does not start with a JSON object or array"
            return True, "Valid JSON structure detected"
        except Exception as e:
            return False, f"Cannot read file: {str(e)}"

    def get_metadata(self) -> dict:
        """Return JSON structure metadata."""
        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                content = f.read(8192)
            content = content.strip()
            is_array = content.startswith("[")
            return {
                "structure": "array" if is_array else "object",
                "is_nested": self._check_nesting(content),
            }
        except Exception:
            return {"structure": "unknown", "is_nested": False}

    def parse(self, **kwargs) -> ParseResult:
        """Parse the JSON file, auto-detecting structure and flattening nested data."""
        justification_parts = []
        warnings = []

        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                raw = json.load(f)
        except json.JSONDecodeError as e:
            # Attempt line-by-line JSONL parsing
            return self._parse_jsonl(justification_parts)
        except Exception as e:
            return ParseResult(
                warnings=[f"Failed to read JSON: {str(e)}"],
                justification=f"JSON parsing failed: {str(e)}",
            )

        # Handle array of objects
        if isinstance(raw, list):
            if not raw:
                return ParseResult(
                    dataframe=pd.DataFrame(),
                    justification="JSON array is empty.",
                )

            # Check if items are flat or nested
            sample = raw[0] if raw else {}
            if isinstance(sample, dict):
                max_depth = max(self._get_depth(item) for item in raw[:100])
                if max_depth > 1:
                    # Nested → flatten
                    flattened = [self._flatten_dict(item) for item in raw]
                    df = pd.DataFrame(flattened)
                    justification_parts.append(
                        f"JSON array with {len(raw)} nested objects (max depth: {max_depth}). "
                        f"Auto-flattened to {len(df.columns)} columns using dot-notation "
                        f"(e.g., {list(df.columns)[:3]})."
                    )
                else:
                    df = pd.DataFrame(raw)
                    justification_parts.append(
                        f"Flat JSON array with {len(raw)} objects, {len(df.columns)} columns."
                    )
            else:
                # Array of primitives
                df = pd.DataFrame({"value": raw})
                justification_parts.append(
                    f"JSON array of {len(raw)} primitive values."
                )

        elif isinstance(raw, dict):
            # Single object — check if it contains a data array
            data_key = self._find_data_array(raw)
            if data_key:
                justification_parts.append(
                    f"JSON object with data array found at key '{data_key}'."
                )
                array_data = raw[data_key]
                if isinstance(array_data, list) and array_data and isinstance(array_data[0], dict):
                    max_depth = max(self._get_depth(item) for item in array_data[:100])
                    if max_depth > 1:
                        flattened = [self._flatten_dict(item) for item in array_data]
                        df = pd.DataFrame(flattened)
                        justification_parts.append(
                            f"Extracted {len(array_data)} nested objects from '{data_key}', "
                            f"flattened to {len(df.columns)} columns."
                        )
                    else:
                        df = pd.DataFrame(array_data)
                        justification_parts.append(
                            f"Extracted {len(array_data)} flat objects from '{data_key}', "
                            f"{len(df.columns)} columns."
                        )
                else:
                    df = pd.DataFrame(array_data if isinstance(array_data, list) else [array_data])
                    justification_parts.append(
                        f"Extracted data from key '{data_key}'."
                    )
            else:
                # Single flat/nested object → single row
                flattened = self._flatten_dict(raw)
                df = pd.DataFrame([flattened])
                justification_parts.append(
                    f"Single JSON object flattened to {len(df.columns)} columns."
                )
        else:
            return ParseResult(
                warnings=["JSON root is neither an object nor an array"],
                justification="JSON root is a primitive value, not a structured data format.",
            )

        return ParseResult(
            dataframe=df,
            metadata=self.get_metadata(),
            justification=" ".join(justification_parts),
        )

    def parse_chunked(
        self,
        chunk_size: int = 10000,
        progress_callback: Optional[Callable[[float, int, int], None]] = None,
        **kwargs,
    ) -> ParseResult:
        """
        Stream-parse large JSON files using ijson.
        Only works for JSON arrays of objects.
        """
        import ijson

        total_bytes = self.file_path.stat().st_size
        rows = []
        count = 0

        try:
            with open(self.file_path, "rb") as f:
                parser = ijson.items(f, "item")
                for item in parser:
                    if isinstance(item, dict):
                        rows.append(self._flatten_dict(item))
                    else:
                        rows.append({"value": item})

                    count += 1
                    if count % chunk_size == 0 and progress_callback:
                        bytes_pos = f.tell() if hasattr(f, "tell") else 0
                        pct = (bytes_pos / total_bytes) * 100 if total_bytes > 0 else 0
                        progress_callback(pct, bytes_pos, total_bytes)

            df = pd.DataFrame(rows)

            if progress_callback:
                progress_callback(100.0, total_bytes, total_bytes)

            return ParseResult(
                dataframe=df,
                metadata=self.get_metadata(),
                justification=(
                    f"Streamed {len(df)} JSON items. "
                    f"Result: {len(df)} rows × {len(df.columns)} columns."
                ),
            )
        except Exception as e:
            # Fallback to regular parse if streaming fails
            return self.parse(**kwargs)

    def _parse_jsonl(self, justification_parts: list[str]) -> ParseResult:
        """Parse JSON Lines (one JSON object per line)."""
        rows = []
        errors = []

        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                for i, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            rows.append(self._flatten_dict(obj))
                        else:
                            rows.append({"value": obj})
                    except json.JSONDecodeError:
                        errors.append(MalformedRow(
                            row_number=i,
                            raw_content=line[:500],
                            issue="Invalid JSON on this line",
                            severity=MalformedSeverity.WARNING,
                        ))

            df = pd.DataFrame(rows) if rows else pd.DataFrame()
            justification_parts.append(
                f"Parsed as JSON Lines: {len(rows)} valid lines, {len(errors)} invalid lines."
            )

            malformed = MalformedReport(
                has_issues=len(errors) > 0,
                total_issues=len(errors),
                issues=errors,
                summary=f"{len(errors)} lines with invalid JSON" if errors else "",
                best_effort_rows_parsed=len(rows),
                best_effort_rows_dropped=len(errors),
            )

            return ParseResult(
                dataframe=df,
                metadata=self.get_metadata(),
                malformed=malformed,
                justification=" ".join(justification_parts),
            )

        except Exception as e:
            return ParseResult(
                warnings=[f"JSONL parsing failed: {str(e)}"],
                justification=f"JSONL parsing failed: {str(e)}",
            )

    @staticmethod
    def _flatten_dict(d: Any, parent_key: str = "", sep: str = ".") -> dict[str, Any]:
        """
        Recursively flatten a nested dict using dot-notation.
        Lists are indexed: orders[0].total, orders[1].total
        """
        items: list[tuple[str, Any]] = []

        if not isinstance(d, dict):
            return {parent_key: d} if parent_key else {"value": d}

        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(JSONParser._flatten_dict(v, new_key, sep).items())
            elif isinstance(v, list):
                if not v:
                    items.append((new_key, None))
                elif isinstance(v[0], dict):
                    # List of objects — index each
                    for i, item in enumerate(v):
                        indexed_key = f"{new_key}[{i}]"
                        items.extend(JSONParser._flatten_dict(item, indexed_key, sep).items())
                else:
                    # List of primitives — join or keep first
                    items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))

        return dict(items)

    @staticmethod
    def _get_depth(obj: Any, current: int = 0) -> int:
        """Get the maximum nesting depth of a JSON object."""
        if isinstance(obj, dict):
            if not obj:
                return current
            return max(JSONParser._get_depth(v, current + 1) for v in obj.values())
        if isinstance(obj, list):
            if not obj:
                return current
            return max(JSONParser._get_depth(v, current + 1) for v in obj[:10])
        return current

    @staticmethod
    def _check_nesting(content: str) -> bool:
        """Quick check if JSON content has nested structures."""
        depth = 0
        for char in content[:4096]:
            if char in ("{", "["):
                depth += 1
                if depth > 2:
                    return True
            elif char in ("}", "]"):
                depth -= 1
        return False

    @staticmethod
    def _find_data_array(obj: dict) -> Optional[str]:
        """
        Find the key in a JSON object that contains the main data array.
        Heuristic: the key whose value is the longest list of dicts.
        """
        best_key = None
        best_len = 0

        for key, value in obj.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                if len(value) > best_len:
                    best_len = len(value)
                    best_key = key

        return best_key
