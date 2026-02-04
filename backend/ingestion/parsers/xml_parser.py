"""
XML parser with auto-detection of repeating elements (table rows)
and conversion to tabular format.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd
import xmltodict

from models.schemas import MalformedReport, MalformedRow, MalformedSeverity
from ingestion.parsers.base import BaseParser, ParseResult


class XMLParser(BaseParser):
    """
    Parser for XML files. Auto-detects repeating elements that
    represent table rows and converts them to a DataFrame.
    Handles both attributes and text content.
    """

    def validate(self) -> tuple[bool, str]:
        """Check that the file is valid XML."""
        if not self.file_path.exists():
            return False, f"File not found: {self.file_path}"
        if self.file_path.stat().st_size == 0:
            return False, "File is empty"
        try:
            ET.parse(str(self.file_path))
            return True, "Valid XML document"
        except ET.ParseError as e:
            return False, f"Invalid XML: {str(e)}"
        except Exception as e:
            return False, f"Cannot read file: {str(e)}"

    def get_metadata(self) -> dict:
        """Extract XML structure metadata."""
        try:
            tree = ET.parse(str(self.file_path))
            root = tree.getroot()
            # Count elements at each level
            tag_counts: dict[str, int] = {}
            for elem in root.iter():
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                tag_counts[tag] = tag_counts.get(tag, 0) + 1

            return {
                "root_tag": root.tag,
                "element_counts": tag_counts,
                "total_elements": sum(tag_counts.values()),
            }
        except Exception as e:
            return {"error": str(e)}

    def parse(self, **kwargs) -> ParseResult:
        """
        Parse XML by:
        1. Converting to dict with xmltodict
        2. Finding the most-repeated element (likely table rows)
        3. Flattening into a DataFrame
        """
        justification_parts = []

        try:
            with open(self.file_path, "r", encoding=self.encoding, errors="replace") as f:
                content = f.read()

            parsed = xmltodict.parse(content)
        except Exception as e:
            return ParseResult(
                warnings=[f"XML parsing failed: {str(e)}"],
                justification=f"Could not parse XML: {str(e)}",
            )

        # Find the data array within the parsed structure
        rows, path = self._find_data_array(parsed)

        if not rows:
            # Try flattening the whole thing
            try:
                flat = self._flatten_xml_dict(parsed)
                df = pd.DataFrame([flat])
                justification_parts.append(
                    f"No repeating elements detected. Flattened entire XML to "
                    f"{len(df.columns)} columns."
                )
                return ParseResult(
                    dataframe=df,
                    metadata=self.get_metadata(),
                    justification=" ".join(justification_parts),
                    warnings=["No repeating elements found — treated as single-row dataset"],
                )
            except Exception as e:
                return ParseResult(
                    warnings=[f"Could not convert XML to tabular format: {str(e)}"],
                    justification=f"XML structure is not tabular: {str(e)}",
                )

        # Convert rows to flat dicts
        if isinstance(rows, dict):
            rows = [rows]

        flat_rows = []
        malformed_issues = []

        for i, row in enumerate(rows):
            if isinstance(row, dict):
                flat_rows.append(self._flatten_xml_dict(row))
            elif isinstance(row, str):
                flat_rows.append({"value": row})
            else:
                malformed_issues.append(MalformedRow(
                    row_number=i + 1,
                    raw_content=str(row)[:500],
                    issue=f"Unexpected element type: {type(row).__name__}",
                    severity=MalformedSeverity.WARNING,
                ))

        df = pd.DataFrame(flat_rows) if flat_rows else pd.DataFrame()

        justification_parts.append(
            f"Found repeating elements at path '{path}'. "
            f"Extracted {len(df)} rows × {len(df.columns)} columns."
        )

        malformed = MalformedReport(
            has_issues=len(malformed_issues) > 0,
            total_issues=len(malformed_issues),
            issues=malformed_issues,
            summary=f"{len(malformed_issues)} non-standard elements" if malformed_issues else "",
        )

        return ParseResult(
            dataframe=df,
            metadata=self.get_metadata(),
            malformed=malformed,
            justification=" ".join(justification_parts),
        )

    def _find_data_array(self, obj: Any, path: str = "") -> tuple[Optional[list], str]:
        """
        Recursively search for the longest list of dicts in the parsed XML.
        This represents the most likely set of 'table rows'.
        """
        best_arr: Optional[list] = None
        best_path = ""
        best_len = 0

        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key

                if isinstance(value, list):
                    if len(value) > best_len:
                        best_arr = value
                        best_path = current_path
                        best_len = len(value)

                elif isinstance(value, dict):
                    sub_arr, sub_path = self._find_data_array(value, current_path)
                    if sub_arr and len(sub_arr) > best_len:
                        best_arr = sub_arr
                        best_path = sub_path
                        best_len = len(sub_arr)

        return best_arr, best_path

    @staticmethod
    def _flatten_xml_dict(d: Any, parent_key: str = "", sep: str = ".") -> dict[str, Any]:
        """Flatten a nested dict from XML, handling attributes and text nodes."""
        items: list[tuple[str, Any]] = []

        if not isinstance(d, dict):
            return {parent_key: d} if parent_key else {"value": d}

        for k, v in d.items():
            # xmltodict uses @ prefix for attributes, #text for text content
            if k.startswith("@"):
                new_key = f"{parent_key}{sep}{k[1:]}" if parent_key else k[1:]
            elif k == "#text":
                new_key = parent_key if parent_key else "text"
            else:
                new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(XMLParser._flatten_xml_dict(v, new_key, sep).items())
            elif isinstance(v, list):
                if not v:
                    items.append((new_key, None))
                elif isinstance(v[0], dict):
                    for i, item in enumerate(v):
                        indexed = f"{new_key}[{i}]"
                        items.extend(XMLParser._flatten_xml_dict(item, indexed, sep).items())
                else:
                    items.append((new_key, str(v)))
            else:
                items.append((new_key, v))

        return dict(items)
