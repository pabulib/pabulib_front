"""
Validation utilities for PB files using the pabulib-checker library.

Includes a pre-validation sanitizer that converts float-like project costs
(e.g., "40000.0") into integers to prevent the Checker from failing on
ValueError when parsing costs as int.
"""

import logging
import os
import re
import tempfile
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _sanitize_pb_for_checker(file_path: Path) -> Path:
    """
    Create a sanitized copy of the PB file where project cost values that look
    like decimals (e.g., "40000.0") are converted to integer strings (e.g., "40000").
    This avoids failures in downstream tooling that expects integer costs.

    The transformation is conservative: only values matching ^\d+\.\d+$ are
    converted. Thousand separators or other formats are left untouched.

    Returns the path to the sanitized temporary file.
    """
    import csv

    # Read entire file via csv with semicolon delimiter
    with file_path.open("r", encoding="utf-8", newline="") as f:
        content = f.read()

    reader = csv.reader(StringIO(content), delimiter=";")
    rows = []
    section = None
    expecting_header = False
    header = []
    cost_idx = -1

    decimal_re = re.compile(r"^\d+\.\d+$")

    for row in reader:
        if not row:
            # Preserve blank rows as-is
            rows.append(row)
            continue
        first = str(row[0]).strip().lower() if row else ""
        if first in {"meta", "projects", "votes"}:
            section = first
            expecting_header = True
            header = []
            cost_idx = -1
            rows.append(row)
            continue

        if expecting_header:
            header = row
            if section == "projects":
                try:
                    cost_idx = next(
                        (
                            i
                            for i, k in enumerate(header)
                            if str(k).strip().lower() == "cost"
                        ),
                        -1,
                    )
                except Exception:
                    cost_idx = -1
            rows.append(row)
            expecting_header = False
            continue

        # Body rows
        if section == "projects" and cost_idx >= 0 and cost_idx < len(row):
            val = str(row[cost_idx]).strip()
            if decimal_re.match(val):
                try:
                    row[cost_idx] = str(int(float(val)))
                except Exception:
                    # leave as-is if conversion fails
                    pass
        rows.append(row)

    # Write to a temp file in the same temp directory for the uploads
    # Use NamedTemporaryFile with delete=False so we can pass the path to checker
    tmp_dir = file_path.parent
    fd, tmp_name = tempfile.mkstemp(prefix="san_", suffix=".pb", dir=str(tmp_dir))
    os.close(fd)
    out_path = Path(tmp_name)
    try:
        import csv as _csv

        with out_path.open("w", encoding="utf-8", newline="") as out:
            w = _csv.writer(out, delimiter=";", lineterminator="\n")
            for r in rows:
                w.writerow(r)
    except Exception:
        # If writing fails, fall back to original file path
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        return file_path

    return out_path


def validate_pb_file(file_path: Path) -> Dict[str, Any]:
    """
    Validate a single .pb file using the pabulib checker.

    Args:
        file_path: Path to the .pb file to validate

    Returns:
        dict: Validation result with structure:
            {
                "valid": bool,
                "errors": dict or None,
                "warnings": dict or None,
                "error_message": str or None (if validation failed to run)
            }
    """
    try:
        from pabulib.checker import Checker

        # Pre-sanitize file for checker (decimal project costs -> ints)
        sanitized_path = _sanitize_pb_for_checker(file_path)

        checker = Checker()
        results = checker.process_files([str(sanitized_path)])

        # Get the file key (first non-metadata/summary key)
        file_key = None
        for key in results.keys():
            if key not in ["metadata", "summary"]:
                file_key = key
                break

        if not file_key:
            return {
                "valid": False,
                "errors": None,
                "warnings": None,
                "error_message": "Checker did not return results for this file",
            }

        file_results = results[file_key].get("results", {})

        # Check if file is valid
        if isinstance(file_results, str):
            # File is valid if results is a string (e.g., "File looks correct!")
            result = {
                "valid": True,
                "errors": None,
                "warnings": None,
                "error_message": None,
            }
            return result
        else:
            # File has errors or warnings
            errors = file_results.get("errors", {})
            warnings = file_results.get("warnings", {})

            result = {
                "valid": len(errors) == 0,  # Valid only if no errors
                "errors": errors if errors else None,
                "warnings": warnings if warnings else None,
                "error_message": None,
            }
            return result

    except ImportError:
        logger.error("pabulib-checker not installed")
        return {
            "valid": False,
            "errors": None,
            "warnings": None,
            "error_message": "Validation library not installed",
        }
    except Exception as e:
        logger.exception("Error during validation of %s", file_path)
        return {
            "valid": False,
            "errors": None,
            "warnings": None,
            "error_message": f"Validation error: {e.__class__.__name__}: {str(e)}. This file cannot be checked and is likely corrupted or malformed.",
        }
    finally:
        # Clean up sanitized temp file if different from original
        try:
            if (
                "sanitized_path" in locals()
                and sanitized_path != file_path
                and sanitized_path.exists()
            ):
                sanitized_path.unlink()
        except Exception:
            pass


def format_validation_summary(validation: Dict[str, Any]) -> str:
    """
    Format a short validation summary for display.

    Args:
        validation: Validation result dict from validate_pb_file

    Returns:
        str: Short summary like "✓ Valid", "✗ 3 errors", "⚠ 2 warnings"
    """
    if validation.get("error_message"):
        return f"⚠ {validation['error_message']}"

    if validation.get("valid"):
        if validation.get("warnings"):
            warning_count = sum(len(w) for w in validation["warnings"].values())
            return f"✓ Valid (⚠ {warning_count} warning{'s' if warning_count != 1 else ''})"
        return "✓ Valid"

    if validation.get("errors"):
        error_count = sum(len(e) for e in validation["errors"].values())
        return f"✗ {error_count} error{'s' if error_count != 1 else ''}"

    return "⚠ Unknown"


def count_issues(validation: Dict[str, Any]) -> Dict[str, int]:
    """
    Count errors and warnings in validation result.

    Args:
        validation: Validation result dict from validate_pb_file

    Returns:
        dict: {"errors": int, "warnings": int}
    """
    error_count = 0
    warning_count = 0

    if validation.get("errors"):
        error_count = sum(len(e) for e in validation["errors"].values())

    if validation.get("warnings"):
        warning_count = sum(len(w) for w in validation["warnings"].values())

    return {"errors": error_count, "warnings": warning_count}
