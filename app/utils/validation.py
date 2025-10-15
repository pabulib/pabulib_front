"""
Validation utilities for PB files using the pabulib-checker library.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


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

        checker = Checker()
        results = checker.process_files([str(file_path)])

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
            return {
                "valid": True,
                "errors": None,
                "warnings": None,
                "error_message": None,
            }
        else:
            # File has errors or warnings
            errors = file_results.get("errors", {})
            warnings = file_results.get("warnings", {})

            return {
                "valid": len(errors) == 0,  # Valid only if no errors
                "errors": errors if errors else None,
                "warnings": warnings if warnings else None,
                "error_message": None,
            }

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
            "error_message": f"Validation error: {str(e)}",
        }


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
