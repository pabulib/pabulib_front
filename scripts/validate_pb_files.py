#!/usr/bin/env python3
"""
Example script demonstrating how to use the pabulib checker.

This script shows how to validate .pb files using the checker library.
Can be used as a reference for integrating validation into the main app.
"""

import json
import os
from pathlib import Path


def validate_pb_file(file_path):
    """
    Validate a single .pb file using the pabulib checker.

    Args:
        file_path: Path to the .pb file to validate

    Returns:
        dict: Validation results
    """
    try:
        from pabulib.checker import Checker
    except ImportError:
        print("ERROR: pabulib checker not installed!")
        print("Install it with: pip install git+https://github.com/pabulib/checker.git")
        print("Or run: python scripts/update_checker.py")
        return None

    checker = Checker()
    results = checker.process_files([file_path])

    return results


def validate_directory(directory_path, max_files=None):
    """
    Validate all .pb files in a directory.

    Args:
        directory_path: Path to directory containing .pb files
        max_files: Maximum number of files to process (None for all)

    Returns:
        dict: Validation results for all files
    """
    try:
        from pabulib.checker import Checker
    except ImportError:
        print("ERROR: pabulib checker not installed!")
        print("Install it with: pip install pabulib-checker")
        print("Or run: python scripts/update_checker.py")
        return None

    pb_files = list(Path(directory_path).glob("*.pb"))

    if max_files:
        pb_files = pb_files[:max_files]

    if not pb_files:
        print(f"No .pb files found in {directory_path}")
        return None

    print(f"Found {len(pb_files)} .pb files to validate...")

    checker = Checker()
    file_paths = [str(f) for f in pb_files]
    results = checker.process_files(file_paths)

    return results


def print_summary(results):
    """Print a formatted summary of validation results."""
    if not results:
        return

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    # Print metadata
    if "metadata" in results:
        meta = results["metadata"]
        print(f"\nFiles processed: {meta.get('processed', 0)}")
        print(f"Valid files: {meta.get('valid', 0)}")
        print(f"Invalid files: {meta.get('invalid', 0)}")

    # Print summary of errors
    if "summary" in results and results["summary"]:
        print("\nError Summary:")
        for error_type, count in results["summary"].items():
            print(f"  - {error_type}: {count}")
    else:
        print("\n✓ No errors found!")

    print("=" * 60)


def print_file_details(results, show_valid=False):
    """Print detailed results for each file."""
    if not results:
        return

    print("\n" + "=" * 60)
    print("FILE DETAILS")
    print("=" * 60)

    for key, value in results.items():
        # Skip metadata and summary
        if key in ["metadata", "summary"]:
            continue

        file_result = value.get("results", "")
        webpage_name = value.get("webpage_name", "Unknown")

        print(f"\nFile: {key}")
        print(f"Webpage name: {webpage_name}")

        if isinstance(file_result, str):
            if show_valid or file_result != "File looks correct!":
                print(f"Status: {file_result}")
        else:
            # Has errors or warnings
            if "errors" in file_result:
                print("ERRORS:")
                print(json.dumps(file_result["errors"], indent=2))

            if "warnings" in file_result:
                print("WARNINGS:")
                print(json.dumps(file_result["warnings"], indent=2))

    print("=" * 60)


def main():
    """Main function - example usage."""
    import sys

    if len(sys.argv) > 1:
        # Validate specific file or directory from command line
        path = sys.argv[1]

        if os.path.isfile(path):
            print(f"Validating file: {path}")
            results = validate_pb_file(path)
        elif os.path.isdir(path):
            print(f"Validating directory: {path}")
            max_files = int(sys.argv[2]) if len(sys.argv) > 2 else None
            results = validate_directory(path, max_files)
        else:
            print(f"ERROR: {path} is not a valid file or directory")
            return
    else:
        # Default: validate pb_files directory
        pb_files_dir = os.environ.get("PB_FILES_DIR", "./pb_files")
        print(f"Validating directory: {pb_files_dir}")
        print("(Use: python scripts/validate_pb_files.py <path> [max_files])")
        results = validate_directory(pb_files_dir, max_files=10)

    if results:
        print_summary(results)
        print_file_details(results, show_valid=False)

        # Optionally save results to JSON
        output_file = "validation_results.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nFull results saved to: {output_file}")


if __name__ == "__main__":
    main()
