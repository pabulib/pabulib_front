"""Snapshot service for creating deterministic download links (minimal schema)."""

import hashlib
import io
import json
import secrets
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# NOTE: This is the canonical list of user-facing download filters that we persist
# into snapshot context and render inside `_PERMANENT_DOWNLOAD_LINK.txt`.
# If any filter is added, removed, renamed, or reinterpreted in the UI/search flow,
# update this list and the surrounding snapshot logic in tandem so permanent-link
# notes stay accurate. Keep `app/services/SNAPSHOT_SYSTEM.md` in sync as well.
FILTER_SPECS = [
    ("search", "Search", "text"),
    ("country", "Country", "text"),
    ("city", "City", "text"),
    ("year", "Year", "text"),
    ("votes_min", "Minimum votes", "number"),
    ("votes_max", "Maximum votes", "number"),
    ("projects_min", "Minimum projects", "number"),
    ("projects_max", "Maximum projects", "number"),
    ("len_min", "Minimum budget", "number"),
    ("len_max", "Maximum budget", "number"),
    ("type", "Vote type", "text"),
    ("rule", "Rule", "text"),
    ("exclude_fully", "Exclude fully artificial data", "boolean"),
    ("exclude_experimental", "Exclude experimental data", "boolean"),
    ("require_geo", "Require geo", "boolean"),
    ("require_beneficiaries", "Require beneficiaries", "boolean"),
    ("require_category", "Require category", "boolean"),
    ("require_new", "Require new files only", "boolean"),
]


def empty_filter_context() -> Dict[str, Any]:
    """Return the canonical filter state with all supported filters unset."""
    return {key: None for key, _label, _kind in FILTER_SPECS}


def normalize_filter_context(raw_filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize request filters into a stable JSON-friendly shape."""
    normalized = empty_filter_context()
    if not raw_filters:
        return normalized

    for key, _label, kind in FILTER_SPECS:
        value = raw_filters.get(key)
        if kind == "boolean":
            if value in {True, "true", "True", "1", 1}:
                normalized[key] = True
            elif value in {False, "false", "False", "0", 0}:
                normalized[key] = False
            else:
                normalized[key] = None
            continue

        if value is None:
            normalized[key] = None
            continue

        if isinstance(value, str):
            value = value.strip()
        if value == "" or value == []:
            normalized[key] = None
        else:
            normalized[key] = value

    return normalized


def format_filter_context_lines(filters: Optional[Dict[str, Any]]) -> List[str]:
    """Render the canonical filter state as human-readable text lines."""
    normalized = normalize_filter_context(filters)
    lines: List[str] = []
    for key, label, kind in FILTER_SPECS:
        value = normalized.get(key)
        if value is None:
            display = "not applied"
        elif kind == "boolean":
            display = "true" if value else "false"
        else:
            display = str(value)
        lines.append(f"- {label}: {display}")
    return lines


def build_snapshot_url(snapshot_id: str, base_url: str, context_id: Optional[str] = None) -> str:
    """Build a snapshot URL, optionally preserving request-specific context."""
    snapshot_url = f"{base_url}/download/snapshot/{snapshot_id}"
    if context_id:
        snapshot_url = f"{snapshot_url}?context={context_id}"
    return snapshot_url


def create_snapshot_context(
    snapshot_id: str, download_name: str, filters: Optional[Dict[str, Any]] = None
) -> Optional[str]:
    """Persist request-scoped metadata for rendering permalink notes."""
    from ..db import get_session
    from ..models import DownloadSnapshotContext

    normalized = normalize_filter_context(filters)
    context_id = secrets.token_hex(8)

    try:
        with get_session() as session:
            session.add(
                DownloadSnapshotContext(
                    context_id=context_id,
                    snapshot_id=snapshot_id,
                    download_name=download_name,
                    filters_json=json.dumps(normalized, sort_keys=True),
                )
            )
            session.commit()
        return context_id
    except Exception:
        return None


def get_snapshot_context(context_id: Optional[str], snapshot_id: str) -> Optional[Dict[str, Any]]:
    """Load request-scoped snapshot context if it belongs to the snapshot."""
    if not context_id:
        return None

    from ..db import get_session
    from ..models import DownloadSnapshotContext

    try:
        with get_session() as session:
            row = (
                session.query(DownloadSnapshotContext)
                .filter(
                    DownloadSnapshotContext.context_id == context_id,
                    DownloadSnapshotContext.snapshot_id == snapshot_id,
                )
                .first()
            )
            if not row:
                return None
            filters = empty_filter_context()
            if row.filters_json:
                try:
                    filters = normalize_filter_context(json.loads(row.filters_json))
                except Exception:
                    filters = empty_filter_context()
            return {
                "context_id": row.context_id,
                "snapshot_id": row.snapshot_id,
                "download_name": row.download_name,
                "filters": filters,
                "created_at": row.created_at,
            }
    except Exception:
        return None


def create_deterministic_hash(file_ids: List[int]) -> str:
    """Create a deterministic hash based on sorted file IDs."""
    sorted_ids = sorted(file_ids)
    hash_input = ",".join(str(id) for id in sorted_ids)
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


def create_link_text_file(
    snapshot_id: str,
    download_name: str,
    base_url: str,
    filters: Optional[Dict[str, Any]] = None,
    context_id: Optional[str] = None,
    file_count: Optional[int] = None,
) -> str:
    """Create text file content with permanent download link."""
    snapshot_url = build_snapshot_url(snapshot_id, base_url, context_id=context_id)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    filter_lines = "\n".join(format_filter_context_lines(filters))

    content = f"""Permanent Download Link
========================

This download was created on: {timestamp}
Download name: {download_name}
File count: {file_count if file_count is not None else "unknown"}

Permanent Link (never expires):
{snapshot_url}

Filter context:
{filter_lines}

What is this?
This link will always download the exact same files that were included 
in your original download, even if the source files are updated later.

Technical Details:
- Snapshot ID: {snapshot_id}
- This link is deterministic: the same set of files always generates 
  the same permanent URL
- The link works independently of when files were last updated
- Files are served from their current storage location, not duplicated

---
Generated by PabuLib
"""
    return content


def create_download_snapshot(
    file_pairs: List[Tuple[str, Path]], download_name: str
) -> str:
    """Create or retrieve existing snapshot for a file set using minimal mapping.

    Stores only (snapshot_id, download_name) and (snapshot_id, file_id) tuples.
    """
    from ..db import get_session
    from ..models import DownloadSnapshot, DownloadSnapshotFile, PBFile

    # Get PBFile IDs for the provided file names (latest ingested match by name)
    file_ids: List[int] = []
    with get_session() as session:
        for name, _path in file_pairs:
            pb_file = (
                session.query(PBFile)
                .filter(PBFile.file_name == name)
                .order_by(PBFile.ingested_at.desc())
                .first()
            )
            if pb_file:
                file_ids.append(int(pb_file.id))

    # Create deterministic token from sorted IDs
    snapshot_id = create_deterministic_hash(file_ids)

    # Upsert minimal snapshot and mapping
    with get_session() as session:
        existing = (
            session.query(DownloadSnapshot)
            .filter(DownloadSnapshot.snapshot_id == snapshot_id)
            .first()
        )
        if not existing:
            session.add(
                DownloadSnapshot(
                    snapshot_id=snapshot_id,
                    download_name=download_name,
                )
            )
            for fid in file_ids:
                session.add(
                    DownloadSnapshotFile(
                        snapshot_id=snapshot_id,
                        file_id=fid,
                    )
                )
            session.commit()

    return snapshot_id


def create_download_snapshot_from_ids(file_ids: List[int], download_name: str) -> str:
    """Create or retrieve a snapshot using explicit PBFile IDs.

    This avoids name->id lookups and guarantees the exact version mapping.
    """
    from ..db import get_session
    from ..models import DownloadSnapshot, DownloadSnapshotFile

    # Deterministic token from sorted IDs
    snapshot_id = create_deterministic_hash(file_ids)

    with get_session() as session:
        existing = (
            session.query(DownloadSnapshot)
            .filter(DownloadSnapshot.snapshot_id == snapshot_id)
            .first()
        )
        if not existing:
            session.add(
                DownloadSnapshot(
                    snapshot_id=snapshot_id,
                    download_name=download_name,
                )
            )
            for fid in sorted(set(int(x) for x in file_ids)):
                session.add(
                    DownloadSnapshotFile(
                        snapshot_id=snapshot_id,
                        file_id=fid,
                    )
                )
            session.commit()

    return snapshot_id


def _calculate_file_hash(file_path: Path) -> str:
    """(Optional) Calculate SHA-256 hash of a file. Unused in minimal schema."""
    sha256_hash = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def get_snapshot_info(snapshot_id: str):
    """Get snapshot information (minimal: name + files count)."""
    from ..db import get_session
    from ..models import DownloadSnapshot, DownloadSnapshotFile

    with get_session() as session:
        snapshot = (
            session.query(DownloadSnapshot)
            .filter(
                DownloadSnapshot.snapshot_id == snapshot_id,
                DownloadSnapshot.is_active == True,
            )
            .first()
        )
        if not snapshot:
            return None

        files = (
            session.query(DownloadSnapshotFile)
            .filter(DownloadSnapshotFile.snapshot_id == snapshot_id)
            .all()
        )
        return {
            "snapshot_id": snapshot.snapshot_id,
            "download_name": snapshot.download_name,
            "file_count": len(files),
            "created_at": snapshot.created_at,
            "files": [{"file_id": f.file_id} for f in files],
        }


def serve_snapshot_download(
    snapshot_id: str, base_url: str = "", context_id: Optional[str] = None
):
    """Serve snapshot by recreating ZIP from original files with link text file."""
    from flask import abort, request, send_file

    from ..db import get_session
    from ..models import DownloadSnapshot, DownloadSnapshotFile, PBFile

    snapshot_info = get_snapshot_info(snapshot_id)
    if not snapshot_info:
        abort(404)

    # Get actual file paths
    file_paths = []
    with get_session() as session:
        files = (
            session.query(DownloadSnapshotFile)
            .filter(DownloadSnapshotFile.snapshot_id == snapshot_id)
            .all()
        )

        for file_record in files:
            pb_file = (
                session.query(PBFile).filter(PBFile.id == file_record.file_id).first()
            )

            if pb_file and pb_file.path:
                file_path = Path(pb_file.path)
                if file_path.exists():
                    file_paths.append(file_path)

    if not file_paths:
        abort(404)

    # Get base URL for link generation
    if not base_url:
        base_url = request.host_url.rstrip("/")
    snapshot_context = get_snapshot_context(context_id, snapshot_id)
    context_filters = (snapshot_context or {}).get("filters")
    context_download_name = (snapshot_context or {}).get("download_name")
    effective_download_name = context_download_name or snapshot_info["download_name"]

    # Always create ZIP with files + link text file
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Add all the actual files
        for path in file_paths:
            if path.exists():
                zf.write(path, arcname=path.name)

        # Add the permanent link text file
        link_content = create_link_text_file(
            snapshot_id,
            effective_download_name,
            base_url,
            filters=context_filters,
            context_id=(snapshot_context or {}).get("context_id"),
            file_count=snapshot_info.get("file_count"),
        )
        zf.writestr("_PERMANENT_DOWNLOAD_LINK.txt", link_content.encode("utf-8"))

    memory_file.seek(0)
    return send_file(
        memory_file,
        as_attachment=True,
        download_name=effective_download_name,
        mimetype="application/zip",
    )


def create_snapshot_for_cache_file(download_name: str, file_pairs=None) -> str:
    """Create snapshot for current file set."""
    if file_pairs is None:
        from ..db import get_session
        from ..models import PBFile

        with get_session() as session:
            current_files = (
                session.query(PBFile).filter(PBFile.is_current == True).all()
            )
            file_pairs = []
            for pb_file in current_files:
                file_path = Path(pb_file.path)
                if file_path.exists():
                    file_pairs.append((pb_file.file_name, file_path))

    return create_download_snapshot(file_pairs=file_pairs, download_name=download_name)


def create_download_with_link(
    file_pairs: List[Tuple[str, Path]],
    download_name: str,
    base_url: str,
    filters: Optional[Dict[str, Any]] = None,
) -> tuple[io.BytesIO, str, Optional[str]]:
    """Create a ZIP download with files and permanent link text file."""
    # Create snapshot first
    snapshot_id = create_download_snapshot(
        file_pairs=file_pairs, download_name=download_name
    )
    context_id = create_snapshot_context(
        snapshot_id=snapshot_id, download_name=download_name, filters=filters
    )

    # Create ZIP in memory with files + link
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Add all the actual files
        for name, path in file_pairs:
            if path.exists():
                zf.write(path, arcname=name)

        # Add the permanent link text file
        link_content = create_link_text_file(
            snapshot_id,
            download_name,
            base_url,
            filters=filters,
            context_id=context_id,
            file_count=len(file_pairs),
        )
        zf.writestr("_PERMANENT_DOWNLOAD_LINK.txt", link_content.encode("utf-8"))

    memory_file.seek(0)
    return memory_file, snapshot_id, context_id


def add_link_to_existing_zip(
    zip_path: Path,
    snapshot_id: str,
    download_name: str,
    base_url: str,
    filters: Optional[Dict[str, Any]] = None,
    context_id: Optional[str] = None,
) -> io.BytesIO:
    """Add link text file to an existing ZIP file."""
    memory_file = io.BytesIO()

    # Copy existing ZIP contents and add link file
    with zipfile.ZipFile(zip_path, "r") as source_zip:
        with zipfile.ZipFile(
            memory_file, "w", compression=zipfile.ZIP_DEFLATED
        ) as target_zip:
            # Copy all existing files
            for item in source_zip.infolist():
                data = source_zip.read(item.filename)
                target_zip.writestr(item, data)

            # Add the permanent link text file
            link_content = create_link_text_file(
                snapshot_id,
                download_name,
                base_url,
                filters=filters,
                context_id=context_id,
                file_count=max(0, len(source_zip.namelist()) - (1 if "_PERMANENT_DOWNLOAD_LINK.txt" in source_zip.namelist() else 0)),
            )
            target_zip.writestr(
                "_PERMANENT_DOWNLOAD_LINK.txt", link_content.encode("utf-8")
            )

    memory_file.seek(0)
    return memory_file
