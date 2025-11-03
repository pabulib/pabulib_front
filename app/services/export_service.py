from __future__ import annotations

import json
import os
import threading
import zipfile
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import List, Tuple

from ..db import get_session
from ..models import PBFile
from ..utils.pb_utils import pb_folder as _pb_folder
from .snapshot_service import (
    create_link_text_file as _create_link_text_file,
    create_snapshot_for_cache_file as _create_snapshot_for_cache_file,
)

# Simple lock to avoid concurrent rebuilds
_EXPORT_LOCK = threading.Lock()


def _cache_dir() -> Path:
    """Return the workspace cache directory at project root (../..), creating it if needed."""
    # __file__ = app/services/export_service.py
    # parents[0] => app/services, [1] => app, [2] => project root
    d = Path(__file__).resolve().parents[2] / "cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _signature_file() -> Path:
    return _cache_dir() / ".all_pb_files.signature.json"


@dataclass(frozen=True)
class CurrentSet:
    count: int
    items: Tuple[Tuple[str, str], ...]  # (file_name, file_mtime_iso)

    def digest(self) -> str:
        h = sha256()
        h.update(str(self.count).encode("utf-8"))
        for name, mtime in self.items:
            h.update(b"\n")
            h.update(name.encode("utf-8"))
            h.update(b"|")
            h.update(mtime.encode("utf-8"))
        return h.hexdigest()


def _fetch_current_set() -> CurrentSet:
    """Fetch the current set of PB files from DB (is_current=True).

    Returns an immutable structure with a stable digest over file_name+mtime.
    """
    with get_session() as s:
        rows: List[Tuple[str, datetime]] = [
            (r.file_name, r.file_mtime)
            for r in (
                s.query(PBFile.file_name, PBFile.file_mtime)
                .filter(PBFile.is_current == True)  # noqa: E712
                .order_by(PBFile.file_name.asc())
                .all()
            )
        ]
    items: List[Tuple[str, str]] = [
        (name, (mtime or datetime.utcfromtimestamp(0)).isoformat())
        for name, mtime in rows
    ]
    return CurrentSet(count=len(items), items=tuple(items))


def _load_previous_signature() -> dict | None:
    p = _signature_file()
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_signature(sig: str, relpath: str | None) -> None:
    data = {
        "signature": sig,
        "built_at": datetime.utcnow().isoformat(),
        "last_zip_relpath": relpath,
    }
    try:
        _signature_file().write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        # non-fatal
        pass


def _build_zip(zip_name: str = "all_pb_files.zip") -> Path:
    """Build a fresh ZIP of all .pb files currently on disk in pb_files/.

    The ZIP is written to cache/<timestamp>/<zip_name> and that path is returned.
    Also embeds a `_PERMANENT_DOWNLOAD_LINK.txt` so the archive is ready to serve
    without any request-time mutation.
    """
    pb_dir = _pb_folder()
    files = [p for p in sorted(pb_dir.glob("*.pb")) if p.is_file()]
    if not files:
        raise RuntimeError("No .pb files found to export")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = _cache_dir() / ts
    out_dir.mkdir(parents=True, exist_ok=True)
    out_zip = out_dir / zip_name

    with zipfile.ZipFile(out_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            zf.write(p, arcname=p.name)
        # Create or reuse a snapshot for the current set, and write link note
        try:
            # This uses the current DB set at build time, matching files we just zipped
            snapshot_id = _create_snapshot_for_cache_file(download_name=out_zip.name)
            base_url = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
            link_txt = _create_link_text_file(snapshot_id, out_zip.name, base_url)
            zf.writestr("_PERMANENT_DOWNLOAD_LINK.txt", link_txt.encode("utf-8"))
        except Exception:
            # Non-fatal: zip will still be usable without the link file
            pass

    return out_zip


def build_if_changed(zip_name: str = "all_pb_files.zip") -> Path | None:
    """Build the ZIP if the current pb_files set changed since the last build.

    Returns the zip Path if built, or None if no change.
    """
    curr = _fetch_current_set()
    sig = curr.digest()
    prev = _load_previous_signature() or {}
    if prev.get("signature") == sig:
        return None

    with _EXPORT_LOCK:
        # Re-check after acquiring lock to avoid duplicate builds
        prev = _load_previous_signature() or {}
        if prev.get("signature") == sig:
            return None
        out_zip = _build_zip(zip_name=zip_name)
        # Store relpath relative to cache for convenience
        relpath = out_zip.relative_to(_cache_dir())
        _save_signature(sig, str(relpath).replace("\\", "/"))
        return out_zip


def trigger_build_if_changed_background(zip_name: str = "all_pb_files.zip") -> None:
    """Spawn a background thread to build the ZIP if needed.

    This avoids blocking request/CLI flows.
    """

    def _worker():
        try:
            out = build_if_changed(zip_name=zip_name)
            # Optional: we could add logging here if a logger is available
        except Exception:
            # Swallow errors in background to not impact user flow
            pass

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
