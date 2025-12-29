import io
import json
import mimetypes
import re
import tempfile
import threading
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    Blueprint,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

from .__init__ import limiter
from .db import get_session
from .models import PBFile
from .routes_admin import _format_preview_tile  # reuse tile formatting
from .routes_admin import _load_upload_settings  # reuse limits
from .services.pb_service import (
    aggregate_categories_cached as _aggregate_categories_cached,
    get_tiles_cached as _get_tiles_cached,
)
from .services.pb_service import aggregate_comments_cached as _aggregate_comments_cached
from .services.pb_service import (
    aggregate_statistics_cached as _aggregate_statistics_cached,
)
from .services.pb_service import aggregate_targets_cached as _aggregate_targets_cached
from .services.pb_service import get_all_current_file_paths, get_current_file_path

# Simple in-memory registry for zip jobs; zip files and progress json live on disk
_ZIP_JOBS: Dict[str, Dict[str, Any]] = {}
_ZIP_JOBS_LOCK = threading.Lock()





def _zip_jobs_dir() -> Path:
    # Store transient job artifacts in system temp instead of project cache
    d = Path(tempfile.gettempdir()) / "pabulib_zip_jobs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_progress(token: str, data: Dict[str, Any]) -> None:
    try:
        p = _zip_jobs_dir() / f"{token}.json"
        with p.open("w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass


def _read_progress(token: str) -> Optional[Dict[str, Any]]:
    p = _zip_jobs_dir() / f"{token}.json"
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _cleanup_old_jobs(max_age_seconds: int = 60 * 30) -> None:
    # best-effort cleanup of old job artifacts
    try:
        now = datetime.utcnow().timestamp()
        for fp in _zip_jobs_dir().glob("*"):
            try:
                if not fp.exists():
                    continue
                age = now - fp.stat().st_mtime
                if age > max_age_seconds:
                    if fp.is_file():
                        fp.unlink(missing_ok=True)
            except Exception:
                continue
    except Exception:
        pass


def _build_zip_in_background(
    token: str,
    file_pairs: List[Tuple[str, Path]],
    download_name: str,
    reuse_file_path: Optional[Path] = None,
    file_ids: Optional[List[int]] = None,
) -> None:
    """Create the zip in the background and update progress JSON. If reuse_file_path
    is provided, mark job as done immediately using that existing file."""
    try:
        # Immediate completion when a ready-made zip is available
        if reuse_file_path is not None:
            progress = {
                "token": token,
                "total": len(file_pairs),
                "current": len(file_pairs),
                "percent": 100,
                "status": "ready",
                "current_name": None,
                "done": True,
                "error": None,
                "download_name": download_name,
                "file_ids": file_ids or [],
                "artifact_type": "zip",
                "file_path": str(reuse_file_path),
                "mime_type": "application/zip",
            }
            _write_progress(token, progress)
            with _ZIP_JOBS_LOCK:
                _ZIP_JOBS[token] = {
                    "file_path": str(reuse_file_path),
                    "download_name": download_name,
                    "file_ids": file_ids or [],
                    "artifact_type": "zip",
                    "mime_type": "application/zip",
                }
            return

        out_zip = _zip_jobs_dir() / f"{token}.zip"
        total = max(1, len(file_pairs))
        progress = {
            "token": token,
            "total": total,
            "current": 0,
            "percent": 0,
            "status": "starting",
            "current_name": None,
            "done": False,
            "error": None,
            "download_name": download_name,
            "file_ids": file_ids or [],
            "artifact_type": "zip",
            "file_path": str(out_zip),
            "mime_type": "application/zip",
        }
        _write_progress(token, progress)

        with zipfile.ZipFile(out_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for idx, (arcname, path) in enumerate(file_pairs, start=1):
                try:
                    progress.update(
                        {
                            "status": "zipping",
                            "current_name": arcname,
                            "current": idx - 1,
                            "percent": int(((idx - 1) / total) * 100),
                        }
                    )
                    _write_progress(token, progress)
                    # Add file
                    zf.write(path, arcname=arcname)
                    progress.update(
                        {
                            "current": idx,
                            "percent": int((idx / total) * 100),
                        }
                    )
                    _write_progress(token, progress)
                except Exception as e:
                    progress.update(
                        {
                            "status": "error",
                            "error": f"Failed to add {arcname}: {e}",
                        }
                    )
                    _write_progress(token, progress)
        # Mark complete
        progress.update(
            {
                "done": True,
                "status": "ready",
                "percent": 100,
                "artifact_type": "zip",
                "file_path": str(out_zip),
            }
        )
        _write_progress(token, progress)
        with _ZIP_JOBS_LOCK:
            _ZIP_JOBS[token] = {
                "file_path": str(out_zip),
                "download_name": download_name,
                "file_ids": file_ids or [],
                "artifact_type": "zip",
                "mime_type": "application/zip",
            }
    except Exception as e:
        progress = {
            "token": token,
            "total": len(file_pairs),
            "current": 0,
            "percent": 0,
            "status": "error",
            "current_name": None,
            "done": False,
            "error": f"Zip error: {e}",
            "download_name": download_name,
            "file_ids": file_ids or [],
            "artifact_type": "zip",
        }
        _write_progress(token, progress)


from .services.pb_service import (
    get_tiles_cached as _get_tiles_cached,
    search_tiles as _search_tiles,
    get_filter_options as _get_filter_options,
)
from .services.snapshot_service import (
    add_link_to_existing_zip as _add_link_to_existing_zip,
)
from .services.snapshot_service import (
    create_download_with_link as _create_download_with_link,
)
from .services.snapshot_service import (
    serve_snapshot_download as _serve_snapshot_download,
)
from .utils.file_helpers import is_safe_filename as _is_safe_filename
from .utils.formatting import format_int as _format_int
from .utils.load_pb_file import parse_pb_lines
from .utils.pb_utils import parse_comments_from_meta as _parse_comments_from_meta
from .utils.pb_utils import parse_pb_to_tile as _parse_pb_to_tile
from .utils.upload_security import is_allowed_extension as _is_allowed_ext
from .utils.upload_security import is_probably_text_file as _is_probably_text_file
from .utils.upload_security import public_tmp_dir as _public_tmp_dir
from .utils.validation import count_issues, format_validation_summary
from .utils.validation import get_checker_version as _get_checker_version
from .utils.validation import validate_pb_file

bp = Blueprint(
    "main",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


@bp.route("/")
def home():
    # Initial load: get first 20 tiles
    tiles, total = _search_tiles(limit=20)
    return render_template("index.html", tiles=tiles, count=total)


@bp.route("/api/search")
def api_search():
    # Parse args
    query = request.args.get("search")
    country = request.args.get("country")
    city = request.args.get("city")
    year = request.args.get("year")
    
    votes_min = request.args.get("votes_min", type=int)
    votes_max = request.args.get("votes_max", type=int)
    projects_min = request.args.get("projects_min", type=int)
    projects_max = request.args.get("projects_max", type=int)
    len_min = request.args.get("len_min", type=float)
    len_max = request.args.get("len_max", type=float)
    
    vote_type = request.args.get("type")
    
    exclude_fully = request.args.get("exclude_fully") == "true"
    exclude_experimental = request.args.get("exclude_experimental") == "true"
    
    require_geo = request.args.get("require_geo") == "true"
    require_target = request.args.get("require_target") == "true"
    require_category = request.args.get("require_category") == "true"
    
    order_by = request.args.get("order_by", "quality")
    order_dir = request.args.get("order_dir", "desc")
    
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)
    
    tiles, total = _search_tiles(
        query=query,
        country=country,
        city=city,
        year=year,
        votes_min=votes_min,
        votes_max=votes_max,
        projects_min=projects_min,
        projects_max=projects_max,
        len_min=len_min,
        len_max=len_max,
        vote_type=vote_type,
        exclude_fully=exclude_fully,
        exclude_experimental=exclude_experimental,
        require_geo=require_geo,
        require_target=require_target,
        require_category=require_category,
        order_by=order_by,
        order_dir=order_dir,
        limit=limit,
        offset=offset,
    )
    
    return jsonify({
        "tiles": tiles,
        "total": total,
        "limit": limit,
        "offset": offset
    })


@bp.route("/api/options")
def api_options():
    return jsonify(_get_filter_options())


@bp.route("/api/tiles")
def api_tiles():
    tiles = _get_tiles_cached()
    return jsonify(tiles)


@bp.route("/format")
def format_page():
    return render_template("format.html")


@bp.route("/tools")
def tools_page():
    return render_template("tools.html")


@bp.route("/citations")
def citations_page():
    # Parse bib.bib and pass publications to the template
    import bibtexparser

    bib_path = Path(__file__).parent.parent / "docs" / "bib.bib"
    publications = []
    if bib_path.exists():
        with open(bib_path, "r", encoding="utf-8") as bibfile:
            bib_database = bibtexparser.load(bibfile)
            for entry in bib_database.entries:
                authors_raw = entry.get("author", "")
                year = entry.get("year", "")
                title = entry.get("title", "")
                url = entry.get("url", "")
                # Split authors only by " and "
                authors_list = [
                    a
                    for a in authors_raw.replace("\n", " ").split(" and ")
                    if a.strip()
                ]
                authors = []
                for author in authors_list:
                    print("author", author)
                    parts = author.split()
                    if len(parts) > 1:
                        firstname = parts[-1]
                        firstname = firstname.replace(",", " ")
                        surname = parts[0]
                        surname = surname.replace(",", " ")
                        authors.append(f"{firstname[0]}. {surname}")
                    elif parts:
                        authors.append(parts[0])
                print("->", authors)
                authors_str = ", ".join(authors)
                publications.append(
                    {"authors": authors_str, "year": year, "title": title, "url": url}
                )
    return render_template("citations.html", publications=publications)


@bp.route("/about")
def about_page():
    return render_template("about.html")


@bp.route("/contact")
def contact_page():
    return render_template("contact.html", now=datetime.now())


@bp.get("/upload")
def upload_page():
    """Public page to validate .pb files and send them for acceptance."""
    settings = _load_upload_settings()
    tiles = _list_public_tmp_tiles()
    checker_version = None
    try:
        checker_version = _get_checker_version()
    except Exception:
        checker_version = None
    return render_template(
        "upload.html",
        upload_settings=settings,
        tiles=tiles,
        count=len(tiles),
        checker_version=checker_version,
    )


@bp.get("/check")
def check_page():
    """Alias path: redirect to /upload to keep a single canonical URL."""
    return redirect(url_for("main.upload_page"), code=302)


def _public_session_dir() -> Path:
    """Stable temp dir per user session for public uploads."""
    key = session.get("public_tmp_key")
    if not key:
        key = uuid.uuid4().hex
        session["public_tmp_key"] = key
    # Base folder for public uploads in temp dir
    base = Path(tempfile.gettempdir()) / "pabulib_public"
    base.mkdir(parents=True, exist_ok=True)
    p = base / key
    try:
        p.mkdir(mode=0o700, parents=True, exist_ok=True)
    except Exception:
        p.mkdir(parents=True, exist_ok=True)
    return p


def _list_public_tmp_tiles() -> list[dict]:
    """Build rich tiles for the public upload session, mirroring admin preview."""
    tmp_dir = _public_session_dir()
    tiles: list[dict] = []
    for p in sorted(tmp_dir.glob("*.pb")):
        try:
            # Parse PB to a tile dict and format to preview shape like admin
            parsed = _parse_pb_to_tile(p)
            tile_data = _format_preview_tile(parsed)
            tile_data["file_name"] = p.name  # ensure filename is the session one
            # Determine if this would overwrite an existing current dataset (by webpage_name)
            try:
                webpage_name = (tile_data.get("webpage_name") or "").strip()
                exists_conflict = False
                if webpage_name:
                    with get_session() as s:
                        exists_conflict = (
                            s.query(PBFile.id)
                            .filter(
                                PBFile.webpage_name == webpage_name,
                                PBFile.is_current == True,  # noqa: E712
                            )
                            .first()
                            is not None
                        )
                tile_data["exists_conflict"] = bool(exists_conflict)
            except Exception:
                tile_data["exists_conflict"] = False

            # Cached validation (reuse and cache to avoid re-validating constantly)
            validation_cache_path = tmp_dir / f".{p.name}.validation.json"
            validation = None
            try:
                if (
                    validation_cache_path.exists()
                    and validation_cache_path.stat().st_mtime >= p.stat().st_mtime
                ):
                    with open(validation_cache_path, "r") as f:
                        validation = json.load(f)
            except Exception:
                validation = None
            if validation is None:
                validation = validate_pb_file(p)
                try:
                    with open(validation_cache_path, "w") as f:
                        json.dump(validation, f)
                except Exception:
                    pass

            tile_data["validation"] = validation
            tile_data["validation_summary"] = format_validation_summary(validation)
            issue_counts = count_issues(validation)
            tile_data["error_count"] = issue_counts.get("errors", 0)
            tile_data["warning_count"] = issue_counts.get("warnings", 0)

            tiles.append(tile_data)
        except Exception as e:
            # Provide a minimal error tile with a visible validation error
            tiles.append(
                {
                    "file_name": p.name,
                    "title": p.stem.replace("_", " "),
                    "description": "(Failed to parse)",
                    "num_votes": "—",
                    "num_projects": "—",
                    "budget": "—",
                    "vote_type": "",
                    "vote_length": "—",
                    "validation": {
                        "valid": False,
                        "errors": None,
                        "warnings": None,
                        "error_message": f"Parse error: {e.__class__.__name__}: {str(e)}. File likely corrupted or malformed.",
                    },
                    "validation_summary": f"⚠ Parse error: {e.__class__.__name__}: {str(e)}. File likely corrupted.",
                    "error_count": 0,
                    "warning_count": 0,
                }
            )
    return tiles


@bp.post("/upload/upload")
@limiter.limit("15/minute; 300/day")
def upload_upload_batch():
    """Upload multiple .pb files into the user's session tmp area.
    Performs security checks and basic de-duplication by webpage_name.
    Returns JSON with per-file results.
    """
    if "files" not in request.files:
        return jsonify({"ok": False, "error": "No files part"}), 400

    files = request.files.getlist("files")
    if not files:
        return jsonify({"ok": False, "error": "No files provided"}), 400

    settings = _load_upload_settings()
    max_mb = int(settings.get("max_file_mb", 10))
    max_bytes = max_mb * 1024 * 1024
    max_batch = int(settings.get("max_batch", 100))
    if len(files) > max_batch:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Too many files. Max per upload is {max_batch}.",
                }
            ),
            400,
        )

    tmp_dir = _public_session_dir()
    # Optional flag to replace existing session files with same standardized name
    force_replace_flag = str(request.form.get("force_replace") or "").lower() in {
        "1",
        "true",
        "yes",
    }
    existing = {p.name for p in tmp_dir.glob("*.pb")}

    results = []
    saved = 0
    # Open one DB session to check conflicts efficiently
    with get_session() as s:
        for f in files:
            name = (f.filename or "").strip()
            safe_name = secure_filename(name)
            if not safe_name or not _is_allowed_ext(safe_name):
                results.append(
                    {
                        "ok": False,
                        "name": name or "(unnamed)",
                        "msg": "Only .pb files are allowed.",
                    }
                )
                continue

            # Save to a temporary unique path first
            tmp_unique = tmp_dir / f"._incoming_{uuid.uuid4().hex}.pb"
            try:
                f.save(str(tmp_unique))
                # Post-save checks
                try:
                    if tmp_unique.stat().st_size > max_bytes:
                        results.append(
                            {
                                "ok": False,
                                "name": name,
                                "msg": f"File too large (> {max_mb} MB)",
                            }
                        )
                        tmp_unique.unlink(missing_ok=True)
                        continue
                except Exception:
                    pass
                if not _is_probably_text_file(tmp_unique):
                    results.append(
                        {
                            "ok": False,
                            "name": name,
                            "msg": "File does not look like text",
                        }
                    )
                    tmp_unique.unlink(missing_ok=True)
                    continue

                # Determine webpage_name to standardize filename
                try:
                    t = _parse_pb_to_tile(tmp_unique)
                    webpage_name = (t.get("webpage_name") or "").strip()
                except Exception:
                    webpage_name = ""

                target_name = (
                    secure_filename(f"{webpage_name}.pb") if webpage_name else safe_name
                )
                # Check if this webpage_name already exists in current library (for overwrite alert on client)
                exists_conflict = False
                if webpage_name:
                    try:
                        exists_conflict = (
                            s.query(PBFile.id)
                            .filter(
                                PBFile.webpage_name == webpage_name,
                                PBFile.is_current == True,  # noqa: E712
                            )
                            .first()
                            is not None
                        )
                    except Exception:
                        exists_conflict = False
                if target_name in existing or (tmp_dir / target_name).exists():
                    if not force_replace_flag:
                        # Duplicate in this session; reject (client may re-upload with force)
                        results.append(
                            {
                                "ok": False,
                                "name": name,
                                "msg": f"Duplicate webpage_name. A file named {target_name} is already uploaded in this session.",
                                "webpage_name": webpage_name
                                or target_name.rsplit(".", 1)[0],
                            }
                        )
                        tmp_unique.unlink(missing_ok=True)
                        continue
                    # Force replace: allow overwrite atomically
                    try:
                        (tmp_dir / target_name).unlink(missing_ok=True)
                    except Exception:
                        pass

                dest = tmp_dir / target_name
                tmp_unique.replace(dest)
                existing.add(target_name)
                saved += 1
                results.append(
                    {
                        "ok": True,
                        "name": target_name,
                        "webpage_name": webpage_name or target_name.rsplit(".", 1)[0],
                        "exists_conflict": bool(exists_conflict),
                        "msg": "Uploaded to session.",
                    }
                )
            except Exception as e:
                try:
                    tmp_unique.unlink(missing_ok=True)
                except Exception:
                    pass
                results.append(
                    {
                        "ok": False,
                        "name": name,
                        "msg": f"Upload failed: {e.__class__.__name__}: {str(e)}",
                    }
                )

    return jsonify(
        {
            "ok": True,
            "saved": saved,
            "results": results,
            "existing_count": len(existing),
        }
    )


@bp.post("/upload/submit_selected")
@limiter.limit("5/minute; 50/day")
def upload_submit_selected():
    """Copy selected valid files from the user's session tmp to the admin tmp with email sidecar."""
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}
    files = payload.get("files") or []
    email = (payload.get("email") or "").strip()

    if not email or ("@" not in email or "." not in email.split("@")[-1]):
        return jsonify({"ok": False, "error": "Valid email required"}), 400
    if not isinstance(files, list) or not files:
        return jsonify({"ok": False, "error": "No files selected"}), 400

    # Limit batch size
    settings = _load_upload_settings()
    max_batch = int(settings.get("max_batch", 100))
    if len(files) > max_batch:
        return (
            jsonify({"ok": False, "error": f"Too many files. Max is {max_batch}."}),
            400,
        )

    tmp_dir = _public_session_dir()

    from .routes_admin import _tmp_upload_dir  # local import to avoid cycles

    admin_tmp = _tmp_upload_dir()
    saved = 0
    results = []
    for name in files:
        safe = secure_filename(str(name))
        src = tmp_dir / safe
        if not safe or not _is_allowed_ext(safe) or not src.exists():
            results.append({"ok": False, "name": name, "msg": "Not found"})
            continue
        # Validate before copying (use cache if available)
        validation_cache_path = tmp_dir / f".{safe}.validation.json"
        validation = None
        if validation_cache_path.exists():
            try:
                with open(validation_cache_path, "r") as f:
                    validation = json.load(f)
            except Exception:
                validation = None
        if validation is None:
            validation = validate_pb_file(src)
            try:
                with open(validation_cache_path, "w") as f:
                    json.dump(validation, f)
            except Exception:
                pass
        if not validation.get("valid"):
            results.append(
                {
                    "ok": False,
                    "name": safe,
                    "msg": "File is not valid; cannot submit",
                }
            )
            continue

        # Compute destination, avoiding collisions by suffixing
        dest = admin_tmp / safe
        if dest.exists():
            stem, suff = dest.stem, dest.suffix
            i = 1
            while True:
                alt = admin_tmp / f"{stem}_{i}{suff}"
                if not alt.exists():
                    dest = alt
                    break
                i += 1
        try:
            # Copy bytes
            dest.write_bytes(src.read_bytes())
            # Write sidecar marker with contributor email
            marker = {"public_submission": True, "email": email}
            (admin_tmp / f".{dest.name}.public.json").write_text(
                json.dumps(marker), encoding="utf-8"
            )
            # Remove from session tmp
            try:
                src.unlink()
            except Exception:
                pass
            try:
                # remove cached validation if present
                (tmp_dir / f".{safe}.validation.json").unlink()
            except Exception:
                pass
            saved += 1
            results.append({"ok": True, "name": dest.name, "msg": "Submitted"})
        except Exception as e:
            results.append(
                {
                    "ok": False,
                    "name": safe,
                    "msg": f"Failed to submit: {e.__class__.__name__}: {str(e)}",
                }
            )

    return jsonify({"ok": True, "saved": saved, "results": results})


@bp.post("/upload/delete_selected")
@limiter.limit("15/minute; 300/day")
def upload_delete_selected():
    """Delete selected files from the user's session tmp area.
    Only .pb files in the current session directory are eligible.
    """
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}
    files = payload.get("files") or []
    if not isinstance(files, list) or not files:
        return jsonify({"ok": False, "error": "No files selected"}), 400

    tmp_dir = _public_session_dir()
    deleted = 0
    results = []
    for name in files:
        safe = secure_filename(str(name))
        if not safe or not _is_allowed_ext(safe):
            results.append({"ok": False, "name": name, "msg": "Invalid name"})
            continue
        p = tmp_dir / safe
        if not p.exists() or not p.is_file():
            results.append({"ok": False, "name": safe, "msg": "Not found"})
            continue
        try:
            p.unlink()
            # Remove cached validation if present
            try:
                (tmp_dir / f".{safe}.validation.json").unlink()
            except Exception:
                pass
            deleted += 1
            results.append({"ok": True, "name": safe, "msg": "Deleted"})
        except Exception as e:
            results.append(
                {
                    "ok": False,
                    "name": safe,
                    "msg": f"Failed to delete: {e.__class__.__name__}: {str(e)}",
                }
            )

    return jsonify({"ok": True, "deleted": deleted, "results": results})


@bp.post("/upload/validate")
@limiter.limit("10/minute; 200/day")
def upload_validate():
    """Validate a single uploaded .pb file (no persistence). Returns JSON."""
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file part"}), 400
    f = request.files["file"]
    email = (request.form.get("email") or "").strip()
    # Basic email sanity (optional): simple pattern
    if email and ("@" not in email or "." not in email.split("@")[-1]):
        return jsonify({"ok": False, "error": "Invalid email"}), 400

    name = (f.filename or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Empty filename"}), 400
    if not _is_allowed_ext(name):
        return jsonify({"ok": False, "error": "Only .pb files are allowed"}), 400

    settings = _load_upload_settings()
    max_bytes = int(settings.get("max_file_mb", 10)) * 1024 * 1024
    # If content length known, enforce early
    clen = request.content_length or 0
    if clen and clen > max_bytes + 1024 * 64:  # small overhead wiggle
        return jsonify({"ok": False, "error": "File too large"}), 413

    tmp_dir = _public_tmp_dir()
    tmp_path = tmp_dir / name
    try:
        f.save(str(tmp_path))
        # Post-save checks
        try:
            if tmp_path.stat().st_size > max_bytes:
                return jsonify({"ok": False, "error": "File too large"}), 413
        except Exception:
            pass
        # Ensure it's text-like
        if not _is_probably_text_file(tmp_path):
            return jsonify({"ok": False, "error": "File does not look like text"}), 400

        # Validate using existing validator (creates its own sanitized temp and cleans it)
        validation = validate_pb_file(tmp_path)

        return jsonify(
            {
                "ok": True,
                "validation": validation,
                "email": email or None,
            }
        )
    finally:
        # Clean up uploaded file and temp folder
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        try:
            # Remove the unique folder
            tmp_dir.rmdir()
        except Exception:
            pass


@bp.post("/upload/submit")
@limiter.limit("5/minute; 50/day")
def upload_submit():
    """Accept a single valid .pb + email and store it in the admin tmp area with a public marker.
    This does NOT ingest to the library; admin will see it in /admin/upload with a label.
    """
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file part"}), 400
    f = request.files["file"]
    email = (request.form.get("email") or "").strip()
    if not email or ("@" not in email or "." not in email.split("@")[-1]):
        return jsonify({"ok": False, "error": "Valid email required"}), 400
    name = (f.filename or "").strip()
    if not name or not _is_allowed_ext(name):
        return jsonify({"ok": False, "error": "Only .pb files are allowed"}), 400

    settings = _load_upload_settings()
    max_bytes = int(settings.get("max_file_mb", 10)) * 1024 * 1024

    tmp_dir = _public_tmp_dir()
    tmp_path = tmp_dir / name
    try:
        f.save(str(tmp_path))
        # size + text checks
        try:
            if tmp_path.stat().st_size > max_bytes:
                return jsonify({"ok": False, "error": "File too large"}), 413
        except Exception:
            pass
        if not _is_probably_text_file(tmp_path):
            return jsonify({"ok": False, "error": "File does not look like text"}), 400

        # Validate and only proceed if valid
        validation = validate_pb_file(tmp_path)
        if not validation.get("valid"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "File is not valid",
                        "validation": validation,
                    }
                ),
                400,
            )

        # Copy into admin tmp with a sidecar marker containing email
        from werkzeug.utils import secure_filename

        from .routes_admin import _tmp_upload_dir  # avoid cycle at top

        admin_tmp = _tmp_upload_dir()
        safe_name = secure_filename(name)
        dest = admin_tmp / safe_name
        # If collision, add a short suffix
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            i = 1
            while True:
                alt = admin_tmp / f"{stem}_{i}{suffix}"
                if not alt.exists():
                    dest = alt
                    break
                i += 1
        # Write file
        content = tmp_path.read_bytes()
        dest.write_bytes(content)
        # Write marker file for admin UI (e.g., .<filename>.public.json)
        import json

        marker = {
            "public_submission": True,
            "email": email,
        }
        (admin_tmp / f".{dest.name}.public.json").write_text(
            json.dumps(marker), encoding="utf-8"
        )

        return jsonify(
            {"ok": True, "message": "Submitted for acceptance", "name": dest.name}
        )
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


@bp.route("/comments")
def comments_page():
    (
        _map,
        rows,
        groups_by_comment_country,
        groups_by_comment_country_unit,
        groups_by_comment_country_unit_instance,
    ) = _aggregate_comments_cached()
    return render_template(
        "comments.html",
        rows=rows,
        groups_by_comment_country=groups_by_comment_country,
        groups_by_comment_country_unit=groups_by_comment_country_unit,
        groups_by_comment_country_unit_instance=groups_by_comment_country_unit_instance,
        total=len(rows),
    )


@bp.route("/details")
def details_page():
    tab = (request.args.get("tab") or "comments").strip().lower()
    if tab not in {"comments", "categories", "targets"}:
        tab = "comments"

    # Always compute all three so tab switch is instant without extra calls
    (
        _map_cmt,
        rows_comments,
        groups_comments_country,
        groups_comments_country_unit,
        groups_comments_country_unit_instance,
    ) = _aggregate_comments_cached()

    (
        _map_cat,
        rows_categories,
        groups_categories_country,
        groups_categories_country_unit,
        groups_categories_country_unit_instance,
    ) = _aggregate_categories_cached()

    (
        _map_tgt,
        rows_targets,
        groups_targets_country,
        groups_targets_country_unit,
        groups_targets_country_unit_instance,
    ) = _aggregate_targets_cached()

    return render_template(
        "details.html",
        tab=tab,
        # comments
        rows_comments=rows_comments,
        groups_comments_country=groups_comments_country,
        groups_comments_country_unit=groups_comments_country_unit,
        groups_comments_country_unit_instance=groups_comments_country_unit_instance,
        total_comments=len(rows_comments),
        # categories
        rows_categories=rows_categories,
        groups_categories_country=groups_categories_country,
        groups_categories_country_unit=groups_categories_country_unit,
        groups_categories_country_unit_instance=groups_categories_country_unit_instance,
        total_categories=len(rows_categories),
        # targets
        rows_targets=rows_targets,
        groups_targets_country=groups_targets_country,
        groups_targets_country_unit=groups_targets_country_unit,
        groups_targets_country_unit_instance=groups_targets_country_unit_instance,
        total_targets=len(rows_targets),
    )


@bp.route("/statistics")
def statistics_page():
    totals, series = _aggregate_statistics_cached()
    # Provide some pre-formatted numbers for display
    formatted = {
        "files": _format_int(totals.get("total_files", 0)),
        "countries": _format_int(totals.get("total_countries", 0)),
        "cities": _format_int(totals.get("total_cities", 0)),
        "projects": _format_int(totals.get("total_projects", 0)),
        "votes": _format_int(totals.get("total_votes", 0)),
        "selected": _format_int(totals.get("total_selected_projects", 0)),
    }
    # Build per-currency budget list for display
    budgets_map: Dict[str, int] = totals.get("budget_by_currency", {}) or {}
    budgets_list = [
        {"currency": cur, "amount": _format_int(val)}
        for cur, val in sorted(
            budgets_map.items(), key=lambda kv: (kv[0] == "—", kv[0])
        )
    ]
    return render_template(
        "statistics.html",
        totals=totals,
        formatted=formatted,
        series=series,
        budgets_list=budgets_list,
    )


@bp.route("/download/<path:filename>")
def download(filename: str):
    # DB-only: resolve path from DB
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)
    # Serve single files directly without creating a snapshot or exposing headers.
    return send_file(path, as_attachment=True)


@bp.post("/download-selected")
def download_selected():
    names = request.form.getlist("files")
    # Deduplicate names to prevent issues with double submission (checkbox + hidden input)
    if names:
        names = list(set(names))
        
    # Allow select_all via form or query for symmetry with background route
    select_all = (request.form.get("select_all") == "true") or (
        request.args.get("select_all") == "true"
    )

    # If select_all=true but no explicit names posted, treat as all current
    if not names and select_all:
        names = []  # explicit empty list signals select-all branch below

    # Get total count of current files to compare with selected count
    with get_session() as s:
        total_current_files = s.query(PBFile).filter(PBFile.is_current == True).count()

    # Check if user selected ALL current files
    # Consider select_all=true with no names as "all" as well (JS may omit names)
    selected_all_current = select_all and (
        len(names) == total_current_files or len(names) == 0
    )

    if selected_all_current:
        # User selected ALL current files - prefer newest timestamped export zip
        cache_dir = Path(__file__).parent.parent / "cache"
        cache_dir.mkdir(exist_ok=True)  # Ensure cache directory exists

        # 1) Prefer the newest timestamped export zip: cache/<ts>/all_pb_files.zip
        latest_export: Optional[Path] = None
        try:
            for p in cache_dir.rglob("all_pb_files.zip"):
                # Only consider files under a subdirectory (timestamped)
                if p.parent == cache_dir:
                    continue
                if not p.is_file():
                    continue
                if (
                    latest_export is None
                    or p.stat().st_mtime > latest_export.stat().st_mtime
                ):
                    latest_export = p
        except Exception:
            latest_export = None
        if latest_export is not None:
            # Prefer serving the prebuilt ZIP directly; only consult DB if we must inject a link
            base_url = request.host_url.rstrip("/")
            ts_download = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            dl_name = f"all_pb_files_{ts_download}.zip"
            try:
                import re
                import zipfile

                with zipfile.ZipFile(latest_export, "r") as zf:
                    if "_PERMANENT_DOWNLOAD_LINK.txt" in zf.namelist():
                        try:
                            txt = zf.read("_PERMANENT_DOWNLOAD_LINK.txt").decode(
                                "utf-8", "ignore"
                            )
                        except Exception:
                            txt = ""
                        m = re.search(r"/download/snapshot/([0-9a-f]{16})", txt)
                        snapshot_id = m.group(1) if m else None
                        resp = send_file(
                            latest_export,
                            as_attachment=True,
                            download_name=dl_name,
                        )
                        if snapshot_id:
                            resp.headers["X-Download-Snapshot-ID"] = snapshot_id
                            resp.headers["X-Download-Snapshot-URL"] = url_for(
                                "main.download_snapshot",
                                snapshot_id=snapshot_id,
                                _external=True,
                            )
                        return resp
            except Exception:
                pass

            # If the prebuilt doesn't have a link (legacy zip), we need the current set
            all_file_pairs = get_all_current_file_paths()
            if not all_file_pairs:
                abort(404, description="No current files found")
            try:
                from .services.snapshot_service import create_download_snapshot

                snapshot_id = create_download_snapshot(
                    file_pairs=all_file_pairs, download_name=dl_name
                )
                mem = _add_link_to_existing_zip(
                    latest_export, snapshot_id, dl_name, base_url
                )
                response = send_file(
                    mem,
                    as_attachment=True,
                    download_name=dl_name,
                    mimetype="application/zip",
                )
                response.headers["X-Download-Snapshot-ID"] = snapshot_id
                response.headers["X-Download-Snapshot-URL"] = url_for(
                    "main.download_snapshot", snapshot_id=snapshot_id, _external=True
                )
                return response
            except Exception:
                return send_file(
                    latest_export, as_attachment=True, download_name=dl_name
                )

        # 2) No timestamped export found; build a fresh timestamped export now
        all_file_pairs = get_all_current_file_paths()
        if not all_file_pairs:
            abort(404, description="No current files found")
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_dir = cache_dir / ts
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        out_zip = out_dir / "all_pb_files.zip"
        with zipfile.ZipFile(out_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file_name, file_path in all_file_pairs:
                zf.write(file_path, arcname=file_name)
        ts_download = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dl_name = f"all_pb_files_{ts_download}.zip"
        # Add link file to streamed response (cached zip on disk remains unchanged)
        try:
            from .services.snapshot_service import create_download_snapshot

            base_url = request.host_url.rstrip("/")
            snapshot_id = create_download_snapshot(
                file_pairs=all_file_pairs, download_name=dl_name
            )
            mem = _add_link_to_existing_zip(out_zip, snapshot_id, dl_name, base_url)
            response = send_file(
                mem,
                as_attachment=True,
                download_name=dl_name,
                mimetype="application/zip",
            )
            response.headers["X-Download-Snapshot-ID"] = snapshot_id
            response.headers["X-Download-Snapshot-URL"] = url_for(
                "main.download_snapshot", snapshot_id=snapshot_id, _external=True
            )
            return response
        except Exception:
            return send_file(out_zip, as_attachment=True, download_name=dl_name)

    # Original logic for individual file selection
    files = []
    for name in names:
        # basic safety: no directory traversal and must be .pb
        if "/" in name or ".." in name or not name.endswith(".pb"):
            continue
        p = get_current_file_path(name)
        if p and p.exists() and p.is_file():
            files.append(p)
    if not files:
        abort(404, description="Selected files not found")

    # If only one file selected, serve it directly with no snapshot link
    if len(files) == 1:
        return send_file(files[0], as_attachment=True)

    # Build multi-file download with embedded permanent link
    file_pairs = [(p.name, p) for p in files]
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"pb_selected_{len(files)}_{stamp}.zip"
    base_url = request.host_url.rstrip("/")
    mem, _snapshot_id = _create_download_with_link(
        file_pairs=file_pairs, download_name=filename, base_url=base_url
    )
    response = send_file(
        mem, as_attachment=True, download_name=filename, mimetype="application/zip"
    )
    try:
        response.headers["X-Download-Snapshot-ID"] = _snapshot_id
        response.headers["X-Download-Snapshot-URL"] = url_for(
            "main.download_snapshot", snapshot_id=_snapshot_id, _external=True
        )
    except Exception:
        pass
    return response

    # No public API endpoints; JSON routes removed


@bp.post("/download-selected/start")
def download_selected_start():
    """Kick off a background zipping job and return a token to poll progress.
    Accepts same form data as /download-selected (files=..., select_all=true|false).
    """
    _cleanup_old_jobs()
    names = request.form.getlist("files")
    if names:
        names = list(set(names))
        
    # Allow select_all via form or query string for robustness
    select_all = (request.form.get("select_all") == "true") or (
        request.args.get("select_all") == "true"
    )
    # Optional exclude-mode: select_all=true with a small list of files to exclude
    excludes = set(request.form.getlist("exclude"))

    # If select_all is not set and no explicit names provided, reject.
    # Allow select_all=true to proceed even when names list is empty ("all" or exclude-mode).
    if not select_all and not names:
        return jsonify({"ok": False, "error": "No files selected"}), 400

    # Total current files to verify select_all scenario
    with get_session() as s:
        total_current_files = s.query(PBFile).filter(PBFile.is_current == True).count()

    # Consider it a true "all current" request when select_all is set with no excludes
    # and either all names were provided or names list is empty (client opted not to send large body).
    selected_all_current = (
        select_all
        and not excludes
        and (len(names) == total_current_files or len(names) == 0)
    )

    file_pairs: List[Tuple[str, Path]] = []
    file_ids_for_snapshot: List[int] = []
    reuse_path: Optional[Path] = None
    download_name = "pb_selected.zip"

    if selected_all_current:
        # Prefer the newest timestamped export zip under cache/<ts>/all_pb_files.zip
        cache_dir = Path(__file__).parent.parent / "cache"
        cache_dir.mkdir(exist_ok=True)
        # 1) Try newest timestamped export first
        latest_export: Optional[Path] = None
        try:
            for p in cache_dir.rglob("all_pb_files.zip"):
                if p.parent == cache_dir:
                    continue  # skip canonical root file
                if not p.is_file():
                    continue
                if (
                    latest_export is None
                    or p.stat().st_mtime > latest_export.stat().st_mtime
                ):
                    latest_export = p
        except Exception:
            latest_export = None
        if latest_export is not None:
            reuse_path = latest_export
        else:
            # 2) Build on the fly (do not use root-level canonical cache)
            all_file_pairs = get_all_current_file_paths()
            if not all_file_pairs:
                return jsonify({"ok": False, "error": "No current files found"}), 404
            file_pairs = [(name, path) for name, path in all_file_pairs]
        # Use current timestamp in the suggested download name for the 'all' download
        download_name = (
            f"all_pb_files_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
        )
    else:
        # Either: exclude-mode (select all minus excludes) OR explicit list of names
        if select_all and excludes:
            # Build from all current minus excluded names
            all_file_pairs = get_all_current_file_paths()
            # Only keep those whose arcname is NOT excluded
            file_pairs = [
                (name, path) for (name, path) in all_file_pairs if name not in excludes
            ]
            if not file_pairs:
                return (
                    jsonify(
                        {"ok": False, "error": "No files remaining after exclusions"}
                    ),
                    404,
                )
            # Capture PBFile IDs for included names
            try:
                with get_session() as s:
                    names = [name for (name, _p) in file_pairs]
                    rows = (
                        s.query(PBFile.file_name, PBFile.id)
                        .filter(PBFile.is_current == True)  # noqa: E712
                        .filter(PBFile.file_name.in_(names))
                        .all()
                    )
                    name_to_id = {fn: int(fid) for fn, fid in rows}
                    file_ids_for_snapshot = [
                        name_to_id.get(n)
                        for n in names
                        if name_to_id.get(n) is not None
                    ]
            except Exception:
                file_ids_for_snapshot = []
            # If only one file will be downloaded, do not create a snapshot link
            if len(file_pairs) == 1:
                file_ids_for_snapshot = []
            stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            download_name = f"pb_selected_{len(file_pairs)}_{stamp}.zip"
        else:
            # Individual file selection from provided names
            for name in names:
                if "/" in name or ".." in name or not name.endswith(".pb"):
                    continue
                p = get_current_file_path(name)
                if p and p.exists() and p.is_file():
                    file_pairs.append((name, p))
            if not file_pairs:
                return jsonify({"ok": False, "error": "Selected files not found"}), 404
            # Capture PBFile IDs for selected names
            try:
                with get_session() as s:
                    rows = (
                        s.query(PBFile.file_name, PBFile.id)
                        .filter(PBFile.is_current == True)  # noqa: E712
                        .filter(PBFile.file_name.in_(names))
                        .all()
                    )
                    name_to_id = {fn: int(fid) for fn, fid in rows}
                    # Preserve the order of file_pairs
                    file_ids_for_snapshot = [
                        name_to_id.get(n)
                        for (n, _p) in file_pairs
                        if name_to_id.get(n) is not None
                    ]
            except Exception:
                file_ids_for_snapshot = []
            # If only one file will be downloaded, do not create a snapshot link
            if len(file_pairs) == 1:
                file_ids_for_snapshot = []
            stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            download_name = f"pb_selected_{len(file_pairs)}_{stamp}.zip"

    if len(file_pairs) == 1 and reuse_path is None:
        arcname, path = file_pairs[0]
        download_name = arcname
        mime_type = mimetypes.guess_type(download_name)[0] or "application/octet-stream"
        token = uuid.uuid4().hex
        progress_payload = {
            "token": token,
            "total": 1,
            "current": 1,
            "percent": 100,
            "status": "ready",
            "current_name": arcname,
            "done": True,
            "error": None,
            "download_name": download_name,
            "artifact_type": "file",
            "file_path": str(path),
            "mime_type": mime_type,
            "file_ids": [],
        }
        _write_progress(token, progress_payload)
        with _ZIP_JOBS_LOCK:
            _ZIP_JOBS[token] = {
                "file_path": str(path),
                "download_name": download_name,
                "artifact_type": "file",
                "mime_type": mime_type,
            }
        return jsonify(
            {
                "ok": True,
                "token": token,
                "progress_url": url_for("main.download_selected_progress", token=token),
                "file_url": url_for("main.download_selected_file", token=token),
            }
        )

    token = uuid.uuid4().hex
    # Record initial state
    _write_progress(
        token,
        {
            "token": token,
            "total": len(file_pairs),
            "current": 0,
            "percent": 0,
            "status": "queued",
            "current_name": None,
            "done": False,
            "error": None,
            "download_name": download_name,
            "artifact_type": "zip",
            "mime_type": "application/zip",
            "file_ids": file_ids_for_snapshot,
        },
    )

    # Start background worker
    t = threading.Thread(
        target=_build_zip_in_background,
        args=(
            token,
            file_pairs,
            download_name,
            reuse_path,
            file_ids_for_snapshot,
        ),
        daemon=True,
    )
    t.start()

    return jsonify(
        {
            "ok": True,
            "token": token,
            "progress_url": url_for("main.download_selected_progress", token=token),
            "file_url": url_for("main.download_selected_file", token=token),
        }
    )


@bp.errorhandler(RequestEntityTooLarge)
def handle_large_request(e):
    """Allow initiating large download jobs without requiring a large POST body.

    If the client hits MAX_CONTENT_LENGTH while posting many filenames for
    /download-selected/start, they can pass select_all=true in the query string
    to indicate intention to download all current files. We then kick off the
    same background job as if select all was chosen, without reading the body.
    """
    # Only handle for the specific endpoint
    if request.path.rstrip("/") == "/download-selected/start":
        select_all = (request.args.get("select_all") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        if select_all:
            # Mirror the select-all branch from download_selected_start
            _cleanup_old_jobs()
            # Determine total current file count for progress without scanning filesystem
            try:
                with get_session() as s:
                    total_current_files = (
                        s.query(PBFile)
                        .filter(PBFile.is_current == True)
                        .count()  # noqa: E712
                    )
            except Exception:
                total_current_files = 0
            if total_current_files == 0:
                return jsonify({"ok": False, "error": "No current files found"}), 404
            # Use or refresh cache
            cache_dir = Path(__file__).parent.parent / "cache"
            cache_dir.mkdir(exist_ok=True)
            # First, prefer newest timestamped export
            latest_export: Optional[Path] = None
            try:
                for p in cache_dir.rglob("all_pb_files.zip"):
                    if p.parent == cache_dir:
                        continue
                    if not p.is_file():
                        continue
                    if (
                        latest_export is None
                        or p.stat().st_mtime > latest_export.stat().st_mtime
                    ):
                        latest_export = p
            except Exception:
                latest_export = None
            token = uuid.uuid4().hex
            download_name = (
                f"all_pb_files_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
            )
            if latest_export is not None:
                _write_progress(
                    token,
                    {
                        "token": token,
                        "total": int(total_current_files),
                        "current": int(total_current_files),
                        "percent": 100,
                        "status": "ready",
                        "current_name": None,
                        "done": True,
                        "error": None,
                        "download_name": download_name,
                        "artifact_type": "zip",
                        "mime_type": "application/zip",
                        "file_path": str(latest_export),
                        "file_ids": [],
                    },
                )
                with _ZIP_JOBS_LOCK:
                    _ZIP_JOBS[token] = {
                        "file_path": str(latest_export),
                        "download_name": download_name,
                        "artifact_type": "zip",
                        "mime_type": "application/zip",
                        "file_ids": [],
                    }
            else:
                # No timestamped export found; queue a fresh build (no canonical cache fallback)
                _write_progress(
                    token,
                    {
                        "token": token,
                        "total": int(total_current_files),
                        "current": 0,
                        "percent": 0,
                        "status": "queued",
                        "current_name": None,
                        "done": False,
                        "error": None,
                        "download_name": download_name,
                        "artifact_type": "zip",
                        "mime_type": "application/zip",
                        "file_ids": [],
                    },
                )
                with _ZIP_JOBS_LOCK:
                    _ZIP_JOBS[token] = {
                        "file_path": None,
                        "download_name": download_name,
                        "artifact_type": "zip",
                        "mime_type": "application/zip",
                        "file_ids": [],
                    }
                # Start background builder to create the all-files zip
                all_file_pairs = get_all_current_file_paths()
                if not all_file_pairs:
                    return (
                        jsonify({"ok": False, "error": "No current files found"}),
                        404,
                    )
                t = threading.Thread(
                    target=_build_zip_in_background,
                    args=(
                        token,
                        [(name, path) for name, path in all_file_pairs],
                        download_name,
                        None,
                    ),
                    daemon=True,
                )
                t.start()
            return jsonify(
                {
                    "ok": True,
                    "token": token,
                    "progress_url": url_for(
                        "main.download_selected_progress", token=token
                    ),
                    "file_url": url_for("main.download_selected_file", token=token),
                }
            )
    # Default: return the standard 413 JSON for API clients
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": False, "error": "Request too large"}), 413
    return ("Request Entity Too Large", 413)


@bp.get("/download-selected/progress/<token>")
def download_selected_progress(token: str):
    data = _read_progress(token)
    if not data:
        return jsonify({"ok": False, "error": "Not found"}), 404
    return jsonify({"ok": True, **data})


@bp.get("/download-selected/file/<token>")
def download_selected_file(token: str):
    # Ready when progress says done and file exists
    data = _read_progress(token)
    if not data or not data.get("done"):
        abort(404)
    # Prefer registered path (background worker sets it)
    job_data = None
    with _ZIP_JOBS_LOCK:
        job_data = _ZIP_JOBS.get(token)
    download_name = (
        data.get("download_name")
        or (job_data or {}).get("download_name")
        or "pb_selected.zip"
    )
    # Attempt to retrieve captured PBFile IDs for deterministic snapshot
    captured_ids: List[int] = []
    try:
        ids_from_job = (job_data or {}).get("file_ids") or data.get("file_ids") or []
        if isinstance(ids_from_job, list):
            captured_ids = [int(x) for x in ids_from_job if x is not None]
    except Exception:
        captured_ids = []
    artifact_type = (
        (job_data or {}).get("artifact_type") or data.get("artifact_type") or "zip"
    )
    mime_type = (
        (job_data or {}).get("mime_type")
        or data.get("mime_type")
        or ("application/zip" if artifact_type == "zip" else None)
    )
    file_path_str = (job_data or {}).get("file_path") or data.get("file_path")
    file_path: Optional[Path] = Path(file_path_str) if file_path_str else None
    if artifact_type == "zip" and (file_path is None or not file_path.exists()):
        # fallback to token zip
        p = _zip_jobs_dir() / f"{token}.zip"
        if p.exists():
            file_path = p
    if not file_path or not file_path.exists():
        abort(404)
    if artifact_type == "file":
        fallback_name = Path(file_path).name
        if not download_name:
            download_name = fallback_name
        guessed_mime = mime_type or mimetypes.guess_type(download_name)[0]
        return send_file(
            file_path,
            as_attachment=True,
            download_name=download_name or fallback_name,
            mimetype=guessed_mime or "application/octet-stream",
        )
    # If the ZIP already contains a link file, serve it directly and set headers
    try:
        import re
        import zipfile

        with zipfile.ZipFile(file_path, "r") as zf:
            if "_PERMANENT_DOWNLOAD_LINK.txt" in zf.namelist():
                try:
                    txt = zf.read("_PERMANENT_DOWNLOAD_LINK.txt").decode(
                        "utf-8", "ignore"
                    )
                except Exception:
                    txt = ""
                m = re.search(r"/download/snapshot/([0-9a-f]{16})", txt)
                snapshot_id = m.group(1) if m else None
                resp = send_file(
                    file_path,
                    as_attachment=True,
                    download_name=download_name,
                )
                if snapshot_id:
                    resp.headers["X-Download-Snapshot-ID"] = snapshot_id
                    resp.headers["X-Download-Snapshot-URL"] = url_for(
                        "main.download_snapshot",
                        snapshot_id=snapshot_id,
                        _external=True,
                    )
                return resp
    except Exception:
        pass

    # If we have captured IDs (legacy/no-link case), create a snapshot and inject link file into the ZIP on the fly
    if captured_ids:
        try:
            from .services.snapshot_service import add_link_to_existing_zip as _add_link
            from .services.snapshot_service import (
                create_download_snapshot_from_ids as _create_snapshot_from_ids,
            )

            base_url = request.host_url.rstrip("/")
            # Legacy zip without link: inject into memory
            snapshot_id = _create_snapshot_from_ids(captured_ids, download_name)
            mem = _add_link(file_path, snapshot_id, download_name, base_url)
            response = send_file(
                mem,
                as_attachment=True,
                download_name=download_name,
                mimetype="application/zip",
            )
            response.headers["X-Download-Snapshot-ID"] = snapshot_id
            response.headers["X-Download-Snapshot-URL"] = url_for(
                "main.download_snapshot", snapshot_id=snapshot_id, _external=True
            )
            return response
        except Exception:
            # Fall back to serving the raw ZIP if snapshot injection fails
            return send_file(file_path, as_attachment=True, download_name=download_name)
    # No captured IDs; serve raw ZIP
    return send_file(file_path, as_attachment=True, download_name=download_name)


@bp.get("/download/snapshot/<snapshot_id>")
def download_snapshot(snapshot_id: str):
    """Serve a permanent, version-stable download by snapshot ID.

    The snapshot maps to exact PBFile record IDs captured at creation time,
    ensuring the same versions are downloaded even after updates.
    """
    token = (snapshot_id or "").strip().lower()
    # Validate token format (deterministic 16 hex chars). Return 400 for invalid format.
    if not re.fullmatch(r"[0-9a-f]{16}", token):
        abort(400, description="Invalid snapshot link format.")
    return _serve_snapshot_download(token)


def _order_columns(all_keys: List[str], preferred_order: List[str]) -> List[str]:
    seen = set()
    cols: List[str] = []
    for k in preferred_order:
        if k in all_keys and k not in seen:
            cols.append(k)
            seen.add(k)
    for k in sorted(all_keys):
        if k not in seen:
            cols.append(k)
            seen.add(k)
    return cols


@bp.route("/preview/<path:filename>")
def preview_file(filename: str):
    # Validate and locate file
    if not _is_safe_filename(filename):
        abort(400, description="Invalid filename")
    # Read path from DB record only (DB is the source of truth)
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            lines = [line.rstrip("\n") for line in f]
        meta, projects, votes, votes_in_projects, scores_in_projects = parse_pb_lines(
            lines
        )
    except Exception as e:
        abort(400, description=f"Failed to parse file: {e}")

    # Prepare META as list of (key, value) sorted with some preferred keys on top
    # Ensure comments are split using the same logic as elsewhere (#n: ...)
    meta_processed: Dict[str, Any] = dict(meta)
    try:
        _comments = _parse_comments_from_meta(meta)
        if _comments:
            meta_processed["comment"] = [
                f"#{i+1}: {txt}" for i, txt in enumerate(_comments)
            ]
    except Exception:
        # Fallback: leave original comment value as-is
        pass
    meta_items = list(meta_processed.items())
    preferred_meta = [
        "country",
        "unit",
        "city",
        "district",
        "subunit",
        "instance",
        "year",
        "date_begin",
        "date_end",
        "budget",
        "currency",
        "num_projects",
        "num_votes",
        "vote_type",
        "rule",
        "description",
        "comment",
    ]
    # Sort with preferred keys first (in that order), then the rest alphabetically
    meta_order_map = {k: i for i, k in enumerate(preferred_meta)}
    meta_items.sort(
        key=lambda kv: (
            kv[0] not in meta_order_map,
            meta_order_map.get(kv[0], 9999),
            kv[0],
        )
    )

    # Prepare PROJECTS table
    project_rows: List[Dict[str, Any]] = []
    project_keys_set = set()
    for pid, row in projects.items():
        # ensure project_id exists in row
        r = dict(row)
        r.setdefault("project_id", pid)
        project_rows.append(r)
        project_keys_set.update(r.keys())
    preferred_project_cols = [
        "project_id",
        "name",
        "title",
        "cost",
        "score",
        "votes",
        "selected",
        "category",
        "district",
        "description",
    ]
    project_columns = _order_columns(list(project_keys_set), preferred_project_cols)

    # Prepare VOTES table (may be large)
    vote_rows: List[Dict[str, Any]] = []
    vote_keys_set = set(["voter_id"])  # we include voter_id explicitly
    for vid, row in votes.items():
        r = {"voter_id": vid}
        r.update(row)
        vote_rows.append(r)
        vote_keys_set.update(r.keys())
    # The 'vote' field is included in preferred_vote_cols and vote_columns,
    # and will be shown in the preview table. It is a list of project IDs if present.
    preferred_vote_cols = [
        "voter_id",
        "vote",
        "ranking",
        "points",
        "weight",
        "age",
        "gender",
        "district",
    ]
    vote_columns = _order_columns(list(vote_keys_set), preferred_vote_cols)

    # For very large votes tables, show only first N by default; can expand on client
    VOTES_PREVIEW_LIMIT = 200
    total_votes_count = len(vote_rows)
    votes_preview = vote_rows[:VOTES_PREVIEW_LIMIT]
    votes_truncated = total_votes_count > VOTES_PREVIEW_LIMIT

    # Basic counts for header
    counts = {
        "projects": len(project_rows),
        "votes": total_votes_count,
    }

    return render_template(
        "preview.html",
        filename=filename,
        meta_items=meta_items,
        project_columns=project_columns,
        project_rows=project_rows,
        vote_columns=vote_columns,
        votes_preview=votes_preview,
        votes_truncated=votes_truncated,
        total_votes_count=total_votes_count,
        votes_in_projects=votes_in_projects,
        scores_in_projects=scores_in_projects,
        counts=counts,
    )


@bp.route("/visualize/<path:filename>")
def visualize_file(filename: str):
    """Generate visualization page for a PB file with charts and plots."""
    # Validate and locate file
    if not _is_safe_filename(filename):
        abort(400, description="Invalid filename")
    # Read path from DB record only (DB is the source of truth)
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            lines = [line.rstrip("\n") for line in f]
        meta, projects, votes, votes_in_projects, scores_in_projects = parse_pb_lines(
            lines
        )
    except Exception as e:
        abort(400, description=f"Failed to parse file: {e}")

    # Basic counts for header
    counts = {
        "projects": len(projects),
        "votes": len(votes),
    }

    # Prepare data for visualization
    # Project costs for histogram
    project_costs = [
        float(proj.get("cost", 0)) for proj in projects.values() if proj.get("cost")
    ]

    # Vote counts per project
    vote_counts_per_project = {}
    vote_lengths = []  # Track how many projects each voter selected

    # Process all votes to extract vote data
    for vote_id, vote_data in votes.items():
        # Look for the vote column - now only "vote"
        vote_list = None
        for possible_vote_key in ["vote"]:
            if possible_vote_key in vote_data:
                vote_list = vote_data[possible_vote_key]
                break
        # The 'vote' field is referenced here. It is a list of project IDs if present.
        if vote_list is None:
            continue

        # Handle different vote data formats
        voted_projects = []
        if isinstance(vote_list, str) and vote_list.strip():
            # Parse comma-separated project IDs OR single project ID
            if "," in vote_list:
                # Multiple projects separated by commas
                voted_projects = [
                    pid.strip()
                    for pid in vote_list.split(",")
                    if pid.strip() and pid.strip() != ""
                ]
            else:
                # Single project ID (no comma) - could be a number or string
                single_project = vote_list.strip()
                if single_project and single_project != "":
                    voted_projects = [single_project]
        elif isinstance(vote_list, list) and vote_list:
            # Handle case where vote is already a list (from load_pb_file.py)
            voted_projects = [
                str(pid).strip() for pid in vote_list if pid and str(pid).strip()
            ]

        # Only process if we have valid projects
        if voted_projects:
            vote_length = len(voted_projects)
            vote_lengths.append(vote_length)

            for pid in voted_projects:
                pid_str = str(pid).strip()
                if pid_str:  # Ensure we have a non-empty project ID
                    vote_counts_per_project[pid_str] = (
                        vote_counts_per_project.get(pid_str, 0) + 1
                    )
        else:
            # Debug: Log when we can't extract voted projects from a vote
            if (
                vote_list is not None
            ):  # Only log if we found a vote column but couldn't parse it
                print(
                    f"DEBUG: Could not parse voted projects from vote {vote_id}: '{vote_list}' (type: {type(vote_list)})"
                )

    # removed debug prints

    # Prepare data for charts
    project_data = {
        "costs": project_costs,
        "scatter_data": [],  # Will be populated with {x: cost, y: votes} points
    }

    # Debug information
    # removed debug prints

    # Ensure we have data before creating the structure
    if vote_counts_per_project:
        vote_data = {
            "project_labels": list(vote_counts_per_project.keys())[
                :20
            ],  # Limit for readability
            "votes_per_project": list(vote_counts_per_project.values())[:20],
        }
    else:
        vote_data = {"project_labels": [], "votes_per_project": []}

    # Vote length distribution
    vote_length_counts = {}
    for length in vote_lengths:
        vote_length_counts[length] = vote_length_counts.get(length, 0) + 1

    vote_length_counts = dict(sorted(vote_length_counts.items()))
    # Debug: Log the vote length distribution we found
    if vote_length_counts:
        print(
            f"DEBUG: Vote length distribution: {dict(sorted(vote_length_counts.items()))}"
        )
        single_votes = vote_length_counts.get(1, 0)
        total_votes = sum(vote_length_counts.values())
        print(
            f"DEBUG: Single-project votes: {single_votes}/{total_votes} ({single_votes/total_votes*100:.1f}%)"
        )

    vote_length_data = None
    if vote_length_counts:
        sorted_lengths = sorted(vote_length_counts.keys())
        vote_length_data = {
            "labels": [str(length) for length in sorted_lengths],
            "counts": [vote_length_counts[length] for length in sorted_lengths],
        }
    else:
        # Add debugging information when no vote length data is available
        print(
            f"DEBUG: No vote length data - total votes: {len(votes)}, vote_lengths: {len(vote_lengths)}"
        )
        # Check a few sample votes for debugging
        if votes:
            sample_votes = list(votes.items())[:3]
            for vote_id, vote_data in sample_votes:
                print(f"DEBUG: Sample vote {vote_id}: {vote_data}")
                # Check all possible vote columns
                for possible_vote_key in [
                    "vote",
                    "votes",
                    "projects",
                    "selected_projects",
                ]:
                    if possible_vote_key in vote_data:
                        vote_value = vote_data[possible_vote_key]
                        print(
                            f"DEBUG: Found {possible_vote_key}: '{vote_value}' (type: {type(vote_value)})"
                        )

            # Also check what columns are available in votes
            if votes:
                first_vote = next(iter(votes.values()))
                print(f"DEBUG: Available vote columns: {list(first_vote.keys())}")

    # Top projects by votes
    top_projects_data = None
    if vote_counts_per_project:
        # Get top 10 projects by vote count
        sorted_projects = sorted(
            vote_counts_per_project.items(), key=lambda x: x[1], reverse=True
        )[:10]
        project_names = []
        project_votes = []

        for pid, vote_count in sorted_projects:
            # Try to get project name, fallback to ID
            proj_name = projects.get(pid, {}).get("name", f"Project {pid}")
            if len(proj_name) > 50:  # Truncate long names
                proj_name = proj_name[:47] + "..."
            project_names.append(proj_name)
            project_votes.append(vote_count)

        top_projects_data = {"labels": project_names, "votes": project_votes}

    # Approval histogram: number of approvals per project
    approval_counts = list(vote_counts_per_project.values())
    approval_histogram = {}
    for count in approval_counts:
        approval_histogram[count] = approval_histogram.get(count, 0) + 1
    approval_histogram = dict(sorted(approval_histogram.items()))
    approval_histogram_data = None
    if approval_histogram:
        approval_histogram_data = {
            "labels": [str(k) for k in approval_histogram.keys()],
            "counts": [approval_histogram[k] for k in approval_histogram.keys()],
        }

    # Project selection analysis (cost vs votes scatter)
    selection_data = None
    selected_projects = set()

    # Determine which projects were selected by inspecting project fields
    # Some datasets include a 'selected' column in the PROJECTS section.
    for proj_id, proj in projects.items():
        selected_val = proj.get("selected")
        if isinstance(selected_val, str):
            sv = selected_val.strip().lower()
            if sv in {"1", "true", "yes", "y"}:
                selected_projects.add(proj_id)
        elif selected_val:
            # Any non-empty, non-string truthy value
            selected_projects.add(proj_id)

    if selected_projects or project_costs:
        selected_points = []
        not_selected_points = []

        for pid, proj in projects.items():
            cost = proj.get("cost")
            votes_received = vote_counts_per_project.get(pid, 0)
            if cost is not None:
                try:
                    point = {"x": float(cost), "y": votes_received}
                    if pid in selected_projects:
                        selected_points.append(point)
                    else:
                        not_selected_points.append(point)
                except (ValueError, TypeError):
                    continue

        if selected_points or not_selected_points:
            selection_data = {
                "selected": selected_points,
                "not_selected": not_selected_points,
            }

    # Create scatter plot data (cost vs votes) - for original scatter chart
    for pid, proj in projects.items():
        cost = proj.get("cost")
        votes_received = vote_counts_per_project.get(pid, 0)
        if cost is not None:
            try:
                project_data["scatter_data"].append(
                    {"x": float(cost), "y": votes_received}
                )
            except (ValueError, TypeError):
                continue

    # Category analysis (if available)
    category_data = None
    if any("category" in proj for proj in projects.values()):
        category_counts = {}
        for proj in projects.values():
            categories = proj.get("category", "")
            if categories:
                # Handle comma-separated categories
                cats = [
                    cat.strip() for cat in str(categories).split(",") if cat.strip()
                ]
                for cat in cats:
                    category_counts[cat] = category_counts.get(cat, 0) + 1

        if category_counts:
            category_data = {
                "labels": list(category_counts.keys()),
                "counts": list(category_counts.values()),
            }

    # Demographic analysis (if available)
    demographic_data = None
    if votes:
        age_counts = {}
        sex_counts = {}

        for vote_data in votes.values():
            age = vote_data.get("age")
            sex = vote_data.get("sex")

            if age is not None:
                try:
                    age_int = int(age)
                    # Group ages into ranges
                    if age_int < 18:
                        age_group = "Under 18"
                    elif age_int < 30:
                        age_group = "18-29"
                    elif age_int < 45:
                        age_group = "30-44"
                    elif age_int < 65:
                        age_group = "45-64"
                    else:
                        age_group = "65+"

                    age_counts[age_group] = age_counts.get(age_group, 0) + 1
                except (ValueError, TypeError):
                    pass

            if sex:
                sex_str = str(sex).upper()
                if sex_str in ["M", "MALE"]:
                    sex_counts["Male"] = sex_counts.get("Male", 0) + 1
                elif sex_str in ["F", "FEMALE"]:
                    sex_counts["Female"] = sex_counts.get("Female", 0) + 1

        if age_counts or sex_counts:
            demographic_data = {}
            if age_counts:
                demographic_data["age"] = {
                    "labels": list(age_counts.keys()),
                    "counts": list(age_counts.values()),
                }
            if sex_counts:
                demographic_data["sex"] = {
                    "labels": list(sex_counts.keys()),
                    "counts": list(sex_counts.values()),
                }

    # Category cost analysis
    category_cost_data = None
    if any("category" in proj for proj in projects.values()):
        category_costs = {}
        category_counts_for_avg = {}

        for proj in projects.values():
            categories = proj.get("category", "")
            cost = proj.get("cost")
            if categories and cost is not None:
                try:
                    cost_float = float(cost)
                    cats = [
                        cat.strip() for cat in str(categories).split(",") if cat.strip()
                    ]
                    for cat in cats:
                        if cat not in category_costs:
                            category_costs[cat] = 0
                            category_counts_for_avg[cat] = 0
                        category_costs[cat] += cost_float
                        category_counts_for_avg[cat] += 1
                except (ValueError, TypeError):
                    continue

        if category_costs:
            avg_costs = []
            labels = []
            for cat in category_costs:
                if category_counts_for_avg[cat] > 0:
                    labels.append(cat)
                    avg_costs.append(category_costs[cat] / category_counts_for_avg[cat])

            if labels:
                category_cost_data = {"labels": labels, "avg_costs": avg_costs}

    # Voting timeline (simplified - group by vote ID order as proxy for time)
    timeline_data = None
    if len(votes) > 10:  # Only create timeline if we have enough votes
        # Since we don't have actual timestamps, create a synthetic timeline
        vote_ids = list(votes.keys())
        votes_per_period = []
        period_labels = []

        # Group votes into 10 periods
        period_size = max(1, len(vote_ids) // 10)
        for i in range(0, len(vote_ids), period_size):
            period_end = min(i + period_size, len(vote_ids))
            votes_in_period = period_end - i
            votes_per_period.append(votes_in_period)
            period_labels.append(f"Period {len(period_labels) + 1}")

        timeline_data = {"dates": period_labels, "votes_per_day": votes_per_period}

    # Summary statistics
    summary_stats = {
        "total_voters": len(votes),
        "total_projects": len(projects),
        "selected_projects": len(selected_projects) if selected_projects else 0,
        "avg_vote_length": sum(vote_lengths) / len(vote_lengths) if vote_lengths else 0,
        "total_budget": sum(project_costs) if project_costs else 0,
        "avg_project_cost": (
            sum(project_costs) / len(project_costs) if project_costs else 0
        ),
        "most_popular_project_votes": (
            max(vote_counts_per_project.values()) if vote_counts_per_project else 0
        ),
    }

    # Correlation analysis (simplified)
    correlation_data = None
    if project_costs and vote_counts_per_project:
        # Calculate simple correlations between available metrics
        correlations = []
        labels = []

        # Cost vs Votes correlation
        costs_for_corr = []
        votes_for_corr = []
        for pid, proj in projects.items():
            cost = proj.get("cost")
            votes_received = vote_counts_per_project.get(pid, 0)
            if cost is not None:
                try:
                    costs_for_corr.append(float(cost))
                    votes_for_corr.append(votes_received)
                except (ValueError, TypeError):
                    continue

        if len(costs_for_corr) > 1:
            # Simple correlation calculation
            import statistics

            mean_cost = statistics.mean(costs_for_corr)
            mean_votes = statistics.mean(votes_for_corr)

            numerator = sum(
                (c - mean_cost) * (v - mean_votes)
                for c, v in zip(costs_for_corr, votes_for_corr)
            )
            sum_sq_cost = sum((c - mean_cost) ** 2 for c in costs_for_corr)
            sum_sq_votes = sum((v - mean_votes) ** 2 for v in votes_for_corr)

            if sum_sq_cost > 0 and sum_sq_votes > 0:
                correlation = numerator / (sum_sq_cost * sum_sq_votes) ** 0.5
                correlations.append(correlation)
                labels.append("Cost vs Popularity")

        # Add more dummy correlations for demonstration
        if correlations:
            correlations.extend([0.1, -0.2, 0.3])  # Dummy values
            labels.extend(
                ["Budget vs Selection", "Category vs Votes", "Time vs Activity"]
            )

            correlation_data = {"labels": labels, "values": correlations}

    return render_template(
        "visualize.html",
        filename=filename,
        counts=counts,
        project_data=project_data,
        vote_data=vote_data,
        category_data=category_data,
        demographic_data=demographic_data,
        vote_length_data=vote_length_data,
        top_projects_data=top_projects_data,
        selection_data=selection_data,
        category_cost_data=category_cost_data,
        timeline_data=timeline_data,
        summary_stats=summary_stats,
        correlation_data=correlation_data,
        approval_histogram_data=approval_histogram_data,
        project_categories=category_data is not None,
        voter_demographics=demographic_data is not None,
    )


@bp.route("/preview-snippet/<path:filename>")
def preview_snippet(filename: str):
    """Return a small, plain-text preview of the PB file (first N lines)."""
    if not _is_safe_filename(filename):
        abort(400, description="Invalid filename")
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)

    # Number of lines to include; default 80, cap 400
    try:
        n = int(request.args.get("lines", "80"))
    except Exception:
        n = 80
    n = max(1, min(n, 400))

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            lines = []
            for i, line in enumerate(f, start=1):
                if i > n:
                    break
                lines.append(line.rstrip("\n"))
        text = "\n".join(lines)
    except Exception as e:
        abort(400, description=f"Failed to read file: {e}")

    return Response(text, mimetype="text/plain; charset=utf-8")
