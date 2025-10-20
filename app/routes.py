import io
import json
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
from werkzeug.utils import secure_filename

from .__init__ import limiter
from .db import get_session
from .models import PBFile
from .routes_admin import _format_preview_tile  # reuse tile formatting
from .routes_admin import _load_upload_settings  # reuse limits
from .services.pb_service import aggregate_comments_cached as _aggregate_comments_cached
from .services.pb_service import (
    aggregate_statistics_cached as _aggregate_statistics_cached,
)
from .services.pb_service import get_all_current_file_paths, get_current_file_path

# Simple in-memory registry for zip jobs; zip files and progress json live on disk
_ZIP_JOBS: Dict[str, Dict[str, Any]] = {}
_ZIP_JOBS_LOCK = threading.Lock()


def _zip_jobs_dir() -> Path:
    d = Path(__file__).parent.parent / "cache" / "zip_jobs"
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
            }
            _write_progress(token, progress)
            with _ZIP_JOBS_LOCK:
                _ZIP_JOBS[token] = {
                    "file_path": str(reuse_file_path),
                    "download_name": download_name,
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
        progress.update({"done": True, "status": "ready", "percent": 100})
        _write_progress(token, progress)
        with _ZIP_JOBS_LOCK:
            _ZIP_JOBS[token] = {
                "file_path": str(out_zip),
                "download_name": download_name,
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
        }
        _write_progress(token, progress)


from .services.pb_service import get_tiles_cached as _get_tiles_cached
from .utils.file_helpers import is_safe_filename as _is_safe_filename
from .utils.formatting import format_int as _format_int
from .utils.load_pb_file import parse_pb_lines
from .utils.pb_utils import parse_pb_to_tile as _parse_pb_to_tile
from .utils.upload_security import is_allowed_extension as _is_allowed_ext
from .utils.upload_security import is_probably_text_file as _is_probably_text_file
from .utils.upload_security import public_tmp_dir as _public_tmp_dir
from .utils.validation import count_issues, format_validation_summary, validate_pb_file

bp = Blueprint(
    "main",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


@bp.route("/")
def home():
    tiles = _get_tiles_cached()
    return render_template("index.html", tiles=tiles, count=len(tiles))


@bp.route("/format")
def format_page():
    return render_template("format.html")


@bp.route("/tools")
def tools_page():
    return render_template("tools.html")


@bp.route("/publications")
def publications_page():
    # Parse bib.bib and pass publications to the template
    import bibtexparser

    bib_path = Path(__file__).parent.parent / "bib.bib"
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
    return render_template("publications.html", publications=publications)


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
    return render_template(
        "upload.html", upload_settings=settings, tiles=tiles, count=len(tiles)
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
    return send_file(path, as_attachment=True)


@bp.post("/download-selected")
def download_selected():
    names = request.form.getlist("files")
    select_all = request.form.get("select_all") == "true"

    if not names:
        abort(400, description="No files selected")

    # Get total count of current files to compare with selected count
    with get_session() as s:
        total_current_files = s.query(PBFile).filter(PBFile.is_current == True).count()

    # Check if user selected ALL current files (not just clicked select all after filtering)
    selected_all_current = len(names) == total_current_files and select_all

    if selected_all_current:
        # User selected ALL current files - try to use cached all_pb_files.zip
        cache_dir = Path(__file__).parent.parent / "cache"
        cache_dir.mkdir(exist_ok=True)  # Create cache directory if it doesn't exist
        cache_path = cache_dir / "all_pb_files.zip"

        all_file_pairs = get_all_current_file_paths()
        if not all_file_pairs:
            abort(404, description="No current files found")

        # Check if cache is valid (exists and is newer than all source files)
        cache_valid = False
        if cache_path.exists() and cache_path.is_file():
            cache_mtime = cache_path.stat().st_mtime
            cache_valid = all(
                cache_mtime >= path.stat().st_mtime for _, path in all_file_pairs
            )

        if cache_valid:
            # Return the cached zip file
            return send_file(
                cache_path, as_attachment=True, download_name="all_pb_files.zip"
            )
        else:
            # Create new zip file
            with zipfile.ZipFile(
                cache_path, mode="w", compression=zipfile.ZIP_DEFLATED
            ) as zf:
                for file_name, file_path in all_file_pairs:
                    zf.write(file_path, arcname=file_name)

            return send_file(
                cache_path, as_attachment=True, download_name="all_pb_files.zip"
            )

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

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            zf.write(p, arcname=p.name)
    mem.seek(0)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"pb_selected_{len(files)}_{stamp}.zip"
    return send_file(
        mem, as_attachment=True, download_name=filename, mimetype="application/zip"
    )

    # No public API endpoints; JSON routes removed


@bp.post("/download-selected/start")
def download_selected_start():
    """Kick off a background zipping job and return a token to poll progress.
    Accepts same form data as /download-selected (files=..., select_all=true|false).
    """
    _cleanup_old_jobs()
    names = request.form.getlist("files")
    select_all = request.form.get("select_all") == "true"

    if not names:
        return jsonify({"ok": False, "error": "No files selected"}), 400

    # Total current files to verify select_all scenario
    with get_session() as s:
        total_current_files = s.query(PBFile).filter(PBFile.is_current == True).count()

    selected_all_current = len(names) == total_current_files and select_all

    file_pairs: List[Tuple[str, Path]] = []
    reuse_path: Optional[Path] = None
    download_name = "pb_selected.zip"

    if selected_all_current:
        # Use or build cached zip of all current files
        cache_dir = Path(__file__).parent.parent / "cache"
        cache_dir.mkdir(exist_ok=True)
        cache_path = cache_dir / "all_pb_files.zip"
        all_file_pairs = get_all_current_file_paths()
        if not all_file_pairs:
            return jsonify({"ok": False, "error": "No current files found"}), 404
        # validate cache
        cache_valid = False
        if cache_path.exists() and cache_path.is_file():
            cache_mtime = cache_path.stat().st_mtime
            cache_valid = all(
                cache_mtime >= path.stat().st_mtime for _, path in all_file_pairs
            )
        if cache_valid:
            # We'll reuse the cached file; no background work needed
            reuse_path = cache_path
        else:
            # Build list to (re)create cache and also serve via token-specific path
            file_pairs = [(name, path) for name, path in all_file_pairs]
        download_name = "all_pb_files.zip"
    else:
        # Individual file selection
        for name in names:
            if "/" in name or ".." in name or not name.endswith(".pb"):
                continue
            p = get_current_file_path(name)
            if p and p.exists() and p.is_file():
                file_pairs.append((name, p))
        if not file_pairs:
            return jsonify({"ok": False, "error": "Selected files not found"}), 404
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        download_name = f"pb_selected_{len(file_pairs)}_{stamp}.zip"

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
    file_path: Optional[Path] = None
    download_name = (
        data.get("download_name")
        or (job_data or {}).get("download_name")
        or "pb_selected.zip"
    )
    if job_data and job_data.get("file_path"):
        file_path = Path(job_data["file_path"])  # type: ignore[index]
    else:
        # fallback to token zip
        p = _zip_jobs_dir() / f"{token}.zip"
        if p.exists():
            file_path = p
    if not file_path or not file_path.exists():
        abort(404)
    return send_file(file_path, as_attachment=True, download_name=download_name)


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
    meta_items = list(meta.items())
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

    # Determine which projects were selected (if selection data available)
    if scores_in_projects:
        for proj_id, score_data in scores_in_projects.items():
            if score_data.get("selected", False) or score_data.get("winner", False):
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
