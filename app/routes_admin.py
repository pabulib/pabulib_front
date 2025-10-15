from __future__ import annotations

import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename

from .db import get_session
from .models import AdminUser, PBFile
from .services import pb_service
from .utils.formatting import format_budget as _format_budget
from .utils.formatting import format_int as _format_int
from .utils.formatting import format_vote_length as _format_vote_length
from .utils.pb_utils import build_group_key as _build_group_key
from .utils.pb_utils import parse_pb_to_tile as _parse_pb_to_tile
from .utils.pb_utils import pb_depreciated_folder as _pb_depr_folder
from .utils.pb_utils import pb_folder as _pb_folder
from .utils.validation import count_issues, format_validation_summary, validate_pb_file

bp = Blueprint(
    "admin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


@bp.before_request
def _require_admin_login():
    # Allow login page and static files under this blueprint
    if request.endpoint in {"admin.login"}:
        return None
    # Some servers may resolve static as 'admin.static'
    if request.endpoint and request.endpoint.startswith("admin.static"):
        return None
    # Only guard /admin* routes for this blueprint
    if request.path.startswith("/admin"):
        if not session.get("admin_user_id"):
            nxt = request.url
            return redirect(url_for("admin.login", next=nxt))
    return None


@bp.route("/admin/login", methods=["GET", "POST"])
def login():
    error: Optional[str] = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if not username or not password:
            error = "Username and password are required."
        else:
            user_id: Optional[int] = None
            pwd_hash: Optional[str] = None
            with get_session() as s:
                row = (
                    s.query(AdminUser.id, AdminUser.password_hash)
                    .filter(
                        AdminUser.username == username, AdminUser.is_active == True
                    )  # noqa: E712
                    .one_or_none()
                )
                if row is not None:
                    user_id, pwd_hash = row
            if (
                not user_id
                or not pwd_hash
                or not check_password_hash(pwd_hash, password)
            ):
                error = "Invalid credentials."
            else:
                session["admin_user_id"] = int(user_id)
                # Redirect to next or dashboard
                dest = request.args.get("next") or url_for("admin.admin_dashboard")
                return redirect(dest)
    return render_template("admin/login.html", error=error)


@bp.route("/admin/logout")
def logout():
    session.pop("admin_user_id", None)
    return redirect(url_for("admin.login"))


@bp.route("/admin")
def admin_dashboard():
    # Fetch all active/current files and show their recorded filesystem mtime
    with get_session() as s:
        rows: List[PBFile] = (
            s.query(PBFile)
            .filter(PBFile.is_current == True)  # noqa: E712
            .order_by(PBFile.file_mtime.desc(), PBFile.file_name.asc())
            .all()
        )

        # Convert to plain dicts so templates don't rely on active DB session
        files: List[Dict[str, Any]] = [
            {
                "file_name": r.file_name,
                "path": r.path,
                "country": r.country,
                "unit": r.unit,
                "instance": r.instance,
                "subunit": r.subunit,
                "year": r.year,
                "file_mtime": r.file_mtime,
                "ingested_at": r.ingested_at,
                "webpage_name": r.webpage_name,
                "vote_type": r.vote_type,
            }
            for r in rows
        ]

    # Optional banner message
    msg = request.args.get("message")
    succ = request.args.get("success")
    success: Optional[bool] = None
    if succ is not None:
        success = succ in {"1", "true", "True"}

    return render_template(
        "admin/admin_dashboard.html",
        files=files,
        count=len(files),
        message=msg,
        success=success,
    )


@bp.route("/admin/deleted")
def admin_deleted():
    # Fetch all deleted/archived files (is_current = False)
    with get_session() as s:
        rows: List[PBFile] = (
            s.query(PBFile)
            .filter(PBFile.is_current == False)  # noqa: E712
            .order_by(PBFile.ingested_at.desc(), PBFile.file_name.asc())
            .all()
        )

        # Convert to plain dicts so templates don't rely on active DB session
        files: List[Dict[str, Any]] = [
            {
                "file_name": r.file_name,
                "path": r.path,
                "country": r.country,
                "unit": r.unit,
                "instance": r.instance,
                "subunit": r.subunit,
                "year": r.year,
                "file_mtime": r.file_mtime,
                "ingested_at": r.ingested_at,
                "webpage_name": r.webpage_name,
                "vote_type": r.vote_type,
                "supersedes_id": r.supersedes_id,
                "id": r.id,
                "deleted_at": _extract_deletion_timestamp(r.path),
                "file_exists": Path(r.path).exists() if r.path else False,
            }
            for r in rows
        ]

    # Optional banner message
    msg = request.args.get("message")
    succ = request.args.get("success")
    success: Optional[bool] = None
    if succ is not None:
        success = succ in {"1", "true", "True"}

    return render_template(
        "admin/admin_deleted.html",
        files=files,
        count=len(files),
        message=msg,
        success=success,
    )


def _tmp_upload_dir() -> Path:
    # Use container/host temp dir with a stable subfolder
    base = Path(tempfile.gettempdir()) / "pabulib_uploads"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _extract_deletion_timestamp(file_path: str) -> Optional[datetime]:
    """Extract deletion timestamp from archived file path"""
    if not file_path:
        return None

    # Look for timestamp patterns in the path like: 20241015T143022Z or replaced_20241015T143022Z
    timestamp_pattern = r"(?:replaced_)?(\d{8}T\d{6}Z)"
    match = re.search(timestamp_pattern, file_path)

    if match:
        timestamp_str = match.group(1)
        try:
            # Parse the timestamp: YYYYMMDDTHHMMSSZ
            return datetime.strptime(timestamp_str, "%Y%m%dT%H%M%SZ")
        except ValueError:
            pass

    return None


def _list_tmp_tiles() -> list[dict]:
    tmp_dir = _tmp_upload_dir()
    tiles: list[dict] = []
    for p in sorted(tmp_dir.glob("*.pb")):
        try:
            t = _parse_pb_to_tile(p)
            tile_data = _format_preview_tile(t)

            # Add validation
            validation = validate_pb_file(p)
            tile_data["validation"] = validation
            tile_data["validation_summary"] = format_validation_summary(validation)
            issue_counts = count_issues(validation)
            tile_data["error_count"] = issue_counts["errors"]
            tile_data["warning_count"] = issue_counts["warnings"]

            tiles.append(tile_data)
        except Exception as e:
            # Skip unreadable files, but still show a minimal entry
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
                        "error_message": f"Parse error: {str(e)}",
                    },
                    "validation_summary": f"⚠ Parse error: {str(e)}",
                    "error_count": 0,
                    "warning_count": 0,
                }
            )
    return tiles


def _format_preview_tile(tile: dict) -> dict:
    # Convert parse_pb_to_tile output to the public tile shape used on main page
    budget = tile.get("budget_raw")
    currency = tile.get("currency") or ""
    return {
        "file_name": tile.get("file_name", ""),
        "title": tile.get("title")
        or tile.get("webpage_name")
        or tile.get("file_name", "").replace("_", " "),
        "webpage_name": tile.get("webpage_name") or "",
        "description": tile.get("description") or "",
        "currency": currency,
        "num_votes": _format_int(int(tile.get("num_votes_raw") or 0)),
        "num_votes_raw": int(tile.get("num_votes_raw") or 0),
        "num_projects": _format_int(int(tile.get("num_projects_raw") or 0)),
        "num_projects_raw": int(tile.get("num_projects_raw") or 0),
        "num_selected_projects": (
            _format_int(int(tile.get("num_selected_projects_raw")))
            if tile.get("num_selected_projects_raw") is not None
            else "—"
        ),
        "num_selected_projects_raw": (
            int(tile.get("num_selected_projects_raw"))
            if tile.get("num_selected_projects_raw") is not None
            else None
        ),
        "budget": (
            _format_budget(currency, int(budget or 0)) if budget is not None else "—"
        ),
        "budget_raw": budget,
        "vote_type": tile.get("vote_type") or "",
        "vote_length": _format_vote_length(tile.get("vote_length_raw")),
        "vote_length_raw": tile.get("vote_length_raw"),
        "country": tile.get("country") or "",
        "unit": tile.get("unit") or "",
        "city": tile.get("unit") or "",
        "instance": tile.get("instance") or "",
        "subunit": tile.get("subunit") or "",
        "year": str(tile.get("year_raw")) if tile.get("year_raw") is not None else "",
        "year_raw": tile.get("year_raw"),
        "fully_funded": bool(tile.get("fully_funded") or False),
        "experimental": bool(tile.get("experimental") or False),
        "quality": float(tile.get("quality") or 0.0),
        "rule_raw": tile.get("rule_raw") or "",
        "edition": tile.get("edition") or "",
        "language": tile.get("language") or "",
    }


@bp.get("/admin/upload")
def upload_tiles():
    tiles = _list_tmp_tiles()
    # Precompute existence/conflict flags for each tile to adjust UI
    try:
        with get_session() as s:
            for t in tiles:
                name = (t.get("file_name") or "").strip()
                webpage_name = (t.get("webpage_name") or "").strip()
                webpage_conflict = False
                if webpage_name:
                    webpage_conflict = (
                        s.query(PBFile.id)
                        .filter(
                            PBFile.webpage_name == webpage_name,
                            PBFile.is_current == True,  # noqa: E712
                        )
                        .first()
                        is not None
                    )
                # Overwrite determination is based only on webpage_name
                t["exists_conflict"] = bool(webpage_conflict)
                t["name_conflict"] = False
                t["webpage_conflict"] = bool(webpage_conflict)
                t["group_conflict"] = False
    except Exception:
        # If any error, don't block rendering; flags will be absent/false
        pass
    # Support message via query params after redirects
    msg = request.args.get("message")
    succ = request.args.get("success")
    success: Optional[bool] = None
    if succ is not None:
        success = succ in {"1", "true", "True"}
    return render_template(
        "admin/upload_tiles.html",
        message=msg,
        success=success,
        tiles=tiles,
        count=len(tiles),
    )


@bp.post("/admin/upload")
def upload_tiles_post():
    if "files" not in request.files:
        tiles = _list_tmp_tiles()
        return render_template(
            "admin/upload_tiles.html",
            message="No files part in request.",
            success=False,
            tiles=tiles,
            count=len(tiles),
        )
    files = request.files.getlist("files")
    if not files:
        tiles = _list_tmp_tiles()
        return render_template(
            "admin/upload_tiles.html",
            message="Please choose at least one .pb file.",
            success=False,
            tiles=tiles,
            count=len(tiles),
        )

    tmp_dir = _tmp_upload_dir()
    saved = 0
    results = []
    overwrites = []  # Track files that will overwrite temp files

    for f in files:
        fname = secure_filename(f.filename or "").strip()
        if not fname:
            results.append(
                {
                    "ok": False,
                    "name": "(unnamed)",
                    "msg": "Empty filename.",
                    "validation": None,
                }
            )
            continue
        if not fname.endswith(".pb"):
            results.append(
                {
                    "ok": False,
                    "name": fname,
                    "msg": "Only .pb files are allowed.",
                    "validation": None,
                }
            )
            continue
        try:
            target = tmp_dir / fname
            # Check if temp file already exists (not yet published)
            if target.exists():
                overwrites.append(fname)
            f.save(str(target))

            # Validate the file after saving
            try:
                validation = validate_pb_file(target)
                validation_summary = format_validation_summary(validation)
            except Exception as val_err:
                # Don't fail the upload if validation fails
                current_app.logger.exception("Validation error for %s", fname)
                validation = {
                    "valid": False,
                    "errors": None,
                    "warnings": None,
                    "error_message": f"Validation failed: {str(val_err)}",
                }
                validation_summary = f"⚠ Validation failed: {str(val_err)}"

            saved += 1
            results.append(
                {
                    "ok": True,
                    "name": fname,
                    "msg": "Uploaded to /tmp.",
                    "validation": validation,
                    "validation_summary": validation_summary,
                }
            )
        except Exception as e:
            results.append(
                {
                    "ok": False,
                    "name": fname,
                    "msg": f"Failed to save: {e}",
                    "validation": None,
                }
            )

    tiles = _list_tmp_tiles()

    # Build message with overwrite warning if any temp files were overwritten
    msg = f"Processed {len(files)} file(s). Uploaded {saved}."
    if overwrites:
        msg += f" WARNING: Overwrote {len(overwrites)} existing temp file(s): {', '.join(overwrites)}"

    return render_template(
        "admin/upload_tiles.html",
        message=msg,
        success=saved > 0,
        results=results,
        tiles=tiles,
        count=len(tiles),
    )


@bp.post("/admin/upload/ingest")
def upload_tiles_ingest():
    name = (request.form.get("name") or "").strip()
    confirm = (request.form.get("confirm") or "").strip() in {"1", "true", "True"}
    if not name or "/" in name or ".." in name or not name.endswith(".pb"):
        abort(400)
    tmp_path = _tmp_upload_dir() / name
    if not tmp_path.exists() or not tmp_path.is_file():
        abort(404)

    logger = current_app.logger
    logger.debug("Ingest requested for %s (tmp: %s)", name, tmp_path)

    # Parse at tmp first; if parsing fails, don't move
    try:
        tile_preview = _parse_pb_to_tile(tmp_path)
    except Exception as e:
        logger.exception("Failed to parse PB file before ingest: %s", name)
        if request.headers.get("X-Requested-With") == "fetch" or request.is_json:
            return jsonify({"ok": False, "error": f"Parse error: {e}"}), 400
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Failed to parse {name}: {e}",
                success=0,
            )
        )

    group_key = _build_group_key(
        tile_preview.get("country") or "",
        tile_preview.get("unit") or "",
        tile_preview.get("instance") or "",
        tile_preview.get("subunit") or "",
    )

    # If a current record exists for this webpage_name and no confirm provided, request confirmation
    with get_session() as s:
        webpage_exists = False
        webpage_val = (tile_preview.get("webpage_name") or "").strip()
        if webpage_val:
            webpage_exists = (
                s.query(PBFile.id)
                .filter(
                    PBFile.webpage_name == webpage_val,
                    PBFile.is_current == True,
                )  # noqa: E712
                .first()
                is not None
            )
    if webpage_exists and not confirm:
        # For fetch-based calls, return a 409 to trigger a prompt client-side
        return (
            jsonify(
                {
                    "ok": False,
                    "requires_confirm": True,
                    "name_conflict": False,
                    "webpage_conflict": bool(webpage_exists),
                    "group_conflict": False,
                    "message": "A current record exists for this dataset. Confirm overwrite to proceed.",
                }
            ),
            409,
        )

    # Move into pb_files and ingest into DB
    dest_dir = _pb_folder()
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / name
    # If there is a current record matching the same webpage_name, archive its file first
    archived_to: Optional[Path] = None
    try:
        with get_session() as s:
            prev_rec = None
            webpage_val = (tile_preview.get("webpage_name") or "").strip()
            if webpage_val:
                prev_rec = (
                    s.query(PBFile)
                    .filter(
                        PBFile.webpage_name == webpage_val,
                        PBFile.is_current == True,
                    )  # noqa: E712
                    .one_or_none()
                )
                if prev_rec:
                    logger.debug(
                        "Archiving previous by webpage_name=%s: %s",
                        webpage_val,
                        prev_rec.path,
                    )
            if prev_rec and prev_rec.path:
                src_path = Path(prev_rec.path)
                if src_path.exists():
                    archive_root = _pb_depr_folder()
                    # Use UTC timestamp-based folder name to keep history
                    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                    dest_folder = archive_root / ts
                    dest_folder.mkdir(parents=True, exist_ok=True)
                    # Preserve the previous file's own name in the archive
                    try:
                        prev_name = (
                            getattr(prev_rec, "file_name", None)
                            or Path(prev_rec.path).name
                        )
                    except Exception:
                        prev_name = name
                    archived_to = dest_folder / prev_name
                    try:
                        if archived_to.exists():
                            archived_to.unlink()
                    except Exception:
                        logger.warning(
                            "Could not unlink existing archived target before move: %s",
                            archived_to,
                        )
                    shutil.move(str(src_path), str(archived_to))
                    logger.debug("Archived previous file to %s", archived_to)
                    # Update DB path for the old record to point to archived location
                    prev_rec.path = str(archived_to)
                else:
                    logger.debug(
                        "Previous file missing on disk, skip archive: %s", src_path
                    )
            else:
                logger.debug("No previous record found to archive (webpage/group)")
    except Exception as e:
        # Archival failures shouldn't block ingest; proceed with move of new file
        logger.exception("Archival step failed but will proceed with ingest: %s", e)
    try:
        if target.exists():
            try:
                target.unlink()
            except Exception:
                logger.warning(
                    "Could not unlink existing target before move: %s", target
                )
        # Use shutil.move to support cross-device moves (copies then removes)
        shutil.move(str(tmp_path), str(target))
        logger.debug("Moved %s to %s", name, target)
    except Exception as e:
        # On failure, return JSON or redirect with error
        logger.exception("Failed to move %s into pb folder", name)
        if request.headers.get("X-Requested-With") == "fetch" or request.is_json:
            return jsonify({"ok": False, "error": f"Failed to move file: {e}"}), 500
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Failed to upload {name}: {e}",
                success=0,
            )
        )

    # Insert as current version (reuse parsed tile)
    tile = tile_preview
    stat = target.stat()
    file_mtime = datetime.utcfromtimestamp(stat.st_mtime)

    webpage_name = tile.get("webpage_name") or None
    country = tile.get("country") or None
    unit = tile.get("unit") or None
    instance = tile.get("instance") or None
    subunit = tile.get("subunit") or None
    year = tile.get("year_raw")
    description = tile.get("description") or None
    currency = tile.get("currency") or None
    num_votes = int(tile.get("num_votes_raw") or 0)
    num_projects = int(tile.get("num_projects_raw") or 0)
    num_selected_projects_raw = tile.get("num_selected_projects_raw")
    num_selected_projects = (
        int(num_selected_projects_raw)
        if num_selected_projects_raw is not None
        else None
    )
    budget = tile.get("budget_raw")
    vote_type = tile.get("vote_type") or None
    vote_length = tile.get("vote_length_raw")
    fully_funded = bool(tile.get("fully_funded") or False)
    experimental = bool(tile.get("experimental") or False)
    rule_raw = tile.get("rule_raw") or None
    edition = tile.get("edition") or None
    language = tile.get("language") or None
    quality = float(tile.get("quality") or 0.0)

    group_key = _build_group_key(
        country or "",
        unit or "",
        instance or "",
        subunit or "",
    )

    try:
        with get_session() as s:
            prev = None
            if webpage_name:
                prev = (
                    s.query(PBFile)
                    .filter(
                        PBFile.webpage_name == webpage_name,
                        PBFile.is_current == True,
                    )  # noqa: E712
                    .order_by(PBFile.ingested_at.desc())
                    .first()
                )
            supersedes_id = prev.id if prev else None
            if prev:
                prev.is_current = False

            rec = PBFile(
                file_name=name,
                path=str(target),
                country=country,
                unit=unit,
                instance=instance,
                subunit=subunit,
                webpage_name=webpage_name,
                year=year,
                description=description,
                currency=currency,
                num_votes=num_votes,
                num_projects=num_projects,
                num_selected_projects=num_selected_projects,
                budget=budget,
                vote_type=vote_type,
                vote_length=vote_length,
                fully_funded=fully_funded,
                experimental=experimental,
                rule_raw=rule_raw,
                edition=edition,
                language=language,
                quality=quality,
                file_mtime=file_mtime,
                ingested_at=datetime.utcnow(),
                is_current=True,
                supersedes_id=supersedes_id,
                group_key=group_key,
            )
            s.add(rec)
    except Exception as e:
        logger.exception("DB error while ingesting %s", name)
        if request.headers.get("X-Requested-With") == "fetch" or request.is_json:
            return jsonify({"ok": False, "error": f"DB error during upload: {e}"}), 500
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Failed to upload {name}: {e}",
                success=0,
            )
        )

    try:
        pb_service.invalidate_caches()
    except Exception:
        pass

    # If called via fetch, return JSON ok; otherwise redirect with a message
    if request.headers.get("X-Requested-With") == "fetch" or request.is_json:
        return jsonify({"ok": True, "message": f"Uploaded {name}."})
    return redirect(
        url_for(
            "admin.upload_tiles",
            message=f"Uploaded {name}.",
            success=1,
        )
    )


@bp.post("/admin/upload/delete")
def upload_tiles_delete():
    name = (request.form.get("name") or "").strip()
    if not name or "/" in name or ".." in name:
        abort(400)
    p = _tmp_upload_dir() / name
    if p.exists() and p.is_file():
        try:
            p.unlink()
        except Exception:
            pass
    return redirect(url_for("admin.upload_tiles"))


@bp.get("/admin/upload/download/<path:name>")
def upload_tiles_download(name: str):
    if not name or "/" in name or ".." in name or not name.endswith(".pb"):
        abort(400)
    p = _tmp_upload_dir() / name
    if not p.exists() or not p.is_file():
        abort(404)
    return send_file(p, as_attachment=True)


@bp.get("/admin/upload/check")
def upload_tiles_check():
    """Check if ingesting a given temp file would overwrite an existing current record.
    Returns JSON {exists: bool, name: str, group_key: str | None}.
    """
    name = (request.args.get("name") or "").strip()
    if not name or "/" in name or ".." in name or not name.endswith(".pb"):
        return jsonify({"error": "invalid name"}), 400
    tmp_path = _tmp_upload_dir() / name
    if not tmp_path.exists() or not tmp_path.is_file():
        return jsonify({"error": "not found"}), 404
    try:
        tile_preview = _parse_pb_to_tile(tmp_path)
    except Exception:
        tile_preview = {"webpage_name": ""}
    webpage_exists = False
    with get_session() as s:
        webpage_val = (tile_preview.get("webpage_name") or "").strip()
        if webpage_val:
            webpage_exists = (
                s.query(PBFile.id)
                .filter(
                    PBFile.webpage_name == webpage_val,
                    PBFile.is_current == True,
                )  # noqa: E712
                .first()
                is not None
            )
    return jsonify(
        {
            "exists": bool(webpage_exists),
            "name_conflict": False,
            "webpage_conflict": bool(webpage_exists),
            "group_conflict": False,
            "name": name,
            "group_key": None,
        }
    )


@bp.post("/admin/files/delete")
def admin_delete_file():
    name = (request.form.get("name") or "").strip()
    if not name or "/" in name or ".." in name:
        abort(400)
    from .utils.pb_utils import pb_depreciated_folder as _pb_depr_folder

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archived = None
    deleted_records = 0
    errors: list[str] = []
    with get_session() as s:
        try:
            recs: list[PBFile] = (
                s.query(PBFile)
                .filter(
                    PBFile.file_name == name, PBFile.is_current == True
                )  # noqa: E712
                .all()
            )
        except Exception as e:
            current_app.logger.exception("Delete query failed for %s", name)
            if request.headers.get("X-Requested-With") == "fetch":
                return (
                    jsonify(
                        {"ok": False, "error": f"DB error while locating file: {e}"}
                    ),
                    500,
                )
            return redirect(
                url_for(
                    "admin.admin_dashboard",
                    message=f"Failed to delete {name}: DB error while locating file.",
                    success=0,
                )
            )
        if not recs:
            if request.headers.get("X-Requested-With") == "fetch":
                return jsonify({"ok": False, "error": "Not found or not current"}), 404
            return redirect(
                url_for("admin.admin_dashboard", message="File not found.", success=0)
            )
        # Try to archive on-disk file(s) if present (handle duplicates defensively)
        for rec in recs:
            try:
                if rec.path:
                    src = Path(rec.path)
                    if src.exists():
                        dest_dir = _pb_depr_folder() / ts
                        dest_dir.mkdir(parents=True, exist_ok=True)
                        dest = dest_dir / Path(rec.path).name
                        try:
                            if dest.exists():
                                dest.unlink()
                        except Exception:
                            pass
                        shutil.move(str(src), str(dest))
                        archived = str(dest)
                # Update DB: mark not current and update path if archived
                rec.is_current = False
                if archived:
                    rec.path = archived
                deleted_records += 1
            except Exception as e:
                current_app.logger.exception(
                    "Archival failed during single delete for %s (id=%s)",
                    name,
                    getattr(rec, "id", "?"),
                )
                errors.append(str(e))
    try:
        pb_service.invalidate_caches()
    except Exception:
        pass
    if request.headers.get("X-Requested-With") == "fetch":
        if errors:
            return jsonify(
                {"ok": True, "deleted_records": deleted_records, "errors": errors}
            )
        return jsonify({"ok": True, "deleted_records": deleted_records})
    return redirect(
        url_for(
            "admin.admin_dashboard",
            message=(
                f"Deleted {name}."
                if not errors
                else f"Deleted {name} with issues: {'; '.join(errors)}"
            ),
            success=1 if deleted_records else 0,
        )
    )


@bp.post("/admin/files/delete_bulk")
def admin_delete_files_bulk():
    # Expect JSON body: { names: [file_name, ...] }
    try:
        payload = request.get_json(silent=True) or {}
        names = payload.get("names") or []
        names = [str(n).strip() for n in names if isinstance(n, str) and n.strip()]
    except Exception:
        names = []
    if not names:
        return jsonify({"ok": False, "error": "No names provided"}), 400
    from .utils.pb_utils import pb_depreciated_folder as _pb_depr_folder

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dest_root = _pb_depr_folder() / ts
    try:
        dest_root.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    deleted_names = 0
    errors: list[dict] = []
    with get_session() as s:
        for name in names:
            try:
                recs: list[PBFile] = (
                    s.query(PBFile)
                    .filter(
                        PBFile.file_name == name, PBFile.is_current == True
                    )  # noqa: E712
                    .all()
                )
            except Exception as e:
                current_app.logger.exception("Bulk delete query failed for %s", name)
                errors.append(
                    {"name": name, "error": f"DB error while locating file: {e}"}
                )
                continue
            if not recs:
                errors.append({"name": name, "error": "Not found or not current"})
                continue
            name_had_success = False
            for rec in recs:
                archived = None
                try:
                    if rec.path:
                        src = Path(rec.path)
                        if src.exists():
                            dest = dest_root / Path(rec.path).name
                            try:
                                if dest.exists():
                                    dest.unlink()
                            except Exception:
                                pass
                            shutil.move(str(src), str(dest))
                            archived = str(dest)
                except Exception as e:
                    current_app.logger.exception(
                        "Archival failed during bulk delete for %s (id=%s)",
                        name,
                        getattr(rec, "id", "?"),
                    )
                    errors.append({"name": name, "error": f"Failed to archive: {e}"})
                # Update DB regardless of whether file existed on disk
                try:
                    rec.is_current = False
                    if archived:
                        rec.path = archived
                    name_had_success = True
                except Exception as e:
                    errors.append({"name": name, "error": f"Failed to update DB: {e}"})
            if name_had_success:
                deleted_names += 1
    try:
        pb_service.invalidate_caches()
    except Exception:
        pass
    return jsonify({"ok": True, "deleted": deleted_names, "errors": errors})


@bp.post("/admin/files/replace")
def admin_replace_file():
    """Replace an existing file with a new upload from tmp directory"""
    existing_name = (request.form.get("existing_name") or "").strip()
    new_name = (request.form.get("new_name") or "").strip()
    confirm = (request.form.get("confirm") or "").strip() in {"1", "true", "True"}

    if not existing_name or not new_name:
        abort(400)
    if (
        "/" in existing_name
        or ".." in existing_name
        or not existing_name.endswith(".pb")
    ):
        abort(400)
    if "/" in new_name or ".." in new_name or not new_name.endswith(".pb"):
        abort(400)

    logger = current_app.logger

    # Check if existing file exists in database
    with get_session() as s:
        existing_rec = (
            s.query(PBFile)
            .filter(
                PBFile.file_name == existing_name, PBFile.is_current == True
            )  # noqa: E712
            .one_or_none()
        )
        if not existing_rec:
            if request.headers.get("X-Requested-With") == "fetch":
                return (
                    jsonify(
                        {"ok": False, "error": "Original file not found or not current"}
                    ),
                    404,
                )
            return redirect(
                url_for(
                    "admin.admin_dashboard",
                    message="Original file not found.",
                    success=0,
                )
            )

    # Check if new file exists in tmp
    tmp_path = _tmp_upload_dir() / new_name
    if not tmp_path.exists() or not tmp_path.is_file():
        if request.headers.get("X-Requested-With") == "fetch":
            return (
                jsonify({"ok": False, "error": "Replacement file not found in tmp"}),
                404,
            )
        return redirect(
            url_for(
                "admin.upload_tiles",
                message="Replacement file not found in tmp.",
                success=0,
            )
        )

    # Parse replacement file first
    try:
        tile_preview = _parse_pb_to_tile(tmp_path)
    except Exception as e:
        logger.exception("Failed to parse replacement PB file: %s", new_name)
        if request.headers.get("X-Requested-With") == "fetch":
            return jsonify({"ok": False, "error": f"Parse error: {e}"}), 400
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Failed to parse {new_name}: {e}",
                success=0,
            )
        )

    with get_session() as s:
        # Check if another current file has same webpage_name (excluding the one being replaced)
        conflicts = []
        webpage_val = (tile_preview.get("webpage_name") or "").strip()
        if webpage_val:
            webpage_conflict = (
                s.query(PBFile.id)
                .filter(
                    PBFile.webpage_name == webpage_val,
                    PBFile.is_current == True,
                    PBFile.id != existing_rec.id,
                )  # noqa: E712
                .first()
            )
            if webpage_conflict:
                conflicts.append(f"webpage_name '{webpage_val}'")

    if conflicts and not confirm:
        if request.headers.get("X-Requested-With") == "fetch":
            return (
                jsonify(
                    {
                        "ok": False,
                        "requires_confirm": True,
                        "conflicts": conflicts,
                        "message": f"Replacement would conflict with existing records: {', '.join(conflicts)}. Confirm to proceed.",
                    }
                ),
                409,
            )
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Replacement conflicts with: {', '.join(conflicts)}. Use force replace if intended.",
                success=0,
            )
        )

    # Archive the existing file
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archived_to = None

    try:
        if existing_rec.path:
            src_path = Path(existing_rec.path)
            if src_path.exists():
                archive_root = _pb_depr_folder()
                dest_folder = archive_root / f"replaced_{ts}"
                dest_folder.mkdir(parents=True, exist_ok=True)
                archived_to = dest_folder / existing_name

                if archived_to.exists():
                    archived_to.unlink()
                shutil.move(str(src_path), str(archived_to))
                logger.info(
                    "Archived replaced file %s to %s", existing_name, archived_to
                )
            else:
                logger.warning("Original file missing on disk: %s", src_path)
    except Exception as e:
        logger.exception("Failed to archive original file: %s", e)
        if request.headers.get("X-Requested-With") == "fetch":
            return (
                jsonify({"ok": False, "error": f"Failed to archive original: {e}"}),
                500,
            )
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Failed to archive original: {e}",
                success=0,
            )
        )

    # Move new file to destination (keep original filename or use new one)
    dest_dir = _pb_folder()
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / existing_name  # Keep original filename

    try:
        if target.exists():
            target.unlink()
        shutil.move(str(tmp_path), str(target))
        logger.info("Moved replacement file %s to %s", new_name, target)
    except Exception as e:
        logger.exception("Failed to move replacement file")
        if request.headers.get("X-Requested-With") == "fetch":
            return (
                jsonify({"ok": False, "error": f"Failed to move replacement: {e}"}),
                500,
            )
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Failed to move replacement: {e}",
                success=0,
            )
        )

    # Update database record with new data
    tile = tile_preview
    stat = target.stat()
    file_mtime = datetime.utcfromtimestamp(stat.st_mtime)
    # Compute group_key for the new record (for grouping/display); identity is by webpage_name
    group_key = _build_group_key(
        tile.get("country") or "",
        tile.get("unit") or "",
        tile.get("instance") or "",
        tile.get("subunit") or "",
    )

    with get_session() as s:
        # Mark old record as not current and update its path to archived location
        existing_rec.is_current = False
        if archived_to:
            existing_rec.path = str(archived_to)

        # Create new record with updated data
        new_rec = PBFile(
            file_name=existing_name,  # Keep original filename
            path=str(target),
            country=tile.get("country"),
            unit=tile.get("unit"),
            instance=tile.get("instance"),
            subunit=tile.get("subunit"),
            webpage_name=tile.get("webpage_name"),
            year=tile.get("year_raw"),
            description=tile.get("description"),
            currency=tile.get("currency"),
            num_votes=int(tile.get("num_votes_raw") or 0),
            num_projects=int(tile.get("num_projects_raw") or 0),
            num_selected_projects=(
                int(tile.get("num_selected_projects_raw"))
                if tile.get("num_selected_projects_raw") is not None
                else None
            ),
            budget=tile.get("budget_raw"),
            vote_type=tile.get("vote_type"),
            vote_length=tile.get("vote_length_raw"),
            fully_funded=bool(tile.get("fully_funded") or False),
            experimental=bool(tile.get("experimental") or False),
            rule_raw=tile.get("rule_raw"),
            edition=tile.get("edition"),
            language=tile.get("language"),
            quality=float(tile.get("quality") or 0.0),
            file_mtime=file_mtime,
            ingested_at=datetime.utcnow(),
            is_current=True,
            supersedes_id=existing_rec.id,
            group_key=group_key or "",
        )
        s.add(new_rec)

    try:
        pb_service.invalidate_caches()
    except Exception:
        pass

    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify(
            {"ok": True, "message": f"Replaced {existing_name} with {new_name}"}
        )
    return redirect(
        url_for(
            "admin.admin_dashboard",
            message=f"Successfully replaced {existing_name}",
            success=1,
        )
    )


@bp.get("/admin/files/history/<path:name>")
def admin_file_history(name: str):
    """Show version history for a file"""
    if not name or "/" in name or ".." in name or not name.endswith(".pb"):
        abort(400)

    with get_session() as s:
        # Get all versions of this file (by name or supersession chain)
        current_rec = (
            s.query(PBFile)
            .filter(PBFile.file_name == name, PBFile.is_current == True)  # noqa: E712
            .one_or_none()
        )

        if not current_rec:
            abort(404)

        # Build version chain backwards from current
        versions = [current_rec]
        next_id = current_rec.supersedes_id

        while next_id:
            prev_rec = s.query(PBFile).filter(PBFile.id == next_id).one_or_none()
            if prev_rec:
                versions.append(prev_rec)
                next_id = prev_rec.supersedes_id
            else:
                break

        # Convert to plain dicts
        history = []
        for i, rec in enumerate(versions):
            history.append(
                {
                    "version": len(versions) - i,  # Latest = highest number
                    "id": rec.id,
                    "file_name": rec.file_name,
                    "path": rec.path,
                    "is_current": rec.is_current,
                    "ingested_at": rec.ingested_at,
                    "file_mtime": rec.file_mtime,
                    "country": rec.country,
                    "unit": rec.unit,
                    "year": rec.year,
                    "num_votes": rec.num_votes,
                    "num_projects": rec.num_projects,
                    "quality": rec.quality,
                    "supersedes_id": rec.supersedes_id,
                }
            )

    return render_template(
        "admin/file_history.html", file_name=name, history=history, count=len(history)
    )


@bp.get("/admin/deleted/download/<int:file_id>")
def admin_download_deleted_file(file_id: int):
    """Download a deleted/archived file by its database ID"""
    with get_session() as s:
        rec = (
            s.query(PBFile)
            .filter(PBFile.id == file_id, PBFile.is_current == False)  # noqa: E712
            .one_or_none()
        )

        if not rec:
            abort(404)

        if not rec.path:
            abort(404)

        file_path = Path(rec.path)
        if not file_path.exists():
            abort(404)

        # Use the original filename for download
        download_name = rec.file_name or file_path.name

        return send_file(
            file_path,
            as_attachment=True,
            download_name=download_name,
            mimetype="application/octet-stream",
        )


@bp.post("/admin/deleted/delete/<int:file_id>")
def admin_delete_deleted_file(file_id: int):
    """Permanently remove an archived (non-current) file from disk.

    Does NOT remove the database record; only deletes the on-disk archived file.
    """
    # Only allow action on non-current (deleted/archived) files
    with get_session() as s:
        rec = (
            s.query(PBFile)
            .filter(PBFile.id == file_id, PBFile.is_current == False)  # noqa: E712
            .one_or_none()
        )
        if not rec:
            if request.headers.get("X-Requested-With") == "fetch":
                return jsonify({"ok": False, "error": "Not found"}), 404
            abort(404)

        removed = False
        try:
            if rec.path:
                p = Path(rec.path)
                if p.exists():
                    try:
                        p.unlink()
                        removed = True
                    except Exception:
                        removed = False
        except Exception:
            removed = False

    try:
        pb_service.invalidate_caches()
    except Exception:
        pass

    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": True, "removed": removed})
    return redirect(
        url_for(
            "admin.admin_deleted",
            message=("File removed from disk" if removed else "File not found on disk"),
            success=1 if removed else 0,
        )
    )
