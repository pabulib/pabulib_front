from __future__ import annotations

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

    return render_template(
        "admin/admin_dashboard.html",
        files=files,
        count=len(files),
    )


def _tmp_upload_dir() -> Path:
    # Use container/host temp dir with a stable subfolder
    base = Path(tempfile.gettempdir()) / "pabulib_uploads"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _list_tmp_tiles() -> list[dict]:
    tmp_dir = _tmp_upload_dir()
    tiles: list[dict] = []
    for p in sorted(tmp_dir.glob("*.pb")):
        try:
            t = _parse_pb_to_tile(p)
            tiles.append(_format_preview_tile(t))
        except Exception:
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
        "num_selected_projects": _format_int(
            int(tile.get("num_selected_projects_raw") or 0)
        ),
        "num_selected_projects_raw": int(tile.get("num_selected_projects_raw") or 0),
        "budget": (
            _format_budget(currency, int(budget or 0)) if budget is not None else "—"
        ),
        "budget_raw": budget,
        "vote_type": tile.get("vote_type") or "",
        "vote_length": _format_vote_length(tile.get("vote_length_raw")),
        "vote_length_raw": tile.get("vote_length_raw"),
        "country": tile.get("country") or "",
        "city": tile.get("unit") or "",
        "year": str(tile.get("year_raw")) if tile.get("year_raw") is not None else "",
        "year_raw": tile.get("year_raw"),
        "fully_funded": bool(tile.get("fully_funded") or False),
        "has_selected_col": bool(tile.get("has_selected_col") or False),
        "experimental": bool(tile.get("experimental") or False),
        "quality": float(tile.get("quality") or 0.0),
        "rule_raw": tile.get("rule_raw") or "",
        "edition": tile.get("edition") or "",
        "language": tile.get("language") or "",
    }


@bp.get("/admin/upload")
def upload_tiles():
    tiles = _list_tmp_tiles()
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
    for f in files:
        fname = secure_filename(f.filename or "").strip()
        if not fname:
            results.append({"ok": False, "name": "(unnamed)", "msg": "Empty filename."})
            continue
        if not fname.endswith(".pb"):
            results.append(
                {"ok": False, "name": fname, "msg": "Only .pb files are allowed."}
            )
            continue
        try:
            target = tmp_dir / fname
            f.save(str(target))
            saved += 1
            results.append({"ok": True, "name": fname, "msg": "Uploaded to /tmp."})
        except Exception as e:
            results.append({"ok": False, "name": fname, "msg": f"Failed to save: {e}"})

    tiles = _list_tmp_tiles()
    return render_template(
        "admin/upload_tiles.html",
        message=f"Processed {len(files)} file(s). Uploaded {saved}.",
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

    # If a current record exists for this file name, webpage_name, or group and no confirm provided, request confirmation
    with get_session() as s:
        name_exists = (
            s.query(PBFile.id)
            .filter(PBFile.file_name == name, PBFile.is_current == True)  # noqa: E712
            .first()
            is not None
        )
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
        group_exists = False
        if group_key:
            group_exists = (
                s.query(PBFile.id)
                .filter(
                    PBFile.group_key == group_key, PBFile.is_current == True
                )  # noqa: E712
                .first()
                is not None
            )
    if (name_exists or webpage_exists or group_exists) and not confirm:
        # For fetch-based calls, return a 409 to trigger a prompt client-side
        return (
            jsonify(
                {
                    "ok": False,
                    "requires_confirm": True,
                    "name_conflict": bool(name_exists),
                    "webpage_conflict": bool(webpage_exists),
                    "group_conflict": bool(group_exists),
                    "message": "A current record exists for this dataset. Confirm overwrite to proceed.",
                }
            ),
            409,
        )

    # Move into pb_files and ingest into DB
    dest_dir = _pb_folder()
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / name
    # If there is a current record matching the same dataset, archive its file first (prefer webpage_name, then group)
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
            if prev_rec is None and group_key:
                prev_rec = (
                    s.query(PBFile)
                    .filter(
                        PBFile.group_key == group_key,
                        PBFile.is_current == True,
                    )  # noqa: E712
                    .one_or_none()
                )
                if prev_rec:
                    logger.debug(
                        "Archiving previous by group_key=%s: %s",
                        group_key,
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
                message=f"Failed to ingest {name}: {e}",
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
    budget = tile.get("budget_raw")
    vote_type = tile.get("vote_type") or None
    vote_length = tile.get("vote_length_raw")
    fully_funded = bool(tile.get("fully_funded") or False)
    has_selected_col = bool(tile.get("has_selected_col") or False)
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
            prev = (
                s.query(PBFile)
                .filter(PBFile.group_key == group_key, PBFile.is_current == True)
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
                budget=budget,
                vote_type=vote_type,
                vote_length=vote_length,
                fully_funded=fully_funded,
                has_selected_col=has_selected_col,
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
            return jsonify({"ok": False, "error": f"DB error during ingest: {e}"}), 500
        return redirect(
            url_for(
                "admin.upload_tiles",
                message=f"Failed to ingest {name}: {e}",
                success=0,
            )
        )

    try:
        pb_service.invalidate_caches()
    except Exception:
        pass

    # If called via fetch, return JSON ok; otherwise redirect with a message
    if request.headers.get("X-Requested-With") == "fetch" or request.is_json:
        return jsonify({"ok": True, "message": f"Ingested {name}."})
    return redirect(
        url_for(
            "admin.upload_tiles",
            message=f"Ingested {name}.",
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
        group_key = _build_group_key(
            tile_preview.get("country") or "",
            tile_preview.get("unit") or "",
            tile_preview.get("instance") or "",
            tile_preview.get("subunit") or "",
        )
    except Exception:
        group_key = None
    name_exists = False
    group_exists = False
    webpage_exists = False
    with get_session() as s:
        name_exists = (
            s.query(PBFile.id)
            .filter(PBFile.file_name == name, PBFile.is_current == True)  # noqa: E712
            .first()
            is not None
        )
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
        if group_key:
            group_exists = (
                s.query(PBFile.id)
                .filter(
                    PBFile.group_key == group_key, PBFile.is_current == True
                )  # noqa: E712
                .first()
                is not None
            )
    return jsonify(
        {
            "exists": bool(name_exists or webpage_exists or group_exists),
            "name_conflict": bool(name_exists),
            "webpage_conflict": bool(webpage_exists),
            "group_conflict": bool(group_exists),
            "name": name,
            "group_key": group_key,
        }
    )
