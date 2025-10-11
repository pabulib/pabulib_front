from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import (
    Blueprint,
    abort,
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
    return render_template(
        "admin/upload_tiles.html",
        message=None,
        success=None,
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
    if not name or "/" in name or ".." in name or not name.endswith(".pb"):
        abort(400)
    tmp_path = _tmp_upload_dir() / name
    if not tmp_path.exists() or not tmp_path.is_file():
        abort(404)

    # Move into pb_files and ingest into DB
    dest_dir = _pb_folder()
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / name
    tmp_path.replace(target)

    # Parse and insert as current version
    tile = _parse_pb_to_tile(target)
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

    try:
        pb_service.invalidate_caches()
    except Exception:
        pass

    return redirect(url_for("admin.upload_tiles"))


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
