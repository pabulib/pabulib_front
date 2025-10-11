from __future__ import annotations

from typing import Any, Dict, List, Optional

from flask import Blueprint, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash

from .db import get_session
from .models import AdminUser, PBFile

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
