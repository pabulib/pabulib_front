from __future__ import annotations

from typing import Any, Dict, List

from flask import Blueprint, render_template

from .db import get_session
from .models import PBFile

bp = Blueprint(
    "admin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


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
            }
            for r in rows
        ]

    return render_template(
        "admin/admin_dashboard.html",
        files=files,
        count=len(files),
    )
