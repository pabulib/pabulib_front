import io
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from flask import Blueprint, abort, current_app, render_template, request, send_file

from .utils.load_pb_file import parse_pb_lines

bp = Blueprint(
    "main",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


def _workspace_root() -> Path:
    # assume this repo layout
    return Path(__file__).resolve().parents[1]


def _pb_folder() -> Path:
    return _workspace_root() / "pb_files"


def _read_file_lines(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return [line.rstrip("\n") for line in f]


def _format_int(num: int) -> str:
    return f"{num:,}".replace(",", " ")


def _format_budget(currency: str, amount: int) -> str:
    formatted = _format_int(amount)
    return f"{formatted} {currency}" if currency else formatted


def build_tile_data(pb_path: Path) -> Dict[str, Any]:
    lines = _read_file_lines(pb_path)
    meta, projects, votes, votes_in_projects, scores_in_projects = parse_pb_lines(lines)

    # Webpage name pattern: Country_Unit_Instance_Subunit
    country = str(meta.get("country", "")).strip()
    unit = str(meta.get("unit", meta.get("city", meta.get("district", "")))).strip()
    instance = str(meta.get("instance", meta.get("year", ""))).strip()
    subunit = str(meta.get("subunit", "")).strip()
    webpage_parts = [p for p in [country, unit, instance, subunit] if p]
    webpage_name = "_".join(webpage_parts)
    # Title for UI: readable spaces
    title = (
        webpage_name.replace("_", " ")
        if webpage_name
        else pb_path.stem.replace("_", " ")
    )

    # Description
    description = meta.get("description", "")

    # Numbers
    num_votes = int(meta.get("num_votes", len(votes)))
    num_projects = int(meta.get("num_projects", len(projects)))
    currency = meta.get("currency", "")
    budget_raw = meta.get("budget")
    try:
        budget = (
            int(budget_raw)
            if budget_raw is not None and str(budget_raw).isdigit()
            else None
        )
    except Exception:
        budget = None
    budget_str = _format_budget(currency, budget) if budget is not None else "—"

    vote_type = meta.get("vote_type", meta.get("rule", "")).lower()

    # vote length: average number of items chosen per voter from VOTES section
    try:
        lengths = []
        for v in votes.values():
            sel = str(v.get("vote", "")).strip()
            if not sel:
                continue
            lengths.append(len([s for s in sel.split(",") if s]))
        vote_length = f"{(sum(lengths)/len(lengths)):.3f}" if lengths else "—"
    except Exception:
        vote_length = "—"

    return {
        "file_name": pb_path.name,
        "title": title,
        "webpage_name": webpage_name,
        "description": description,
        "num_votes": _format_int(num_votes),
        "num_projects": _format_int(num_projects),
        "budget": budget_str,
        "vote_type": vote_type,
        "vote_length": vote_length,
    }


@bp.route("/")
def home():
    folder = _pb_folder()
    if not folder.exists():
        abort(404, description="PB files folder not found")

    files = sorted(folder.glob("*.pb"))
    tiles = [build_tile_data(p) for p in files]
    return render_template("index.html", tiles=tiles, count=len(tiles))


@bp.route("/download/<path:filename>")
def download(filename: str):
    path = _pb_folder() / filename
    if not path.exists() or not path.is_file():
        abort(404)
    return send_file(path, as_attachment=True)


@bp.post("/download-selected")
def download_selected():
    names = request.form.getlist("files")
    if not names:
        abort(400, description="No files selected")
    base = _pb_folder()
    files = []
    for name in names:
        # basic safety: no directory traversal and must be .pb
        if "/" in name or ".." in name or not name.endswith(".pb"):
            continue
        p = base / name
        if p.exists() and p.is_file():
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
