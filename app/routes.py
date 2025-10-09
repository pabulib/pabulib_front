import io
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Blueprint, abort, current_app, render_template, request, send_file

from .utils.load_pb_file import parse_pb_lines

bp = Blueprint(
    "main",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


# Simple in-process cache so we don't parse 1000+ files every request
_TILES_CACHE: Optional[List[Dict[str, Any]]] = None
_CACHE_SIGNATURE: Optional[str] = None


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

    vote_type = str(meta.get("vote_type", meta.get("rule", ""))).lower()

    # vote length: average number of items chosen per voter from VOTES section
    vote_length_float = None
    try:
        lengths = []
        for v in votes.values():
            sel = str(v.get("vote", "")).strip()
            if not sel:
                continue
            lengths.append(len([s for s in sel.split(",") if s]))
        if lengths:
            vote_length_float = sum(lengths) / len(lengths)
        vote_length = (
            f"{vote_length_float:.3f}" if vote_length_float is not None else "—"
        )
    except Exception:
        vote_length = "—"
        vote_length_float = None

    # fully funded heuristic: if all projects are selected OR sum(selected costs) >= budget
    fully_funded = False
    try:
        selected_flags = [
            str(p.get("selected", "0")).strip() for p in projects.values()
        ]
        all_selected = len(selected_flags) > 0 and all(v == "1" for v in selected_flags)
        sum_selected_cost = 0
        for p in projects.values():
            if str(p.get("selected", "0")).strip() == "1":
                c = p.get("cost")
                if isinstance(c, str) and c.isdigit():
                    sum_selected_cost += int(c)
                elif isinstance(c, int):
                    sum_selected_cost += c
        fully_funded = all_selected or (
            budget is not None and sum_selected_cost >= budget
        )
    except Exception:
        fully_funded = False

    # experimental flag from META, if present
    experimental = str(meta.get("experimental", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

    # normalize city label (use unit/city/district unified as "city" for filters)
    city_label = unit

    # parse instance to int if possible (for year filtering)
    try:
        year_int = int(instance)
    except Exception:
        year_int = None

    # quality metric: (avg vote length)^3 * (num_projects)^2 * (num_votes)
    vlen = vote_length_float or 0.0
    quality = (vlen**3) * (float(num_projects) ** 2) * float(num_votes)

    return {
        "file_name": pb_path.name,
        "title": title,
        "webpage_name": webpage_name,
        "description": description,
        "num_votes": _format_int(num_votes),
        "num_votes_raw": num_votes,
        "num_projects": _format_int(num_projects),
        "num_projects_raw": num_projects,
        "budget": budget_str,
        "budget_raw": budget,
        "vote_type": vote_type,
        "vote_length": vote_length,
        "vote_length_raw": vote_length_float,
        "country": country,
        "city": city_label,
        "year": instance,
        "year_raw": year_int,
        "fully_funded": fully_funded,
        "experimental": experimental,
        "quality": quality,
    }


def _compute_signature(paths: List[Path]) -> str:
    # A simple signature based on names + mtimes + sizes
    parts = []
    for p in paths:
        try:
            st = p.stat()
            parts.append(f"{p.name}:{int(st.st_mtime)}:{st.st_size}")
        except FileNotFoundError:
            continue
    return "|".join(sorted(parts))


def _get_tiles_cached() -> List[Dict[str, Any]]:
    global _TILES_CACHE, _CACHE_SIGNATURE
    folder = _pb_folder()
    files = sorted(folder.glob("*.pb"))
    signature = _compute_signature(files)
    if _TILES_CACHE is None or signature != _CACHE_SIGNATURE:
        _TILES_CACHE = [build_tile_data(p) for p in files]
        _CACHE_SIGNATURE = signature
    return _TILES_CACHE


@bp.route("/")
def home():
    folder = _pb_folder()
    if not folder.exists():
        abort(404, description="PB files folder not found")

    tiles = _get_tiles_cached()
    return render_template("index.html", tiles=tiles, count=len(tiles))


@bp.route("/format")
def format_page():
    return render_template("format.html")


@bp.route("/code")
def code_page():
    return render_template("code.html")


@bp.route("/publications")
def publications_page():
    return render_template("publications.html")


@bp.route("/about")
def about_page():
    return render_template("about.html")


@bp.route("/contact")
def contact_page():
    return render_template("contact.html")


# @bp.route("/upload")
# def upload_page():
#     return "To be implemented"
# Placeholder page for now; can be implemented later
# return render_template(
#     "index.html", tiles=_get_tiles_cached(), count=len(_get_tiles_cached())
# )


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
