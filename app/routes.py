import io
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    render_template,
    request,
    send_file,
    url_for,
)

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

# Cache for comments aggregation
_COMMENTS_CACHE: Optional[
    Tuple[Dict[str, List[str]], List[Tuple[str, int, List[str]]]]
] = None
_COMMENTS_SIGNATURE: Optional[str] = None

# Cache for statistics aggregation
_STATS_CACHE: Optional[Tuple[Dict[str, Any], Dict[str, Any]]] = None
_STATS_SIGNATURE: Optional[str] = None


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


def _format_short_number(n: float) -> str:
    """Format large numbers into a short human-readable string (e.g., 1.2K, 3.4M).
    Uses base 1000 units and rounds to one decimal when needed.
    """
    try:
        num = float(n)
    except Exception:
        return "—"
    neg = num < 0
    num = abs(num)
    units = ["", "K", "M", "B", "T", "Q"]
    i = 0
    while num >= 1000 and i < len(units) - 1:
        num /= 1000.0
        i += 1
    # Use no decimals for small integers, one decimal otherwise
    if num >= 100 or abs(num - round(num)) < 1e-6:
        s = f"{int(round(num))}{units[i]}"
    else:
        s = f"{num:.1f}{units[i]}"
    return f"-{s}" if neg else s


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
    selected_count = 0
    has_selected_col = False
    try:
        selected_flags = []
        for p in projects.values():
            if "selected" in p:
                has_selected_col = True
            selected_flags.append(str(p.get("selected", "0")).strip())
        all_selected = len(selected_flags) > 0 and all(v == "1" for v in selected_flags)
        sum_selected_cost = 0
        for p in projects.values():
            if str(p.get("selected", "0")).strip() == "1":
                selected_count += 1
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
        selected_count = 0

    # experimental flag from META, if present
    experimental = str(meta.get("experimental", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

    # normalize city label (use unit/city/district unified as "city" for filters)
    city_label = unit

    # Determine election year from date_begin (dd.mm.yyyy or yyyy), fallback to instance/year
    year_str = ""
    year_int: Optional[int] = None
    try:
        date_begin = str(meta.get("date_begin", "")).strip()
        if date_begin:
            # look for a 4-digit year anywhere in the string
            import re

            m = re.search(r"(\d{4})", date_begin)
            if m:
                y = int(m.group(1))
                if 1900 <= y <= 2100:
                    year_int = y
                    year_str = str(y)
        if year_int is None:
            # fallback: try instance or meta year directly
            for cand in [meta.get("year"), instance]:
                if cand is None:
                    continue
                s = str(cand).strip()
                if s.isdigit():
                    y = int(s)
                    if 1900 <= y <= 2100:
                        year_int = y
                        year_str = str(y)
                        break
    except Exception:
        year_int = None
        year_str = ""

    # quality metric: (avg vote length)^3 * (num_projects)^2 * (num_votes)
    vlen = vote_length_float or 0.0
    quality = (vlen**3) * (float(num_projects) ** 2) * float(num_votes)
    quality_short = _format_short_number(quality)

    # extra meta fields potentially useful in UI
    rule_raw = str(meta.get("rule", "")).strip()
    edition = str(meta.get("edition", "")).strip()
    language = str(meta.get("language", "")).strip()

    return {
        "file_name": pb_path.name,
        "title": title,
        "webpage_name": webpage_name,
        "description": description,
        "currency": currency,
        "num_votes": _format_int(num_votes),
        "num_votes_raw": num_votes,
        "num_projects": _format_int(num_projects),
        "num_projects_raw": num_projects,
        "num_selected_projects": _format_int(selected_count),
        "num_selected_projects_raw": selected_count,
        "budget": budget_str,
        "budget_raw": budget,
        "vote_type": vote_type,
        "vote_length": vote_length,
        "vote_length_raw": vote_length_float,
        "country": country,
        "city": city_label,
        "year": year_str,
        "year_raw": year_int,
        "fully_funded": fully_funded,
        "has_selected_col": has_selected_col,
        "experimental": experimental,
        "quality": quality,
        "quality_short": quality_short,
        "rule_raw": rule_raw,
        "edition": edition,
        "language": language,
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


def _extract_comments_from_meta(meta: Dict[str, Any]) -> List[str]:
    """
    Parse comments from META string values following the pattern:
    comment;#1: text. #2: text. ...
    Returns a list of comment strings in index order. If no comments, returns [].
    """
    raw = str(meta.get("comment", "")).strip()
    if not raw:
        return []
    # Normalize separators and ensure consistent spaces
    s = raw.replace("\n", " ")
    parts: List[str] = []
    current = []
    expecting = 1
    i = 0
    # Simple state machine: find occurrences of #n: and capture until next #n+1:
    while True:
        marker = f"#{expecting}:"
        next_marker = f"#{expecting + 1}:"
        start = s.find(marker)
        if start == -1:
            # If no #1: but the whole string exists, take as single comment
            if expecting == 1 and s:
                text = s
                text = text.strip().strip(";.")
                if text:
                    parts.append(text)
            break
        start_text = start + len(marker)
        end = s.find(next_marker, start_text)
        if end == -1:
            chunk = s[start_text:]
        else:
            chunk = s[start_text:end]
        text = chunk.strip().strip(";.")
        if text:
            parts.append(text)
        expecting += 1
        if end == -1:
            break
    return parts


def _aggregate_comments_cached() -> (
    Tuple[Dict[str, List[str]], List[Tuple[str, int, List[str]]]]
):
    """
    Returns a tuple:
    - mapping from comment text -> list of filenames using it
    - list of tuples (comment, count, files) sorted by count desc then comment asc
    Uses a cache based on files signature.
    """
    global _COMMENTS_CACHE, _COMMENTS_SIGNATURE
    folder = _pb_folder()
    files = sorted(folder.glob("*.pb"))
    signature = _compute_signature(files)
    if _COMMENTS_CACHE is not None and signature == _COMMENTS_SIGNATURE:
        return _COMMENTS_CACHE

    mapping: Dict[str, List[str]] = {}
    for p in files:
        try:
            lines = _read_file_lines(p)
            meta, _, _, _, _ = parse_pb_lines(lines)
            comments = _extract_comments_from_meta(meta)
            for c in comments:
                mapping.setdefault(c, []).append(p.name)
        except Exception:
            # Ignore broken files for comments aggregation
            continue

    # Create sorted list
    rows: List[Tuple[str, int, List[str]]] = []
    for c, flist in mapping.items():
        rows.append((c, len(flist), sorted(flist)))
    rows.sort(key=lambda t: (-t[1], t[0].lower()))

    _COMMENTS_CACHE = (mapping, rows)
    _COMMENTS_SIGNATURE = signature
    return _COMMENTS_CACHE


def _aggregate_statistics_cached() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Compute and cache aggregate statistics across all PB files based on tile data.
    Returns (totals, series) where totals is a dict of overall counters and series
    contains grouped data for charts.
    """
    global _STATS_CACHE, _STATS_SIGNATURE
    folder = _pb_folder()
    files = sorted(folder.glob("*.pb"))
    signature = _compute_signature(files)
    if _STATS_CACHE is not None and signature == _STATS_SIGNATURE:
        return _STATS_CACHE

    tiles = _get_tiles_cached()
    total_files = len(tiles)
    countries = set()
    cities = set()  # (country, city)
    sum_projects = 0
    sum_votes = 0
    sum_selected = 0
    sum_budget = 0
    budget_by_currency_total: Dict[str, int] = {}

    by_year: Dict[str, int] = {}
    votes_by_country: Dict[str, int] = {}
    budget_by_country: Dict[str, int] = {}
    budget_by_country_by_currency: Dict[str, Dict[str, int]] = {}
    vote_types: Dict[str, int] = {}
    votes_by_city: Dict[str, int] = {}

    for t in tiles:
        country = t.get("country") or ""
        city = t.get("city") or ""
        year = t.get("year_raw")
        num_projects = int(t.get("num_projects_raw") or 0)
        num_votes = int(t.get("num_votes_raw") or 0)
        num_selected = int(t.get("num_selected_projects_raw") or 0)
        budget = t.get("budget_raw")
        currency = (t.get("currency") or "").strip() or "—"
        vtype = (t.get("vote_type") or "").strip().lower() or "unknown"

        if country:
            countries.add(country)
        if country or city:
            cities.add((country, city))

        sum_projects += num_projects
        sum_votes += num_votes
        sum_selected += num_selected
        if isinstance(budget, int):
            sum_budget += budget
            budget_by_currency_total[currency] = (
                budget_by_currency_total.get(currency, 0) + budget
            )

        if year is not None:
            by_year[str(year)] = by_year.get(str(year), 0) + 1
        if country:
            votes_by_country[country] = votes_by_country.get(country, 0) + num_votes
            if isinstance(budget, int):
                budget_by_country[country] = budget_by_country.get(country, 0) + budget
                by_cur = budget_by_country_by_currency.setdefault(currency, {})
                by_cur[country] = by_cur.get(country, 0) + budget
        vote_types[vtype] = vote_types.get(vtype, 0) + 1

        label = f"{country} – {city}".strip(" –")
        votes_by_city[label] = votes_by_city.get(label, 0) + num_votes

    totals: Dict[str, Any] = {
        "total_files": total_files,
        "total_countries": len(countries),
        "total_cities": len(cities),
        "total_projects": sum_projects,
        "total_votes": sum_votes,
        "total_funded_projects": sum_selected,
        "total_budget": sum_budget,
        "budget_by_currency": budget_by_currency_total,
    }

    # Prepare series as sorted arrays for charts
    series_files_per_year = [{"label": y, "value": c} for y, c in by_year.items()]
    try:
        series_files_per_year.sort(key=lambda d: int(d["label"]))
    except Exception:
        series_files_per_year.sort(key=lambda d: str(d["label"]))

    series: Dict[str, Any] = {
        "files_per_year": series_files_per_year,
        "votes_per_country": sorted(
            [{"label": k, "value": v} for k, v in votes_by_country.items()],
            key=lambda d: d["value"],
            reverse=True,
        ),
        "budget_per_country": sorted(
            [{"label": k, "value": v} for k, v in budget_by_country.items()],
            key=lambda d: d["value"],
            reverse=True,
        ),
        "budget_per_country_by_currency": {
            cur: sorted(
                [{"label": k, "value": v} for k, v in by_cur.items()],
                key=lambda d: d["value"],
                reverse=True,
            )
            for cur, by_cur in budget_by_country_by_currency.items()
        },
        "available_currencies": sorted(
            list(budget_by_currency_total.keys()), key=lambda s: (s == "—", s)
        ),
        "vote_types": sorted(
            [{"label": k, "value": v} for k, v in vote_types.items()],
            key=lambda d: d["value"],
            reverse=True,
        ),
        "top_cities_by_votes": sorted(
            [{"label": k, "value": v} for k, v in votes_by_city.items()],
            key=lambda d: d["value"],
            reverse=True,
        )[:15],
    }

    _STATS_CACHE = (totals, series)
    _STATS_SIGNATURE = signature
    return _STATS_CACHE


@bp.route("/")
def home():
    folder = _pb_folder()
    if not folder.exists():
        abort(404, description="PB files folder not found")

    tiles = _get_tiles_cached()
    # Pre-warm comments aggregation so navigating to Comments is instant
    try:
        _aggregate_comments_cached()
    except Exception:
        # Non-fatal if comments pre-warm fails
        pass
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


@bp.route("/comments")
def comments_page():
    _map, rows = _aggregate_comments_cached()
    return render_template("comments.html", rows=rows, total=len(rows))


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
        "funded": _format_int(totals.get("total_funded_projects", 0)),
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


def _is_safe_filename(name: str) -> bool:
    # basic safety for path traversal and extension
    return (
        name.endswith(".pb")
        and ".." not in name
        and not name.startswith("/")
        and "/" not in name
        and "\\" not in name
    )


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
    path = _pb_folder() / filename
    if not path.exists() or not path.is_file():
        abort(404)

    # Parse file
    try:
        lines = _read_file_lines(path)
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


@bp.route("/preview-snippet/<path:filename>")
def preview_snippet(filename: str):
    """Return a small, plain-text preview of the PB file (first N lines)."""
    if not _is_safe_filename(filename):
        abort(400, description="Invalid filename")
    path = _pb_folder() / filename
    if not path.exists() or not path.is_file():
        abort(404)

    # Number of lines to include; default 80, cap 400
    try:
        n = int(request.args.get("lines", "80"))
    except Exception:
        n = 80
    n = max(1, min(n, 400))

    try:
        lines = _read_file_lines(path)[:n]
        text = "\n".join(lines)
    except Exception as e:
        abort(400, description=f"Failed to read file: {e}")

    return Response(text, mimetype="text/plain; charset=utf-8")
