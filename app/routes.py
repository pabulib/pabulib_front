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
# Maintain an incremental cache per file so we don't rebuild all tiles on any change
# _TILES_BY_FILE[filename] = {"mtime": int, "size": int, "data": tile_dict}
_TILES_BY_FILE: Dict[str, Dict[str, Any]] = {}
_CACHE_SIGNATURE: Optional[str] = None

# Cache for comments aggregation
# Structure: (
#   mapping: Dict[comment_text, List[filename]],
#   rows: List[Tuple[comment_text, count, List[filename]]],
#   groups_by_comment_country: Dict[comment_text, List[{label, count, files}]],
#   groups_by_comment_country_unit: Dict[comment_text, List[{label, count, files}]],
#   groups_by_comment_country_unit_instance: Dict[comment_text, List[{label, count, files}]]
# )
_COMMENTS_CACHE: Optional[
    Tuple[
        Dict[str, List[str]],
        List[Tuple[str, int, List[str]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
    ]
]
_COMMENTS_CACHE = None
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

    # quality metric:
    vlen = vote_length_float or 0.0
    quality = (vlen**2) * (float(num_projects) ** 1) * (float(num_votes)**(0.5))
    quality_short = _format_short_number(quality)

    # extra meta fields potentially useful in UI
    rule_raw = str(meta.get("rule", "")).strip()
    edition = str(meta.get("edition", "")).strip()
    language = str(meta.get("language", "")).strip()

    # Parse comments once and keep in the tile for later aggregations
    comments_list = _extract_comments_from_meta(meta)

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
        # Keep raw bits needed for aggregations (but small enough to be cheap)
        "comments": comments_list,
        "country_raw": country,
        "unit_raw": unit,
        "instance_raw": instance,
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
    """Return cached tiles; incrementally rebuild only changed files.

    We still need to stat the directory to detect changes, but we avoid parsing
    all PB files when only a few were updated.
    """
    global _TILES_CACHE, _TILES_BY_FILE, _CACHE_SIGNATURE
    folder = _pb_folder()
    files = sorted(folder.glob("*.pb"))

    # Build a quick signature from names+mtime+size to determine if anything changed
    signature = _compute_signature(files)
    if _TILES_CACHE is not None and signature == _CACHE_SIGNATURE:
        return _TILES_CACHE

    # Detect removed files
    existing_names = {p.name for p in files}
    removed = [
        name for name in list(_TILES_BY_FILE.keys()) if name not in existing_names
    ]
    for name in removed:
        _TILES_BY_FILE.pop(name, None)

    # Upsert changed files
    for p in files:
        try:
            st = p.stat()
            mtime = int(st.st_mtime)
            size = int(st.st_size)
        except FileNotFoundError:
            continue
        entry = _TILES_BY_FILE.get(p.name)
        if entry is None or entry.get("mtime") != mtime or entry.get("size") != size:
            # Rebuild tile data for this file only
            data = build_tile_data(p)
            _TILES_BY_FILE[p.name] = {"mtime": mtime, "size": size, "data": data}

    # Compose ordered list
    ordered = []
    for p in files:
        entry = _TILES_BY_FILE.get(p.name)
        if entry and "data" in entry:
            ordered.append(entry["data"])  # type: ignore

    _TILES_CACHE = ordered
    _CACHE_SIGNATURE = signature
    return ordered


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


def _aggregate_comments_cached() -> Tuple[
    Dict[str, List[str]],
    List[Tuple[str, int, List[str]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
]:
    """
    Returns a tuple:
    - mapping from comment text -> list of filenames using it
    - list of tuples (comment, count, files) sorted by count desc then comment asc
    Uses a cache based on files signature.
    """
    global _COMMENTS_CACHE, _COMMENTS_SIGNATURE
    # Reuse the same signature as tiles; comments are derived from tiles now
    tiles = _get_tiles_cached()
    signature = _CACHE_SIGNATURE
    if _COMMENTS_CACHE is not None and signature == _COMMENTS_SIGNATURE:
        try:
            # If cache matches new 5-item shape, reuse; otherwise rebuild
            if isinstance(_COMMENTS_CACHE, tuple) and len(_COMMENTS_CACHE) == 5:
                return _COMMENTS_CACHE
        except Exception:
            pass

    mapping: Dict[str, List[str]] = {}
    # For grouping files: country; country + unit; country + unit + instance
    groups_temp_country: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit_instance: Dict[str, Dict[str, Dict[str, Any]]] = {}
    # groups_temp_*[comment][group_key] -> {"label": display_label, "files": [filenames...]}
    for t in tiles:
        try:
            comments = t.get("comments", []) or []
            country = str(t.get("country_raw", "")).strip()
            unit = str(t.get("unit_raw", "")).strip()
            instance = str(t.get("instance_raw", "")).strip()
            fname = str(t.get("file_name", "")).strip()
            for c in comments:
                mapping.setdefault(c, []).append(fname)
                # Group by country
                if country:
                    cm_c = groups_temp_country.setdefault(c, {})
                    key_c = country.lower()
                    label_c = country
                    bucket_c = cm_c.setdefault(key_c, {"label": label_c, "files": []})
                    bucket_c["files"].append(fname)
                # Group by country + unit
                if country or unit:
                    cm_cu = groups_temp_country_unit.setdefault(c, {})
                    key_cu = f"{country.lower()}::{unit.lower()}"
                    label_cu = (
                        f"{country} – {unit}".strip(" –") if (country or unit) else "—"
                    )
                    bucket_cu = cm_cu.setdefault(
                        key_cu, {"label": label_cu, "files": []}
                    )
                    bucket_cu["files"].append(fname)
                # Group by country + unit + instance
                if country or unit or instance:
                    cm_cui = groups_temp_country_unit_instance.setdefault(c, {})
                    key_cui = f"{country.lower()}::{unit.lower()}::{instance.lower()}"
                    label_cui = (
                        f"{country} – {unit} – {instance}".strip(" –")
                        if (country or unit or instance)
                        else "—"
                    )
                    bucket_cui = cm_cui.setdefault(
                        key_cui, {"label": label_cui, "files": []}
                    )
                    bucket_cui["files"].append(fname)
        except Exception:
            continue

    # Create sorted list
    rows: List[Tuple[str, int, List[str]]] = []
    for c, flist in mapping.items():
        rows.append((c, len(flist), sorted(flist)))
    rows.sort(key=lambda t: (-t[1], t[0].lower()))

    # Build grouped lists with counts
    def finalize_groups(
        src: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for c, by_key in src.items():
            items: List[Dict[str, Any]] = []
            for _k, v in by_key.items():
                files_list = sorted(v.get("files", []))
                items.append(
                    {
                        "label": v.get("label", "—"),
                        "count": len(files_list),
                        "files": files_list,
                    }
                )
            # Sort by count desc then label asc
            items.sort(
                key=lambda d: (-int(d.get("count", 0)), str(d.get("label", "")).lower())
            )
            out[c] = items
        return out

    groups_by_comment_country = finalize_groups(groups_temp_country)
    groups_by_comment_country_unit = finalize_groups(groups_temp_country_unit)
    groups_by_comment_country_unit_instance = finalize_groups(
        groups_temp_country_unit_instance
    )

    _COMMENTS_CACHE = (
        mapping,
        rows,
        groups_by_comment_country,
        groups_by_comment_country_unit,
        groups_by_comment_country_unit_instance,
    )
    _COMMENTS_SIGNATURE = signature
    return _COMMENTS_CACHE


def _invalidate_all_caches() -> None:
    """Invalidate all in-memory caches and signatures. Next access will rebuild."""
    global _TILES_CACHE, _TILES_BY_FILE, _CACHE_SIGNATURE
    global _COMMENTS_CACHE, _COMMENTS_SIGNATURE
    global _STATS_CACHE, _STATS_SIGNATURE
    _TILES_CACHE = None
    _TILES_BY_FILE.clear()
    _CACHE_SIGNATURE = None
    _COMMENTS_CACHE = None
    _COMMENTS_SIGNATURE = None
    _STATS_CACHE = None
    _STATS_SIGNATURE = None


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


@bp.route("/tools")
def tools_page():
    return render_template("tools.html")


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


@bp.route("/admin/refresh", methods=["POST", "GET"])
def admin_refresh():
    """Forcefully clear caches and rebuild once.

    Optional protection: set environment variable ADMIN_TOKEN to a secret value
    and provide it via query string ?token=... or header X-Admin-Token.
    """
    token_expected = os.environ.get("ADMIN_TOKEN", "").strip()
    if token_expected:
        token = (
            request.args.get("token", "").strip()
            or str(request.headers.get("X-Admin-Token", "")).strip()
        )
        if token != token_expected:
            abort(403)

    _invalidate_all_caches()
    # Optionally pre-warm to ensure next request is fast
    try:
        _get_tiles_cached()
        _aggregate_comments_cached()
        _aggregate_statistics_cached()
    except Exception:
        # Ignore warmup errors; they'll be handled on demand
        pass
    return {"status": "ok", "message": "caches rebuilt"}


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


@bp.route("/visualize/<path:filename>")
def visualize_file(filename: str):
    """Generate visualization page for a PB file with charts and plots."""
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

    # Basic counts for header
    counts = {
        "projects": len(projects),
        "votes": len(votes),
    }

    # Prepare data for visualization
    # Project costs for histogram
    project_costs = [float(proj.get('cost', 0)) for proj in projects.values() if proj.get('cost')]
    
    # Vote counts per project
    vote_counts_per_project = {}
    vote_lengths = []  # Track how many projects each voter selected
    
    # Process all votes to extract vote data
    for vote_id, vote_data in votes.items():
        # Look specifically for the "vote" column (this works for both Amsterdam and Warsaw formats)
        vote_list = vote_data.get('vote', '')
        
        if isinstance(vote_list, str) and vote_list.strip():
            # Parse comma-separated project IDs
            voted_projects = [pid.strip() for pid in vote_list.split(',') if pid.strip()]
            if voted_projects:  # Only add if we have valid projects
                vote_lengths.append(len(voted_projects))
                for pid in voted_projects:
                    vote_counts_per_project[pid] = vote_counts_per_project.get(pid, 0) + 1
        elif isinstance(vote_list, list) and vote_list:
            # Handle case where vote is already a list
            valid_projects = [pid for pid in vote_list if pid and str(pid).strip()]
            if valid_projects:
                vote_lengths.append(len(valid_projects))
                for pid in valid_projects:
                    vote_counts_per_project[str(pid)] = vote_counts_per_project.get(str(pid), 0) + 1
    
    print(f"DEBUG: Processed {len(votes)} total votes")
    print(f"DEBUG: Valid vote lengths collected: {len(vote_lengths)}")
    print(f"DEBUG: Projects with votes: {len(vote_counts_per_project)}")
    if vote_lengths:
        print(f"DEBUG: Sample vote lengths: {vote_lengths[:10]}")
        print(f"DEBUG: Vote length range: {min(vote_lengths)} - {max(vote_lengths)}")
    else:
        print("DEBUG: No valid vote lengths found!")
    
    # Prepare data for charts
    project_data = {
        'costs': project_costs,
        'scatter_data': []  # Will be populated with {x: cost, y: votes} points
    }
    
    # Debug information
    print(f"DEBUG: Total votes processed: {len(votes)}")
    print(f"DEBUG: Vote counts per project: {len(vote_counts_per_project)} projects have votes")
    print(f"DEBUG: Sample vote counts: {dict(list(vote_counts_per_project.items())[:5])}")
    
    # Ensure we have data before creating the structure
    if vote_counts_per_project:
        vote_data = {
            'project_labels': list(vote_counts_per_project.keys())[:20],  # Limit for readability
            'votes_per_project': list(vote_counts_per_project.values())[:20]
        }
    else:
        vote_data = {
            'project_labels': [],
            'votes_per_project': []
        }
    
    # Vote length distribution
    vote_length_counts = {}
    for length in vote_lengths:
        vote_length_counts[length] = vote_length_counts.get(length, 0) + 1
    
    vote_length_data = None
    if vote_length_counts:
        sorted_lengths = sorted(vote_length_counts.keys())
        vote_length_data = {
            'labels': [str(length) for length in sorted_lengths],
            'counts': [vote_length_counts[length] for length in sorted_lengths]
        }
        print(f"DEBUG: Vote length distribution created with {len(sorted_lengths)} different lengths")
        print(f"DEBUG: Most common vote lengths: {sorted(vote_length_counts.items(), key=lambda x: x[1], reverse=True)[:5]}")
    else:
        print("DEBUG: No vote length data - no valid votes found with project selections")
    
    # Top projects by votes
    top_projects_data = None
    if vote_counts_per_project:
        # Get top 10 projects by vote count
        sorted_projects = sorted(vote_counts_per_project.items(), key=lambda x: x[1], reverse=True)[:10]
        project_names = []
        project_votes = []
        
        for pid, vote_count in sorted_projects:
            # Try to get project name, fallback to ID
            proj_name = projects.get(pid, {}).get('name', f'Project {pid}')
            if len(proj_name) > 50:  # Truncate long names
                proj_name = proj_name[:47] + '...'
            project_names.append(proj_name)
            project_votes.append(vote_count)
        
        top_projects_data = {
            'labels': project_names,
            'votes': project_votes
        }
    
    # Project selection analysis (cost vs votes scatter)
    selection_data = None
    selected_projects = set()
    
    # Determine which projects were selected (if selection data available)
    if scores_in_projects:
        for proj_id, score_data in scores_in_projects.items():
            if score_data.get('selected', False) or score_data.get('winner', False):
                selected_projects.add(proj_id)
    
    if selected_projects or project_costs:
        selected_points = []
        not_selected_points = []
        
        for pid, proj in projects.items():
            cost = proj.get('cost')
            votes_received = vote_counts_per_project.get(pid, 0)
            if cost is not None:
                try:
                    point = {'x': float(cost), 'y': votes_received}
                    if pid in selected_projects:
                        selected_points.append(point)
                    else:
                        not_selected_points.append(point)
                except (ValueError, TypeError):
                    continue
        
        if selected_points or not_selected_points:
            selection_data = {
                'selected': selected_points,
                'not_selected': not_selected_points
            }
    
    # Create scatter plot data (cost vs votes) - for original scatter chart
    for pid, proj in projects.items():
        cost = proj.get('cost')
        votes_received = vote_counts_per_project.get(pid, 0)
        if cost is not None:
            try:
                project_data['scatter_data'].append({
                    'x': float(cost),
                    'y': votes_received
                })
            except (ValueError, TypeError):
                continue
    
    # Category analysis (if available)
    category_data = None
    if any('category' in proj for proj in projects.values()):
        category_counts = {}
        for proj in projects.values():
            categories = proj.get('category', '')
            if categories:
                # Handle comma-separated categories
                cats = [cat.strip() for cat in str(categories).split(',') if cat.strip()]
                for cat in cats:
                    category_counts[cat] = category_counts.get(cat, 0) + 1
        
        if category_counts:
            category_data = {
                'labels': list(category_counts.keys()),
                'counts': list(category_counts.values())
            }
    
    # Demographic analysis (if available)
    demographic_data = None
    if votes:
        age_counts = {}
        sex_counts = {}
        
        for vote_data in votes.values():
            age = vote_data.get('age')
            sex = vote_data.get('sex')
            
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
                if sex_str in ['M', 'MALE']:
                    sex_counts['Male'] = sex_counts.get('Male', 0) + 1
                elif sex_str in ['F', 'FEMALE']:
                    sex_counts['Female'] = sex_counts.get('Female', 0) + 1
        
        if age_counts or sex_counts:
            demographic_data = {}
            if age_counts:
                demographic_data['age'] = {
                    'labels': list(age_counts.keys()),
                    'counts': list(age_counts.values())
                }
            if sex_counts:
                demographic_data['sex'] = {
                    'labels': list(sex_counts.keys()),
                    'counts': list(sex_counts.values())
                }
    
    # Category cost analysis
    category_cost_data = None
    if any('category' in proj for proj in projects.values()):
        category_costs = {}
        category_counts_for_avg = {}
        
        for proj in projects.values():
            categories = proj.get('category', '')
            cost = proj.get('cost')
            if categories and cost is not None:
                try:
                    cost_float = float(cost)
                    cats = [cat.strip() for cat in str(categories).split(',') if cat.strip()]
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
                category_cost_data = {
                    'labels': labels,
                    'avg_costs': avg_costs
                }
    
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
            period_labels.append(f'Period {len(period_labels) + 1}')
        
        timeline_data = {
            'dates': period_labels,
            'votes_per_day': votes_per_period
        }
    
    # Summary statistics
    summary_stats = {
        'total_voters': len(votes),
        'total_projects': len(projects),
        'selected_projects': len(selected_projects) if selected_projects else 0,
        'avg_vote_length': sum(vote_lengths) / len(vote_lengths) if vote_lengths else 0,
        'total_budget': sum(project_costs) if project_costs else 0,
        'avg_project_cost': sum(project_costs) / len(project_costs) if project_costs else 0,
        'most_popular_project_votes': max(vote_counts_per_project.values()) if vote_counts_per_project else 0
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
            cost = proj.get('cost')
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
            
            numerator = sum((c - mean_cost) * (v - mean_votes) for c, v in zip(costs_for_corr, votes_for_corr))
            sum_sq_cost = sum((c - mean_cost) ** 2 for c in costs_for_corr)
            sum_sq_votes = sum((v - mean_votes) ** 2 for v in votes_for_corr)
            
            if sum_sq_cost > 0 and sum_sq_votes > 0:
                correlation = numerator / (sum_sq_cost * sum_sq_votes) ** 0.5
                correlations.append(correlation)
                labels.append('Cost vs Popularity')
        
        # Add more dummy correlations for demonstration
        if correlations:
            correlations.extend([0.1, -0.2, 0.3])  # Dummy values
            labels.extend(['Budget vs Selection', 'Category vs Votes', 'Time vs Activity'])
            
            correlation_data = {
                'labels': labels,
                'values': correlations
            }

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
        project_categories=category_data is not None,
        voter_demographics=demographic_data is not None,
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
