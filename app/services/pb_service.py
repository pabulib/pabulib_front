from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..db import get_session
from ..models import PBComment, PBFile, RefreshState
from ..utils.formatting import (
    format_budget,
    format_int,
    format_short_number,
    format_vote_length,
)

# Optional optimization helper (load_only)
try:  # pragma: no cover
    from sqlalchemy.orm import load_only as _sa_load_only  # type: ignore
except Exception:  # pragma: no cover
    _sa_load_only = None  # type: ignore


_TILES_CACHE: Optional[List[Dict[str, Any]]] = None
_COMMENTS_CACHE: Optional[
    Tuple[
        Dict[str, List[str]],
        List[Tuple[str, int, List[str]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
    ]
] = None
_STATS_CACHE: Optional[Tuple[Dict[str, Any], Dict[str, Any]]] = None


def _db_signature() -> Optional[str]:
    try:
        with get_session() as s:
            rs = s.get(RefreshState, "pb")
            return (
                rs.last_completed_at.isoformat()
                if rs and rs.last_completed_at
                else None
            )
    except Exception:
        return None


def invalidate_caches() -> None:
    global _TILES_CACHE, _COMMENTS_CACHE, _STATS_CACHE
    _TILES_CACHE = None
    _COMMENTS_CACHE = None
    _STATS_CACHE = None


def get_tiles_cached() -> List[Dict[str, Any]]:
    global _TILES_CACHE
    db_sig = _db_signature()
    if _TILES_CACHE is not None and getattr(_TILES_CACHE, "_db_sig", None) == db_sig:
        return _TILES_CACHE

    with get_session() as s:
        rows = (
            s.query(
                PBFile.file_name,
                PBFile.webpage_name,
                PBFile.description,
                PBFile.currency,
                PBFile.num_votes,
                PBFile.num_projects,
                PBFile.num_selected_projects,
                PBFile.budget,
                PBFile.vote_type,
                PBFile.vote_length,
                PBFile.country,
                PBFile.unit,
                PBFile.year,
                PBFile.fully_funded,
                PBFile.experimental,
                PBFile.quality,
                PBFile.rule_raw,
                PBFile.edition,
                PBFile.language,
                PBFile.instance,
                PBFile.subunit,
            )
            .filter(PBFile.is_current == True)  # noqa: E712
            .order_by(
                PBFile.country,
                PBFile.unit,
                PBFile.instance,
                PBFile.subunit,
                PBFile.file_name,
            )
            .all()
        )

    tiles: List[Dict[str, Any]] = []
    for r in rows:
        (
            file_name,
            webpage_name,
            description,
            currency,
            num_votes,
            num_projects,
            num_selected_projects,
            budget,
            vote_type,
            vote_length,
            country,
            unit,
            year,
            fully_funded,
            experimental,
            quality,
            rule_raw,
            edition,
            language,
            instance,
            subunit,
        ) = r
        tiles.append(
            {
                "file_name": file_name,
                "title": webpage_name or file_name.replace("_", " "),
                "webpage_name": webpage_name or "",
                "description": description or "",
                "currency": currency or "",
                "num_votes": format_int(int(num_votes or 0)),
                "num_votes_raw": int(num_votes or 0),
                "num_projects": format_int(int(num_projects or 0)),
                "num_projects_raw": int(num_projects or 0),
                "num_selected_projects": format_int(int(num_selected_projects or 0)),
                "num_selected_projects_raw": int(num_selected_projects or 0),
                "budget": (
                    format_budget(currency or "", int(budget or 0))
                    if budget is not None
                    else "—"
                ),
                "budget_raw": budget,
                "vote_type": vote_type or "",
                "vote_length": format_vote_length(vote_length),
                "vote_length_raw": vote_length,
                "country": country or "",
                "city": unit or "",
                "year": str(year) if year is not None else "",
                "year_raw": year,
                "fully_funded": bool(fully_funded),
                "experimental": bool(experimental),
                "quality": quality or 0.0,
                "quality_short": format_short_number(quality or 0.0),
                "rule_raw": rule_raw or "",
                "edition": edition or "",
                "language": language or "",
                "comments": [],
                "country_raw": country or "",
                "unit_raw": unit or "",
                "instance_raw": instance or "",
            }
        )

    try:
        setattr(tiles, "_db_sig", db_sig)
    except Exception:
        pass
    _TILES_CACHE = tiles
    return _TILES_CACHE


def aggregate_comments_cached() -> Tuple[
    Dict[str, List[str]],
    List[Tuple[str, int, List[str]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
]:
    global _COMMENTS_CACHE
    db_sig = _db_signature()
    if (
        _COMMENTS_CACHE is not None
        and getattr(_COMMENTS_CACHE, "_db_sig", None) == db_sig
    ):
        return _COMMENTS_CACHE

    with get_session() as s:
        q = (
            s.query(
                PBComment.text,
                PBFile.file_name,
                PBFile.country,
                PBFile.unit,
                PBFile.instance,
            )
            .join(PBFile, PBFile.id == PBComment.file_id)
            .filter(PBFile.is_current == True)  # noqa: E712
            .filter(PBComment.is_active == True)  # noqa: E712
        )
        rows = q.all()

    mapping: Dict[str, List[str]] = {}
    groups_temp_country: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit_instance: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for ctext, fname, country, unit, instance in rows:
        c = (ctext or "").strip()
        country = (country or "").strip()
        unit = (unit or "").strip()
        instance = (instance or "").strip()
        if not c:
            continue
        mapping.setdefault(c, []).append(fname)
        if country:
            cm_c = groups_temp_country.setdefault(c, {})
            key_c = country.lower()
            bucket_c = cm_c.setdefault(key_c, {"label": country, "files": []})
            bucket_c["files"].append(fname)
        if country or unit:
            cm_cu = groups_temp_country_unit.setdefault(c, {})
            key_cu = f"{country.lower()}::{unit.lower()}"
            label_cu = f"{country} – {unit}".strip(" –") if (country or unit) else "—"
            bucket_cu = cm_cu.setdefault(key_cu, {"label": label_cu, "files": []})
            bucket_cu["files"].append(fname)
        if country or unit or instance:
            cm_cui = groups_temp_country_unit_instance.setdefault(c, {})
            key_cui = f"{country.lower()}::{unit.lower()}::{instance.lower()}"
            label_cui = (
                f"{country} – {unit} – {instance}".strip(" –")
                if (country or unit or instance)
                else "—"
            )
            bucket_cui = cm_cui.setdefault(key_cui, {"label": label_cui, "files": []})
            bucket_cui["files"].append(fname)

    rows_list: List[Tuple[str, int, List[str]]] = []
    for c, flist in mapping.items():
        rows_list.append((c, len(flist), sorted(flist)))
    rows_list.sort(key=lambda t: (-t[1], t[0].lower()))

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
            items.sort(
                key=lambda d: (-int(d.get("count", 0)), str(d.get("label", "")).lower())
            )
            out[c] = items
        return out

    _COMMENTS_CACHE = (
        mapping,
        rows_list,
        finalize_groups(groups_temp_country),
        finalize_groups(groups_temp_country_unit),
        finalize_groups(groups_temp_country_unit_instance),
    )
    try:
        setattr(_COMMENTS_CACHE, "_db_sig", db_sig)  # type: ignore[attr-defined]
    except Exception:
        pass
    return _COMMENTS_CACHE


def aggregate_statistics_cached() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    global _STATS_CACHE
    db_sig = _db_signature()
    if _STATS_CACHE is not None and getattr(_STATS_CACHE, "_db_sig", None) == db_sig:
        return _STATS_CACHE

    with get_session() as s:
        q = s.query(PBFile)
        if _sa_load_only:
            q = q.options(
                _sa_load_only(
                    PBFile.country,
                    PBFile.unit,
                    PBFile.year,
                    PBFile.num_projects,
                    PBFile.num_votes,
                    PBFile.num_selected_projects,
                    PBFile.budget,
                    PBFile.currency,
                    PBFile.vote_type,
                )
            )
        pb_files: List[PBFile] = q.filter(PBFile.is_current == True).all()  # noqa: E712

        # Process data while still within the session context
        total_files = len(pb_files)
        countries = set()
        cities = set()
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

        for r in pb_files:
            country = r.country or ""
            city = r.unit or ""
            year = r.year
            num_projects = int(r.num_projects or 0)
            num_votes = int(r.num_votes or 0)
            num_selected = int(r.num_selected_projects or 0)
            budget = r.budget
            currency = (r.currency or "").strip() or "—"
            vtype = (r.vote_type or "").strip().lower() or "unknown"

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
                    budget_by_country[country] = (
                        budget_by_country.get(country, 0) + budget
                    )
                    by_cur = budget_by_country_by_currency.setdefault(currency, {})
                    by_cur[country] = by_cur.get(country, 0) + budget
            vote_types[vtype] = vote_types.get(vtype, 0) + 1

            label = f"{country} – {city}".strip(" –")
            votes_by_city[label] = votes_by_city.get(label, 0) + num_votes

    # Process results after session closes
    totals: Dict[str, Any] = {
        "total_files": total_files,
        "total_countries": len(countries),
        "total_cities": len(cities),
        "total_projects": sum_projects,
        "total_votes": sum_votes,
        "total_selected_projects": sum_selected,
        "total_budget": sum_budget,
        "budget_by_currency": budget_by_currency_total,
    }

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
    try:
        setattr(_STATS_CACHE, "_db_sig", db_sig)  # type: ignore[attr-defined]
    except Exception:
        pass
    return _STATS_CACHE


def debug_db_overview() -> Dict[str, Any]:
    try:
        with get_session() as s:
            total_files = s.query(PBFile).count()
            current_files = (
                s.query(PBFile).filter(PBFile.is_current == True).count()
            )  # noqa: E712
            total_comments = s.query(PBComment).count()
            active_comments = (
                s.query(PBComment).filter(PBComment.is_active == True).count()
            )  # noqa: E712
            sample = [
                fn
                for (fn,) in s.query(PBFile.file_name)
                .filter(PBFile.is_current == True)  # noqa: E712
                .order_by(PBFile.file_name)
                .limit(5)
                .all()
            ]
        return {
            "files": {"total": total_files, "current": current_files, "sample": sample},
            "comments": {"total": total_comments, "active": active_comments},
        }
    except Exception as e:
        return {"error": str(e)}


def get_all_current_file_paths() -> List[Tuple[str, Path]]:
    """Return a list of (filename, path) tuples for all current PB files.
    Only includes files where is_current=True and the path exists on disk.
    """
    try:
        with get_session() as s:
            rows = (
                s.query(PBFile.file_name, PBFile.path)
                .filter(PBFile.is_current == True)  # noqa: E712
                .order_by(PBFile.file_name)
                .all()
            )

            result = []
            for file_name, path_str in rows:
                if path_str:
                    path = Path(path_str)
                    if path.exists() and path.is_file():
                        result.append((file_name, path))
            return result
    except Exception:
        return []


def get_current_file_path(filename: str) -> Optional[Path]:
    """Return the absolute file path for the current version of the given file name,
    based on the database record. Returns None if not found or path missing.
    """
    try:
        with get_session() as s:
            r = (
                s.query(PBFile)
                .filter(
                    PBFile.file_name == filename, PBFile.is_current == True
                )  # noqa: E712
                .one_or_none()
            )
            if r and r.path:
                # Access the path attribute while still within the session
                path_str = r.path
                return Path(path_str)
    except Exception:
        return None
    return None
