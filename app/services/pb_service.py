from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, asc, desc, func, or_
from ..db import get_session
from ..models import (
    CheckerValidationCache,
    PBCategory,
    PBBeneficiary,
    PBComment,
    PBFile,
    RefreshState,
)
from ..utils.formatting import (
    format_budget,
    format_int,
    format_short_number,
    format_vote_length,
)
from ..utils.load_pb_file import parse_pb_lines as _parse_pb_lines
from ..utils.search_normalization import build_search_text_norm, fold_search_text
from ..utils.validation import (
    checker_public_label,
    checker_public_short_label,
    checker_public_status,
    checker_public_tooltip,
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
_CATEGORIES_CACHE: Optional[
    Tuple[
        Dict[str, List[str]],
        List[Tuple[str, int, List[str]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
    ]
] = None
_BENEFICIARIES_CACHE: Optional[
    Tuple[
        Dict[str, List[str]],
        List[Tuple[str, int, List[str]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
    ]
] = None
_RULES_CACHE: Optional[
    Tuple[
        Dict[str, List[str]],
        List[Tuple[str, int, List[str]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
        Dict[str, List[Dict[str, Any]]],
    ]
] = None
_CITY_SLUG_CACHE: Optional[Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]] = None

_SEARCH_ORDER_COLUMNS = {
    "quality": PBFile.quality,
    "votes": PBFile.num_votes,
    "num_votes": PBFile.num_votes,
    "projects": PBFile.num_projects,
    "num_projects": PBFile.num_projects,
    "budget": PBFile.budget,
    "year": PBFile.year,
    "vote_length": PBFile.vote_length,
}

_NEW_FILE_WINDOW_DAYS = 183


def _new_file_cutoff() -> datetime:
    return datetime.utcnow() - timedelta(days=_NEW_FILE_WINDOW_DAYS)


def build_pbfile_search_text_norm(
    file_name: Optional[str],
    webpage_name: Optional[str],
    description: Optional[str],
    country: Optional[str],
    unit: Optional[str],
    instance: Optional[str],
    subunit: Optional[str],
) -> str:
    return build_search_text_norm(
        file_name,
        webpage_name,
        description,
        country,
        unit,
        instance,
        subunit,
    )


def compute_is_new_value(first_ingested_at: Optional[datetime]) -> bool:
    return bool(first_ingested_at and first_ingested_at >= _new_file_cutoff())


def compute_is_first_addition(
    session, file_name: Optional[str], webpage_name: Optional[str]
) -> bool:
    predicates = []
    clean_file_name = (file_name or "").strip()
    clean_webpage_name = (webpage_name or "").strip()
    if clean_file_name:
        predicates.append(PBFile.file_name == clean_file_name)
    if clean_webpage_name:
        predicates.append(PBFile.webpage_name == clean_webpage_name)
    if not predicates:
        return True
    return session.query(PBFile.id).filter(or_(*predicates)).first() is None


def backfill_pbfile_derived_fields() -> None:
    with get_session() as s:
        rows: List[PBFile] = (
            s.query(PBFile)
            .order_by(PBFile.ingested_at.asc(), PBFile.id.asc())
            .all()
        )
        if not rows:
            return

        seen_file_names = set()
        seen_webpage_names = set()
        derived_changed = False

        for row in rows:
            old_search_text_norm = row.search_text_norm
            old_is_first_addition = row.is_first_addition
            row.search_text_norm = build_pbfile_search_text_norm(
                row.file_name,
                row.webpage_name,
                row.description,
                row.country,
                row.unit,
                row.instance,
                row.subunit,
            )
            file_key = (row.file_name or "").strip().casefold()
            webpage_key = (row.webpage_name or "").strip().casefold()
            prior_exists = bool(file_key and file_key in seen_file_names) or bool(
                webpage_key and webpage_key in seen_webpage_names
            )
            row.is_first_addition = not prior_exists
            if row.search_text_norm != old_search_text_norm or row.is_first_addition != old_is_first_addition:
                derived_changed = True
            if file_key:
                seen_file_names.add(file_key)
            if webpage_key:
                seen_webpage_names.add(webpage_key)

        if derived_changed:
            invalidate_caches()


def _slugify_text(value: str) -> str:
    text = fold_search_text(value).strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def _build_city_slug_maps(cities: List[str]) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    city_to_slug: Dict[str, str] = {}
    slug_to_city: Dict[str, str] = {}
    folded_city_to_city: Dict[str, str] = {}
    for city in sorted({c for c in cities if c}):
        base = _slugify_text(city) or "city"
        slug = base
        idx = 2
        while slug in slug_to_city and slug_to_city[slug] != city:
            slug = f"{base}-{idx}"
            idx += 1
        city_to_slug[city] = slug
        slug_to_city[slug] = city
        folded_city_to_city[city.casefold()] = city
    return city_to_slug, slug_to_city, folded_city_to_city


def _get_city_slug_maps() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    global _CITY_SLUG_CACHE
    if _CITY_SLUG_CACHE is not None:
        return _CITY_SLUG_CACHE

    with get_session() as s:
        cities = [
            r[0]
            for r in s.query(PBFile.unit)
            .filter(PBFile.is_current == True)
            .distinct()
            .all()
            if r[0]
        ]
    city_to_slug, slug_to_city, folded_city_to_city = _build_city_slug_maps(cities)
    _CITY_SLUG_CACHE = (city_to_slug, slug_to_city, folded_city_to_city)
    return _CITY_SLUG_CACHE


def _resolve_city_filter_value(city: Optional[str]) -> Optional[str]:
    if city is None:
        return None
    token = str(city).strip()
    if not token:
        return token

    city_to_slug, slug_to_city, folded_city_to_city = _get_city_slug_maps()
    # Keep old links working: exact city (including diacritics) still resolves.
    exact_city = folded_city_to_city.get(token.casefold())
    if exact_city:
        return exact_city

    # New links can use ASCII slugs (e.g. "krakow").
    by_slug = slug_to_city.get(token)
    if by_slug:
        return by_slug

    normalized = _slugify_text(token)
    if normalized:
        return slug_to_city.get(normalized, token)
    return token


def _read_meta_only(path: Path) -> Dict[str, Any]:
    """Read only the META section of a PB file and return its key/value map.

    This avoids parsing large VOTES/PROJECTS sections when we only need META
    constraints (min_length/max_length/max_sum_cost, etc.).
    """
    try:
        lines: List[str] = []
        with path.open("r", encoding="utf-8", newline="") as f:
            in_meta = False
            for raw in f:
                line = raw.rstrip("\n")
                up = line.strip().upper()
                if up == "META":
                    in_meta = True
                    lines.append("META")
                    continue
                if not in_meta:
                    # skip preamble until META
                    continue
                # include header row and subsequent rows until next section
                if up in {"PROJECTS", "VOTES"}:
                    break
                lines.append(line)
        if not lines:
            return {}
        meta, _projects, _votes, _v_in_p, _s_in_p = _parse_pb_lines(lines)
        # normalize keys to lowercase for robust lookups
        return {str(k).strip().lower(): v for k, v in (meta or {}).items()}
    except Exception:
        return {}


def _parse_int(val: Any) -> Optional[int]:
    try:
        s = str(val).strip()
        if not s:
            return None
        # handle floats like "10.0" gracefully
        return int(float(s))
    except Exception:
        return None


def _compute_approval_labels_from_meta(
    meta: Dict[str, Any],
) -> Tuple[Optional[str], bool, Optional[str]]:
    """Return (k_label, knapsack, k_type) derived from META.

    k_label examples: 'Any k', '2<k', 'k<=10', 'k=5', '2<k<=5'.
    knapsack: True when max_sum_cost or similar constraints are present.
    k_type in {'any','lower','upper','exact','range'} or None when hidden.
    """
    # Detect knapsack-style constraint first
    has_knapsack = False
    for key in ("max_sum_cost", "max_sum_cost_per_category", "max_total_cost"):
        if key in meta and str(meta.get(key)).strip() != "":
            has_knapsack = True
            break
    # Additionally, some datasets encode hint in subunit as 'vote knapsacks'
    subunit_val = str(meta.get("subunit", "")).strip().lower()
    if (not has_knapsack) and ("knapsack" in subunit_val):
        has_knapsack = True
    # If knapsack, we suppress k label entirely per requirement
    if has_knapsack:
        return None, True, None

    # Detect k-bounds
    min_k = _parse_int(meta.get("min_length"))
    max_k = _parse_int(meta.get("max_length"))

    k_label: Optional[str]
    k_type: Optional[str]
    # Treat min_k == 1 as trivial lower bound (do not display as lower)
    if min_k == 1:
        min_k = None

    if min_k is None and max_k is None:
        k_label = "Any k"
        k_type = "any"
    elif min_k is not None and max_k is not None and min_k == max_k:
        k_label = f"k={min_k}"
        k_type = "exact"
    elif min_k is not None and max_k is not None:
        # Use single-glyph inequality characters for clarity: m≤k≤n
        k_label = f"{min_k}≤k≤{max_k}"
        k_type = "range"
    elif min_k is not None:
        # Lower bound shown as m≤k
        k_label = f"{min_k}≤k"
        k_type = "lower"
    else:
        # only upper bound: use ≤
        k_label = f"k≤{max_k}"
        k_type = "upper"

    return k_label, False, k_type


def _compute_ordinal_k_from_meta(
    meta: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    """Return (k_label, k_type) for ordinal ballots derived from META.

    Uses the same rules as Approval for k-bounds formatting:
    - Ignore min=1 as trivial
    - Exact when min==max => k=n
    - Range uses single-glyph inequalities: m≤k≤n
    - Lower-only: m≤k
    - Upper-only: k≤n
    - None: 'Any k'
    """
    min_k = _parse_int(meta.get("min_length"))
    max_k = _parse_int(meta.get("max_length"))

    if min_k == 1:
        min_k = None

    if min_k is None and max_k is None:
        return "Any k", "any"
    if min_k is not None and max_k is not None and min_k == max_k:
        return f"k={min_k}", "exact"
    if min_k is not None and max_k is not None:
        return f"{min_k}≤k≤{max_k}", "range"
    if min_k is not None:
        return f"{min_k}≤k", "lower"
    # only upper bound
    return f"k≤{max_k}", "upper"


def _compute_cumulative_points_from_meta(
    meta: Dict[str, Any],
) -> Optional[str]:
    """Return a compact label for cumulative points constraints derived from META.

    We focus on the total points available to distribute (sum constraints):
    - min_sum_points – lower bound (optional)
    - max_sum_points – upper bound (optional)

    Formatting mirrors k-bounds style with single-glyph inequalities:
    - Both equal: 'pts=n'
    - Range: 'm≤pts≤n'
    - Lower-only: 'm≤pts'
    - Upper-only: 'pts≤n'

    If neither bound is present, return None (no label shown).
    """
    min_sum = _parse_int(meta.get("min_sum_points"))
    max_sum = _parse_int(meta.get("max_sum_points"))

    # Treat min_sum in {0,1} as trivial lower bound for display
    # Rationale: when the lower bound is 1 and an upper bound exists (e.g., 1≤pts≤10),
    # display can omit the lower bound as it conveys little extra information -> pts≤10.
    # We generalize and drop 1 as a lower-only bound for consistency.
    if min_sum in (0, 1):
        min_sum = None

    if min_sum is None and max_sum is None:
        return None
    if min_sum is not None and max_sum is not None and min_sum == max_sum:
        return f"pts={max_sum}"
    if min_sum is not None and max_sum is not None:
        return f"{min_sum}≤pts≤{max_sum}"
    if min_sum is not None:
        return f"{min_sum}≤pts"
    # only upper bound present
    return f"pts≤{max_sum}"


def _db_signature() -> Optional[str]:
    try:
        with get_session() as s:
            rs = s.get(RefreshState, "pb")
            refresh_sig = (
                rs.last_completed_at.isoformat()
                if rs and rs.last_completed_at
                else ""
            )
            checker_sig_row = (
                s.query(CheckerValidationCache.checked_at)
                .order_by(desc(CheckerValidationCache.checked_at))
                .first()
            )
            checker_sig = (
                checker_sig_row[0].isoformat()
                if checker_sig_row and checker_sig_row[0]
                else ""
            )
            return f"{refresh_sig}|{checker_sig}"
    except Exception:
        return None


def invalidate_caches() -> None:
    global _TILES_CACHE, _COMMENTS_CACHE, _STATS_CACHE, _CATEGORIES_CACHE, _BENEFICIARIES_CACHE, _RULES_CACHE, _CITY_SLUG_CACHE
    _TILES_CACHE = None
    _COMMENTS_CACHE = None
    _STATS_CACHE = None
    _CATEGORIES_CACHE = None
    _BENEFICIARIES_CACHE = None
    _RULES_CACHE = None
    _CITY_SLUG_CACHE = None


def _row_to_tile(
    r: Any,
    comments_map: Dict[int, List[str]],
) -> Dict[str, Any]:
    """Convert a raw SQLAlchemy row tuple into the tile dict
    returned by the public API.  Both search_tiles() and get_tiles_cached()
    query the same columns in the same order and use this helper."""
    (
        file_id,
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
        has_geo,
        has_category,
        has_beneficiaries,
        min_length,
        max_length,
        min_sum_points,
        max_sum_points,
        max_sum_cost,
        max_sum_cost_per_category,
        max_total_cost,
        is_first_addition,
        ingested_at,
        first_ingested_at,
        file_mtime,
        checker_status,
        checker_error_count,
        checker_warning_count,
        checker_file_mtime,
    ) = r

    cache_is_fresh = bool(
        checker_status
        and checker_file_mtime is not None
        and file_mtime is not None
        and checker_file_mtime == file_mtime
    )
    public_checker_status = (
        checker_public_status(checker_status) if cache_is_fresh else "not_checked"
    )
    checker_error_total = int(checker_error_count or 0) if cache_is_fresh else 0
    checker_warning_total = int(checker_warning_count or 0) if cache_is_fresh else 0

    meta: Dict[str, Any] = {"subunit": subunit}
    if min_length is not None:
        meta["min_length"] = min_length
    if max_length is not None:
        meta["max_length"] = max_length
    if min_sum_points is not None:
        meta["min_sum_points"] = min_sum_points
    if max_sum_points is not None:
        meta["max_sum_points"] = max_sum_points
    if max_sum_cost is not None:
        meta["max_sum_cost"] = max_sum_cost
    if max_sum_cost_per_category is not None:
        meta["max_sum_cost_per_category"] = max_sum_cost_per_category
    if max_total_cost is not None:
        meta["max_total_cost"] = max_total_cost

    vtype = (vote_type or "").strip().lower()
    approval_k_label = None
    approval_knapsack = False
    approval_k_type = None
    ordinal_k_label = None
    ordinal_k_type = None
    cumulative_points_label = None

    if vtype == "approval":
        approval_k_label, approval_knapsack, approval_k_type = (
            _compute_approval_labels_from_meta(meta)
        )
    elif vtype == "ordinal":
        ordinal_k_label, ordinal_k_type = _compute_ordinal_k_from_meta(meta)
    elif vtype == "cumulative":
        cumulative_points_label = _compute_cumulative_points_from_meta(meta)

    return {
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
            format_budget(currency or "", int(float(budget or 0)))
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
        "comments": comments_map.get(file_id, []),
        "country_raw": country or "",
        "unit_raw": unit or "",
        "instance_raw": instance or "",
        "has_geo": bool(has_geo),
        "has_category": bool(has_category),
        "has_beneficiaries": bool(has_beneficiaries),
        "is_new": compute_is_new_value(first_ingested_at or ingested_at),
        "approval_k_label": approval_k_label,
        "approval_knapsack": approval_knapsack,
        "approval_k_type": approval_k_type,
        "ordinal_k_label": ordinal_k_label,
        "ordinal_k_type": ordinal_k_type,
        "cumulative_points_label": cumulative_points_label,
        "checker_status": public_checker_status,
        "checker_status_label": checker_public_label(public_checker_status),
        "checker_short_label": checker_public_short_label(public_checker_status),
        "checker_tooltip": checker_public_tooltip(
            public_checker_status,
            error_count=checker_error_total,
            warning_count=checker_warning_total,
        ),
        "checker_error_count": checker_error_total,
        "checker_warning_count": checker_warning_total,
        "has_checker_result": cache_is_fresh,
        "show_checker_badge": True,
    }


def _apply_search_filters(
    q,
    query: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    year: Optional[str] = None,
    votes_min: Optional[int] = None,
    votes_max: Optional[int] = None,
    projects_min: Optional[int] = None,
    projects_max: Optional[int] = None,
    len_min: Optional[float] = None,
    len_max: Optional[float] = None,
    vote_type: Optional[str] = None,
    exclude_fully: bool = False,
    exclude_experimental: bool = False,
    require_geo: bool = False,
    require_beneficiaries: bool = False,
    require_category: bool = False,
    require_new: bool = False,
):
    if query:
        # Split query into tokens (AND logic for each token)
        for token in query.split():
            term = f"%{fold_search_text(token)}%"
            criteria = [PBFile.search_text_norm.like(term)]
            if token.isdigit():
                criteria.append(PBFile.year == int(token))
            
            q = q.filter(or_(*criteria))
    
    if country:
        q = q.filter(PBFile.country == country)
    if city:
        city = _resolve_city_filter_value(city)
        q = q.filter(PBFile.unit == city)
    if year:
        try:
            q = q.filter(PBFile.year == int(year))
        except:
            pass
    
    if votes_min is not None:
        q = q.filter(PBFile.num_votes >= votes_min)
    if votes_max is not None:
        q = q.filter(PBFile.num_votes <= votes_max)
        
    if projects_min is not None:
        q = q.filter(PBFile.num_projects >= projects_min)
    if projects_max is not None:
        q = q.filter(PBFile.num_projects <= projects_max)
        
    if len_min is not None:
        q = q.filter(PBFile.vote_length >= len_min)
    if len_max is not None:
        q = q.filter(PBFile.vote_length <= len_max)
        
    if vote_type:
        q = q.filter(PBFile.vote_type.ilike(vote_type))
        
    if exclude_fully:
        q = q.filter(PBFile.fully_funded == False)  # noqa: E712
    if exclude_experimental:
        q = q.filter(PBFile.experimental == False)  # noqa: E712
        
    if require_geo:
        q = q.filter(PBFile.has_geo == True)  # noqa: E712
    if require_beneficiaries:
        q = q.filter(PBFile.has_beneficiaries == True)  # noqa: E712
    if require_category:
        q = q.filter(PBFile.has_category == True)  # noqa: E712
    if require_new:
        q = q.filter(
            func.coalesce(PBFile.first_ingested_at, PBFile.ingested_at)
            >= _new_file_cutoff(),
        )
        
    return q


def get_filtered_file_paths(
    query: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    year: Optional[str] = None,
    votes_min: Optional[int] = None,
    votes_max: Optional[int] = None,
    projects_min: Optional[int] = None,
    projects_max: Optional[int] = None,
    len_min: Optional[float] = None,
    len_max: Optional[float] = None,
    vote_type: Optional[str] = None,
    exclude_fully: bool = False,
    exclude_experimental: bool = False,
    require_geo: bool = False,
    require_beneficiaries: bool = False,
    require_category: bool = False,
    require_new: bool = False,
) -> List[Tuple[str, Path]]:
    
    with get_session() as s:
        q = s.query(PBFile.file_name).filter(PBFile.is_current == True)  # noqa: E712
        q = _apply_search_filters(
            q, query, country, city, year, votes_min, votes_max,
            projects_min, projects_max, len_min, len_max, vote_type,
            exclude_fully, exclude_experimental, require_geo, require_beneficiaries, require_category, require_new
        )
        rows = q.all()
        
        results = []
        for (name,) in rows:
            p = get_current_file_path(name)
            if p and p.exists():
                results.append((name, p))
        return results


def search_tiles(
    query: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    year: Optional[str] = None,
    votes_min: Optional[int] = None,
    votes_max: Optional[int] = None,
    projects_min: Optional[int] = None,
    projects_max: Optional[int] = None,
    len_min: Optional[float] = None,
    len_max: Optional[float] = None,
    vote_type: Optional[str] = None,
    exclude_fully: bool = False,
    exclude_experimental: bool = False,
    require_geo: bool = False,
    require_beneficiaries: bool = False,
    require_category: bool = False,
    require_new: bool = False,
    order_by: str = "quality",
    order_dir: str = "desc",
    limit: int = 50,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    
    with get_session() as s:
        q = s.query(
            PBFile.id,
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
            PBFile.has_geo,
            PBFile.has_category,
            PBFile.has_beneficiaries,
            PBFile.min_length,
            PBFile.max_length,
            PBFile.min_sum_points,
            PBFile.max_sum_points,
            PBFile.max_sum_cost,
            PBFile.max_sum_cost_per_category,
            PBFile.max_total_cost,
            PBFile.is_first_addition,
            PBFile.ingested_at,
            PBFile.first_ingested_at,
            PBFile.file_mtime,
            CheckerValidationCache.checker_status,
            CheckerValidationCache.error_count,
            CheckerValidationCache.warning_count,
            CheckerValidationCache.file_mtime,
        ).outerjoin(
            CheckerValidationCache,
            CheckerValidationCache.file_id == PBFile.id,
        ).filter(PBFile.is_current == True)  # noqa: E712

        q = _apply_search_filters(
            q,
            query=query,
            country=country,
            city=city,
            year=year,
            votes_min=votes_min,
            votes_max=votes_max,
            projects_min=projects_min,
            projects_max=projects_max,
            len_min=len_min,
            len_max=len_max,
            vote_type=vote_type,
            exclude_fully=exclude_fully,
            exclude_experimental=exclude_experimental,
            require_geo=require_geo,
            require_beneficiaries=require_beneficiaries,
            require_category=require_category,
            require_new=require_new,
        )

        # Count total before pagination
        total_count = q.count()

        # Ordering
        sort_col = _SEARCH_ORDER_COLUMNS.get(order_by, PBFile.quality)
        if order_dir == "asc":
            q = q.order_by(asc(sort_col))
        else:
            q = q.order_by(desc(sort_col))
            
        # Secondary sort
        q = q.order_by(PBFile.file_name)

        rows = q.offset(offset).limit(limit).all()
        
        # Fetch comments for these files
        file_ids = [r[0] for r in rows]
        comments_map = {}
        if file_ids:
            comments_rows = s.query(PBComment.file_id, PBComment.text).filter(PBComment.file_id.in_(file_ids), PBComment.is_active == True).order_by(PBComment.file_id, PBComment.idx).all()
            for fid, text in comments_rows:
                if fid not in comments_map:
                    comments_map[fid] = []
                comments_map[fid].append(text)
        
        tiles: List[Dict[str, Any]] = []
        for r in rows:
            tiles.append(_row_to_tile(r, comments_map))
            
        return tiles, total_count


def get_filter_options() -> Dict[str, Any]:
    with get_session() as s:
        countries = [r[0] for r in s.query(PBFile.country).filter(PBFile.is_current == True).distinct().order_by(PBFile.country).all() if r[0]]
        cities = [r[0] for r in s.query(PBFile.unit).filter(PBFile.is_current == True).distinct().order_by(PBFile.unit).all() if r[0]]
        years = [str(r[0]) for r in s.query(PBFile.year).filter(PBFile.is_current == True).distinct().order_by(PBFile.year.desc()).all() if r[0] is not None]
        
        # Get all valid combinations for client-side filtering
        comb_rows = s.query(PBFile.country, PBFile.unit, PBFile.year).filter(PBFile.is_current == True).distinct().all()
        combinations = [
            {
                "c": r[0],      # country
                "u": r[1],      # unit/city
                "y": str(r[2]) if r[2] is not None else None # year
            }
            for r in comb_rows
        ]

    city_slug_map, _slug_to_city, _folded_city_to_city = _get_city_slug_maps()
        
    return {
        "countries": countries,
        "cities": cities,
        "city_slug_map": city_slug_map,
        "years": years,
        "combinations": combinations
    }


def get_filter_availability(
    query: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    year: Optional[str] = None,
    votes_min: Optional[int] = None,
    votes_max: Optional[int] = None,
    projects_min: Optional[int] = None,
    projects_max: Optional[int] = None,
    len_min: Optional[float] = None,
    len_max: Optional[float] = None,
    vote_type: Optional[str] = None,
    exclude_fully: bool = False,
    exclude_experimental: bool = False,
    require_geo: bool = False,
    require_beneficiaries: bool = False,
    require_category: bool = False,
    require_new: bool = False,
) -> Dict[str, Any]:
    with get_session() as s:
        countries_q = s.query(PBFile.country).filter(PBFile.is_current == True)  # noqa: E712
        countries_q = _apply_search_filters(
            countries_q,
            query=query,
            country=None,
            city=city,
            year=year,
            votes_min=votes_min,
            votes_max=votes_max,
            projects_min=projects_min,
            projects_max=projects_max,
            len_min=len_min,
            len_max=len_max,
            vote_type=vote_type,
            exclude_fully=exclude_fully,
            exclude_experimental=exclude_experimental,
            require_geo=require_geo,
            require_beneficiaries=require_beneficiaries,
            require_category=require_category,
            require_new=require_new,
        )
        available_countries = [
            r[0]
            for r in countries_q.distinct().order_by(PBFile.country).all()
            if r[0]
        ]

        cities_q = s.query(PBFile.unit).filter(PBFile.is_current == True)  # noqa: E712
        cities_q = _apply_search_filters(
            cities_q,
            query=query,
            country=country,
            city=None,
            year=year,
            votes_min=votes_min,
            votes_max=votes_max,
            projects_min=projects_min,
            projects_max=projects_max,
            len_min=len_min,
            len_max=len_max,
            vote_type=vote_type,
            exclude_fully=exclude_fully,
            exclude_experimental=exclude_experimental,
            require_geo=require_geo,
            require_beneficiaries=require_beneficiaries,
            require_category=require_category,
            require_new=require_new,
        )
        available_cities = [
            r[0] for r in cities_q.distinct().order_by(PBFile.unit).all() if r[0]
        ]

        years_q = s.query(PBFile.year).filter(PBFile.is_current == True)  # noqa: E712
        years_q = _apply_search_filters(
            years_q,
            query=query,
            country=country,
            city=city,
            year=None,
            votes_min=votes_min,
            votes_max=votes_max,
            projects_min=projects_min,
            projects_max=projects_max,
            len_min=len_min,
            len_max=len_max,
            vote_type=vote_type,
            exclude_fully=exclude_fully,
            exclude_experimental=exclude_experimental,
            require_geo=require_geo,
            require_beneficiaries=require_beneficiaries,
            require_category=require_category,
            require_new=require_new,
        )
        available_years = [
            str(r[0])
            for r in years_q.distinct().order_by(PBFile.year.desc()).all()
            if r[0] is not None
        ]

    return {
        "available_countries": available_countries,
        "available_cities": available_cities,
        "available_years": available_years,
    }


def get_tiles_cached() -> List[Dict[str, Any]]:
    global _TILES_CACHE
    import time
    t0 = time.time()
    db_sig = _db_signature()
    if _TILES_CACHE is not None and getattr(_TILES_CACHE, "_db_sig", None) == db_sig:
        _logger.debug("get_tiles_cached hit cache (%d tiles) in %.4fs", len(_TILES_CACHE), time.time() - t0)
        return _TILES_CACHE

    _logger.debug("get_tiles_cached MISS — rebuilding")
    t1 = time.time()
    with get_session() as s:
        rows = (
            s.query(
                PBFile.id,
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
                PBFile.has_geo,
                PBFile.has_category,
                PBFile.has_beneficiaries,
                PBFile.min_length,
                PBFile.max_length,
                PBFile.min_sum_points,
                PBFile.max_sum_points,
                PBFile.max_sum_cost,
                PBFile.max_sum_cost_per_category,
                PBFile.max_total_cost,
                PBFile.is_first_addition,
                PBFile.ingested_at,
                PBFile.first_ingested_at,
                PBFile.file_mtime,
                CheckerValidationCache.checker_status,
                CheckerValidationCache.error_count,
                CheckerValidationCache.warning_count,
                CheckerValidationCache.file_mtime,
            )
            .outerjoin(CheckerValidationCache, CheckerValidationCache.file_id == PBFile.id)
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
        
        # Fetch all comments
        comments_rows = s.query(PBComment.file_id, PBComment.text).filter(PBComment.is_active == True).order_by(PBComment.file_id, PBComment.idx).all()

    # Group comments
    comments_map = {}
    for fid, text in comments_rows:
        if fid not in comments_map:
            comments_map[fid] = []
        comments_map[fid].append(text)

    tiles: List[Dict[str, Any]] = []
    for r in rows:
        tiles.append(_row_to_tile(r, comments_map))

    try:
        setattr(tiles, "_db_sig", db_sig)
    except Exception:
        pass
    _TILES_CACHE = tiles
    _logger.debug("get_tiles_cached rebuilt in %.4fs (total %.4fs)", time.time() - t1, time.time() - t0)
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


def _aggregate_label_cached(
    kind: str,
) -> Tuple[
    Dict[str, List[str]],
    List[Tuple[str, int, List[str]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
]:
    """Generic aggregator for categories/beneficiaries.

    kind: 'category' or 'beneficiary'
    Returns same tuple shape as aggregate_comments_cached.
    """
    global _CATEGORIES_CACHE, _BENEFICIARIES_CACHE
    table = PBCategory if kind == "category" else PBBeneficiary
    global_cache = _CATEGORIES_CACHE if kind == "category" else _BENEFICIARIES_CACHE
    db_sig = _db_signature()
    if global_cache is not None and getattr(global_cache, "_db_sig", None) == db_sig:
        return global_cache

    with get_session() as s:
        q = (
            s.query(
                table.norm,  # use normalized label for grouping
                table.value,  # original for display (pick one later)
                PBFile.file_name,
                PBFile.country,
                PBFile.unit,
                PBFile.instance,
            )
            .join(PBFile, PBFile.id == table.file_id)
            .filter(PBFile.is_current == True)  # noqa: E712
            .filter(table.is_active == True)  # type: ignore  # noqa: E712
        )
        rows = q.all()

    # We will map by norm; store one display variant per norm (first seen)
    display_for_norm: Dict[str, str] = {}
    mapping: Dict[str, List[str]] = {}
    groups_temp_country: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit_instance: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for norm, value, fname, country, unit, instance in rows:
        key = (norm or "").strip().lower()
        if not key:
            continue
        if key not in display_for_norm:
            display_for_norm[key] = value or key
        mapping.setdefault(key, []).append(fname)
        country = (country or "").strip()
        unit = (unit or "").strip()
        instance = (instance or "").strip()
        if country:
            cm_c = groups_temp_country.setdefault(key, {})
            key_c = country.lower()
            bucket_c = cm_c.setdefault(key_c, {"label": country, "files": []})
            bucket_c["files"].append(fname)
        if country or unit:
            cm_cu = groups_temp_country_unit.setdefault(key, {})
            key_cu = f"{country.lower()}::{unit.lower()}"
            label_cu = f"{country} – {unit}".strip(" –") if (country or unit) else "—"
            bucket_cu = cm_cu.setdefault(key_cu, {"label": label_cu, "files": []})
            bucket_cu["files"].append(fname)
        if country or unit or instance:
            cm_cui = groups_temp_country_unit_instance.setdefault(key, {})
            key_cui = f"{country.lower()}::{unit.lower()}::{instance.lower()}"
            label_cui = (
                f"{country} – {unit} – {instance}".strip(" –")
                if (country or unit or instance)
                else "—"
            )
            bucket_cui = cm_cui.setdefault(key_cui, {"label": label_cui, "files": []})
            bucket_cui["files"].append(fname)

    rows_list: List[Tuple[str, int, List[str]]] = []
    for norm_key, flist in mapping.items():
        display = display_for_norm.get(norm_key, norm_key)
        rows_list.append((display, len(flist), sorted(flist)))
    rows_list.sort(key=lambda t: (-t[1], t[0].lower()))

    def finalize_groups(
        src: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for norm_key, by_key in src.items():
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
            out[display_for_norm.get(norm_key, norm_key)] = items
        return out

    result = (
        {display_for_norm.get(k, k): v for k, v in mapping.items()},
        rows_list,
        finalize_groups(groups_temp_country),
        finalize_groups(groups_temp_country_unit),
        finalize_groups(groups_temp_country_unit_instance),
    )
    try:
        setattr(result, "_db_sig", db_sig)  # type: ignore[attr-defined]
    except Exception:
        pass
    if kind == "category":
        _CATEGORIES_CACHE = result
        return _CATEGORIES_CACHE
    else:
        _BENEFICIARIES_CACHE = result
        return _BENEFICIARIES_CACHE


def aggregate_categories_cached() -> Tuple[
    Dict[str, List[str]],
    List[Tuple[str, int, List[str]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
]:
    return _aggregate_label_cached("category")


def aggregate_beneficiaries_cached() -> Tuple[
    Dict[str, List[str]],
    List[Tuple[str, int, List[str]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
]:
    return _aggregate_label_cached("beneficiary")


def aggregate_rules_cached() -> Tuple[
    Dict[str, List[str]],
    List[Tuple[str, int, List[str]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
]:
    """Aggregate rules (from PBFile.rule_raw field) with smart grouping.

    For common rules like 'greedy', show location summaries instead of all files.
    Returns same tuple shape as aggregate_categories_cached.
    """
    global _RULES_CACHE
    db_sig = _db_signature()
    if _RULES_CACHE is not None and getattr(_RULES_CACHE, "_db_sig", None) == db_sig:
        return _RULES_CACHE

    with get_session() as s:
        q = s.query(
            PBFile.rule_raw,
            PBFile.file_name,
            PBFile.country,
            PBFile.unit,
            PBFile.instance,
        ).filter(PBFile.is_current == True)  # noqa: E712
        rows = q.all()

    # Map rule -> list of files
    mapping: Dict[str, List[str]] = {}
    # Track unique locations per rule for smart display
    groups_temp_country: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit: Dict[str, Dict[str, Dict[str, Any]]] = {}
    groups_temp_country_unit_instance: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for rule_raw, fname, country, unit, instance in rows:
        key = (rule_raw or "unknown").strip()
        if not key:
            key = "unknown"

        mapping.setdefault(key, []).append(fname)
        country = (country or "").strip()
        unit = (unit or "").strip()
        instance = (instance or "").strip()

        # Group by country
        if country:
            cm_c = groups_temp_country.setdefault(key, {})
            key_c = country.lower()
            bucket_c = cm_c.setdefault(key_c, {"label": country, "files": []})
            bucket_c["files"].append(fname)

        # Group by country + unit
        if country or unit:
            cm_cu = groups_temp_country_unit.setdefault(key, {})
            key_cu = f"{country.lower()}::{unit.lower()}"
            label_cu = f"{country} – {unit}".strip(" –") if (country or unit) else "—"
            bucket_cu = cm_cu.setdefault(key_cu, {"label": label_cu, "files": []})
            bucket_cu["files"].append(fname)

        # Group by country + unit + instance
        if country or unit or instance:
            cm_cui = groups_temp_country_unit_instance.setdefault(key, {})
            key_cui = f"{country.lower()}::{unit.lower()}::{instance.lower()}"
            label_cui = (
                f"{country} – {unit} – {instance}".strip(" –")
                if (country or unit or instance)
                else "—"
            )
            bucket_cui = cm_cui.setdefault(key_cui, {"label": label_cui, "files": []})
            bucket_cui["files"].append(fname)

    # Build rows list
    rows_list: List[Tuple[str, int, List[str]]] = []
    for rule_key, flist in mapping.items():
        rows_list.append((rule_key, len(flist), sorted(flist)))
    rows_list.sort(key=lambda t: (-t[1], t[0].lower()))

    def finalize_groups(
        src: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for rule_key, by_key in src.items():
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
            out[rule_key] = items
        return out

    result = (
        {k: v for k, v in mapping.items()},
        rows_list,
        finalize_groups(groups_temp_country),
        finalize_groups(groups_temp_country_unit),
        finalize_groups(groups_temp_country_unit_instance),
    )
    try:
        setattr(result, "_db_sig", db_sig)  # type: ignore[attr-defined]
    except Exception:
        pass
    _RULES_CACHE = result
    return _RULES_CACHE


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
        votes_projects_scatter: List[Dict[str, Any]] = []

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

            if num_projects or num_votes:
                city_label = f"{city}, {country}".strip(", ")
                votes_projects_scatter.append(
                    {"x": num_projects, "y": num_votes, "label": city_label or "—"}
                )
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
        "votes_projects_scatter": votes_projects_scatter,
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


def get_comment_usages(include_inactive: bool = True) -> List[Dict[str, Any]]:
    """Return flat list of comment usages with file context for admin UI.

    Each item contains:
    - text: Comment text
    - idx: 1-based index within the file's META section
    - is_active: Whether the comment is active (comment belongs to current file version)
    - file_name: PB filename
    - country, unit, instance, subunit, year
    - is_current: Whether the file is the current one for its group
    - ingested_at: Timestamp the file was ingested
    """
    try:
        with get_session() as s:
            q = s.query(
                PBComment.text,
                PBComment.idx,
                PBComment.is_active,
                PBFile.file_name,
                PBFile.country,
                PBFile.unit,
                PBFile.instance,
                PBFile.subunit,
                PBFile.year,
                PBFile.is_current,
                PBFile.ingested_at,
            ).join(PBFile, PBFile.id == PBComment.file_id)
            if not include_inactive:
                q = q.filter(PBComment.is_active == True)  # noqa: E712
            rows = q.order_by(
                PBComment.is_active.desc(),
                PBComment.text.asc(),
                PBFile.country.asc(),
                PBFile.unit.asc(),
                PBFile.instance.asc(),
                PBFile.file_name.asc(),
            ).all()

            out: List[Dict[str, Any]] = []
            for (
                text,
                idx,
                is_active,
                file_name,
                country,
                unit,
                instance,
                subunit,
                year,
                is_current,
                ingested_at,
            ) in rows:
                out.append(
                    {
                        "text": text or "",
                        "idx": int(idx or 0),
                        "is_active": bool(is_active),
                        "file_name": file_name or "",
                        "country": country or "",
                        "unit": unit or "",
                        "instance": instance or "",
                        "subunit": subunit or "",
                        "year": year,
                        "is_current": bool(is_current),
                        "ingested_at": ingested_at,
                    }
                )
            return out
    except Exception:
        return []
