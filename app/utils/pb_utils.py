from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .load_pb_file import parse_pb_lines


def parse_comments_from_meta(meta: Dict[str, Any]) -> List[str]:
    """Extract processed comments from META['comment'].

    Format: a single string possibly containing multiple segments marked with
    sequential markers like "#1:", "#2:", ... on one or multiple lines.
    Returns a list of plain comment texts without trailing punctuation.
    """
    raw = str(meta.get("comment", "")).strip()
    if not raw:
        return []
    # Normalize to single line to simplify marker search
    s = raw.replace("\n", " ")
    parts: List[str] = []
    expecting = 1
    while True:
        marker = f"#{expecting}:"
        next_marker = f"#{expecting + 1}:"
        start = s.find(marker)
        if start == -1:
            # No marker found. If it's the first expected marker, treat whole
            # string as a single comment.
            if expecting == 1 and s:
                txt = s.strip().strip(";.")
                if txt:
                    parts.append(txt)
            break
        start_text = start + len(marker)
        end = s.find(next_marker, start_text)
        chunk = s[start_text:] if end == -1 else s[start_text:end]
        txt = chunk.strip().strip(";.")
        if txt:
            parts.append(txt)
        expecting += 1
        if end == -1:
            break
    return parts


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def pb_folder() -> Path:
    # Allow overriding the PB files directory via env var.
    # If PB_FILES_DIR is relative, resolve it against the workspace root.
    env_val = os.environ.get("PB_FILES_DIR")
    if env_val:
        p = Path(env_val).expanduser()
        if not p.is_absolute():
            p = workspace_root() / p
        return p
    return workspace_root() / "pb_files"


def pb_depreciated_folder() -> Path:
    """Return the folder path for archived (depreciated) PB files.
    If PB_FILES_DEPRECIATED_DIR is set, use it (resolve relative to workspace root).
    Otherwise, default to <workspace>/pb_files_depreciated.
    """
    env_val = os.environ.get("PB_FILES_DEPRECIATED_DIR")
    if env_val:
        p = Path(env_val).expanduser()
        if not p.is_absolute():
            p = workspace_root() / p
        return p
    return workspace_root() / "pb_files_depreciated"


def read_file_lines(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return [line.rstrip("\n") for line in f]


def compute_webpage_name(meta: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    country = str(meta.get("country", "")).strip()
    unit = str(meta.get("unit", meta.get("city", meta.get("district", "")))).strip()
    instance = str(meta.get("instance", meta.get("year", ""))).strip()
    subunit = str(meta.get("subunit", "")).strip()
    webpage_parts = [p for p in [country, unit, instance, subunit] if p]
    webpage_name = "_".join(webpage_parts)
    return webpage_name, country, unit, instance, subunit


def build_group_key(country: str, unit: str, instance: str, subunit: str) -> str:
    parts = [country or "", unit or "", instance or "", subunit or ""]
    key = "|".join(p.strip().lower() for p in parts)
    # Keep within 191 chars for MySQL utf8mb4 safe indexing; if longer, append a stable short hash
    MAXLEN = 191
    if len(key) <= MAXLEN:
        return key
    try:
        import hashlib

        h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        # leave room for separator and hash
        prefix = key[: MAXLEN - 1 - len(h)]
        return f"{prefix}_{h}"
    except Exception:
        return key[:MAXLEN]


def parse_pb_to_tile(pb_path: Path) -> Dict[str, Any]:
    lines = read_file_lines(pb_path)
    meta, projects, votes, votes_in_projects, scores_in_projects = parse_pb_lines(lines)

    webpage_name, country, unit, instance, subunit = compute_webpage_name(meta)
    title = (
        webpage_name.replace("_", " ")
        if webpage_name
        else pb_path.stem.replace("_", " ")
    )

    description = meta.get("description", "")
    comments = parse_comments_from_meta(meta)
    currency = meta.get("currency", "")
    try:
        num_votes = int(meta.get("num_votes", len(votes)))
    except Exception:
        num_votes = len(votes)
    try:
        num_projects = int(meta.get("num_projects", len(projects)))
    except Exception:
        num_projects = len(projects)
    budget_raw = meta.get("budget")
    try:
        # Convert to float first, then to int to truncate decimal part
        budget = int(float(budget_raw)) if budget_raw is not None else None
    except (ValueError, TypeError):
        budget = None
    vote_type = str(meta.get("vote_type", meta.get("rule", ""))).lower()

    # vote length
    vote_length_float: Optional[float] = None
    try:
        lengths: List[int] = []
        for v in votes.values():
            # Only the 'vote' field is used for vote length calculation.
            # Other columns (e.g., 'age', 'sex', etc.) do not affect this value.
            sel = v.get("vote", "")
            if isinstance(sel, list):
                lengths.append(len([s for s in sel if s]))
            elif isinstance(sel, str):
                sel = sel.strip()
                if not sel:
                    continue
                lengths.append(len([s for s in sel.split(",") if s]))
        if lengths:
            vote_length_float = sum(lengths) / len(lengths)
    except Exception:
        vote_length_float = None

    # fully funded heuristic
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
                # Robust cost parsing: accept ints, floats, and numeric strings like '40000' or '40000.0'
                try:
                    if isinstance(c, (int, float)):
                        sum_selected_cost += int(float(c))
                    elif isinstance(c, str):
                        # Normalize decimal comma and whitespace
                        cs = c.strip().replace(",", ".")
                        sum_selected_cost += int(float(cs))
                except Exception:
                    # Ignore non-parsable costs for the fully_funded heuristic
                    pass
        fully_funded = all_selected or (
            budget is not None and sum_selected_cost >= budget
        )
    except Exception:
        fully_funded = False
        selected_count = 0

    # If there's no selected column, set selected_count to None
    if not has_selected_col:
        selected_count = None

    # Detect year
    year_int: Optional[int] = None
    try:
        import re

        date_begin = str(meta.get("date_begin", "")).strip()
        if date_begin:
            m = re.search(r"(\d{4})", date_begin)
            if m:
                y = int(m.group(1))
                if 1900 <= y <= 2100:
                    year_int = y
        if year_int is None:
            for cand in [meta.get("year"), instance]:
                if cand is None:
                    continue
                s = str(cand).strip()
                if s.isdigit():
                    y = int(s)
                    if 1900 <= y <= 2100:
                        year_int = y
                        break
    except Exception:
        year_int = None

    # Quality metric (same definition as routes)
    vlen = vote_length_float or 0.0
    quality = (vlen**2) * (float(num_projects) ** 1) * (float(num_votes) ** 0.5)

    rule_raw = str(meta.get("rule", "")).strip()
    edition = str(meta.get("edition", "")).strip()
    language = str(meta.get("language", "")).strip()

    experimental = str(meta.get("experimental", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

    # Detect if PROJECTS section contains usable geographic coordinates.
    # Be robust to different header casings (e.g., Latitude/LATITUDE) and ensure values are numeric.
    has_geo = False
    has_category = False
    has_target = False
    # Collect unique category/target tokens and their counts per file
    # Use normalized keys (lowercased, trimmed) for uniqueness per file to avoid duplicates like
    # "Groen en Duurzaamheid" vs "groen en duurzaamheid".
    category_counts: dict[str, int] = {}
    category_display: dict[str, str] = {}
    target_counts: dict[str, int] = {}
    target_display: dict[str, str] = {}
    try:
        if projects:
            # Helper: try to coerce a value to float using dot/comma decimal
            def _to_float(val: Any) -> Optional[float]:
                try:
                    if val is None:
                        return None
                    s = str(val).strip()
                    if not s:
                        return None
                    # Normalize decimal comma
                    s = s.replace(",", ".")
                    return float(s)
                except Exception:
                    return None

            # For each project, detect latitude/longitude keys case-insensitively
            for p in projects.values():
                if not isinstance(p, dict):
                    continue
                lower_map = {str(k).strip().lower(): v for k, v in p.items()}
                # Common variants
                cand_lat_keys = ("latitude", "lat")
                cand_lon_keys = ("longitude", "lon", "long")
                lat_val = None
                lon_val = None
                for lk in cand_lat_keys:
                    if lk in lower_map:
                        lat_val = _to_float(lower_map[lk])
                        if lat_val is not None:
                            break
                for lk in cand_lon_keys:
                    if lk in lower_map:
                        lon_val = _to_float(lower_map[lk])
                        if lon_val is not None:
                            break
                # Basic sanity bounds check for lat/lon (only if both present)
                if lat_val is not None and lon_val is not None:
                    if -90.0 <= lat_val <= 90.0 and -180.0 <= lon_val <= 180.0:
                        has_geo = True
                # Detect category/target presence and collect tokens (comma-separated or lists)
                for ck in ("category", "categories"):
                    if ck in lower_map:
                        val = lower_map[ck]
                        tokens: list[str] = []
                        if isinstance(val, list):
                            tokens = [str(x).strip() for x in val if str(x).strip()]
                        else:
                            s = str(val).strip()
                            if s:
                                tokens = [t.strip() for t in s.split(",") if t.strip()]
                        for t in tokens:
                            has_category = True
                            norm = t.lower()
                            if norm not in category_display:
                                category_display[norm] = t
                            category_counts[norm] = category_counts.get(norm, 0) + 1
                for tk in ("target", "targets"):
                    if tk in lower_map:
                        val = lower_map[tk]
                        tokens: list[str] = []
                        if isinstance(val, list):
                            tokens = [str(x).strip() for x in val if str(x).strip()]
                        else:
                            s = str(val).strip()
                            if s:
                                tokens = [t.strip() for t in s.split(",") if t.strip()]
                        for t in tokens:
                            has_target = True
                            norm = t.lower()
                            if norm not in target_display:
                                target_display[norm] = t
                            target_counts[norm] = target_counts.get(norm, 0) + 1
                # If all flags are detected we can stop scanning further projects
                if has_geo and has_category and has_target:
                    # don't break early anymore; we want full token sets across projects
                    pass
    except Exception:
        has_geo = False
        has_category = False
        has_target = False

    return {
        "file_name": pb_path.name,
        "path": str(pb_path),
        "title": title,
        "webpage_name": webpage_name,
        "description": description,
        "comments": comments,
        "currency": currency,
        "num_votes_raw": num_votes,
        "num_projects_raw": num_projects,
        "num_selected_projects_raw": selected_count,
        "budget_raw": budget,
        "vote_type": vote_type,
        "vote_length_raw": vote_length_float,
        "country": country,
        "unit": unit,
        "instance": instance,
        "subunit": subunit,
        "year_raw": year_int,
        "fully_funded": fully_funded,
        "experimental": experimental,
        "quality": quality,
        "rule_raw": rule_raw,
        "edition": edition,
        "language": language,
        "has_geo": has_geo,
        "has_category": has_category,
        "has_target": has_target,
        # Provide collected tokens for ingestion/aggregation.
        # categories/targets arrays are human-friendly display values (first seen per norm).
        "categories": sorted(category_display.values(), key=lambda s: s.lower()),
        "targets": sorted(target_display.values(), key=lambda s: s.lower()),
        # counts keyed by normalized token; display maps normalized -> representative value
        "categories_counts": category_counts,
        "targets_counts": target_counts,
        "categories_display": category_display,
        "targets_display": target_display,
    }
