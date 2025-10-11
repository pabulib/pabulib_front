from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .load_pb_file import parse_pb_lines


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
        budget = (
            int(budget_raw)
            if budget_raw is not None and str(budget_raw).isdigit()
            else None
        )
    except Exception:
        budget = None
    vote_type = str(meta.get("vote_type", meta.get("rule", ""))).lower()

    # vote length
    vote_length_float: Optional[float] = None
    try:
        lengths: List[int] = []
        for v in votes.values():
            sel = str(v.get("vote", "")).strip()
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

    return {
        "file_name": pb_path.name,
        "path": str(pb_path),
        "title": title,
        "webpage_name": webpage_name,
        "description": description,
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
        "has_selected_col": has_selected_col,
        "experimental": experimental,
        "quality": quality,
        "rule_raw": rule_raw,
        "edition": edition,
        "language": language,
    }
