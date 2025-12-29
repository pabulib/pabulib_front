#!/usr/bin/env python3
"""
Refresh/injest PB files into the database.

Behavior:
- Scans pb_files/*.pb
- For each file, parses and computes metadata; raw JSON payloads are NOT stored in DB
- Uses (country, unit, instance, subunit) group to version records; marks latest mtime as current
- Skips files whose mtime is <= last refresh unless --full is provided

Usage:
  python -m scripts.db_refresh [--full]
Env:
  MYSQL_* or DATABASE_URL must be configured (docker-compose provides these)
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy.exc import OperationalError

from app.db import Base, engine, get_session
from app.models import PBCategory, PBComment, PBFile, PBTarget, RefreshState
from app.services import export_service
from app.services.pb_service import invalidate_caches as _invalidate_pb_caches
from app.utils.load_pb_file import parse_pb_lines
from app.utils.pb_utils import (
    build_group_key,
    compute_webpage_name,
    parse_pb_to_tile,
    pb_folder,
    read_file_lines,
)


def ensure_db(max_tries: int = 30, sleep_secs: float = 2.0) -> None:
    """Ensure DB schema exists with simple retry to avoid startup races.

    Attempts to run create_all; if a transient OperationalError occurs, retries a few times.
    """
    print("[DB] Ensuring schema exists...", flush=True)
    attempt = 0
    last_err: Exception | None = None
    while attempt < max_tries:
        attempt += 1
        try:
            Base.metadata.create_all(bind=engine)
            print("[DB] Schema ready", flush=True)
            return
        except OperationalError as e:
            last_err = e
            print(
                f"[DB] create_all failed (attempt {attempt}/{max_tries}): {e.__class__.__name__}: {e}",
                flush=True,
            )
            time.sleep(sleep_secs)
        except Exception as e:
            # Non-Operational errors are re-raised immediately
            print(f"[DB] create_all unexpected error: {e}", flush=True)
            raise
    # If we exhausted retries, raise the last OperationalError
    if last_err:
        raise last_err


def _parse_comments_from_meta(meta: Dict[str, Any]) -> list[str]:
    raw = str(meta.get("comment", "")).strip()
    if not raw:
        return []
    s = raw.replace("\n", " ")
    parts: list[str] = []
    expecting = 1
    while True:
        marker = f"#{expecting}:"
        next_marker = f"#{expecting + 1}:"
        start = s.find(marker)
        if start == -1:
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


def collect_files() -> List[Path]:
    folder = pb_folder()
    folder.mkdir(parents=True, exist_ok=True)
    files = sorted(folder.glob("*.pb"))
    return files


def load_last_refresh() -> datetime | None:
    with get_session() as s:
        rs = s.get(RefreshState, "pb")
        return rs.last_refresh_at if rs else None


def save_refresh_timestamp(kind: str, when: datetime) -> None:
    with get_session() as s:
        rs = s.get(RefreshState, kind)
        if not rs:
            rs = RefreshState(key=kind)
            s.add(rs)
        rs.last_refresh_at = when
        rs.last_completed_at = when


def ingest_file(
    p: Path,
) -> tuple[
    PBFile, list[str], dict[str, int], dict[str, int], dict[str, str], dict[str, str]
]:
    lines = read_file_lines(p)
    meta, projects, votes, _vip, _sip = parse_pb_lines(lines)
    tile = parse_pb_to_tile(p)

    webpage_name, country, unit, instance, subunit = compute_webpage_name(meta)
    group_key = build_group_key(country, unit, instance, subunit)

    st = p.stat()
    # Use UTC for file_mtime to be consistent across the app
    mtime = datetime.utcfromtimestamp(int(st.st_mtime))

    def _pi(v):
        try:
            return int(float(str(v).strip()))
        except (ValueError, TypeError):
            return None

    meta_lower = {str(k).strip().lower(): v for k, v in meta.items()}

    record = PBFile(
        file_name=p.name,
        path=str(p),
        country=country or None,
        unit=unit or None,
        instance=instance or None,
        subunit=subunit or None,
        webpage_name=webpage_name or None,
        year=tile.get("year_raw"),
        description=tile.get("description"),
        currency=tile.get("currency"),
        num_votes=tile.get("num_votes_raw"),
        num_projects=tile.get("num_projects_raw"),
        num_selected_projects=tile.get("num_selected_projects_raw"),
        budget=tile.get("budget_raw"),
        vote_type=tile.get("vote_type"),
        vote_length=tile.get("vote_length_raw"),
        fully_funded=bool(tile.get("fully_funded")),
        experimental=bool(tile.get("experimental")),
        rule_raw=tile.get("rule_raw"),
        edition=tile.get("edition"),
        language=tile.get("language"),
        quality=tile.get("quality"),
        has_geo=bool(tile.get("has_geo") or False),
        has_category=bool(tile.get("has_category") or False),
        has_target=bool(tile.get("has_target") or False),
        min_length=_pi(meta_lower.get("min_length")),
        max_length=_pi(meta_lower.get("max_length")),
        min_sum_points=_pi(meta_lower.get("min_sum_points")),
        max_sum_points=_pi(meta_lower.get("max_sum_points")),
        max_sum_cost=_pi(meta_lower.get("max_sum_cost")),
        max_sum_cost_per_category=_pi(meta_lower.get("max_sum_cost_per_category")),
        max_total_cost=_pi(meta_lower.get("max_total_cost")),
        file_mtime=mtime,
        ingested_at=datetime.utcnow(),
        is_current=True,
        group_key=group_key,
    )
    comments = _parse_comments_from_meta(meta)
    # Extract per-file category/target token counts from tile (computed in parse_pb_to_tile)
    cat_counts: dict[str, int] = tile.get("categories_counts") or {}
    tgt_counts: dict[str, int] = tile.get("targets_counts") or {}
    cat_display: dict[str, str] = tile.get("categories_display") or {}
    tgt_display: dict[str, str] = tile.get("targets_display") or {}
    return record, comments, cat_counts, tgt_counts, cat_display, tgt_display


def mark_group_current(s, group_key: str) -> None:
    # Mark only the latest mtime as current within the group
    # Fetch all in group, find max mtime
    items = (
        s.query(PBFile)
        .filter(PBFile.group_key == group_key)
        .order_by(PBFile.file_mtime.desc(), PBFile.id.desc())
        .all()
    )
    latest_id = items[0].id if items else None
    for it in items:
        it.is_current = it.id == latest_id


def refresh(full: bool = False) -> Dict[str, Any]:
    # Align retries with entrypoint.sh envs for consistency
    max_tries = int(os.environ.get("WAIT_FOR_DB_MAX_TRIES", "60"))
    sleep_secs = float(os.environ.get("WAIT_FOR_DB_SLEEP", "2"))
    ensure_db(max_tries=max_tries, sleep_secs=sleep_secs)
    files = collect_files()
    now = datetime.utcnow()
    last = None if full else load_last_refresh()
    processed = 0
    skipped = 0
    failed = 0
    groups_touched: set[str] = set()

    total = len(files)
    print(f"[INFO] Found {total} PB files in {pb_folder()}.", flush=True)
    if last:
        print(
            f"[INFO] Last refresh at {last.isoformat()} — only newer files will be processed.",
            flush=True,
        )
    else:
        print("[INFO] Full refresh (processing all files).", flush=True)

    with get_session() as s:
        for idx, p in enumerate(files, start=1):
            st = p.stat()
            file_mtime = datetime.fromtimestamp(int(st.st_mtime))
            if last and file_mtime <= last:
                skipped += 1
                print(f"[SKIP] {idx}/{total} {p.name} (unchanged)", flush=True)
                continue
            try:
                print(f"[LOAD] {idx}/{total} {p.name}", flush=True)
                rec, comments, cat_counts, tgt_counts, cat_disp, tgt_disp = ingest_file(
                    p
                )
                # Link supersedes when same group exists current
                prev: PBFile | None = (
                    s.query(PBFile)
                    .filter(
                        PBFile.group_key == rec.group_key, PBFile.is_current == True
                    )
                    .one_or_none()  # noqa: E712
                )
                # Idempotency guard: if there is a current record with same or newer file_mtime
                # and same on-disk path, skip creating a new version.
                if prev and prev.file_mtime and rec.file_mtime <= prev.file_mtime:
                    # Also skip if path unchanged (no move/rename)
                    if (prev.path or "") == rec.path:
                        skipped += 1
                        print(
                            f"[SKIP] {idx}/{total} {p.name} (no newer mtime than current)",
                            flush=True,
                        )
                        continue
                if prev:
                    rec.supersedes_id = prev.id
                s.add(rec)
                s.flush()
                # Insert comments for this version (default active)
                for idx_c, text in enumerate(comments, start=1):
                    s.add(
                        PBComment(file_id=rec.id, idx=idx_c, text=text, is_active=True)
                    )
                # Insert categories/targets for this version (default active)
                for norm, cnt in (cat_counts or {}).items():
                    norm_str = str(norm).strip().lower()
                    if norm_str:
                        display = (cat_disp or {}).get(norm_str, norm_str)
                        s.add(
                            PBCategory(
                                file_id=rec.id,
                                value=str(display),
                                norm=norm_str,
                                count_in_file=int(cnt or 1),
                                is_active=True,
                            )
                        )
                for norm, cnt in (tgt_counts or {}).items():
                    norm_str = str(norm).strip().lower()
                    if norm_str:
                        display = (tgt_disp or {}).get(norm_str, norm_str)
                        s.add(
                            PBTarget(
                                file_id=rec.id,
                                value=str(display),
                                norm=norm_str,
                                count_in_file=int(cnt or 1),
                                is_active=True,
                            )
                        )
                groups_touched.add(rec.group_key)
                processed += 1
                print(f"[OK]   {idx}/{total} {p.name}", flush=True)
            except Exception as e:
                failed += 1
                # Ensure session is usable for next iterations
                try:
                    s.rollback()
                except Exception:
                    pass
                print(f"[ERR]  {idx}/{total} {p.name} -> {e}", flush=True)

        # Enforce current per touched group
        if groups_touched:
            print(
                f"[INFO] Marking current versions for {len(groups_touched)} groups...",
                flush=True,
            )
            for g in groups_touched:
                mark_group_current(s, g)
                # Sync comments is_active with file is_current within the group
                files_in_group: list[PBFile] = (
                    s.query(PBFile)
                    .filter(PBFile.group_key == g)
                    .order_by(PBFile.id)
                    .all()
                )
                for f in files_in_group:
                    s.query(PBComment).filter(PBComment.file_id == f.id).update(
                        {PBComment.is_active: bool(f.is_current)},
                        synchronize_session=False,
                    )
                    s.query(PBCategory).filter(PBCategory.file_id == f.id).update(
                        {PBCategory.is_active: bool(f.is_current)},
                        synchronize_session=False,
                    )
                    s.query(PBTarget).filter(PBTarget.file_id == f.id).update(
                        {PBTarget.is_active: bool(f.is_current)},
                        synchronize_session=False,
                    )

        # Deactivate current files (and comments) whose source files disappeared
        present_names = {p.name for p in files}
        missing_currents: list[PBFile] = (
            s.query(PBFile)
            .filter(PBFile.is_current == True)
            .filter(~PBFile.file_name.in_(list(present_names)))  # noqa: E712
            .all()
        )
        for mf in missing_currents:
            mf.is_current = False
            s.query(PBComment).filter(PBComment.file_id == mf.id).update(
                {PBComment.is_active: False}, synchronize_session=False
            )
            s.query(PBCategory).filter(PBCategory.file_id == mf.id).update(
                {PBCategory.is_active: False}, synchronize_session=False
            )
            s.query(PBTarget).filter(PBTarget.file_id == mf.id).update(
                {PBTarget.is_active: False}, synchronize_session=False
            )

    save_refresh_timestamp("pb", now)

    # Invalidate in-process caches so admin/public pages reflect latest immediately
    try:
        _invalidate_pb_caches()
    except Exception:
        pass

    # After a refresh, rebuild global ZIP if the set changed
    try:
        export_service.build_if_changed()
    except Exception:
        # Non-fatal in CLI context
        pass

    return {
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "total": len(files),
        "last_refresh_prev": last.isoformat() if last else None,
        "refreshed_at": now.isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh PB database from files")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Process all files, ignore last refresh time",
    )
    args = parser.parse_args()
    result = refresh(full=args.full)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
