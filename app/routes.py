import io
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, Response, abort, render_template, request, send_file

from .db import get_session
from .models import PBFile
from .services.pb_service import aggregate_comments_cached as _aggregate_comments_cached
from .services.pb_service import (
    aggregate_statistics_cached as _aggregate_statistics_cached,
)
from .services.pb_service import get_all_current_file_paths, get_current_file_path
from .services.pb_service import get_tiles_cached as _get_tiles_cached
from .utils.file_helpers import is_safe_filename as _is_safe_filename
from .utils.formatting import format_int as _format_int
from .utils.load_pb_file import parse_pb_lines

bp = Blueprint(
    "main",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)


@bp.route("/")
def home():
    tiles = _get_tiles_cached()
    return render_template("index.html", tiles=tiles, count=len(tiles))


@bp.route("/format")
def format_page():
    return render_template("format.html")


@bp.route("/tools")
def tools_page():
    return render_template("tools.html")


@bp.route("/publications")
def publications_page():
    # Parse bib.bib and pass publications to the template
    import bibtexparser

    bib_path = Path(__file__).parent.parent / "bib.bib"
    publications = []
    if bib_path.exists():
        with open(bib_path, "r", encoding="utf-8") as bibfile:
            bib_database = bibtexparser.load(bibfile)
            for entry in bib_database.entries:
                authors_raw = entry.get("author", "")
                year = entry.get("year", "")
                title = entry.get("title", "")
                url = entry.get("url", "")
                # Split authors only by " and "
                authors_list = [
                    a
                    for a in authors_raw.replace("\n", " ").split(" and ")
                    if a.strip()
                ]
                authors = []
                for author in authors_list:
                    print("author", author)
                    parts = author.split()
                    if len(parts) > 1:
                        firstname = parts[-1]
                        firstname = firstname.replace(",", " ")
                        surname = parts[0]
                        surname = surname.replace(",", " ")
                        authors.append(f"{firstname[0]}. {surname}")
                    elif parts:
                        authors.append(parts[0])
                print("->", authors)
                authors_str = ", ".join(authors)
                publications.append(
                    {"authors": authors_str, "year": year, "title": title, "url": url}
                )
    return render_template("publications.html", publications=publications)


@bp.route("/about")
def about_page():
    return render_template("about.html")


@bp.route("/contact")
def contact_page():
    return render_template("contact.html", now=datetime.now())


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


@bp.route("/download/<path:filename>")
def download(filename: str):
    # DB-only: resolve path from DB
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)
    return send_file(path, as_attachment=True)


@bp.post("/download-selected")
def download_selected():
    names = request.form.getlist("files")
    select_all = request.form.get("select_all") == "true"

    if not names:
        abort(400, description="No files selected")

    # Get total count of current files to compare with selected count
    with get_session() as s:
        total_current_files = s.query(PBFile).filter(PBFile.is_current == True).count()

    # Check if user selected ALL current files (not just clicked select all after filtering)
    selected_all_current = len(names) == total_current_files and select_all

    if selected_all_current:
        # User selected ALL current files - try to use cached all_pb_files.zip
        cache_dir = Path(__file__).parent.parent / "cache"
        cache_dir.mkdir(exist_ok=True)  # Create cache directory if it doesn't exist
        cache_path = cache_dir / "all_pb_files.zip"

        all_file_pairs = get_all_current_file_paths()
        if not all_file_pairs:
            abort(404, description="No current files found")

        # Check if cache is valid (exists and is newer than all source files)
        cache_valid = False
        if cache_path.exists() and cache_path.is_file():
            cache_mtime = cache_path.stat().st_mtime
            cache_valid = all(
                cache_mtime >= path.stat().st_mtime for _, path in all_file_pairs
            )

        if cache_valid:
            # Return the cached zip file
            return send_file(
                cache_path, as_attachment=True, download_name="all_pb_files.zip"
            )
        else:
            # Create new zip file
            with zipfile.ZipFile(
                cache_path, mode="w", compression=zipfile.ZIP_DEFLATED
            ) as zf:
                for file_name, file_path in all_file_pairs:
                    zf.write(file_path, arcname=file_name)

            return send_file(
                cache_path, as_attachment=True, download_name="all_pb_files.zip"
            )

    # Original logic for individual file selection
    files = []
    for name in names:
        # basic safety: no directory traversal and must be .pb
        if "/" in name or ".." in name or not name.endswith(".pb"):
            continue
        p = get_current_file_path(name)
        if p and p.exists() and p.is_file():
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

    # No public API endpoints; JSON routes removed


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
    # Read path from DB record only (DB is the source of truth)
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            lines = [line.rstrip("\n") for line in f]
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
    # The 'vote' field is included in preferred_vote_cols and vote_columns,
    # and will be shown in the preview table. It is a list of project IDs if present.
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
    # Read path from DB record only (DB is the source of truth)
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            lines = [line.rstrip("\n") for line in f]
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
    project_costs = [
        float(proj.get("cost", 0)) for proj in projects.values() if proj.get("cost")
    ]

    # Vote counts per project
    vote_counts_per_project = {}
    vote_lengths = []  # Track how many projects each voter selected

    # Process all votes to extract vote data
    for vote_id, vote_data in votes.items():
        # Look for the vote column - now only "vote"
        vote_list = None
        for possible_vote_key in ["vote"]:
            if possible_vote_key in vote_data:
                vote_list = vote_data[possible_vote_key]
                break
        # The 'vote' field is referenced here. It is a list of project IDs if present.
        if vote_list is None:
            continue

        # Handle different vote data formats
        voted_projects = []
        if isinstance(vote_list, str) and vote_list.strip():
            # Parse comma-separated project IDs OR single project ID
            if "," in vote_list:
                # Multiple projects separated by commas
                voted_projects = [
                    pid.strip()
                    for pid in vote_list.split(",")
                    if pid.strip() and pid.strip() != ""
                ]
            else:
                # Single project ID (no comma) - could be a number or string
                single_project = vote_list.strip()
                if single_project and single_project != "":
                    voted_projects = [single_project]
        elif isinstance(vote_list, list) and vote_list:
            # Handle case where vote is already a list (from load_pb_file.py)
            voted_projects = [
                str(pid).strip() for pid in vote_list if pid and str(pid).strip()
            ]

        # Only process if we have valid projects
        if voted_projects:
            vote_length = len(voted_projects)
            vote_lengths.append(vote_length)

            for pid in voted_projects:
                pid_str = str(pid).strip()
                if pid_str:  # Ensure we have a non-empty project ID
                    vote_counts_per_project[pid_str] = (
                        vote_counts_per_project.get(pid_str, 0) + 1
                    )
        else:
            # Debug: Log when we can't extract voted projects from a vote
            if (
                vote_list is not None
            ):  # Only log if we found a vote column but couldn't parse it
                print(
                    f"DEBUG: Could not parse voted projects from vote {vote_id}: '{vote_list}' (type: {type(vote_list)})"
                )

    # removed debug prints

    # Prepare data for charts
    project_data = {
        "costs": project_costs,
        "scatter_data": [],  # Will be populated with {x: cost, y: votes} points
    }

    # Debug information
    # removed debug prints

    # Ensure we have data before creating the structure
    if vote_counts_per_project:
        vote_data = {
            "project_labels": list(vote_counts_per_project.keys())[
                :20
            ],  # Limit for readability
            "votes_per_project": list(vote_counts_per_project.values())[:20],
        }
    else:
        vote_data = {"project_labels": [], "votes_per_project": []}

    # Vote length distribution
    vote_length_counts = {}
    for length in vote_lengths:
        vote_length_counts[length] = vote_length_counts.get(length, 0) + 1

    vote_length_counts = dict(sorted(vote_length_counts.items()))
    # Debug: Log the vote length distribution we found
    if vote_length_counts:
        print(
            f"DEBUG: Vote length distribution: {dict(sorted(vote_length_counts.items()))}"
        )
        single_votes = vote_length_counts.get(1, 0)
        total_votes = sum(vote_length_counts.values())
        print(
            f"DEBUG: Single-project votes: {single_votes}/{total_votes} ({single_votes/total_votes*100:.1f}%)"
        )

    vote_length_data = None
    if vote_length_counts:
        sorted_lengths = sorted(vote_length_counts.keys())
        vote_length_data = {
            "labels": [str(length) for length in sorted_lengths],
            "counts": [vote_length_counts[length] for length in sorted_lengths],
        }
    else:
        # Add debugging information when no vote length data is available
        print(
            f"DEBUG: No vote length data - total votes: {len(votes)}, vote_lengths: {len(vote_lengths)}"
        )
        # Check a few sample votes for debugging
        if votes:
            sample_votes = list(votes.items())[:3]
            for vote_id, vote_data in sample_votes:
                print(f"DEBUG: Sample vote {vote_id}: {vote_data}")
                # Check all possible vote columns
                for possible_vote_key in [
                    "vote",
                    "votes",
                    "projects",
                    "selected_projects",
                ]:
                    if possible_vote_key in vote_data:
                        vote_value = vote_data[possible_vote_key]
                        print(
                            f"DEBUG: Found {possible_vote_key}: '{vote_value}' (type: {type(vote_value)})"
                        )

            # Also check what columns are available in votes
            if votes:
                first_vote = next(iter(votes.values()))
                print(f"DEBUG: Available vote columns: {list(first_vote.keys())}")

    # Top projects by votes
    top_projects_data = None
    if vote_counts_per_project:
        # Get top 10 projects by vote count
        sorted_projects = sorted(
            vote_counts_per_project.items(), key=lambda x: x[1], reverse=True
        )[:10]
        project_names = []
        project_votes = []

        for pid, vote_count in sorted_projects:
            # Try to get project name, fallback to ID
            proj_name = projects.get(pid, {}).get("name", f"Project {pid}")
            if len(proj_name) > 50:  # Truncate long names
                proj_name = proj_name[:47] + "..."
            project_names.append(proj_name)
            project_votes.append(vote_count)

        top_projects_data = {"labels": project_names, "votes": project_votes}

    # Project selection analysis (cost vs votes scatter)
    selection_data = None
    selected_projects = set()

    # Determine which projects were selected (if selection data available)
    if scores_in_projects:
        for proj_id, score_data in scores_in_projects.items():
            if score_data.get("selected", False) or score_data.get("winner", False):
                selected_projects.add(proj_id)

    if selected_projects or project_costs:
        selected_points = []
        not_selected_points = []

        for pid, proj in projects.items():
            cost = proj.get("cost")
            votes_received = vote_counts_per_project.get(pid, 0)
            if cost is not None:
                try:
                    point = {"x": float(cost), "y": votes_received}
                    if pid in selected_projects:
                        selected_points.append(point)
                    else:
                        not_selected_points.append(point)
                except (ValueError, TypeError):
                    continue

        if selected_points or not_selected_points:
            selection_data = {
                "selected": selected_points,
                "not_selected": not_selected_points,
            }

    # Create scatter plot data (cost vs votes) - for original scatter chart
    for pid, proj in projects.items():
        cost = proj.get("cost")
        votes_received = vote_counts_per_project.get(pid, 0)
        if cost is not None:
            try:
                project_data["scatter_data"].append(
                    {"x": float(cost), "y": votes_received}
                )
            except (ValueError, TypeError):
                continue

    # Category analysis (if available)
    category_data = None
    if any("category" in proj for proj in projects.values()):
        category_counts = {}
        for proj in projects.values():
            categories = proj.get("category", "")
            if categories:
                # Handle comma-separated categories
                cats = [
                    cat.strip() for cat in str(categories).split(",") if cat.strip()
                ]
                for cat in cats:
                    category_counts[cat] = category_counts.get(cat, 0) + 1

        if category_counts:
            category_data = {
                "labels": list(category_counts.keys()),
                "counts": list(category_counts.values()),
            }

    # Demographic analysis (if available)
    demographic_data = None
    if votes:
        age_counts = {}
        sex_counts = {}

        for vote_data in votes.values():
            age = vote_data.get("age")
            sex = vote_data.get("sex")

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
                if sex_str in ["M", "MALE"]:
                    sex_counts["Male"] = sex_counts.get("Male", 0) + 1
                elif sex_str in ["F", "FEMALE"]:
                    sex_counts["Female"] = sex_counts.get("Female", 0) + 1

        if age_counts or sex_counts:
            demographic_data = {}
            if age_counts:
                demographic_data["age"] = {
                    "labels": list(age_counts.keys()),
                    "counts": list(age_counts.values()),
                }
            if sex_counts:
                demographic_data["sex"] = {
                    "labels": list(sex_counts.keys()),
                    "counts": list(sex_counts.values()),
                }

    # Category cost analysis
    category_cost_data = None
    if any("category" in proj for proj in projects.values()):
        category_costs = {}
        category_counts_for_avg = {}

        for proj in projects.values():
            categories = proj.get("category", "")
            cost = proj.get("cost")
            if categories and cost is not None:
                try:
                    cost_float = float(cost)
                    cats = [
                        cat.strip() for cat in str(categories).split(",") if cat.strip()
                    ]
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
                category_cost_data = {"labels": labels, "avg_costs": avg_costs}

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
            period_labels.append(f"Period {len(period_labels) + 1}")

        timeline_data = {"dates": period_labels, "votes_per_day": votes_per_period}

    # Summary statistics
    summary_stats = {
        "total_voters": len(votes),
        "total_projects": len(projects),
        "selected_projects": len(selected_projects) if selected_projects else 0,
        "avg_vote_length": sum(vote_lengths) / len(vote_lengths) if vote_lengths else 0,
        "total_budget": sum(project_costs) if project_costs else 0,
        "avg_project_cost": (
            sum(project_costs) / len(project_costs) if project_costs else 0
        ),
        "most_popular_project_votes": (
            max(vote_counts_per_project.values()) if vote_counts_per_project else 0
        ),
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
            cost = proj.get("cost")
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

            numerator = sum(
                (c - mean_cost) * (v - mean_votes)
                for c, v in zip(costs_for_corr, votes_for_corr)
            )
            sum_sq_cost = sum((c - mean_cost) ** 2 for c in costs_for_corr)
            sum_sq_votes = sum((v - mean_votes) ** 2 for v in votes_for_corr)

            if sum_sq_cost > 0 and sum_sq_votes > 0:
                correlation = numerator / (sum_sq_cost * sum_sq_votes) ** 0.5
                correlations.append(correlation)
                labels.append("Cost vs Popularity")

        # Add more dummy correlations for demonstration
        if correlations:
            correlations.extend([0.1, -0.2, 0.3])  # Dummy values
            labels.extend(
                ["Budget vs Selection", "Category vs Votes", "Time vs Activity"]
            )

            correlation_data = {"labels": labels, "values": correlations}

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
    path = get_current_file_path(filename)
    if not path or not path.exists() or not path.is_file():
        abort(404)

    # Number of lines to include; default 80, cap 400
    try:
        n = int(request.args.get("lines", "80"))
    except Exception:
        n = 80
    n = max(1, min(n, 400))

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            lines = []
            for i, line in enumerate(f, start=1):
                if i > n:
                    break
                lines.append(line.rstrip("\n"))
        text = "\n".join(lines)
    except Exception as e:
        abort(400, description=f"Failed to read file: {e}")

    return Response(text, mimetype="text/plain; charset=utf-8")
