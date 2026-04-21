"""
Service for computing and caching beta rule comparison data for PB files.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from ..models import PBRuleComparison
from ..utils.load_pb_file import parse_pb_lines


SUPPORTED_ALTERNATIVE_RULES = {"equalshares/add1-comparison"}


def get_or_compute_rule_comparison(
    file_id: int,
    filename: str,
    file_path: Path,
    file_mtime: datetime,
    session: Session,
    alternative_rule: str = "equalshares/add1-comparison",
) -> Dict[str, Any]:
    alternative_rule = str(alternative_rule or "").strip().lower()
    if alternative_rule not in SUPPORTED_ALTERNATIVE_RULES:
        return {
            "ok": False,
            "supported": False,
            "alternative_rule": alternative_rule,
            "message": f"Unsupported comparison rule: {alternative_rule}",
        }

    cached = (
        session.query(PBRuleComparison)
        .filter_by(file_id=file_id, alternative_rule=alternative_rule)
        .first()
    )
    if cached and cached.file_mtime == file_mtime:
        return json.loads(cached.data)

    if cached:
        session.delete(cached)
        session.commit()

    comparison_data = _compute_rule_comparison(filename, file_path, alternative_rule)

    new_cache = PBRuleComparison(
        file_id=file_id,
        alternative_rule=alternative_rule,
        data=json.dumps(comparison_data),
        computed_at=datetime.utcnow(),
        file_mtime=file_mtime,
    )
    session.add(new_cache)
    session.commit()
    return comparison_data


def _compute_rule_comparison(
    filename: str, file_path: Path, alternative_rule: str
) -> Dict[str, Any]:
    with file_path.open("r", encoding="utf-8", newline="") as f:
        lines = [line.rstrip("\n") for line in f]

    meta, projects, votes, _votes_in_projects, _scores_in_projects = parse_pb_lines(lines)

    current_rule = str(meta.get("rule") or "unknown").strip() or "unknown"
    vote_type = str(meta.get("vote_type") or "").strip().lower()
    if vote_type != "approval":
        return {
            "ok": False,
            "supported": False,
            "filename": filename,
            "current_rule": current_rule,
            "alternative_rule": alternative_rule,
            "vote_type": vote_type,
            "message": "Beta rule comparison currently supports approval ballots only.",
        }

    try:
        total_budget = int(float(str(meta.get("budget") or "").strip().replace(",", ".")))
    except Exception:
        return {
            "ok": False,
            "supported": False,
            "filename": filename,
            "current_rule": current_rule,
            "alternative_rule": alternative_rule,
            "vote_type": vote_type,
            "message": "This file does not have a usable budget value for rule comparison.",
        }

    current_winners = sorted(
        pid for pid, project in projects.items() if _is_selected(project.get("selected"))
    )
    if not current_winners:
        return {
            "ok": False,
            "supported": False,
            "filename": filename,
            "current_rule": current_rule,
            "alternative_rule": alternative_rule,
            "vote_type": vote_type,
            "message": "This file does not expose selected winners in PROJECTS, so comparison is not available yet.",
        }

    project_costs = _extract_project_costs(projects)
    if not project_costs:
        return {
            "ok": False,
            "supported": False,
            "filename": filename,
            "current_rule": current_rule,
            "alternative_rule": alternative_rule,
            "vote_type": vote_type,
            "message": "No usable project costs were found for rule comparison.",
        }

    approvers, voter_ids, voter_approvals = _build_approvers(projects, votes)
    if not voter_ids:
        return {
            "ok": False,
            "supported": False,
            "filename": filename,
            "current_rule": current_rule,
            "alternative_rule": alternative_rule,
            "vote_type": vote_type,
            "message": "No approval votes were found for rule comparison.",
        }

    comparison_details: Dict[str, Any] = {}
    if alternative_rule == "equalshares/add1-comparison":
        add1_winners, add1_details = _equal_shares_add1(
            voter_ids, project_costs, approvers, total_budget
        )
        greedy_winners = _greedy_winners(project_costs, approvers, total_budget)
        alternative_winners, comparison_details = _apply_comparison_step(
            voter_approvals=voter_approvals,
            add1_winners=add1_winners,
            greedy_winners=greedy_winners,
        )
        comparison_details.update(add1_details)
    else:  # pragma: no cover
        alternative_winners = []

    return _build_comparison_payload(
        filename=filename,
        current_rule=current_rule,
        alternative_rule=alternative_rule,
        total_budget=total_budget,
        projects=projects,
        approvers=approvers,
        current_winners=current_winners,
        alternative_winners=alternative_winners,
        comparison_details=comparison_details,
    )


def _extract_project_costs(projects: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    costs: Dict[str, int] = {}
    for pid, project in projects.items():
        try:
            raw = str(project.get("cost") or "").strip().replace(",", ".")
            if not raw:
                continue
            cost = int(float(raw))
            if cost > 0:
                costs[str(pid)] = cost
        except Exception:
            continue
    return costs


def _build_approvers(
    projects: Dict[str, Dict[str, Any]], votes: Dict[str, Dict[str, Any]]
) -> tuple[Dict[str, List[str]], List[str], Dict[str, set[str]]]:
    project_ids = {str(pid) for pid in projects.keys()}
    approvers: Dict[str, List[str]] = {pid: [] for pid in project_ids}
    voter_ids: List[str] = []
    voter_approvals: Dict[str, set[str]] = {}

    for voter_id, vote in votes.items():
        raw_vote = vote.get("vote")
        if not raw_vote:
            continue
        voter_id_str = str(voter_id)
        voter_ids.append(voter_id_str)
        seen_for_voter = set()
        selections = raw_vote if isinstance(raw_vote, list) else str(raw_vote).split(",")
        for pid in selections:
            project_id = str(pid).strip()
            if not project_id or project_id not in project_ids or project_id in seen_for_voter:
                continue
            approvers[project_id].append(voter_id_str)
            seen_for_voter.add(project_id)
        voter_approvals[voter_id_str] = seen_for_voter

    return approvers, voter_ids, voter_approvals


def _equal_shares(
    voters: List[str],
    cost: Dict[str, int],
    approvers: Dict[str, List[str]],
    total_budget: int,
    initial_budget_per_voter: float | None = None,
) -> List[str]:
    if not voters:
        return []

    starting_budget = (
        initial_budget_per_voter
        if initial_budget_per_voter is not None
        else total_budget / len(voters)
    )
    budget = {voter: float(starting_budget) for voter in voters}
    remaining = {
        project_id: len(approvers.get(project_id, []))
        for project_id in cost
        if cost[project_id] > 0 and len(approvers.get(project_id, [])) > 0
    }
    winners: List[str] = []

    while True:
        best_projects: List[str] = []
        best_eff_vote_count = 0.0

        remaining_sorted = sorted(
            remaining,
            key=lambda project_id: (
                -remaining[project_id],
                cost.get(project_id, 0),
                -len(approvers.get(project_id, [])),
                project_id,
            ),
        )

        to_remove: List[str] = []
        for project_id in remaining_sorted:
            previous_eff_vote_count = remaining[project_id]
            if previous_eff_vote_count < best_eff_vote_count:
                break

            supporters = approvers.get(project_id, [])
            money_behind_now = sum(budget[voter] for voter in supporters)
            project_cost = cost[project_id]
            if money_behind_now + 1e-9 < project_cost:
                to_remove.append(project_id)
                continue

            paid_so_far = 0.0
            denominator = len(supporters)
            supporters_sorted = sorted(supporters, key=lambda voter: budget[voter])
            for voter in supporters_sorted:
                if denominator <= 0:
                    break
                max_payment = (project_cost - paid_so_far) / denominator
                eff_vote_count = project_cost / max_payment if max_payment > 0 else 0.0
                if max_payment > budget[voter] + 1e-9:
                    paid_so_far += budget[voter]
                    denominator -= 1
                else:
                    remaining[project_id] = eff_vote_count
                    if eff_vote_count > best_eff_vote_count + 1e-9:
                        best_eff_vote_count = eff_vote_count
                        best_projects = [project_id]
                    elif abs(eff_vote_count - best_eff_vote_count) <= 1e-9:
                        best_projects.append(project_id)
                    break

        for project_id in to_remove:
            remaining.pop(project_id, None)

        if not best_projects:
            break

        chosen_project = _break_mes_ties(best_projects, cost, approvers)
        winners.append(chosen_project)
        remaining.pop(chosen_project, None)

        best_max_payment = cost[chosen_project] / best_eff_vote_count
        for voter in approvers.get(chosen_project, []):
            budget[voter] = max(0.0, budget[voter] - min(budget[voter], best_max_payment))

    return winners


def _equal_shares_add1(
    voters: List[str],
    cost: Dict[str, int],
    approvers: Dict[str, List[str]],
    total_budget: int,
) -> tuple[List[str], Dict[str, Any]]:
    base_share = total_budget / len(voters)

    def run_with_increment(increment: int) -> tuple[List[str], int]:
        winners = _equal_shares(
            voters,
            cost,
            approvers,
            total_budget,
            initial_budget_per_voter=base_share + increment,
        )
        return winners, _winner_cost_from_costs(winners, cost)

    best_winners, best_cost = run_with_increment(0)
    best_increment = 0
    high = 1
    high_winners: List[str] = []
    high_cost = 0

    for _ in range(20):
        high_winners, high_cost = run_with_increment(high)
        if high_cost > total_budget or len(high_winners) >= len(cost):
            break
        best_winners, best_cost, best_increment = high_winners, high_cost, high
        high *= 2

    if high_cost <= total_budget:
        best_winners, best_cost, best_increment = high_winners, high_cost, high
    else:
        low = best_increment
        while low + 1 < high:
            mid = (low + high) // 2
            mid_winners, mid_cost = run_with_increment(mid)
            if mid_cost <= total_budget:
                best_winners, best_cost, best_increment = mid_winners, mid_cost, mid
                low = mid
            else:
                high = mid

    return best_winners, {
        "add1_increment": best_increment,
        "add1_cost": best_cost,
        "add1_count": len(best_winners),
    }


def _greedy_winners(
    cost: Dict[str, int],
    approvers: Dict[str, List[str]],
    total_budget: int,
) -> List[str]:
    winners: List[str] = []
    used_budget = 0
    for project_id in sorted(
        cost,
        key=lambda pid: (-len(approvers.get(pid, [])), cost.get(pid, 0), pid),
    ):
        project_cost = cost[project_id]
        if used_budget + project_cost <= total_budget:
            winners.append(project_id)
            used_budget += project_cost
    return winners


def _apply_comparison_step(
    *,
    voter_approvals: Dict[str, set[str]],
    add1_winners: List[str],
    greedy_winners: List[str],
) -> tuple[List[str], Dict[str, Any]]:
    add1_set = set(add1_winners)
    greedy_set = set(greedy_winners)
    add1_preferred = 0
    greedy_preferred = 0
    ties = 0

    for approvals in voter_approvals.values():
        add1_utility = len(approvals & add1_set)
        greedy_utility = len(approvals & greedy_set)
        if greedy_utility > add1_utility:
            greedy_preferred += 1
        elif add1_utility > greedy_utility:
            add1_preferred += 1
        else:
            ties += 1

    switched_to_greedy = greedy_preferred > add1_preferred
    return (
        greedy_winners if switched_to_greedy else add1_winners,
        {
            "comparison_step": True,
            "comparison_selected": "greedy" if switched_to_greedy else "equalshares/add1",
            "add1_preferred_voters": add1_preferred,
            "greedy_preferred_voters": greedy_preferred,
            "tied_voters": ties,
            "greedy_count": len(greedy_winners),
            "add1_before_comparison_count": len(add1_winners),
            "switched_to_greedy": switched_to_greedy,
        },
    )


def _break_mes_ties(
    choices: List[str], cost: Dict[str, int], approvers: Dict[str, List[str]]
) -> str:
    return sorted(
        choices,
        key=lambda project_id: (
            cost.get(project_id, 0),
            -len(approvers.get(project_id, [])),
            project_id,
        ),
    )[0]


def _build_comparison_payload(
    *,
    filename: str,
    current_rule: str,
    alternative_rule: str,
    total_budget: int,
    projects: Dict[str, Dict[str, Any]],
    approvers: Dict[str, List[str]],
    current_winners: List[str],
    alternative_winners: List[str],
    comparison_details: Dict[str, Any],
) -> Dict[str, Any]:
    current_set = set(current_winners)
    alternative_set = set(alternative_winners)
    overlap = sorted(current_set & alternative_set)
    only_current = sorted(current_set - alternative_set)
    only_alternative = sorted(alternative_set - current_set)

    all_difference_ids = sorted(set(only_current) | set(only_alternative))
    difference_rows = [
        {
            "project_id": pid,
            "name": str(projects.get(pid, {}).get("name") or f"Project {pid}"),
            "cost": _safe_project_cost(projects.get(pid, {}).get("cost")),
            "votes": len(approvers.get(pid, [])),
            "selected_current": pid in current_set,
            "selected_alternative": pid in alternative_set,
        }
        for pid in all_difference_ids
    ]
    difference_rows.sort(
        key=lambda row: (
            not row["selected_alternative"],
            -int(row["votes"] or 0),
            int(row["cost"] or 0),
            str(row["project_id"]),
        )
    )

    current_cost = _winner_cost(current_winners, projects)
    alternative_cost = _winner_cost(alternative_winners, projects)

    notes = [
        "Beta version: comparison currently supports approval ballots only.",
        "The alternative outcome uses equalshares/add1-comparison: MES with Add1 completion, followed by a comparison step against a greedy benchmark.",
        "The current outcome is read from the file's selected projects, not recomputed from metadata.",
    ]
    if comparison_details.get("comparison_step"):
        selected = comparison_details.get("comparison_selected", "equalshares/add1")
        notes.append(
            "Comparison step selected "
            f"{selected}: {comparison_details.get('add1_preferred_voters', 0)} voters preferred MES/Add1, "
            f"{comparison_details.get('greedy_preferred_voters', 0)} preferred greedy, "
            f"and {comparison_details.get('tied_voters', 0)} were tied."
        )

    return {
        "ok": True,
        "supported": True,
        "filename": filename,
        "vote_type": "approval",
        "current_rule": current_rule,
        "alternative_rule": alternative_rule,
        "budget": total_budget,
        "summary": {
            "current_count": len(current_winners),
            "alternative_count": len(alternative_winners),
            "overlap_count": len(overlap),
            "only_current_count": len(only_current),
            "only_alternative_count": len(only_alternative),
            "current_cost": current_cost,
            "alternative_cost": alternative_cost,
            "current_budget_left": max(0, total_budget - current_cost),
            "alternative_budget_left": max(0, total_budget - alternative_cost),
        },
        "winners": {
            "current": current_winners,
            "alternative": alternative_winners,
            "overlap": overlap,
            "only_current": only_current,
            "only_alternative": only_alternative,
        },
        "difference_rows": difference_rows,
        "comparison_details": comparison_details,
        "notes": notes,
    }


def _winner_cost(winner_ids: List[str], projects: Dict[str, Dict[str, Any]]) -> int:
    total = 0
    for pid in winner_ids:
        total += _safe_project_cost(projects.get(pid, {}).get("cost"))
    return total


def _winner_cost_from_costs(winner_ids: List[str], costs: Dict[str, int]) -> int:
    return sum(costs.get(pid, 0) for pid in winner_ids)


def _safe_project_cost(value: Any) -> int:
    try:
        return int(float(str(value or "").strip().replace(",", ".")))
    except Exception:
        return 0


def _is_selected(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if text in {"true", "yes", "y"}:
        return True
    try:
        return int(float(text)) > 0
    except Exception:
        return False
