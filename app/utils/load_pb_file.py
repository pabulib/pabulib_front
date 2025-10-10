import csv
from io import StringIO
from typing import Dict, List, Tuple


def parse_pb_lines(lines: List[str]) -> Tuple[Dict, Dict, Dict, bool, bool]:
    meta: Dict = {}
    projects: Dict = {}
    votes: Dict = {}
    section = ""
    header: List[str] = []
    votes_in_projects = False
    scores_in_projects = False

    # Use StringIO to simulate file-like behavior for csv.reader
    reader = csv.reader(StringIO("\n".join(lines)), delimiter=";")

    for row in reader:
        if not row:
            continue
        first = str(row[0]).strip().lower() if row else ""
        if first in ["meta", "projects", "votes"]:
            section = first
            try:
                header = next(reader)
            except StopIteration:
                header = []
            if header:
                check_header = str(header[0]).strip().lower()
                # Validate header for each section
                if section == "projects" and check_header != "project_id":
                    raise ValueError(
                        f"First value in PROJECTS section is not 'project_id': {check_header}"
                    )
                if section == "votes" and check_header != "voter_id":
                    raise ValueError(
                        f"First value in VOTES section is not 'voter_id': {check_header}"
                    )
            continue

        if section == "meta":
            if len(row) >= 2:
                meta[row[0]] = row[1].strip()
            continue

        if section == "projects":
            votes_in_projects = True if ("votes" in header) else votes_in_projects
            scores_in_projects = True if ("score" in header) else scores_in_projects
            if not row:
                continue
            pid = row[0]
            projects[pid] = {"project_id": pid}
            for it, key in enumerate(header[1:]):
                if it + 1 < len(row):
                    projects[pid][key.strip()] = row[it + 1].strip()
            continue

        if section == "votes":
            if not row:
                continue
            vid = row[0]
            if votes.get(vid):
                raise RuntimeError(f"Duplicated Voter ID!! {vid}")
            votes[vid] = {}
            for it, key in enumerate(header[1:]):
                if it + 1 < len(row):
                    votes[vid][key.strip()] = row[it + 1].strip()

    return meta, projects, votes, votes_in_projects, scores_in_projects
