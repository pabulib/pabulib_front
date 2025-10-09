import csv
from io import StringIO
from typing import Dict, List, Tuple


def parse_pb_lines(lines: List[str]) -> Tuple[Dict, Dict, Dict, bool, bool]:
    meta, projects, votes = {}, {}, {}
    section = ""
    header = []

    # Use StringIO to simulate file-like behavior for csv.reader
    reader = csv.reader(StringIO("\n".join(lines)), delimiter=";")

    for row in reader:
        if row:
            if str(row[0]).strip().lower() in ["meta", "projects", "votes"]:
                section = str(row[0]).strip().lower()
                header = next(reader)
                check_header = header[0].strip().lower()
                # Validate header for each section
                if section == "projects" and check_header != "project_id":
                    raise ValueError(
                        f"First value in PROJECTS section is not 'project_id': {check_header}"
                    )
                if section == "votes" and check_header != "voter_id":
                    raise ValueError(
                        f"First value in VOTES section is not 'voter_id': {check_header}"
                    )
            elif section == "meta":
                meta[row[0]] = row[1].strip()
            elif section == "projects":
                votes_in_projects = True if "votes" in header else False
                scores_in_projects = True if "score" in header else False
                projects[row[0]] = {"project_id": row[0]}
                for it, key in enumerate(header[1:]):
                    projects[row[0]][key.strip()] = row[it + 1].strip()
            elif section == "votes":
                if votes.get(row[0]):
                    raise RuntimeError(f"Duplicated Voter ID!! {row[0]}")
                votes[row[0]] = {}
                for it, key in enumerate(header[1:]):
                    votes[row[0]][key.strip()] = row[it + 1].strip()

    return meta, projects, votes, votes_in_projects, scores_in_projects
