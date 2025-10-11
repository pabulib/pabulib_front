from __future__ import annotations

from pathlib import Path


def workspace_root() -> Path:
    # app/utils/file_helpers.py -> app -> repo root
    return Path(__file__).resolve().parents[2]


def pb_folder() -> Path:
    return workspace_root() / "pb_files"


def is_safe_filename(name: str) -> bool:
    # basic safety for path traversal and extension
    return (
        name.endswith(".pb")
        and ".." not in name
        and not name.startswith("/")
        and "/" not in name
        and "\\" not in name
    )


def read_file_lines(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return [line.rstrip("\n") for line in f]


def log(msg: str) -> None:
    try:
        print(f"[APP] {msg}", flush=True)
    except Exception:
        pass
