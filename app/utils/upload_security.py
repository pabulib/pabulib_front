import csv
import os
import re
import shutil
import time
import uuid
from pathlib import Path

from .pb_utils import workspace_root

ALLOWED_EXTENSIONS = {".pb"}
_ARCHIVE_SIGNATURES = (
    (b"PK\x03\x04", "zip archive"),
    (b"PK\x05\x06", "zip archive"),
    (b"PK\x07\x08", "zip archive"),
    (b"\x1f\x8b\x08", "gzip archive"),
    (b"BZh", "bzip2 archive"),
    (b"\xfd7zXZ\x00", "xz archive"),
    (b"7z\xbc\xaf\x27\x1c", "7z archive"),
    (b"Rar!\x1a\x07\x00", "rar archive"),
    (b"Rar!\x1a\x07\x01\x00", "rar archive"),
)
_EMAIL_RE = re.compile(
    r"^[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)+$",
    re.IGNORECASE,
)


def is_allowed_extension(filename: str) -> bool:
    name = (filename or "").lower().strip()
    return any(name.endswith(ext) for ext in ALLOWED_EXTENSIONS)


def is_probably_text_bytes(b: bytes, max_nontext_ratio: float = 0.20) -> bool:
    if not b:
        return True
    # Consider a set of common printable bytes (ASCII + common utf-8 parts)
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27})
    text_chars.extend(range(32, 127))
    # UTF-8 continuation bytes are 0x80-0xBF; they can appear in text
    text_chars.extend(range(0x80, 0xBF + 1))
    nontext = sum(1 for ch in b if ch not in text_chars)
    return nontext / max(1, len(b)) <= max_nontext_ratio


def _sample_offsets(file_size: int, sample_bytes: int) -> list[int]:
    if file_size <= sample_bytes:
        return [0]
    offsets = [0, max(0, file_size - sample_bytes)]
    if file_size > sample_bytes * 2:
        offsets.append(max(0, (file_size // 2) - (sample_bytes // 2)))
    return sorted(set(offsets))


def _detect_archive_bytes(data: bytes) -> str | None:
    for signature, kind in _ARCHIVE_SIGNATURES:
        if data.startswith(signature):
            return kind
    return None


def inspect_uploaded_file(path: Path, sample_bytes: int = 8192) -> tuple[bool, str | None]:
    try:
        if not path.exists() or not path.is_file():
            return False, "Uploaded file not found"
        if path.is_symlink():
            return False, "Symlinks are not allowed"
        file_size = path.stat().st_size
        with path.open("rb") as handle:
            for offset in _sample_offsets(file_size, sample_bytes):
                handle.seek(offset)
                chunk = handle.read(sample_bytes)
                archive_kind = _detect_archive_bytes(chunk)
                if archive_kind:
                    return False, f"Archive payloads are not allowed ({archive_kind})"
                if b"\x00" in chunk:
                    return False, "Binary file content is not allowed"
                if chunk and not is_probably_text_bytes(chunk):
                    return False, "File does not look like text"
    except Exception:
        return False, "Unable to inspect uploaded file"
    return True, None


def is_probably_text_file(path: Path, sample_bytes: int = 4096) -> bool:
    ok, _reason = inspect_uploaded_file(path, sample_bytes=sample_bytes)
    return ok


def validate_email_address(email: str) -> bool:
    value = (email or "").strip()
    if not value or len(value) > 254 or not _EMAIL_RE.fullmatch(value):
        return False
    try:
        local, domain = value.rsplit("@", 1)
    except ValueError:
        return False
    if not local or len(local) > 64:
        return False
    labels = domain.split(".")
    return all(
        label and not label.startswith("-") and not label.endswith("-")
        for label in labels
    )


def is_safe_regular_file(path: Path, base_dir: Path) -> bool:
    try:
        if not path.exists() or not path.is_file() or path.is_symlink():
            return False
        resolved_base = base_dir.resolve()
        resolved_path = path.resolve()
        return resolved_path == resolved_base or resolved_base in resolved_path.parents
    except Exception:
        return False


def detect_formula_injection_cells(path: Path, max_hits: int = 5) -> list[str]:
    hits: list[str] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter=";")
            for row_idx, row in enumerate(reader, start=1):
                for col_idx, value in enumerate(row, start=1):
                    text = str(value or "").lstrip()
                    if not text:
                        continue
                    first = text[0]
                    suspicious = False
                    if first in {"=", "@"}:
                        suspicious = True
                    elif first in {"+", "-"} and len(text) > 1:
                        second = text[1]
                        suspicious = not (second.isdigit() or second == ".")
                    if suspicious:
                        hits.append(f"R{row_idx}C{col_idx}")
                        if len(hits) >= max_hits:
                            return hits
    except Exception:
        return []
    return hits


def cleanup_stale_files(
    base_dir: Path, max_age_seconds: int, skip_names: set[str] | None = None
) -> None:
    skip_names = skip_names or set()
    try:
        cutoff = time.time() - max_age_seconds
    except Exception:
        return
    try:
        for path in base_dir.iterdir():
            try:
                if path.name in skip_names:
                    continue
                if path.stat().st_mtime >= cutoff:
                    continue
                if path.is_symlink() or path.is_file():
                    path.unlink(missing_ok=True)
                elif path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
            except Exception:
                continue
    except Exception:
        return


def cleanup_stale_subdirectories(
    base_dir: Path, max_age_seconds: int, skip_names: set[str] | None = None
) -> None:
    skip_names = skip_names or set()
    try:
        cutoff = time.time() - max_age_seconds
    except Exception:
        return
    try:
        for path in base_dir.iterdir():
            try:
                if path.name in skip_names or not path.is_dir():
                    continue
                if path.stat().st_mtime >= cutoff:
                    continue
                shutil.rmtree(path, ignore_errors=True)
            except Exception:
                continue
    except Exception:
        return


def _resolve_storage_dir(env_var: str, default_relative: str) -> Path:
    env_val = os.environ.get(env_var)
    if env_val:
        path = Path(env_val).expanduser()
        if not path.is_absolute():
            path = workspace_root() / path
        return path
    return workspace_root() / default_relative


def admin_waiting_room_dir() -> Path:
    base = _resolve_storage_dir("ADMIN_UPLOAD_DIR", "var/waiting_room/admin")
    base.mkdir(parents=True, exist_ok=True)
    cleanup_stale_files(
        base,
        max_age_seconds=int(os.environ.get("ADMIN_UPLOAD_TTL_HOURS", "168")) * 3600,
        skip_names={".upload_settings.json"},
    )
    return base


def public_waiting_room_base_dir() -> Path:
    base = _resolve_storage_dir("PUBLIC_UPLOAD_DIR", "var/waiting_room/public")
    base.mkdir(parents=True, exist_ok=True)
    cleanup_stale_subdirectories(
        base,
        max_age_seconds=int(os.environ.get("PUBLIC_UPLOAD_TTL_HOURS", "24")) * 3600,
    )
    return base


def public_tmp_dir() -> Path:
    """Create a dedicated temp directory for public uploads.
    Separate from admin tmp to avoid any cross-contamination.
    """
    base = public_waiting_room_base_dir()
    # Unique per request/session folder
    uid = uuid.uuid4().hex
    p = base / uid
    p.mkdir(mode=0o700, parents=True, exist_ok=True)
    return p
