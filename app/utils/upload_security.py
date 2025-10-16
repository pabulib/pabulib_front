import os
import tempfile
import uuid
from pathlib import Path

ALLOWED_EXTENSIONS = {".pb"}


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


def is_probably_text_file(path: Path, sample_bytes: int = 4096) -> bool:
    try:
        with path.open("rb") as f:
            b = f.read(sample_bytes)
        return is_probably_text_bytes(b)
    except Exception:
        return False


def public_tmp_dir() -> Path:
    """Create a dedicated temp directory for public uploads.
    Separate from admin tmp to avoid any cross-contamination.
    """
    base = Path(tempfile.gettempdir()) / "pabulib_public"
    base.mkdir(parents=True, exist_ok=True)
    # Unique per request/session folder
    uid = uuid.uuid4().hex
    p = base / uid
    p.mkdir(mode=0o700, parents=True, exist_ok=True)
    return p
