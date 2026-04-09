from __future__ import annotations

import os
import unicodedata

from werkzeug.utils import secure_filename

_FILENAME_CHAR_FOLDS = str.maketrans(
    {
        "ą": "a",
        "ć": "c",
        "ę": "e",
        "ł": "l",
        "ń": "n",
        "ó": "o",
        "ś": "s",
        "ź": "z",
        "ż": "z",
        "Ą": "A",
        "Ć": "C",
        "Ę": "E",
        "Ł": "L",
        "Ń": "N",
        "Ó": "O",
        "Ś": "S",
        "Ź": "Z",
        "Ż": "Z",
    }
)


def ascii_fold_filename_part(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.translate(_FILENAME_CHAR_FOLDS)
    return text.encode("ascii", "ignore").decode("ascii")


def normalize_storage_filename(filename: str, fallback_stem: str = "file") -> str:
    raw_name = str(filename or "").strip()
    stem, ext = os.path.splitext(raw_name)
    folded_stem = ascii_fold_filename_part(stem)
    safe_stem = secure_filename(folded_stem).strip("._")
    safe_ext = secure_filename(ascii_fold_filename_part(ext)).lower()

    if not safe_ext.startswith("."):
        safe_ext = f".{safe_ext}" if safe_ext else ""
    if not safe_stem:
        safe_stem = secure_filename(ascii_fold_filename_part(fallback_stem)) or "file"

    return f"{safe_stem}{safe_ext}"

