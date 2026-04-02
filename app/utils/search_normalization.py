from __future__ import annotations

import unicodedata
from typing import Any

_SEARCH_CHAR_FOLDS = str.maketrans(
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
        "Ą": "a",
        "Ć": "c",
        "Ę": "e",
        "Ł": "l",
        "Ń": "n",
        "Ó": "o",
        "Ś": "s",
        "Ź": "z",
        "Ż": "z",
    }
)


def fold_search_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.translate(_SEARCH_CHAR_FOLDS)
    text = text.encode("ascii", "ignore").decode("ascii")
    return " ".join(text.lower().split())


def build_search_text_norm(*parts: Any) -> str:
    return " ".join(filter(None, (fold_search_text(part) for part in parts)))
