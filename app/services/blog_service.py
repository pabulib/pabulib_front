from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional


try:
    import markdown as _markdown_lib
except Exception:  # pragma: no cover
    _markdown_lib = None


_POSTS_DIR = Path(__file__).resolve().parent.parent / "blog_posts"
_FRONT_MATTER_DELIMITER = "---"


@dataclass(frozen=True)
class BlogPost:
    slug: str
    title: str
    author: str
    published_on: date
    summary: str
    tags: List[str]
    body_markdown: str
    body_html: str
    source_path: Path


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return slug or "post"


def _parse_date(raw_value: str, source_path: Path) -> date:
    try:
        return date.fromisoformat((raw_value or "").strip())
    except Exception as exc:
        raise ValueError(f"Invalid blog post date in {source_path.name}") from exc


def _parse_tags(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    tags = [tag.strip() for tag in raw_value.split(",")]
    return [tag for tag in tags if tag]


def _strip_markdown(markdown_text: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", markdown_text or "")
    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"^[#>\-\*\d\.\s]+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _render_markdown(markdown_text: str) -> str:
    if _markdown_lib is None:
        paragraphs = [segment.strip() for segment in (markdown_text or "").split("\n\n")]
        safe_paragraphs = [f"<p>{html.escape(p)}</p>" for p in paragraphs if p]
        return "\n".join(safe_paragraphs)
    return _markdown_lib.markdown(
        markdown_text,
        extensions=["fenced_code", "tables", "sane_lists"],
    )


def _parse_front_matter(raw_text: str, source_path: Path) -> tuple[dict, str]:
    if not raw_text.startswith(f"{_FRONT_MATTER_DELIMITER}\n"):
        raise ValueError(f"Blog post {source_path.name} is missing front matter")

    try:
        _, front_matter_text, body = raw_text.split(
            f"{_FRONT_MATTER_DELIMITER}\n", 2
        )
    except ValueError as exc:
        raise ValueError(
            f"Blog post {source_path.name} has invalid front matter formatting"
        ) from exc

    metadata = {}
    for line in front_matter_text.splitlines():
        if not line.strip():
            continue
        if ":" not in line:
            raise ValueError(
                f"Blog post {source_path.name} has invalid front matter line: {line}"
            )
        key, value = line.split(":", 1)
        metadata[key.strip().lower()] = value.strip()
    return metadata, body.strip()


def _parse_post(source_path: Path) -> BlogPost:
    raw_text = source_path.read_text(encoding="utf-8")
    metadata, body = _parse_front_matter(raw_text, source_path)

    title = metadata.get("title") or source_path.stem.replace("-", " ").title()
    slug = metadata.get("slug") or _slugify(title)
    author = metadata.get("author") or "Pabulib Team"
    published_on = _parse_date(metadata.get("date", ""), source_path)
    summary = metadata.get("summary") or _strip_markdown(body)[:180].rstrip()
    tags = _parse_tags(metadata.get("tags", ""))

    return BlogPost(
        slug=slug,
        title=title,
        author=author,
        published_on=published_on,
        summary=summary,
        tags=tags,
        body_markdown=body,
        body_html=_render_markdown(body),
        source_path=source_path,
    )


def list_blog_posts(tag: Optional[str] = None) -> List[BlogPost]:
    if not _POSTS_DIR.exists():
        return []

    posts = [_parse_post(path) for path in sorted(_POSTS_DIR.glob("*.md"))]
    posts.sort(key=lambda post: (post.published_on, post.slug), reverse=True)

    if tag:
        desired = tag.strip().lower()
        posts = [
            post for post in posts if any(existing.lower() == desired for existing in post.tags)
        ]

    return posts


def get_blog_post(slug: str) -> Optional[BlogPost]:
    for post in list_blog_posts():
        if post.slug == slug:
            return post
    return None


def list_blog_tags() -> List[str]:
    tags = {tag for post in list_blog_posts() for tag in post.tags}
    return sorted(tags, key=lambda value: value.lower())


def blog_sitemap_entries(base_url: str) -> List[dict]:
    posts = list_blog_posts()
    entries = [
        {
            "loc": f"{base_url}/blog",
            "changefreq": "weekly",
            "priority": "0.6",
            "lastmod": posts[0].published_on.isoformat() if posts else datetime.utcnow().date().isoformat(),
        }
    ]
    for post in posts:
        entries.append(
            {
                "loc": f"{base_url}/blog/{post.slug}",
                "changefreq": "monthly",
                "priority": "0.5",
                "lastmod": post.published_on.isoformat(),
            }
        )
    return entries
