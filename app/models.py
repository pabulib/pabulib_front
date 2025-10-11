from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def _group_key(country: str, unit: str, instance: str, subunit: str) -> str:
    parts = [country or "", unit or "", instance or "", subunit or ""]
    return "|".join(p.strip().lower() for p in parts)


class PBFile(Base):
    __tablename__ = "pb_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    path: Mapped[str] = mapped_column(String(512), nullable=False)

    country: Mapped[Optional[str]] = mapped_column(String(120))
    unit: Mapped[Optional[str]] = mapped_column(String(255))
    instance: Mapped[Optional[str]] = mapped_column(String(120))
    subunit: Mapped[Optional[str]] = mapped_column(String(255))
    webpage_name: Mapped[Optional[str]] = mapped_column(String(512))
    year: Mapped[Optional[int]] = mapped_column(Integer)

    description: Mapped[Optional[str]] = mapped_column(Text)
    currency: Mapped[Optional[str]] = mapped_column(String(16))
    num_votes: Mapped[Optional[int]] = mapped_column(Integer)
    num_projects: Mapped[Optional[int]] = mapped_column(Integer)
    budget: Mapped[Optional[int]] = mapped_column(Integer)
    vote_type: Mapped[Optional[str]] = mapped_column(String(64))
    vote_length: Mapped[Optional[float]] = mapped_column()
    fully_funded: Mapped[bool] = mapped_column(Boolean, default=False)
    has_selected_col: Mapped[bool] = mapped_column(Boolean, default=False)
    experimental: Mapped[bool] = mapped_column(Boolean, default=False)
    rule_raw: Mapped[Optional[str]] = mapped_column(String(255))
    edition: Mapped[Optional[str]] = mapped_column(String(64))
    language: Mapped[Optional[str]] = mapped_column(String(32))
    quality: Mapped[Optional[float]] = mapped_column()

    # Versioning & timestamps
    file_mtime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    supersedes_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("pb_files.id"), nullable=True
    )

    # Keep this reasonably small to avoid MySQL index length limits (utf8mb4: 4 bytes/char)
    # We'll also create explicit prefix indexes below.
    group_key: Mapped[str] = mapped_column(String(255))

    __table_args__ = (
        # Standalone index on group_key with a safe prefix length for MySQL utf8mb4
        # Use positional mysql_length to ensure the dialect emits prefix length SQL.
        Index("ix_pb_files_group_key", "group_key", mysql_length=191),
        # Keep multiple versions per group, but only one is_current should be True
        # Only the first column needs a prefix length; provide a per-column mapping
        Index(
            "ix_pb_files_group_current",
            "group_key",
            "is_current",
            mysql_length={"group_key": 191},
        ),
    )


class PBComment(Base):
    __tablename__ = "pb_comments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pb_files.id"), index=True, nullable=False
    )
    idx: Mapped[int] = mapped_column(
        Integer, nullable=False
    )  # 1-based index in META comments
    text: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    # Optional: backref to PBFile (not strictly needed)
    # file = relationship("PBFile", backref="comments")

    __table_args__ = (
        UniqueConstraint("file_id", "idx", name="uq_pb_comments_file_idx"),
    )


class RefreshState(Base):
    __tablename__ = "refresh_state"

    key: Mapped[str] = mapped_column(String(50), primary_key=True)
    last_refresh_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
