import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()


def _build_database_url() -> str:
    # Prefer DATABASE_URL if provided
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    # Compose MySQL URL from individual parts
    user = os.environ.get("MYSQL_USER", "pabulib")
    password = os.environ.get("MYSQL_PASSWORD", "pabulib")
    host = os.environ.get("MYSQL_HOST", "db")
    port = os.environ.get("MYSQL_PORT", "3306")
    dbname = os.environ.get("MYSQL_DATABASE", "pabulib")
    # Use PyMySQL driver for pure-Python MySQL connectivity with UTF-8 charset
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8mb4"


DATABASE_URL = _build_database_url()

# Use pool_pre_ping to avoid stale connections when containers restart
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # validate connections before use
    pool_recycle=280,  # proactively recycle MySQL connections (< 5 min) to avoid server timeouts
    future=True,
)
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,  # keep loaded attributes valid after commit
    future=True,
)


def get_runtime_schema_statements() -> list[str]:
    inspector = inspect(engine)
    if "pb_files" not in inspector.get_table_names():
        return []

    columns = {col["name"] for col in inspector.get_columns("pb_files")}
    indexes = {idx["name"] for idx in inspector.get_indexes("pb_files")}
    statements = []

    if "is_first_addition" not in columns:
        statements.append(
            "ALTER TABLE pb_files ADD COLUMN is_first_addition BOOLEAN NULL"
        )
    if "first_ingested_at" not in columns:
        statements.append(
            "ALTER TABLE pb_files ADD COLUMN first_ingested_at DATETIME NULL"
        )
    if "search_text_norm" not in columns:
        statements.append("ALTER TABLE pb_files ADD COLUMN search_text_norm TEXT NULL")
    if "ix_pb_files_is_first_addition" not in indexes:
        statements.append(
            "CREATE INDEX ix_pb_files_is_first_addition ON pb_files (is_first_addition)"
        )
    if "ix_pb_files_first_ingested_at" not in indexes:
        statements.append(
            "CREATE INDEX ix_pb_files_first_ingested_at ON pb_files (first_ingested_at)"
        )

    return statements


def ensure_runtime_schema() -> None:
    statements = get_runtime_schema_statements()
    if not statements:
        return

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


@contextmanager
def get_session() -> Iterator[Session]:
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        # If the underlying DB connection is already broken, rollback itself
        # can fail (e.g., packet sequence errors). Swallow rollback errors so
        # the original exception is preserved and the session can be closed.
        try:
            session.rollback()
        except Exception:
            pass
        raise
    finally:
        session.close()
