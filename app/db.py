import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
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
