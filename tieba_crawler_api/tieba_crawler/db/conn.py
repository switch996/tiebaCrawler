from __future__ import annotations

import sqlite3
from pathlib import Path

def sqlite_path_from_url(db_url: str) -> Path | str:
    """Parse a SQLAlchemy-like sqlite URL.

    Supported:
      - sqlite:///relative/path.db
      - sqlite:////absolute/path.db
      - sqlite:///:memory:
    """
    if db_url == "sqlite:///:memory:":
        return ":memory:"
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError(f"Only sqlite DB_URL is supported in this demo. Got: {db_url}")
    path_str = db_url[len(prefix):]
    return Path(path_str)

def connect_sqlite(db_url: str) -> sqlite3.Connection:
    db_path = sqlite_path_from_url(db_url)
    if isinstance(db_path, Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), timeout=30)
    else:
        conn = sqlite3.connect(db_path, timeout=30)

    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn
