"""
db.py
-----
Database abstraction layer — works with both SQLite (local) and Postgres (Vercel).

If DATABASE_URL env var is set  → uses pg8000 (pure-Python Postgres, works on Vercel)
Otherwise                       → uses sqlite3 (local .tmp/pcgs_prices.db)

Usage:
    from db import open_conn, fetchall, fetchone, execute, is_postgres
"""

import os
import sqlite3
from pathlib import Path
from urllib.parse import urlparse


def _get_db_url() -> str:
    """Check several common env var names so both local and Vercel work."""
    return (
        os.getenv("DATABASE_URL") or
        os.getenv("POSTGRES_URL") or
        os.getenv("NILEDB_POSTGRES_URL") or
        os.getenv("NILEDB_URL") or
        ""
    )


def is_postgres() -> bool:
    return bool(_get_db_url())


def _sqlite_path() -> Path:
    base = Path(__file__).resolve().parent.parent
    return Path(os.getenv("DB_PATH", str(base / ".tmp" / "pcgs_prices.db")))


def open_conn():
    """
    Return a live database connection, or None if the DB doesn't exist yet.
    Postgres uses pg8000.dbapi2 (pure Python, works on Vercel).
    """
    db_url = _get_db_url()
    if db_url:
        import pg8000.dbapi as pg
        p = urlparse(db_url)
        conn = pg.connect(
            host=p.hostname,
            port=p.port or 5432,
            database=p.path.lstrip("/"),
            user=p.username,
            password=p.password,
            ssl_context=True,
        )
        return conn

    db_path = _sqlite_path()
    if not db_path.exists():
        return None
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ph() -> str:
    """Return the parameter placeholder for the active DB driver."""
    return "%s" if _get_db_url() else "?"


def _rows_to_dicts(cur) -> list[dict]:
    """Convert pg8000 cursor rows (tuples) to dicts using column names."""
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetchall(conn, sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return all rows as dicts."""
    if is_postgres():
        cur = conn.cursor()
        cur.execute(sql, params)
        return _rows_to_dicts(cur)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def fetchone(conn, sql: str, params: tuple = ()) -> dict | None:
    """Execute a SELECT and return the first row as a dict, or None."""
    if is_postgres():
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        return dict(zip(cols, row)) if row else None
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def execute(conn, sql: str, params: tuple = ()):
    """Execute a non-SELECT statement (INSERT, CREATE, etc.)."""
    if is_postgres():
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return cur
    return conn.execute(sql, params)


def executemany(conn, sql: str, rows: list):
    """Execute a batch of statements."""
    if is_postgres():
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
        return cur
    return conn.executemany(sql, rows)


def db_schema_sql() -> list[str]:
    """
    Return CREATE TABLE statements appropriate for the active database.
    Called by init_db() in the scraper.
    """
    if is_postgres():
        return [
            """
            CREATE TABLE IF NOT EXISTS coins (
                id          BIGSERIAL PRIMARY KEY,
                pcgs_num    TEXT NOT NULL,
                description TEXT,
                desig       TEXT,
                category    TEXT,
                scraped_at  TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS prices (
                id       BIGSERIAL PRIMARY KEY,
                coin_id  BIGINT NOT NULL REFERENCES coins(id),
                grade    TEXT NOT NULL,
                price    NUMERIC
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_coins_pcgs_num ON coins(pcgs_num)",
            "CREATE INDEX IF NOT EXISTS idx_coins_description ON coins(description)",
            "CREATE INDEX IF NOT EXISTS idx_prices_coin_id ON prices(coin_id)",
        ]
    else:
        return [
            """
            CREATE TABLE IF NOT EXISTS coins (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pcgs_num    TEXT NOT NULL,
                description TEXT,
                desig       TEXT,
                category    TEXT,
                scraped_at  TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS prices (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                coin_id  INTEGER NOT NULL REFERENCES coins(id),
                grade    TEXT NOT NULL,
                price    REAL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_coins_pcgs_num ON coins(pcgs_num)",
            "CREATE INDEX IF NOT EXISTS idx_coins_description ON coins(description)",
            "CREATE INDEX IF NOT EXISTS idx_prices_coin_id ON prices(coin_id)",
        ]
