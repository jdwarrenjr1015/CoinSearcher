"""
db.py
-----
Database abstraction layer — works with both SQLite (local) and Postgres (Vercel).

If DATABASE_URL env var is set  → uses psycopg2 (Postgres)
Otherwise                       → uses sqlite3 (local .tmp/pcgs_prices.db)

Usage:
    from db import open_conn, fetchall, fetchone, execute, is_postgres
"""

import os
import sqlite3
from pathlib import Path

DATABASE_URL = os.getenv("DATABASE_URL", "")


def is_postgres() -> bool:
    return bool(DATABASE_URL)


def _sqlite_path() -> Path:
    base = Path(__file__).resolve().parent.parent
    return Path(os.getenv("DB_PATH", str(base / ".tmp" / "pcgs_prices.db")))


def open_conn():
    """
    Return a live database connection, or None if the DB doesn't exist yet.

    For SQLite: returns a sqlite3.Connection (row_factory set to sqlite3.Row)
    For Postgres: returns a psycopg2 connection
    """
    if is_postgres():
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
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
    return "%s" if is_postgres() else "?"


def fetchall(conn, sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return all rows as dicts."""
    if is_postgres():
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def fetchone(conn, sql: str, params: tuple = ()) -> dict | None:
    """Execute a SELECT and return the first row as a dict, or None."""
    if is_postgres():
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def execute(conn, sql: str, params: tuple = ()):
    """Execute a non-SELECT statement (INSERT, CREATE, etc.)."""
    if is_postgres():
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur
    return conn.execute(sql, params)


def executemany(conn, sql: str, rows: list):
    """Execute a batch INSERT."""
    if is_postgres():
        import psycopg2.extras
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, sql, rows)
        return cur
    return conn.executemany(sql, rows)


def lastrowid(conn) -> int | None:
    """Get the last inserted row ID (Postgres needs RETURNING id)."""
    # For Postgres, use execute() with RETURNING id in the SQL instead.
    # This is only meaningful for SQLite.
    if is_postgres():
        raise NotImplementedError("Use INSERT ... RETURNING id for Postgres")
    return None  # caller uses conn.execute().lastrowid directly


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
