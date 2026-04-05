"""
DuckDB connection, schema management, and JSON ingestion for the OLX panel.

The database file lives at data/panel.duckdb (relative to the project root).

Connection strategy: each public function opens its own short-lived connection
and closes it on exit. This avoids DuckDB file-lock conflicts when Streamlit
hot-reloads and a new process starts before the old one fully exits.
"""

from __future__ import annotations

import json
import pathlib
from contextlib import contextmanager
from typing import Any, Generator

import duckdb

DB_PATH = pathlib.Path("data/panel.duckdb")


@contextmanager
def _connect() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))
    try:
        _ensure_schema(conn)
        yield conn
    finally:
        conn.close()


def _ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            url             TEXT PRIMARY KEY,
            title           TEXT,
            price_brl       INTEGER,
            price_per_m2    INTEGER,
            area_m2         INTEGER,
            rooms           TEXT,
            bathrooms       TEXT,
            garage_spots    TEXT,
            iptu_brl        INTEGER,
            condo_fee_brl   INTEGER,
            date_listed     TEXT,
            scraped_at      TEXT,
            listing_count   INTEGER,
            price_history   TEXT,
            description     TEXT,
            keywords        TEXT
        )
    """)
    # Migration: add columns to pre-existing databases that lack them
    for col, dtype in [("description", "TEXT"), ("keywords", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {dtype}")
        except Exception:
            pass  # column already exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS duplicate_groups (
            listing_url  TEXT PRIMARY KEY,
            group_id     TEXT NOT NULL,
            group_label  TEXT,
            marked_at    TIMESTAMP DEFAULT current_timestamp
        )
    """)


def load_json(path: str | pathlib.Path) -> int:
    """
    Upsert listings from a scraper JSON output file into the listings table.
    Returns the number of records loaded.
    """
    records: list[dict[str, Any]] = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))

    for r in records:
        for field in ("rooms", "bathrooms", "garage_spots"):
            if r.get(field) is not None:
                r[field] = str(r[field])
        ph = r.get("price_history")
        r["price_history"] = json.dumps(ph) if ph is not None else None
        r.setdefault("listing_count", None)

    if not records:
        return 0

    values_sql = ",".join(
        "(" + ",".join([
            _q(r.get("url")), _q(r.get("title")),
            _n(r.get("price_brl")), _n(r.get("price_per_m2")), _n(r.get("area_m2")),
            _q(r.get("rooms")), _q(r.get("bathrooms")), _q(r.get("garage_spots")),
            _n(r.get("iptu_brl")), _n(r.get("condo_fee_brl")), _q(r.get("date_listed")),
            _q(r.get("scraped_at")), _n(r.get("listing_count")), _q(r.get("price_history")),
            _q(r.get("description")),
        ]) + ")"
        for r in records
    )

    with _connect() as conn:
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _load AS
            SELECT * FROM (VALUES %s) AS t(
                url, title, price_brl, price_per_m2, area_m2,
                rooms, bathrooms, garage_spots,
                iptu_brl, condo_fee_brl, date_listed,
                scraped_at, listing_count, price_history,
                description
            )
        """ % values_sql)
        conn.execute("""
            INSERT INTO listings (
                url, title, price_brl, price_per_m2, area_m2,
                rooms, bathrooms, garage_spots,
                iptu_brl, condo_fee_brl, date_listed,
                scraped_at, listing_count, price_history,
                description
            )
            SELECT * FROM _load
            ON CONFLICT (url) DO UPDATE SET
                title          = excluded.title,
                price_brl      = excluded.price_brl,
                price_per_m2   = excluded.price_per_m2,
                area_m2        = excluded.area_m2,
                rooms          = excluded.rooms,
                bathrooms      = excluded.bathrooms,
                garage_spots   = excluded.garage_spots,
                iptu_brl       = excluded.iptu_brl,
                condo_fee_brl  = excluded.condo_fee_brl,
                date_listed    = excluded.date_listed,
                scraped_at     = excluded.scraped_at,
                listing_count  = excluded.listing_count,
                price_history  = excluded.price_history,
                description    = excluded.description
                -- keywords intentionally excluded: preserved across reloads
        """)

    return len(records)


def assign_group(urls: list[str], group_id: str, group_label: str | None) -> None:
    """Assign a group_id (and optional label) to a set of listing URLs."""
    with _connect() as conn:
        for url in urls:
            conn.execute("""
                INSERT INTO duplicate_groups (listing_url, group_id, group_label)
                VALUES (?, ?, ?)
                ON CONFLICT (listing_url) DO UPDATE SET
                    group_id    = excluded.group_id,
                    group_label = excluded.group_label,
                    marked_at   = now()
            """, [url, group_id, group_label])


def update_keywords(url_kw_pairs: list[tuple[str, list[str]]]) -> None:
    """Batch-update the keywords JSON column for a list of (url, keywords) pairs."""
    with _connect() as conn:
        for url, kws in url_kw_pairs:
            conn.execute(
                "UPDATE listings SET keywords = ? WHERE url = ?",
                [json.dumps(kws, ensure_ascii=False), url],
            )


def remove_from_group(url: str) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM duplicate_groups WHERE listing_url = ?", [url])


def query(sql: str, params: list | None = None):
    """Run a read query and return raw fetchall results."""
    with _connect() as conn:
        return conn.execute(sql, params or []).fetchall()


def query_df(sql: str, params: list | None = None):
    """Run a read query and return a pandas DataFrame."""
    with _connect() as conn:
        return conn.execute(sql, params or []).df()


# ---------------------------------------------------------------------------
# SQL value helpers
# ---------------------------------------------------------------------------

def _q(v: Any) -> str:
    if v is None:
        return "NULL"
    escaped = str(v).replace("'", "''")
    return f"'{escaped}'"


def _n(v: Any) -> str:
    if v is None:
        return "NULL"
    return str(v)
