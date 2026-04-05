"""
Aggregation queries for the OLX panel.

All functions return pandas DataFrames and manage their own DB connections
via db.query_df() / db.query().
"""

from __future__ import annotations

import re
from datetime import date, datetime

import pandas as pd

from src.panel import db

# ---------------------------------------------------------------------------
# Portuguese date parsing
# ---------------------------------------------------------------------------

PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

_DATE_RE = re.compile(r"(\d{1,2})\s+de\s+(\w{3})", re.IGNORECASE)


def _parse_pt_date(text: str | None) -> date | None:
    """Parse '3 de abr, 19:02' or '3 de abr' → date. Returns None on failure."""
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = PT_MONTHS.get(m.group(2).lower())
    if not month:
        return None
    year = datetime.now().year
    try:
        return date(year, month, day)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Browse query
# ---------------------------------------------------------------------------

def get_filtered_listings(
    price_min: int | None = None,
    price_max: int | None = None,
    area_min: int | None = None,
    area_max: int | None = None,
    rooms: list[str] | None = None,
    bathrooms: list[str] | None = None,
) -> pd.DataFrame:
    """
    Return listings joined with duplicate_groups, applying sidebar filters.
    Includes an `in_group` bool column and `group_label` for display.
    """
    conditions = []
    if price_min is not None:
        conditions.append(f"l.price_brl >= {price_min}")
    if price_max is not None:
        conditions.append(f"l.price_brl <= {price_max}")
    if area_min is not None:
        conditions.append(f"l.area_m2 >= {area_min}")
    if area_max is not None:
        conditions.append(f"l.area_m2 <= {area_max}")
    if rooms:
        values = ", ".join(f"'{r}'" for r in rooms)
        conditions.append(f"l.rooms IN ({values})")
    if bathrooms:
        values = ", ".join(f"'{b}'" for b in bathrooms)
        conditions.append(f"l.bathrooms IN ({values})")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    return db.query_df(f"""
        SELECT
            l.url,
            l.title,
            l.price_brl,
            l.price_per_m2,
            l.area_m2,
            l.rooms,
            l.bathrooms,
            l.garage_spots,
            l.iptu_brl,
            l.condo_fee_brl,
            l.date_listed,
            l.scraped_at,
            dg.group_id,
            dg.group_label,
            dg.group_id IS NOT NULL AS in_group
        FROM listings l
        LEFT JOIN duplicate_groups dg ON dg.listing_url = l.url
        {where}
        ORDER BY l.scraped_at DESC
    """)


# ---------------------------------------------------------------------------
# Group detail query
# ---------------------------------------------------------------------------

def get_group_members(group_id: str) -> pd.DataFrame:
    """Return all listings for a single group, ordered chronologically."""
    return db.query_df("""
        SELECT
            l.url,
            l.title,
            l.price_brl,
            l.price_per_m2,
            l.area_m2,
            l.rooms,
            l.bathrooms,
            l.date_listed,
            l.scraped_at,
            dg.group_label
        FROM listings l
        JOIN duplicate_groups dg ON dg.listing_url = l.url
        WHERE dg.group_id = ?
        ORDER BY l.scraped_at ASC
    """, [group_id])


# ---------------------------------------------------------------------------
# Metrics summary query
# ---------------------------------------------------------------------------

def get_groups_summary() -> pd.DataFrame:
    """
    One row per group_id with all aggregated metrics.
    price_trend and days_active are computed in Python after the SQL aggregation.
    """
    raw = db.query_df("""
        SELECT
            dg.group_id,
            COALESCE(MAX(dg.group_label), dg.group_id)   AS group_label,
            COUNT(*)                                       AS listing_count,
            MIN(l.price_brl)                              AS price_min_brl,
            MAX(l.price_brl)                              AS price_max_brl,
            FIRST(l.price_brl ORDER BY l.scraped_at DESC) AS price_latest_brl,
            FIRST(l.price_brl ORDER BY l.scraped_at ASC)  AS price_first_brl,
            MAX(l.price_brl) - MIN(l.price_brl)           AS price_spread_brl,
            ROUND(
                (MAX(l.price_brl) - MIN(l.price_brl))::DOUBLE
                / NULLIF(MIN(l.price_brl), 0) * 100,
                1
            )                                             AS price_spread_pct,
            ROUND(AVG(l.price_per_m2), 0)                 AS avg_price_per_m2,
            LIST(l.date_listed)                           AS date_strings
        FROM duplicate_groups dg
        JOIN listings l ON l.url = dg.listing_url
        GROUP BY dg.group_id
        ORDER BY listing_count DESC
    """)

    trends = []
    days_active_vals = []
    for _, row in raw.iterrows():
        first_p = row["price_first_brl"]
        last_p = row["price_latest_brl"]
        if pd.notna(first_p) and pd.notna(last_p) and first_p > 0:
            delta_pct = (last_p - first_p) / first_p * 100
            if delta_pct > 5:
                trends.append("↑ rising")
            elif delta_pct < -5:
                trends.append("↓ falling")
            else:
                trends.append("→ stable")
        else:
            trends.append("—")

        raw_dates = row["date_strings"]
        date_list = list(raw_dates) if raw_dates is not None else []
        dates = [_parse_pt_date(d) for d in date_list if d]
        dates = [d for d in dates if d is not None]
        if len(dates) >= 2:
            days_active_vals.append((max(dates) - min(dates)).days)
        else:
            days_active_vals.append(None)

    raw["price_trend"] = trends
    raw["days_active"] = days_active_vals

    return raw.drop(columns=["price_first_brl", "date_strings"])
