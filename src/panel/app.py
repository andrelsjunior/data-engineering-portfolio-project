"""
OLX Property Panel — Streamlit app.

Run from project root:
    uv run streamlit run src/panel/app.py
"""

from __future__ import annotations

import sys
import pathlib
import uuid

# Streamlit runs scripts in a way that may not preserve the editable-install
# meta-path finder. Add the project root explicitly so `src.*` is importable.
_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import pandas as pd
import streamlit as st

from src.panel import db, queries

st.set_page_config(page_title="OLX Property Panel", layout="wide")
st.title("OLX Property Panel")

# ---------------------------------------------------------------------------
# Sidebar — data loading + filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Data source")
    json_path = st.text_input(
        "JSON file path",
        value="data/raw/olx_fradinhos_dedup.json",
    )
    if st.button("Load / Reload", type="primary"):
        try:
            n = db.load_json(json_path)
            st.success(f"Loaded {n} records")
        except FileNotFoundError:
            st.error(f"File not found: {json_path}")
        except Exception as exc:
            st.error(str(exc))

    st.divider()
    st.header("Filters")

    bounds = db.query("SELECT MIN(price_brl), MAX(price_brl), MIN(area_m2), MAX(area_m2) FROM listings")
    row = bounds[0] if bounds else (None, None, None, None)
    p_min_db = row[0] or 0
    p_max_db = row[1] or 5_000_000
    a_min_db = row[2] or 0
    a_max_db = row[3] or 1000

    price_range = st.slider("Price (R$)", p_min_db, p_max_db, (p_min_db, p_max_db), step=10_000)
    area_range  = st.slider("Area (m²)",  a_min_db, a_max_db, (a_min_db, a_max_db), step=5)

    all_rooms = [r[0] for r in db.query(
        "SELECT DISTINCT rooms FROM listings WHERE rooms IS NOT NULL ORDER BY 1"
    )]
    selected_rooms = st.multiselect("Rooms", all_rooms, default=[])

    all_baths = [r[0] for r in db.query(
        "SELECT DISTINCT bathrooms FROM listings WHERE bathrooms IS NOT NULL ORDER BY 1"
    )]
    selected_baths = st.multiselect("Bathrooms", all_baths, default=[])

# ---------------------------------------------------------------------------
# Fetch filtered data (shared across tabs)
# ---------------------------------------------------------------------------

df = queries.get_filtered_listings(
    price_min=price_range[0],
    price_max=price_range[1],
    area_min=area_range[0],
    area_max=area_range[1],
    rooms=selected_rooms or None,
    bathrooms=selected_baths or None,
)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_browse, tab_groups, tab_metrics = st.tabs(["Browse", "Duplicate Groups", "Metrics"])

# ── Tab 1: Browse ──────────────────────────────────────────────────────────

with tab_browse:
    st.subheader(f"{len(df)} listings")

    display_cols = [
        "url", "title", "price_brl", "price_per_m2", "area_m2",
        "rooms", "bathrooms", "garage_spots", "date_listed", "group_label",
    ]
    edit_df = df[display_cols].copy()
    edit_df.insert(0, "select", False)

    edited = st.data_editor(
        edit_df,
        column_config={
            "select":       st.column_config.CheckboxColumn("✓", width="small"),
            "url":          st.column_config.LinkColumn("URL", width="small"),
            "title":        st.column_config.TextColumn("Title", width="large"),
            "price_brl":    st.column_config.NumberColumn("Price (R$)", format="R$ %d"),
            "price_per_m2": st.column_config.NumberColumn("R$/m²", format="R$ %d"),
            "area_m2":      st.column_config.NumberColumn("Area (m²)"),
            "group_label":  st.column_config.TextColumn("Group"),
        },
        use_container_width=True,
        hide_index=True,
        key="browse_editor",
    )

    selected_urls = edited.loc[edited["select"], "url"].tolist()

    st.divider()
    col_label, col_btn = st.columns([3, 1])
    with col_label:
        group_label_input = st.text_input(
            "Group label (optional)",
            placeholder="e.g. Casa Fradinhos 175m²",
            label_visibility="collapsed",
        )
    with col_btn:
        mark_btn = st.button(
            "Mark as duplicates",
            disabled=len(selected_urls) < 2,
            type="primary",
        )

    if mark_btn and len(selected_urls) >= 2:
        existing_groups = (
            df.loc[df["url"].isin(selected_urls), "group_id"]
            .dropna()
            .unique()
            .tolist()
        )
        target_group = existing_groups[0] if existing_groups else str(uuid.uuid4())
        label = group_label_input.strip() or None
        db.assign_group(selected_urls, target_group, label)
        st.success(
            f"Grouped {len(selected_urls)} listings"
            + (f" → '{label}'" if label else "")
        )
        st.rerun()

# ── Tab 2: Duplicate Groups ────────────────────────────────────────────────

with tab_groups:
    all_groups = db.query_df("""
        SELECT DISTINCT group_id, COALESCE(group_label, group_id) AS label
        FROM duplicate_groups
        ORDER BY marked_at DESC
    """)

    if all_groups.empty:
        st.info("No duplicate groups yet. Select listings in Browse and mark them.")
    else:
        for _, g_row in all_groups.iterrows():
            gid = g_row["group_id"]
            glabel = g_row["label"]
            members = queries.get_group_members(gid)

            spread = (members["price_brl"].max() - members["price_brl"].min()
                      if members["price_brl"].notna().any() else 0)
            header = f"**{glabel}** — {len(members)} listings — spread R$ {spread:,.0f}"

            with st.expander(header, expanded=False):
                chart_df = members[["date_listed", "price_brl"]].dropna()
                if not chart_df.empty:
                    chart_df = chart_df.rename(
                        columns={"date_listed": "Date listed", "price_brl": "Price (R$)"}
                    )
                    st.bar_chart(chart_df.set_index("Date listed"))

                for _, m_row in members.iterrows():
                    col_info, col_btn = st.columns([6, 1])
                    with col_info:
                        price_fmt = f"R$ {m_row['price_brl']:,.0f}" if pd.notna(m_row["price_brl"]) else "—"
                        st.markdown(
                            f"[{m_row['title'] or m_row['url']}]({m_row['url']}) "
                            f"— {price_fmt} — {m_row['date_listed'] or '—'}"
                        )
                    with col_btn:
                        if st.button("Remove", key=f"rm_{gid}_{m_row['url']}"):
                            db.remove_from_group(m_row["url"])
                            st.rerun()

# ── Tab 3: Metrics ─────────────────────────────────────────────────────────

with tab_metrics:
    summary = queries.get_groups_summary()

    if summary.empty:
        st.info("No groups to summarise yet.")
    else:
        display = summary[[
            "group_label", "listing_count",
            "price_min_brl", "price_max_brl", "price_latest_brl",
            "price_spread_brl", "price_spread_pct",
            "price_trend", "avg_price_per_m2", "days_active",
        ]].copy()

        def _highlight_spread(row: pd.Series) -> list[str]:
            color = "background-color: #fff3cd" if (row.get("price_spread_pct") or 0) > 10 else ""
            return [color] * len(row)

        styled = display.style.apply(_highlight_spread, axis=1).format({
            "price_min_brl":    "R$ {:,.0f}",
            "price_max_brl":    "R$ {:,.0f}",
            "price_latest_brl": "R$ {:,.0f}",
            "price_spread_brl": "R$ {:,.0f}",
            "price_spread_pct": "{:.1f}%",
            "avg_price_per_m2": "R$ {:,.0f}",
        }, na_rep="—")

        st.dataframe(
            styled,
            use_container_width=True,
            column_config={
                "group_label":      st.column_config.TextColumn("Group"),
                "listing_count":    st.column_config.NumberColumn("Listings"),
                "price_min_brl":    st.column_config.TextColumn("Min price"),
                "price_max_brl":    st.column_config.TextColumn("Max price"),
                "price_latest_brl": st.column_config.TextColumn("Latest price"),
                "price_spread_brl": st.column_config.TextColumn("Spread (R$)"),
                "price_spread_pct": st.column_config.TextColumn("Spread %"),
                "price_trend":      st.column_config.TextColumn("Trend"),
                "avg_price_per_m2": st.column_config.TextColumn("Avg R$/m²"),
                "days_active":      st.column_config.NumberColumn("Days active"),
            },
            hide_index=True,
        )
        st.caption("Rows highlighted in yellow have price spread > 10%.")
