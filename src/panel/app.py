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

from src.panel import db, queries, nlp

st.set_page_config(page_title="OLX Property Panel", layout="wide", page_icon="🏠")

# ---------------------------------------------------------------------------
# Sidebar — data loading + filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("## 🏠 OLX Panel")
    st.divider()

    st.markdown("**Data source**")
    json_path = st.text_input(
        "JSON file path",
        value="data/raw/olx_fradinhos_dedup.json",
        label_visibility="collapsed",
    )
    if st.button("↺ Load / Reload", type="primary", use_container_width=True):
        try:
            n = db.load_json(json_path)
            st.success(f"Loaded {n} records")
        except FileNotFoundError:
            st.error(f"File not found: {json_path}")
        except Exception as exc:
            st.error(str(exc))

    st.divider()
    st.markdown("**Filters**")

    bounds = db.query("SELECT MIN(price_brl), MAX(price_brl), MIN(area_m2), MAX(area_m2) FROM listings")
    row = bounds[0] if bounds else (None, None, None, None)
    p_min_db = int(row[0] or 0)
    p_max_db = int(row[1] or 5_000_000)
    a_min_db = int(row[2] or 0)
    a_max_db = int(row[3] or 1000)

    price_range = st.slider("Price (R$)", p_min_db, p_max_db, (p_min_db, p_max_db), step=10_000,
                            format="R$ %d")
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
# Global KPIs (above tabs)
# ---------------------------------------------------------------------------

total_count  = len(df)
grouped_count = int(df["in_group"].sum()) if "in_group" in df.columns else 0
avg_price    = df["price_brl"].dropna().mean()
avg_pm2      = df["price_per_m2"].dropna().mean()
num_groups   = df["group_id"].nunique() if "group_id" in df.columns else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Listings", f"{total_count:,}")
k2.metric("In a group", f"{grouped_count:,}")
k3.metric("Dup. groups", f"{num_groups:,}")
k4.metric("Avg price", f"R$ {avg_price:,.0f}" if pd.notna(avg_price) else "—")
k5.metric("Avg R$/m²",  f"R$ {avg_pm2:,.0f}"  if pd.notna(avg_pm2)  else "—")

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_browse, tab_groups, tab_metrics, tab_suggestions = st.tabs(
    ["🔍 Browse", "🔗 Duplicate Groups", "📊 Metrics", "🤖 Suggestions"]
)

# ── Tab 1: Browse ──────────────────────────────────────────────────────────

with tab_browse:
    # Pull keywords stored by the NLP analysis
    kw_map: dict[str, str] = {}
    kw_rows = db.query("SELECT url, keywords FROM listings WHERE keywords IS NOT NULL")
    for url_val, kw_json in kw_rows:
        try:
            import json as _json
            kws = _json.loads(kw_json)
            kw_map[url_val] = "  ".join(f"#{k}" for k in kws[:5])
        except Exception:
            pass

    display_cols = [
        "url", "title", "price_brl", "price_per_m2", "area_m2",
        "rooms", "bathrooms", "garage_spots", "date_listed", "group_label",
    ]
    edit_df = df[display_cols].copy()
    edit_df["keywords"] = edit_df["url"].map(kw_map).fillna("")
    edit_df.insert(0, "select", False)

    edited = st.data_editor(
        edit_df,
        column_config={
            "select":       st.column_config.CheckboxColumn("✓", width="small"),
            "url":          st.column_config.LinkColumn("🔗", width="small", display_text="view"),
            "title":        st.column_config.TextColumn("Title", width="large"),
            "price_brl":    st.column_config.NumberColumn("Price (R$)", format="R$ %d"),
            "price_per_m2": st.column_config.NumberColumn("R$/m²", format="R$ %d"),
            "area_m2":      st.column_config.NumberColumn("m²", width="small"),
            "rooms":        st.column_config.TextColumn("Beds", width="small"),
            "bathrooms":    st.column_config.TextColumn("Baths", width="small"),
            "garage_spots": st.column_config.TextColumn("Garage", width="small"),
            "date_listed":  st.column_config.TextColumn("Listed", width="medium"),
            "group_label":  st.column_config.TextColumn("Group", width="medium"),
            "keywords":     st.column_config.TextColumn("Keywords", width="large"),
        },
        use_container_width=True,
        hide_index=True,
        key="browse_editor",
    )

    selected_urls = edited.loc[edited["select"], "url"].tolist()
    n_selected = len(selected_urls)

    # ── Mark as duplicates bar ──
    st.divider()
    col_sel_info, col_label, col_btn = st.columns([2, 4, 2])

    with col_sel_info:
        if n_selected == 0:
            st.caption("Check rows above to group them")
        elif n_selected == 1:
            st.warning("Select at least 2 listings")
        else:
            st.success(f"{n_selected} listings selected")

    with col_label:
        group_label_input = st.text_input(
            "Group label",
            placeholder="e.g. Casa Fradinhos 175m²",
            label_visibility="collapsed",
            disabled=n_selected < 2,
        )

    with col_btn:
        mark_btn = st.button(
            "🔗 Mark as duplicates",
            disabled=n_selected < 2,
            type="primary",
            use_container_width=True,
        )

    if mark_btn and n_selected >= 2:
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
            f"Grouped {n_selected} listings"
            + (f" → **{label}**" if label else "")
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
        st.info("No duplicate groups yet — select listings in Browse and click **🔗 Mark as duplicates**.")
    else:
        for _, g_row in all_groups.iterrows():
            gid    = g_row["group_id"]
            glabel = g_row["label"]
            members = queries.get_group_members(gid)

            prices = members["price_brl"].dropna()
            spread_brl = (prices.max() - prices.min()) if len(prices) >= 2 else 0
            spread_pct = spread_brl / prices.min() * 100 if len(prices) >= 2 and prices.min() > 0 else 0

            if spread_pct > 15:
                badge = "🔴"
            elif spread_pct > 5:
                badge = "🟡"
            else:
                badge = "🟢"

            price_range_str = (
                f"R$ {prices.min():,.0f} – {prices.max():,.0f}"
                if len(prices) >= 2 else
                (f"R$ {prices.iloc[0]:,.0f}" if len(prices) == 1 else "no price")
            )
            header = f"{badge} **{glabel}** · {len(members)} listings · {price_range_str} · spread {spread_pct:.1f}%"

            with st.expander(header, expanded=False):
                chart_df = members[["date_listed", "price_brl"]].dropna()
                if not chart_df.empty:
                    chart_df = chart_df.copy()
                    chart_df["price_brl"] = chart_df["price_brl"].astype(float)
                    chart_df = chart_df.rename(
                        columns={"date_listed": "Date listed", "price_brl": "Price (R$)"}
                    )
                    st.bar_chart(chart_df.set_index("Date listed"), y_label="Price (R$)")

                for _, m_row in members.iterrows():
                    col_price, col_info, col_btn = st.columns([2, 5, 1])
                    with col_price:
                        price_fmt = f"R$ {m_row['price_brl']:,.0f}" if pd.notna(m_row["price_brl"]) else "—"
                        st.markdown(f"**{price_fmt}**")
                    with col_info:
                        title = m_row['title'] or m_row['url']
                        st.markdown(
                            f"[{title[:70]}]({m_row['url']}) · "
                            f"{m_row['area_m2'] or '?'}m² · "
                            f"{m_row['rooms'] or '?'} qtos · "
                            f"{m_row['date_listed'] or '—'}"
                        )
                    with col_btn:
                        if st.button("✕", key=f"rm_{gid}_{m_row['url']}", help="Remove from group"):
                            db.remove_from_group(m_row["url"])
                            st.rerun()

# ── Tab 3: Metrics ─────────────────────────────────────────────────────────

with tab_metrics:
    summary = queries.get_groups_summary()

    if summary.empty:
        st.info("No groups to summarise yet.")
    else:
        # Summary KPIs
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Groups", len(summary))
        m2.metric("Avg spread", f"{summary['price_spread_pct'].mean():.1f}%"
                  if summary["price_spread_pct"].notna().any() else "—")
        high_spread = summary.loc[summary["price_spread_pct"].idxmax()] if summary["price_spread_pct"].notna().any() else None
        if high_spread is not None:
            m3.metric("Highest spread", f"{high_spread['price_spread_pct']:.1f}%",
                      delta=high_spread["group_label"], delta_color="off")
        rising = (summary["price_trend"] == "↑ rising").sum()
        falling = (summary["price_trend"] == "↓ falling").sum()
        m4.metric("Trending ↑ / ↓", f"{rising} / {falling}")

        st.divider()

        display = summary[[
            "group_label", "listing_count",
            "price_min_brl", "price_max_brl", "price_latest_brl",
            "price_spread_brl", "price_spread_pct",
            "price_trend", "avg_price_per_m2", "days_active",
        ]].copy()

        def _highlight_spread(row: pd.Series) -> list[str]:
            pct = row.get("price_spread_pct") or 0
            if pct > 15:
                color = "background-color: #f8d7da"   # red-tint
            elif pct > 10:
                color = "background-color: #fff3cd"   # yellow
            else:
                color = ""
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
                "listing_count":    st.column_config.NumberColumn("#"),
                "price_min_brl":    st.column_config.TextColumn("Min price"),
                "price_max_brl":    st.column_config.TextColumn("Max price"),
                "price_latest_brl": st.column_config.TextColumn("Latest"),
                "price_spread_brl": st.column_config.TextColumn("Spread (R$)"),
                "price_spread_pct": st.column_config.TextColumn("Spread %"),
                "price_trend":      st.column_config.TextColumn("Trend"),
                "avg_price_per_m2": st.column_config.TextColumn("Avg R$/m²"),
                "days_active":      st.column_config.NumberColumn("Days"),
            },
            hide_index=True,
        )
        st.caption("🟡 spread > 10%   🔴 spread > 15%")

# ── Tab 4: Suggestions ─────────────────────────────────────────────────────

with tab_suggestions:
    import json as _json

    st.markdown(
        "Automatically detects likely duplicates by combining **TF-IDF description similarity** "
        "with structured-field bonuses (area ±10%, matching room count)."
    )

    # Threshold slider
    threshold = st.slider(
        "Similarity threshold", min_value=0.50, max_value=0.95, value=0.75, step=0.05,
        help="Composite score = 0.7×text_sim + 0.2×area_match + 0.1×rooms_match"
    )

    col_analyze, col_info = st.columns([2, 5])
    with col_analyze:
        analyze_btn = st.button("🔍 Analyze descriptions", type="primary", use_container_width=True)
    with col_info:
        n_with_desc = db.query(
            "SELECT COUNT(*) FROM listings WHERE description IS NOT NULL AND LENGTH(description) > 30"
        )
        n_desc = n_with_desc[0][0] if n_with_desc else 0
        st.caption(f"{n_desc} listings have descriptions available for analysis.")

    if analyze_btn:
        if n_desc < 2:
            st.warning(
                "Not enough descriptions in the database. "
                "Run `python -m src.scrapers.olx --enrich` to scrape detail pages first."
            )
        else:
            with st.spinner("Running TF-IDF analysis…"):
                all_listings = db.query_df(
                    "SELECT url, title, description, area_m2, rooms FROM listings "
                    "WHERE description IS NOT NULL AND LENGTH(description) > 30"
                )
                candidates = nlp.find_duplicate_candidates(all_listings, threshold=threshold)

                # Extract and persist keywords for all analysed listings
                desc_list = all_listings["description"].fillna("").tolist()
                if len(desc_list) >= 2:
                    vec = nlp.build_corpus_model(desc_list)
                    all_kws = nlp.extract_keywords_batch(desc_list, vec, n=8)
                    pairs = list(zip(all_listings["url"].tolist(), all_kws))
                    db.update_keywords(pairs)

            st.session_state["nlp_candidates"] = candidates
            st.session_state["nlp_dismissed"] = st.session_state.get("nlp_dismissed", set())
            st.success(
                f"Found **{len(candidates)}** candidate pairs across "
                f"{n_desc} listings. Keywords saved — reload Browse to see them."
            )

    candidates_df: pd.DataFrame = st.session_state.get("nlp_candidates", pd.DataFrame())
    dismissed: set[tuple[str, str]] = st.session_state.get("nlp_dismissed", set())

    if candidates_df.empty:
        st.info("Click **Analyze descriptions** to run the NLP analysis.")
    else:
        # Filter out dismissed pairs and already-grouped pairs
        already_grouped_urls = set(
            r[0] for r in db.query(
                "SELECT listing_url FROM duplicate_groups"
            )
        )

        visible = candidates_df[
            ~candidates_df.apply(
                lambda r: (r["url_a"], r["url_b"]) in dismissed
                          or (r["url_b"], r["url_a"]) in dismissed,
                axis=1,
            )
        ].copy()

        # Annotate whether already grouped
        visible["a_grouped"] = visible["url_a"].isin(already_grouped_urls)
        visible["b_grouped"] = visible["url_b"].isin(already_grouped_urls)

        if visible.empty:
            st.success("All candidates reviewed!")
        else:
            st.markdown(f"**{len(visible)}** candidate pairs · sorted by composite score")

            for idx, row in visible.iterrows():
                score_pct = int(row["composite"] * 100)
                if score_pct >= 85:
                    badge = "🔴"
                elif score_pct >= 75:
                    badge = "🟡"
                else:
                    badge = "🟢"

                shared_str = "  ".join(f"`{k}`" for k in row["shared_keywords"][:8]) or "—"

                with st.container(border=True):
                    hcol, scol = st.columns([7, 1])
                    with hcol:
                        st.markdown(
                            f"{badge} **{score_pct}% match** "
                            f"(text {int(row['text_sim']*100)}%"
                            + (f" + area" if row["area_bonus"] else "")
                            + (f" + rooms" if row["rooms_bonus"] else "")
                            + ")"
                        )
                    with scol:
                        st.caption(f"#{idx}")

                    lcol, rcol = st.columns(2)
                    with lcol:
                        grouped_tag = " 🔗" if row["a_grouped"] else ""
                        st.markdown(
                            f"**A{grouped_tag}** [{(row['title_a'] or row['url_a'])[:60]}]({row['url_a']})"
                        )
                    with rcol:
                        grouped_tag = " 🔗" if row["b_grouped"] else ""
                        st.markdown(
                            f"**B{grouped_tag}** [{(row['title_b'] or row['url_b'])[:60]}]({row['url_b']})"
                        )

                    st.caption(f"Shared keywords: {shared_str}")

                    btn_col1, btn_col2, btn_col3 = st.columns([2, 2, 5])
                    with btn_col1:
                        if st.button("✓ Accept", key=f"accept_{idx}", type="primary"):
                            existing = (
                                df.loc[df["url"].isin([row["url_a"], row["url_b"]]), "group_id"]
                                .dropna().unique().tolist()
                            )
                            gid = existing[0] if existing else str(uuid.uuid4())
                            db.assign_group([row["url_a"], row["url_b"]], gid, None)
                            dismissed.add((row["url_a"], row["url_b"]))
                            st.session_state["nlp_dismissed"] = dismissed
                            st.rerun()
                    with btn_col2:
                        if st.button("✗ Dismiss", key=f"dismiss_{idx}"):
                            dismissed.add((row["url_a"], row["url_b"]))
                            st.session_state["nlp_dismissed"] = dismissed
                            st.rerun()
