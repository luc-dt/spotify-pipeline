"""
streamlit/pages/1_📊_Executive_Overview.py
-------------------------------------------
Page 1: Executive Portfolio Overview.
Answers: What is happening across the monitored artist portfolio?
Visualizes:
- Portfolio KPIs (artists, tracks, albums, recent drops, cohort momentum)
- Dynamic snapshot date selector
- Single vs. Album catalog strategy breakdown (Donut)
- Activity leaderboard (Rolling 12m drops)
- Full activity mart data table with strategy and tier classifications
"""

import os
import sys

# Ensure streamlit root is on sys.path for local utility imports
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import streamlit as st
from utils import db, formatting, charts

st.set_page_config(
    page_title="Executive Overview | Spotify Intelligence",
    page_icon="📊",
    layout="wide",
)

conn = db.get_connection()
snapshot_dates = db.get_snapshot_dates(conn)

if not snapshot_dates:
    st.error("❌ No snapshot dates found in fact_artist_snapshot.")
    st.stop()

# =============================================================================
# Header & Dynamic Snapshot Selector
# =============================================================================
header_col, selector_col = st.columns([3, 1])

with header_col:
    st.title("📊 Executive Portfolio Overview")
    st.caption("Commercial Activity, Catalog Strategy Distribution, and Rolling 12-Month Publishing Velocity")

with selector_col:
    selected_snapshot = st.selectbox(
        "📅 Extraction Snapshot Date",
        options=snapshot_dates,
        index=0,
        help="Dynamically queries the warehouse state as of this specific snapshot date.",
    )

st.markdown("---")

# =============================================================================
# 1. Executive KPI Strip
# =============================================================================
kpis = db.get_executive_kpis(conn, selected_snapshot)

kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)

with kpi_col1:
    st.metric(
        label="Monitored Artists",
        value=formatting.format_number(kpis["total_artists"]),
        help="Total distinct canonical artist entities active in this snapshot.",
    )

with kpi_col2:
    st.metric(
        label="Catalog Tracks",
        value=formatting.format_number(kpis["total_tracks"]),
        help="Cumulative distinct tracks recorded across the cohort catalog.",
    )

with kpi_col3:
    st.metric(
        label="Studio Albums",
        value=formatting.format_number(kpis["total_albums"]),
        help="Cumulative full-length studio albums in cohort discography.",
    )

with kpi_col4:
    st.metric(
        label="Rolling 12m Drops",
        value=formatting.format_number(kpis["recent_releases_12m"]),
        help="Total new albums and singles published in the preceding 365 days.",
    )

with kpi_col5:
    badge = formatting.momentum_badge(kpis["cohort_avg_momentum"])
    st.metric(
        label="Cohort Avg Momentum",
        value=f"{kpis['cohort_avg_momentum']:.1f} / 100",
        delta=badge,
        delta_color="off",
        help="Calibrated composite publishing momentum index across all artists.",
    )

st.markdown("---")

# =============================================================================
# 2. Visual Layer: Strategy Donut & Activity Leaderboard
# =============================================================================
chart_col1, chart_col2 = st.columns([1, 1.4])

with chart_col1:
    df_strategy = db.get_strategy_distribution(conn, selected_snapshot)
    if not df_strategy.empty:
        fig_donut = charts.create_strategy_donut(df_strategy)
        st.plotly_chart(fig_donut, use_container_width=True)
        st.caption(
            r"**Catalog Strategy Classification** originates from `mart_artist_activity`: "
            r"Single-to-Album ratio $\ge 5.0$ indicates Streaming-First single dominance; "
            r"$2.0$ to $5.0$ indicates Balanced Hybrid; $< 2.0$ indicates Traditional Album focus."
        )
    else:
        st.info("No catalog strategy data available for this snapshot.")

with chart_col2:
    df_activity = db.get_artist_activity(conn, selected_snapshot)
    if not df_activity.empty:
        fig_bar = charts.create_activity_leaderboard(df_activity)
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No activity records available for this snapshot.")

# =============================================================================
# 3. Curated Mart Data Table: mart_artist_activity
# =============================================================================
st.markdown("### 📋 Artist Activity & Publishing Strategy Mart")
st.caption(
    "Querying `mart_artist_activity` in DuckDB. Zero transformation performed in the UI layer."
)

if not df_activity.empty:
    display_df = df_activity[[
        "activity_rank",
        "artist_name",
        "recent_releases_12m",
        "total_albums",
        "total_singles",
        "total_releases",
        "single_to_album_ratio",
        "catalog_strategy",
        "activity_tier",
    ]].copy()

    st.dataframe(
        display_df,
        column_config={
            "activity_rank": st.column_config.NumberColumn("Rank", format="#%d"),
            "artist_name": st.column_config.TextColumn("Artist"),
            "recent_releases_12m": st.column_config.NumberColumn("Drops (12m)", format="%d"),
            "total_albums": st.column_config.NumberColumn("Albums", format="%d"),
            "total_singles": st.column_config.NumberColumn("Singles", format="%d"),
            "total_releases": st.column_config.NumberColumn("Total Releases", format="%d"),
            "single_to_album_ratio": st.column_config.NumberColumn("Single:Album Ratio", format="%.2f"),
            "catalog_strategy": st.column_config.TextColumn("Catalog Strategy"),
            "activity_tier": st.column_config.TextColumn("Activity Tier"),
        },
        hide_index=True,
        use_container_width=True,
    )
else:
    st.warning("No activity mart data available for this snapshot.")
