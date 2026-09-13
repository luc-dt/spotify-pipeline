"""
streamlit/pages/4_📅_Catalog_Trends.py
---------------------------------------
Page 4: Catalog Trends & Release Seasonality.
Answers: How has the monitored catalog evolved over time, and when is music dropped?
Visualizes:
- 12-Month Release Seasonality (Studio Albums vs. Singles) from mart_release_seasonality
- Singles vs. Studio Albums Annual Evolution (1970–2026)
- Lifetime Publishing Cadence across Artists (Days Between Releases)
- Commercial Seasonality Archetype Reference Table
"""

import os
import sys

# Ensure streamlit root is on sys.path for local utility imports
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import streamlit as st
from utils import db, charts

st.set_page_config(
    page_title="Catalog Trends | Spotify Intelligence",
    page_icon="📅",
    layout="wide",
)

conn = db.get_connection()

st.title("📅 Catalog Trends & Release Seasonality")
st.caption("Temporal Drop Timing, Format Evolution (1970–2026), and Lifetime Publishing Cadence")

# =============================================================================
# 1. 12-Month Calendar Release Seasonality
# =============================================================================
st.markdown("### 🍂 12-Calendar-Month Release Seasonality")
st.caption(
    "Querying `mart_release_seasonality` in DuckDB. Aggregates all historical album and single releases by calendar month."
)

df_season = db.get_release_seasonality(conn)
if not df_season.empty:
    fig_season = charts.create_seasonality_heatmap(df_season)
    st.plotly_chart(fig_season, use_container_width=True)

    # Industry Archetype Explanations Table
    with st.expander("📖 Commercial Seasonality Archetypes & Monthly Data Contract", expanded=False):
        st.dataframe(
            df_season[[
                "release_month",
                "month_name",
                "total_releases",
                "studio_albums",
                "singles",
                "single_share_pct",
                "album_share_pct",
                "industry_season_archetype",
            ]],
            column_config={
                "release_month": st.column_config.NumberColumn("Month #", format="%02d"),
                "month_name": st.column_config.TextColumn("Month"),
                "total_releases": st.column_config.NumberColumn("Total Drops", format="%d"),
                "studio_albums": st.column_config.NumberColumn("Albums", format="%d"),
                "singles": st.column_config.NumberColumn("Singles", format="%d"),
                "single_share_pct": st.column_config.NumberColumn("Single Share %", format="%.1f%%"),
                "album_share_pct": st.column_config.NumberColumn("Album Share %", format="%.1f%%"),
                "industry_season_archetype": st.column_config.TextColumn("Industry Archetype"),
            },
            hide_index=True,
            use_container_width=True,
        )
else:
    st.warning("No release seasonality records found in mart.")

st.markdown("---")

# =============================================================================
# 2. Singles vs. Albums Annual Evolution (1970–2026)
# =============================================================================
evo_col, cad_col = st.columns([1.3, 1])

with evo_col:
    st.markdown("### 📈 Singles vs. Albums Evolution (1970–2026)")
    st.caption("Historical shift in release format composition across decades.")

    df_evo = db.get_yearly_format_evolution(conn)
    if not df_evo.empty:
        fig_evo = charts.create_catalog_evolution(df_evo)
        st.plotly_chart(fig_evo, use_container_width=True)
        st.caption(
            "**Empirical Catalog Finding**: The monitored catalog demonstrates an increasing proportion of single releases "
            "over the last decade. This empirical pattern is consistent with industry-wide streaming distribution practices."
        )
    else:
        st.info("No annual evolution data available.")

with cad_col:
    st.markdown("### ⏱️ Lifetime Publishing Cadence")
    st.caption("Median calendar days between successive releases across lifetime discography.")

    snapshot_dates = db.get_snapshot_dates(conn)
    latest_snap = snapshot_dates[0] if snapshot_dates else None
    if latest_snap:
        df_cad = db.get_artist_momentum(conn, latest_snap)
        if not df_cad.empty:
            fig_cad = charts.create_cadence_distribution(df_cad)
            st.plotly_chart(fig_cad, use_container_width=True)
            st.caption(
                "**Cadence Variance**: Shorter median intervals indicate rapid single rollouts, "
                "whereas longer intervals reflect traditional multi-year album release cycles."
            )
        else:
            st.info("No cadence data found.")
    else:
        st.info("No snapshot dates available.")
