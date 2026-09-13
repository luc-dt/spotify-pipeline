"""
streamlit/pages/2_🎤_Artist_360_Momentum.py
--------------------------------------------
Page 2: Artist 360 & Momentum Intelligence.
Answers: Why is this artist considered active, stable, or dormant?
Visualizes:
- Interactive Artist Selector
- Dimensional Profile Card & Key Metrics
- Transparent 3-Pillar Momentum Decomposition (Volume /40, Growth /35, Cadence /25)
- Historical Trajectory Line across Snapshots (Momentum + Catalog Tracks)
- Chronological Album Discography Explorer
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
    page_title="Artist 360 | Spotify Intelligence",
    page_icon="🎤",
    layout="wide",
)

conn = db.get_connection()
artists = db.get_artist_list(conn)
snapshot_dates = db.get_snapshot_dates(conn)

if not artists:
    st.error("❌ No artists found in dim_artist.")
    st.stop()

# =============================================================================
# Selectors: Artist & Snapshot
# =============================================================================
col_art, col_snap = st.columns([2, 1])

with col_art:
    selected_artist = st.selectbox(
        "🎤 Select Artist",
        options=artists,
        index=0,
        help="Drill down into individual artist dimensional profile and trajectory.",
    )

with col_snap:
    selected_snapshot = st.selectbox(
        "📅 Snapshot Date",
        options=snapshot_dates,
        index=0,
        help="Evaluates the artist's momentum and discography as of this snapshot.",
    )

st.markdown("---")

# =============================================================================
# 1. Artist Dimensional Profile Header
# =============================================================================
profile = db.get_artist_profile(conn, selected_artist, selected_snapshot)

if not profile:
    st.warning(f"No profile data found for {selected_artist} on snapshot {selected_snapshot}.")
    st.stop()

prof_col1, prof_col2 = st.columns([1, 3])

with prof_col1:
    if profile.get("image_url"):
        st.image(profile["image_url"], width=200, caption=selected_artist)
    else:
        st.markdown(f"### 🎵 {selected_artist}")
    
    if profile.get("spotify_uri"):
        st.markdown(f"[🔗 Open in Spotify]({profile['spotify_uri']})")

with prof_col2:
    st.subheader(selected_artist)
    genres = profile.get("genres")
    if genres is not None and len(genres) > 0:
        genres_display = ", ".join(list(genres)) if hasattr(genres, "__iter__") else str(genres)
        st.markdown(f"**Genres**: {genres_display}")
    
    tier_badge = formatting.momentum_badge(profile["catalog_momentum_index"])
    strat_badge = formatting.strategy_badge(profile.get("catalog_strategy"))
    act_badge = formatting.activity_tier_badge(profile.get("activity_tier"))
    
    st.markdown(f"**Status**: `{tier_badge}` | `{strat_badge}` | `{act_badge}`")

    # Profile Metric Cards
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.metric("Total Tracks", formatting.format_number(profile["total_tracks"]))
    with m2:
        st.metric("Studio Albums", formatting.format_number(profile.get("total_albums", 0)))
    with m3:
        st.metric("Singles / EPs", formatting.format_number(profile.get("total_singles", 0)))
    with m4:
        cadence = profile.get("median_release_cadence_days")
        st.metric("Median Cadence", f"{cadence:.0f} days" if cadence else "—")
    with m5:
        st.metric("Momentum Rank", f"#{profile.get('momentum_rank', 1)} of {len(artists)}")

st.markdown("---")

# =============================================================================
# 2. Transparent 3-Pillar Momentum Decomposition
# =============================================================================
st.markdown("### 🔬 Calibrated 3-Pillar Momentum Decomposition")
st.caption(
    "Explains why the score exists. Upstream formula: "
    "$\\text{Momentum} = 0.40 \\times S_{\\text{volume}} + 0.35 \\times S_{\\text{expansion}} + 0.25 \\times S_{\\text{cadence}}$."
)

vol = float(profile.get("volume_score", 0.0))
growth = float(profile.get("growth_score", 0.0))
cad = float(profile.get("cadence_score", 0.0))
total_idx = float(profile.get("catalog_momentum_index", 0.0))

fig_pillars = charts.create_momentum_breakdown(vol, growth, cad, total_idx, selected_artist)
st.plotly_chart(fig_pillars, use_container_width=True)

# Transparent Pillar Explanation Strip
exp1, exp2, exp3 = st.columns(3)
with exp1:
    st.markdown(f"""
    **1. Output Volume Pillar ({vol:.1f} / 40.0 pts)**  
    - Rolling 12m Releases: **{profile.get('recent_releases_12m', 0)} drops**
    - Linear scaling: 10 pts per drop up to 10 releases (100% capacity).
    """)

with exp2:
    growth_pct = profile.get("catalog_growth_pct")
    growth_str = f"{growth_pct:+.1f}%" if growth_pct is not None else "Baseline (Initial)"
    st.markdown(f"""
    **2. Catalog Expansion Pillar ({growth:.1f} / 35.0 pts)**  
    - Track Growth Rate: **{growth_str}**
    - Inter-snapshot window delta tracking net library growth.
    """)

with exp3:
    st.markdown(f"""
    **3. Publishing Cadence Pillar ({cad:.1f} / 25.0 pts)**  
    - Lifetime Median Cadence: **{profile.get('median_release_cadence_days', 0):.0f} days**
    - Rewards historical career discipline and publishing consistency.
    """)

st.markdown("---")

# =============================================================================
# 3. Longitudinal Trajectory Across Snapshots
# =============================================================================
st.markdown("### 📈 Historical Momentum & Track Trajectory")
st.caption(
    "Tracks longitudinal changes in Momentum Index and total catalog size across extraction snapshots."
)

df_traj = db.get_artist_trajectory(conn, selected_artist)
if not df_traj.empty and len(df_traj) >= 1:
    fig_traj = charts.create_momentum_trajectory(df_traj, selected_artist)
    st.plotly_chart(fig_traj, use_container_width=True)
else:
    st.info("Insufficient snapshot history for trajectory plotting.")

# =============================================================================
# 4. Discography Explorer: Conformed dim_album
# =============================================================================
st.markdown("### 💿 Chronological Discography")
st.caption("Conformed discography records originating from `dim_album` in the Gold Kimball schema.")

df_disco = db.get_artist_discography(conn, selected_artist)
if not df_disco.empty:
    display_disco = df_disco.copy()
    if "spotify_url" not in display_disco.columns and "album_id" in display_disco.columns:
        display_disco["spotify_url"] = "https://open.spotify.com/album/" + display_disco["album_id"].astype(str)

    st.dataframe(
        display_disco[[
            "album_name",
            "release_date",
            "album_type",
            "total_tracks",
            "spotify_url",
        ]],
        column_config={
            "album_name": st.column_config.TextColumn("Album Title"),
            "release_date": st.column_config.TextColumn("Release Date"),
            "album_type": st.column_config.TextColumn("Format"),
            "total_tracks": st.column_config.NumberColumn("Tracks", format="%d"),
            "spotify_url": st.column_config.LinkColumn(
                "Listen on Spotify",
                display_text="🎵 Open Album",
                help="Click to open this album in Spotify Web Player",
            ),
        },
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("No discography records found.")
