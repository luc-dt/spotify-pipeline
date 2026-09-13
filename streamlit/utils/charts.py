"""
streamlit/utils/charts.py
-------------------------
Centralized Plotly chart factory for the Spotify Music Intelligence Application.
Adheres strictly to zero-transformation principles:
- These builders accept pre-aggregated DataFrames from DuckDB marts.
- No business logic, window functions, or metric formulations are performed here.
- Enforces a cohesive Spotify Dark UI theme (#121212 canvas, #1DB954 primary green).
"""

from typing import Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# =============================================================================
# Spotify Theme Constants
# =============================================================================
SPOTIFY_GREEN = "#1DB954"
SPOTIFY_DARK = "#121212"
SPOTIFY_CARD = "#181818"
SPOTIFY_CARD_LIGHT = "#242424"
SPOTIFY_TEXT_PRIMARY = "#FFFFFF"
SPOTIFY_TEXT_MUTED = "#B3B3B3"
SPOTIFY_GRID = "#2A2A2A"
SPOTIFY_RED = "#E91429"
SPOTIFY_BLUE = "#2E77D0"
SPOTIFY_PURPLE = "#842A9B"
SPOTIFY_AMBER = "#F59B23"

BASE_LAYOUT = dict(
    paper_bgcolor=SPOTIFY_CARD,
    plot_bgcolor=SPOTIFY_CARD,
    font=dict(family="sans-serif", color=SPOTIFY_TEXT_PRIMARY),
    margin=dict(l=40, r=40, t=50, b=40),
    xaxis=dict(
        gridcolor=SPOTIFY_GRID,
        zerolinecolor=SPOTIFY_GRID,
        color=SPOTIFY_TEXT_MUTED,
    ),
    yaxis=dict(
        gridcolor=SPOTIFY_GRID,
        zerolinecolor=SPOTIFY_GRID,
        color=SPOTIFY_TEXT_MUTED,
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(color=SPOTIFY_TEXT_PRIMARY),
    ),
)


# =============================================================================
# Page 1: Executive Overview Charts
# =============================================================================

def create_strategy_donut(df: pd.DataFrame) -> go.Figure:
    """
    Creates a donut chart representing catalog publishing strategies.
    Expects df with ['catalog_strategy', 'artist_count'].
    """
    color_map = {
        "Single-Dominant (Streaming-First)": SPOTIFY_GREEN,
        "Balanced Hybrid": SPOTIFY_BLUE,
        "Album-Focused (Traditional)": SPOTIFY_PURPLE,
    }
    colors = [color_map.get(s, SPOTIFY_AMBER) for s in df["catalog_strategy"]]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=df["catalog_strategy"],
                values=df["artist_count"],
                hole=0.6,
                marker=dict(colors=colors, line=dict(color=SPOTIFY_CARD, width=2)),
                textinfo="label+percent",
                hoverinfo="label+value+percent",
            )
        ]
    )
    layout = BASE_LAYOUT.copy()
    layout.update(
        title=dict(text="Catalog Strategy Breakdown", font=dict(size=16, color=SPOTIFY_TEXT_PRIMARY)),
        showlegend=False,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    fig.update_layout(**layout)
    return fig


def create_activity_leaderboard(df: pd.DataFrame) -> go.Figure:
    """
    Creates a horizontal bar chart of top active artists by rolling 12m releases.
    Expects df with ['artist_name', 'recent_releases_12m', 'activity_tier'].
    """
    # Sort ascending for horizontal bar display (top artist at the top)
    df_sorted = df.sort_values(by="recent_releases_12m", ascending=True)

    fig = go.Figure(
        go.Bar(
            x=df_sorted["recent_releases_12m"],
            y=df_sorted["artist_name"],
            orientation="h",
            marker=dict(
                color=df_sorted["recent_releases_12m"],
                colorscale=[[0, "#2E77D0"], [0.5, "#1ED760"], [1.0, SPOTIFY_GREEN]],
                line=dict(color=SPOTIFY_CARD_LIGHT, width=1),
            ),
            text=df_sorted["recent_releases_12m"].apply(lambda v: f" {v} drops"),
            textposition="auto",
            hovertext=df_sorted["activity_tier"],
            hovertemplate="<b>%{y}</b><br>Drops (12m): %{x}<br>Tier: %{hovertext}<extra></extra>",
        )
    )
    layout = BASE_LAYOUT.copy()
    layout.update(
        title=dict(text="Rolling 12-Month Publishing Activity (Drops)", font=dict(size=16, color=SPOTIFY_TEXT_PRIMARY)),
        xaxis_title="Releases in Rolling 12 Months",
        yaxis_title="",
        margin=dict(l=100, r=40, t=50, b=40),
    )
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Page 2: Artist 360 & Momentum Charts
# =============================================================================

def create_momentum_breakdown(
    volume_score: float,
    growth_score: float,
    cadence_score: float,
    total_index: float,
    artist_name: str,
) -> go.Figure:
    """
    Renders the transparent 3-pillar decomposition horizontal bar chart.
    Displays Volume (/40), Cadence (/35), and Freshness (/25) components.
    """
    categories = ["1. Volume (Drops 12m)", "2. Expansion (Growth %)", "3. Cadence (Consistency)"]
    scores = [volume_score, growth_score, cadence_score]
    max_scores = [40.0, 35.0, 25.0]
    colors = [SPOTIFY_GREEN, SPOTIFY_BLUE, SPOTIFY_AMBER]

    fig = go.Figure()

    # Background max capacity bars
    fig.add_trace(
        go.Bar(
            y=categories,
            x=max_scores,
            orientation="h",
            name="Max Possible",
            marker=dict(color="#282828"),
            hoverinfo="none",
        )
    )

    # Actual earned pillar points
    fig.add_trace(
        go.Bar(
            y=categories,
            x=scores,
            orientation="h",
            name="Earned Points",
            marker=dict(color=colors),
            text=[f"{s:.1f} / {m:.0f} pts" for s, m in zip(scores, max_scores)],
            textposition="inside",
            insidetextanchor="middle",
            hovertemplate="<b>%{y}</b><br>Earned: %{x:.1f} pts<extra></extra>",
        )
    )

    layout = BASE_LAYOUT.copy()
    layout.update(
        barmode="overlay",
        title=dict(
            text=f"Transparent 3-Pillar Momentum Breakdown — {artist_name} ({total_index:.1f} / 100)",
            font=dict(size=16, color=SPOTIFY_TEXT_PRIMARY),
        ),
        xaxis=dict(range=[0, 45], title="Component Points", gridcolor=SPOTIFY_GRID),
        yaxis=dict(autorange="reversed"),
        showlegend=False,
        height=260,
        margin=dict(l=150, r=40, t=50, b=30),
    )
    fig.update_layout(**layout)
    return fig


def create_momentum_trajectory(df: pd.DataFrame, artist_name: str) -> go.Figure:
    """
    Renders longitudinal momentum trajectory line chart across all available snapshots.
    Expects df with ['snapshot_date', 'catalog_momentum_index', 'total_tracks'].
    """
    fig = go.Figure()

    # Primary Y-axis: Momentum Index
    fig.add_trace(
        go.Scatter(
            x=df["snapshot_date"],
            y=df["catalog_momentum_index"],
            mode="lines+markers",
            name="Momentum Index",
            line=dict(color=SPOTIFY_GREEN, width=3),
            marker=dict(size=8, color=SPOTIFY_GREEN, line=dict(color="#FFFFFF", width=1.5)),
            hovertemplate="<b>%{x}</b><br>Momentum Index: %{y:.1f}<extra></extra>",
        )
    )

    # Secondary Y-axis: Total Tracks
    fig.add_trace(
        go.Scatter(
            x=df["snapshot_date"],
            y=df["total_tracks"],
            mode="lines+markers",
            name="Catalog Tracks",
            line=dict(color=SPOTIFY_BLUE, width=2, dash="dot"),
            marker=dict(size=6, color=SPOTIFY_BLUE),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Tracks: %{y}<extra></extra>",
        )
    )

    layout = BASE_LAYOUT.copy()
    layout.update(
        title=dict(text=f"Historical Trajectory Across Snapshots — {artist_name}", font=dict(size=16)),
        xaxis=dict(title="Snapshot Extraction Date", type="category", gridcolor=SPOTIFY_GRID),
        yaxis=dict(title="Catalog Momentum Index [0-100]", range=[0, 100], gridcolor=SPOTIFY_GRID),
        yaxis2=dict(
            title="Total Tracks Catalog",
            overlaying="y",
            side="right",
            showgrid=False,
            color=SPOTIFY_BLUE,
        ),
        hovermode="x unified",
        height=320,
        margin=dict(l=50, r=50, t=50, b=40),
    )
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Page 3: Album & Track Analytics Charts
# =============================================================================

def create_duration_distribution(df: pd.DataFrame) -> go.Figure:
    """
    Creates a duration histogram and box plot across catalog tracks.
    Expects df with ['duration_min', 'artist_name'].
    """
    fig = px.histogram(
        df,
        x="duration_min",
        nbins=40,
        marginal="box",
        color_discrete_sequence=[SPOTIFY_GREEN],
        title="Track Duration Distribution Across Monitored Catalog (Minutes)",
        labels={"duration_min": "Track Duration (Minutes)"},
    )
    median_val = df["duration_min"].median()
    fig.add_vline(
        x=median_val,
        line_dash="dash",
        line_color=SPOTIFY_AMBER,
        annotation_text=f"Median: {median_val:.2f} min",
        annotation_position="top right",
        annotation_font_color=SPOTIFY_AMBER,
    )
    layout = BASE_LAYOUT.copy()
    layout.update(
        bargap=0.1,
        height=380,
        margin=dict(l=40, r=40, t=60, b=40),
    )
    fig.update_layout(**layout)
    return fig


def create_explicit_ratio_chart(df: pd.DataFrame) -> go.Figure:
    """
    Creates a stacked bar chart of Clean vs Explicit track percentages by artist.
    Expects df with ['artist_name', 'clean_pct', 'explicit_pct'].
    """
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=df["artist_name"],
            x=df["clean_pct"],
            name="Clean Tracks (%)",
            orientation="h",
            marker=dict(color=SPOTIFY_GREEN),
            hovertemplate="<b>%{y}</b><br>Clean: %{x:.1f}%<extra></extra>",
        )
    )

    fig.add_trace(
        go.Bar(
            y=df["artist_name"],
            x=df["explicit_pct"],
            name="Explicit Tracks (%)",
            orientation="h",
            marker=dict(color=SPOTIFY_RED),
            hovertemplate="<b>%{y}</b><br>Explicit: %{x:.1f}%<extra></extra>",
        )
    )

    layout = BASE_LAYOUT.copy()
    layout.update(
        barmode="stack",
        title=dict(text="Catalog Clean vs. Explicit Ratio by Artist (%)", font=dict(size=16)),
        xaxis=dict(title="Percentage (%)", range=[0, 100], gridcolor=SPOTIFY_GRID),
        yaxis=dict(autorange="reversed"),
        height=360,
        margin=dict(l=100, r=40, t=50, b=40),
    )
    fig.update_layout(**layout)
    return fig


# =============================================================================
# Page 4: Catalog Trends & Seasonality Charts
# =============================================================================

def create_seasonality_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Creates a release seasonality bar chart highlighting peak drop months.
    Expects df with ['month_name', 'studio_albums', 'singles', 'total_releases'].
    """
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["month_name"],
            y=df["studio_albums"],
            name="Studio Albums",
            marker=dict(color=SPOTIFY_PURPLE),
            hovertemplate="<b>%{x}</b><br>Albums: %{y}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Bar(
            x=df["month_name"],
            y=df["singles"],
            name="Singles / EPs",
            marker=dict(color=SPOTIFY_GREEN),
            hovertemplate="<b>%{x}</b><br>Singles: %{y}<extra></extra>",
        )
    )

    layout = BASE_LAYOUT.copy()
    layout.update(
        barmode="group",
        title=dict(text="Monthly Release Seasonality in Monitored Catalog (12 Calendar Months)", font=dict(size=16)),
        xaxis=dict(title="Release Month", gridcolor=SPOTIFY_GRID),
        yaxis=dict(title="Release Count", gridcolor=SPOTIFY_GRID),
        height=380,
        margin=dict(l=40, r=40, t=50, b=40),
    )
    fig.update_layout(**layout)
    return fig


def create_catalog_evolution(df: pd.DataFrame) -> go.Figure:
    """
    Stacked bar chart showing historical evolution of Singles vs. Albums by release year.
    Expects df with ['release_year', 'studio_albums', 'singles', 'total_releases'].
    """
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["release_year"],
            y=df["studio_albums"],
            name="Studio Albums",
            marker=dict(color=SPOTIFY_PURPLE),
            hovertemplate="Year: %{x}<br>Studio Albums: %{y}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Bar(
            x=df["release_year"],
            y=df["singles"],
            name="Singles",
            marker=dict(color=SPOTIFY_GREEN),
            hovertemplate="Year: %{x}<br>Singles: %{y}<extra></extra>",
        )
    )

    layout = BASE_LAYOUT.copy()
    layout.update(
        barmode="stack",
        title=dict(text="Singles vs. Studio Albums Annual Evolution (1970–2026)", font=dict(size=16)),
        xaxis=dict(title="Release Year", gridcolor=SPOTIFY_GRID),
        yaxis=dict(title="Total Releases", gridcolor=SPOTIFY_GRID),
        height=380,
        margin=dict(l=40, r=40, t=50, b=40),
    )
    fig.update_layout(**layout)
    return fig


def create_cadence_distribution(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart of median days between successive releases across artists.
    Expects df with ['artist_name', 'median_release_cadence_days'].
    """
    df_sorted = df.dropna(subset=["median_release_cadence_days"]).sort_values(
        by="median_release_cadence_days", ascending=False
    )

    fig = go.Figure(
        go.Bar(
            y=df_sorted["artist_name"],
            x=df_sorted["median_release_cadence_days"],
            orientation="h",
            marker=dict(
                color=df_sorted["median_release_cadence_days"],
                colorscale=[[0, SPOTIFY_GREEN], [0.5, SPOTIFY_BLUE], [1.0, SPOTIFY_PURPLE]],
            ),
            text=df_sorted["median_release_cadence_days"].apply(lambda v: f" {v:.0f} days"),
            textposition="auto",
            hovertemplate="<b>%{y}</b><br>Median Cadence: %{x:.1f} days<extra></extra>",
        )
    )

    layout = BASE_LAYOUT.copy()
    layout.update(
        title=dict(text="Median Lifetime Publishing Cadence (Days Between Releases)", font=dict(size=16)),
        xaxis_title="Median Days Between Successive Releases",
        yaxis_title="",
        height=340,
        margin=dict(l=100, r=40, t=50, b=40),
    )
    fig.update_layout(**layout)
    return fig
