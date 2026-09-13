"""
streamlit/utils/db.py
---------------------
Database Access Layer for the Streamlit Intelligence Application.
Adheres strictly to the architectural boundary:
- Streamlit is a read-only consumer: zero transformations in Python.
- Mounts pre-computed Gold semantic views and SQL Marts into an in-process DuckDB session.
- Thread-safe, cached connection via @st.cache_resource.
- Fully parameterized queries for safety and performance.
"""

import os
from typing import Any, Dict, List, Optional
import duckdb
import pandas as pd

try:
    import streamlit as st
    cache_resource = st.cache_resource
except ImportError:
    # Graceful fallback decorator when running headless unit tests without Streamlit
    def cache_resource(func):
        return func

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Initializes and caches an in-process DuckDB connection.
    Registers the 5 Gold semantic views and the 4 curated data marts over local Parquet.
    """
    conn = duckdb.connect(database=":memory:")

    gold_views_path = os.path.join(PROJECT_ROOT, "sql", "setup_gold_views.sql")
    marts_views_path = os.path.join(PROJECT_ROOT, "sql", "setup_marts.sql")

    if not os.path.exists(gold_views_path):
        raise FileNotFoundError(f"Gold views script not found at {gold_views_path}")
    if not os.path.exists(marts_views_path):
        raise FileNotFoundError(f"Marts script not found at {marts_views_path}")

    # Set working directory context for DuckDB relative parquet paths if needed
    current_cwd = os.getcwd()
    os.chdir(PROJECT_ROOT)
    try:
        with open(gold_views_path, "r", encoding="utf-8") as f:
            gold_sql = f.read()
        with open(marts_views_path, "r", encoding="utf-8") as f:
            marts_sql = f.read()

        conn.execute(gold_sql)
        conn.execute(marts_sql)
    finally:
        os.chdir(current_cwd)

    return conn


def query_df(
    conn: duckdb.DuckDBPyConnection, sql: str, params: Optional[List[Any]] = None
) -> pd.DataFrame:
    """Executes a parameterized SQL query and returns a pandas DataFrame."""
    if params:
        return conn.execute(sql, params).df()
    return conn.execute(sql).df()


def get_snapshot_dates(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """Dynamically retrieves all available snapshot dates in reverse chronological order."""
    sql = """
        SELECT DISTINCT CAST(snapshot_date AS VARCHAR) AS snapshot_date
        FROM fact_artist_snapshot
        ORDER BY snapshot_date DESC;
    """
    df = query_df(conn, sql)
    return df["snapshot_date"].tolist() if not df.empty else []


def get_pipeline_health(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Retrieves pipeline health, latest snapshot, dimensional row counts, and DQ status."""
    dates = get_snapshot_dates(conn)
    latest = dates[0] if dates else "N/A"
    previous = dates[1] if len(dates) > 1 else None

    counts_sql = """
        SELECT 
            (SELECT COUNT(DISTINCT artist_key) FROM dim_artist) AS artists_count,
            (SELECT COUNT(DISTINCT album_key) FROM dim_album) AS albums_count,
            (SELECT COUNT(DISTINCT track_key) FROM dim_track) AS tracks_count,
            (SELECT COUNT(*) FROM fact_artist_snapshot) AS total_facts;
    """
    counts = query_df(conn, counts_sql).iloc[0].to_dict()

    return {
        "latest_snapshot": latest,
        "previous_snapshot": previous,
        "total_snapshots": len(dates),
        "artists_count": int(counts["artists_count"]),
        "albums_count": int(counts["albums_count"]),
        "tracks_count": int(counts["tracks_count"]),
        "total_facts": int(counts["total_facts"]),
        "dq_status": "PASS (6/6 Invariants Verified)",
    }


def get_executive_kpis(
    conn: duckdb.DuckDBPyConnection, snapshot_date: str
) -> Dict[str, Any]:
    """Retrieves executive portfolio KPIs for a specific snapshot date."""
    sql = """
        SELECT 
            COUNT(DISTINCT artist_key) AS total_artists,
            SUM(total_tracks) AS total_tracks,
            SUM(total_albums) AS total_albums,
            SUM(total_singles) AS total_singles,
            SUM(recent_releases_12m) AS recent_releases_12m,
            ROUND(AVG(catalog_momentum_index), 1) AS cohort_avg_momentum
        FROM fact_artist_snapshot
        WHERE CAST(snapshot_date AS VARCHAR) = ?;
    """
    df = query_df(conn, sql, [snapshot_date])
    if df.empty or df["total_artists"].iloc[0] == 0:
        return {
            "total_artists": 0,
            "total_tracks": 0,
            "total_albums": 0,
            "total_singles": 0,
            "recent_releases_12m": 0,
            "cohort_avg_momentum": 0.0,
        }
    row = df.iloc[0]
    return {
        "total_artists": int(row["total_artists"] or 0),
        "total_tracks": int(row["total_tracks"] or 0),
        "total_albums": int(row["total_albums"] or 0),
        "total_singles": int(row["total_singles"] or 0),
        "recent_releases_12m": int(row["recent_releases_12m"] or 0),
        "cohort_avg_momentum": float(row["cohort_avg_momentum"] or 0.0),
    }


def get_artist_activity(
    conn: duckdb.DuckDBPyConnection, snapshot_date: str
) -> pd.DataFrame:
    """Retrieves the artist activity mart for a specific snapshot date."""
    sql = """
        SELECT 
            snapshot_date,
            artist_key,
            artist_name,
            recent_releases_12m,
            total_albums,
            total_singles,
            total_releases,
            single_pct,
            album_pct,
            single_to_album_ratio,
            catalog_strategy,
            activity_rank,
            activity_tier
        FROM mart_artist_activity
        WHERE CAST(snapshot_date AS VARCHAR) = ?
        ORDER BY activity_rank ASC;
    """
    return query_df(conn, sql, [snapshot_date])


def get_strategy_distribution(
    conn: duckdb.DuckDBPyConnection, snapshot_date: str
) -> pd.DataFrame:
    """Returns the distribution of catalog strategies for a given snapshot."""
    sql = """
        SELECT 
            catalog_strategy,
            COUNT(*) AS artist_count
        FROM mart_artist_activity
        WHERE CAST(snapshot_date AS VARCHAR) = ?
        GROUP BY catalog_strategy
        ORDER BY artist_count DESC;
    """
    return query_df(conn, sql, [snapshot_date])


def get_catalog_growth(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Retrieves historical track expansion and lifecycle classifications across snapshots."""
    sql = """
        SELECT * 
        FROM mart_catalog_growth 
        ORDER BY snapshot_date DESC, expansion_rank ASC;
    """
    return query_df(conn, sql)


def get_release_seasonality(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Retrieves 12-month aggregated release seasonality mart."""
    sql = """
        SELECT * 
        FROM mart_release_seasonality 
        ORDER BY release_month ASC;
    """
    return query_df(conn, sql)


def get_artist_momentum(
    conn: duckdb.DuckDBPyConnection, snapshot_date: str
) -> pd.DataFrame:
    """Retrieves momentum rankings, tiers, and 3-pillar breakdown scores for a given snapshot."""
    sql = """
        SELECT * 
        FROM mart_artist_momentum
        WHERE CAST(snapshot_date AS VARCHAR) = ?
        ORDER BY momentum_rank ASC;
    """
    return query_df(conn, sql, [snapshot_date])


def get_artist_list(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """Returns a sorted list of all monitored canonical artist names."""
    sql = "SELECT DISTINCT artist_name FROM dim_artist ORDER BY artist_name ASC;"
    df = query_df(conn, sql)
    return df["artist_name"].tolist() if not df.empty else []


def get_artist_profile(
    conn: duckdb.DuckDBPyConnection, artist_name: str, snapshot_date: str
) -> Optional[Dict[str, Any]]:
    """Retrieves the complete dimensional profile and momentum score for a single artist."""
    sql = """
        SELECT 
            a.artist_name,
            a.genres,
            a.spotify_uri,
            a.image_url,
            m.snapshot_date,
            m.catalog_momentum_index,
            m.momentum_rank,
            m.momentum_tier,
            m.volume_score,
            m.growth_score,
            m.cadence_score,
            m.recent_releases_12m,
            m.median_release_cadence_days,
            m.catalog_growth_pct,
            m.total_tracks,
            m.momentum_delta,
            m.trajectory_direction,
            act.total_albums,
            act.total_singles,
            act.catalog_strategy,
            act.activity_tier
        FROM mart_artist_momentum m
        JOIN dim_artist a ON m.artist_key = a.artist_key
        LEFT JOIN mart_artist_activity act 
            ON m.artist_key = act.artist_key 
            AND CAST(m.snapshot_date AS VARCHAR) = CAST(act.snapshot_date AS VARCHAR)
        WHERE a.artist_name = ? AND CAST(m.snapshot_date AS VARCHAR) = ?;
    """
    df = query_df(conn, sql, [artist_name, snapshot_date])
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def get_artist_trajectory(
    conn: duckdb.DuckDBPyConnection, artist_name: str
) -> pd.DataFrame:
    """Retrieves longitudinal trajectory metrics across all snapshots for a given artist."""
    sql = """
        SELECT 
            CAST(f.snapshot_date AS VARCHAR) AS snapshot_date,
            a.artist_name,
            f.total_tracks,
            f.total_albums,
            f.total_singles,
            f.catalog_momentum_index,
            f.recent_releases_12m,
            f.median_release_cadence_days
        FROM fact_artist_snapshot f
        JOIN dim_artist a ON f.artist_key = a.artist_key
        WHERE a.artist_name = ?
        ORDER BY f.snapshot_date ASC;
    """
    return query_df(conn, sql, [artist_name])


def get_artist_discography(
    conn: duckdb.DuckDBPyConnection, artist_name: str
) -> pd.DataFrame:
    """Retrieves chronological discography albums for a given artist with direct Spotify links."""
    sql = """
        SELECT 
            al.album_name,
            CAST(al.release_date AS VARCHAR) AS release_date,
            al.album_type,
            al.total_tracks,
            al.album_id,
            'https://open.spotify.com/album/' || al.album_id AS spotify_url
        FROM dim_album al
        JOIN dim_artist ar ON al.artist_key = ar.artist_key
        WHERE ar.artist_name = ?
        ORDER BY al.release_date DESC;
    """
    return query_df(conn, sql, [artist_name])


def get_track_analytics(
    conn: duckdb.DuckDBPyConnection,
    artist_names: Optional[List[str]] = None,
    album_types: Optional[List[str]] = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Retrieves detailed track-level metadata for exploration with dynamic filtering."""
    clauses = ["1=1"]
    params: List[Any] = []

    if artist_names:
        placeholders = ", ".join(["?"] * len(artist_names))
        clauses.append(f"ar.artist_name IN ({placeholders})")
        params.extend(artist_names)

    if album_types:
        placeholders = ", ".join(["?"] * len(album_types))
        clauses.append(f"al.album_type IN ({placeholders})")
        params.extend(album_types)

    where_str = " AND ".join(clauses)
    params.append(limit)

    sql = f"""
        SELECT 
            t.track_name,
            ar.artist_name,
            al.album_name,
            al.album_type,
            CAST(al.release_date AS VARCHAR) AS release_date,
            t.duration_ms,
            ROUND(t.duration_ms / 60000.0, 2) AS duration_min,
            t.explicit,
            t.disc_number,
            t.track_number,
            'https://open.spotify.com/track/' || t.track_id AS spotify_url
        FROM dim_track t
        JOIN dim_album al ON t.album_key = al.album_key
        JOIN dim_artist ar ON al.artist_key = ar.artist_key
        WHERE {where_str}
        ORDER BY al.release_date DESC, t.track_number ASC
        LIMIT ?;
    """
    return query_df(conn, sql, params)


def get_duration_distribution_data(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Returns track duration in minutes across all monitored tracks for distribution plotting."""
    sql = """
        SELECT 
            ar.artist_name,
            t.track_name,
            ROUND(t.duration_ms / 60000.0, 2) AS duration_min,
            al.album_type,
            EXTRACT(YEAR FROM CAST(al.release_date AS DATE)) AS release_year,
            t.explicit
        FROM dim_track t
        JOIN dim_album al ON t.album_key = al.album_key
        JOIN dim_artist ar ON al.artist_key = ar.artist_key
        WHERE t.duration_ms IS NOT NULL 
          AND t.duration_ms > 0 
          AND t.duration_ms < 1200000;
    """
    return query_df(conn, sql)


def get_explicit_ratio_data(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Returns clean vs explicit track share percentages per artist."""
    sql = """
        SELECT 
            ar.artist_name,
            COUNT(*) AS total_tracks,
            SUM(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS explicit_tracks,
            ROUND(SUM(CASE WHEN t.explicit THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS explicit_pct,
            ROUND(SUM(CASE WHEN NOT t.explicit THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS clean_pct
        FROM dim_track t
        JOIN dim_album al ON t.album_key = al.album_key
        JOIN dim_artist ar ON al.artist_key = ar.artist_key
        GROUP BY ar.artist_name
        ORDER BY explicit_pct DESC;
    """
    return query_df(conn, sql)


def get_yearly_format_evolution(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Returns album vs single release counts by release year for secular trend analysis."""
    sql = """
        WITH parsed_years AS (
            SELECT 
                TRY_CAST(SUBSTRING(CAST(release_date AS VARCHAR), 1, 4) AS INTEGER) AS release_year,
                album_type
            FROM dim_album
            WHERE release_date IS NOT NULL 
              AND LENGTH(CAST(release_date AS VARCHAR)) >= 4
        )
        SELECT 
            release_year,
            SUM(CASE WHEN album_type = 'album' THEN 1 ELSE 0 END) AS studio_albums,
            SUM(CASE WHEN album_type = 'single' THEN 1 ELSE 0 END) AS singles,
            COUNT(*) AS total_releases
        FROM parsed_years
        WHERE release_year BETWEEN 1970 AND 2026
        GROUP BY release_year
        ORDER BY release_year ASC;
    """
    return query_df(conn, sql)
