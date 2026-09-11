"""
scripts/run_analytics.py
------------------------
Execution engine for Spotify Business Analytics queries and SQL Marts using DuckDB.
Connects in-memory, loads Gold views, and executes analytical queries with formatted tabular outputs.
"""

import os
import sys
from typing import Optional

try:
    import duckdb
except ImportError:
    print("❌ duckdb is not installed. Please run: pip install duckdb")
    sys.exit(1)


def get_analytics_connection() -> duckdb.DuckDBPyConnection:
    """Initializes DuckDB connection and loads Gold Lakehouse views."""
    con = duckdb.connect()
    views_path = os.path.join("sql", "setup_gold_views.sql")
    if not os.path.exists(views_path):
        raise FileNotFoundError(f"Missing views definition: {views_path}")

    with open(views_path, "r", encoding="utf-8") as f:
        con.execute(f.read())
    return con


def run_sanity_check(con: duckdb.DuckDBPyConnection) -> None:
    """Executes Section 1: Fact Integrity Sanity Check."""
    query = """
    SELECT 
        f.snapshot_date,
        COUNT(*) AS total_fact_rows,
        COUNT(DISTINCT f.artist_key) AS distinct_artists,
        COUNT(DISTINCT a.artist_name) AS resolved_artist_names,
        SUM(f.total_tracks) AS cohort_total_tracks,
        SUM(f.total_albums) AS cohort_total_albums,
        SUM(f.total_singles) AS cohort_total_singles,
        ROUND(AVG(f.catalog_momentum_index), 2) AS cohort_avg_momentum,
        COUNT(*) - COUNT(a.artist_key) AS orphan_artist_count
    FROM fact_artist_snapshot f
    LEFT JOIN dim_artist a ON f.artist_key = a.artist_key
    GROUP BY f.snapshot_date
    ORDER BY f.snapshot_date ASC;
    """
    print("\n" + "=" * 95)
    print("🔍 SECTION 1: FACT INTEGRITY & COHORT SANITY CHECK")
    print("=" * 95)
    df = con.execute(query).df()
    print(df.to_string(index=False))
    print("=" * 95)


def run_section_2(con: duckdb.DuckDBPyConnection) -> None:
    """Executes Section 2: Artist Activity & Output Mix (Q1, Q2)."""
    q1 = """
    SELECT 
        a.artist_name,
        f.recent_releases_12m,
        f.total_albums,
        f.total_singles,
        f.total_tracks,
        f.catalog_momentum_index,
        DENSE_RANK() OVER (ORDER BY f.recent_releases_12m DESC) AS activity_rank
    FROM fact_artist_snapshot f
    JOIN dim_artist a ON f.artist_key = a.artist_key
    WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_artist_snapshot)
    ORDER BY f.recent_releases_12m DESC, f.total_tracks DESC;
    """
    print("\n" + "=" * 95)
    print("🔥 SECTION 2.1 — Q1: TOP ACTIVE ARTISTS (ROLLING 12 MONTHS)")
    print("=" * 95)
    df_q1 = con.execute(q1).df()
    print(df_q1.to_string(index=False))

    q2 = """
    SELECT 
        a.artist_name,
        f.total_albums,
        f.total_singles,
        (f.total_albums + f.total_singles) AS total_releases,
        ROUND((f.total_singles * 100.0) / NULLIF(f.total_albums + f.total_singles, 0), 1) AS single_pct,
        ROUND((f.total_albums * 100.0) / NULLIF(f.total_albums + f.total_singles, 0), 1) AS album_pct,
        ROUND(CAST(f.total_singles AS DOUBLE) / NULLIF(f.total_albums, 0), 2) AS single_to_album_ratio,
        CASE 
            WHEN (f.total_singles * 100.0) / NULLIF(f.total_albums + f.total_singles, 0) >= 80.0 
                THEN 'Single-Dominant (Streaming-First)'
            WHEN (f.total_singles * 100.0) / NULLIF(f.total_albums + f.total_singles, 0) >= 60.0 
                THEN 'Balanced Hybrid'
            ELSE 'Album-Centric (Traditional)'
        END AS catalog_strategy
    FROM fact_artist_snapshot f
    JOIN dim_artist a ON f.artist_key = a.artist_key
    WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_artist_snapshot)
    ORDER BY single_to_album_ratio DESC;
    """
    print("\n" + "=" * 95)
    print("💿 SECTION 2.2 — Q2: ALBUM VS SINGLE DISTRIBUTION MIX & CATALOG STRATEGY")
    print("=" * 95)
    df_q2 = con.execute(q2).df()
    print(df_q2.to_string(index=False))
    print("=" * 95)


def run_section_3(con: duckdb.DuckDBPyConnection) -> None:
    """Executes Section 3: Catalog Growth Dynamics (Q3, Q4)."""
    q3 = """
    SELECT 
        a.artist_name,
        f.snapshot_date,
        f.total_tracks,
        LAG(f.total_tracks, 1) OVER (PARTITION BY f.artist_key ORDER BY f.snapshot_date ASC) AS prev_tracks,
        f.catalog_growth_pct,
        CASE 
            WHEN f.catalog_growth_pct IS NULL THEN 'Baseline Snapshot (Initial)'
            WHEN f.catalog_growth_pct > 0 THEN 'Catalog Expansion (Active Growth)'
            WHEN f.catalog_growth_pct = 0 THEN 'Static Catalog (Zero Change)'
            ELSE 'Catalog Pruning / Retractions'
        END AS growth_classification
    FROM fact_artist_snapshot f
    JOIN dim_artist a ON f.artist_key = a.artist_key
    ORDER BY f.snapshot_date DESC, COALESCE(f.catalog_growth_pct, -1) DESC, f.total_tracks DESC;
    """
    print("\n" + "=" * 95)
    print("📈 SECTION 3.1 — Q3: CATALOG GROWTH VELOCITY (RELATIVE PERCENTAGE EXPANSION)")
    print("=" * 95)
    df_q3 = con.execute(q3).df()
    print(df_q3.to_string(index=False))

    q4 = """
    WITH track_deltas AS (
        SELECT 
            a.artist_name,
            f.snapshot_date,
            f.total_tracks,
            LAG(f.total_tracks, 1) OVER (PARTITION BY f.artist_key ORDER BY f.snapshot_date ASC) AS prev_tracks,
            f.total_tracks - LAG(f.total_tracks, 1) OVER (PARTITION BY f.artist_key ORDER BY f.snapshot_date ASC) AS net_tracks_added,
            f.catalog_growth_pct
        FROM fact_artist_snapshot f
        JOIN dim_artist a ON f.artist_key = a.artist_key
    )
    SELECT 
        artist_name,
        snapshot_date,
        prev_tracks,
        total_tracks AS current_tracks,
        COALESCE(net_tracks_added, 0) AS net_tracks_added,
        catalog_growth_pct,
        DENSE_RANK() OVER (ORDER BY COALESCE(net_tracks_added, 0) DESC, current_tracks DESC) AS expansion_rank
    FROM track_deltas
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM fact_artist_snapshot)
    ORDER BY net_tracks_added DESC, current_tracks DESC;
    """
    print("\n" + "=" * 95)
    print("📦 SECTION 3.2 — Q4: ABSOLUTE TRACK GROWTH & NET VOLUME ADDED")
    print("=" * 95)
    df_q4 = con.execute(q4).df()
    print(df_q4.to_string(index=False))
    print("=" * 95)


def run_section_4(con: duckdb.DuckDBPyConnection) -> None:
    """Executes Section 4: Release Seasonality & Timing Patterns (Q5, Q6)."""
    q5 = """
    SELECT 
        release_month,
        monthname(make_date(2024, release_month, 1)) AS month_name,
        COUNT(*) AS total_releases,
        COUNT(CASE WHEN album_type = 'album' THEN 1 END) AS studio_albums,
        COUNT(CASE WHEN album_type = 'single' THEN 1 END) AS singles,
        COUNT(CASE WHEN album_type = 'compilation' THEN 1 END) AS compilations,
        ROUND((COUNT(CASE WHEN album_type = 'single' THEN 1 END) * 100.0) / COUNT(*), 1) AS single_share_pct,
        DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS popularity_rank
    FROM dim_album
    WHERE release_month IS NOT NULL
    GROUP BY release_month
    ORDER BY release_month ASC;
    """
    print("\n" + "=" * 95)
    print("📅 SECTION 4.1 — Q5: MONTHLY RELEASE DISTRIBUTION ACROSS HISTORICAL DISCOGRAPHY")
    print("=" * 95)
    df_q5 = con.execute(q5).df()
    print(df_q5.to_string(index=False))

    q6 = """
    WITH artist_monthly_releases AS (
        SELECT 
            a.artist_name,
            al.release_month,
            monthname(make_date(2024, al.release_month, 1)) AS month_name,
            COUNT(*) AS releases_in_month,
            ROW_NUMBER() OVER (PARTITION BY a.artist_name ORDER BY COUNT(*) DESC, al.release_month ASC) AS month_rank
        FROM dim_album al
        JOIN dim_artist a ON al.artist_key = a.artist_key
        WHERE al.release_month IS NOT NULL
        GROUP BY a.artist_name, al.release_month
    ),
    artist_total_releases AS (
        SELECT 
            a.artist_name,
            COUNT(*) AS lifetime_releases
        FROM dim_album al
        JOIN dim_artist a ON al.artist_key = a.artist_key
        GROUP BY a.artist_name
    )
    SELECT 
        m.artist_name,
        m.month_name AS peak_release_month,
        m.releases_in_month AS peak_month_drops,
        t.lifetime_releases AS total_catalog_releases,
        ROUND((m.releases_in_month * 100.0) / t.lifetime_releases, 1) AS peak_month_concentration_pct
    FROM artist_monthly_releases m
    JOIN artist_total_releases t ON m.artist_name = t.artist_name
    WHERE m.month_rank = 1
    ORDER BY total_catalog_releases DESC;
    """
    print("\n" + "=" * 95)
    print("🎯 SECTION 4.2 — Q6: ARTIST-SPECIFIC RELEASE SEASONALITY & CADENCE CONCENTRATION")
    print("=" * 95)
    df_q6 = con.execute(q6).df()
    print(df_q6.to_string(index=False))
    print("=" * 95)


def run_section_5(con: duckdb.DuckDBPyConnection) -> None:
    """Executes Section 5: Catalog Momentum Trajectory & Performance (Q7, Q8)."""
    q7 = """
    SELECT 
        DENSE_RANK() OVER (ORDER BY f.catalog_momentum_index DESC) AS momentum_rank,
        a.artist_name,
        f.catalog_momentum_index,
        f.total_tracks,
        f.recent_releases_12m,
        f.median_release_cadence_days,
        f.catalog_growth_pct,
        CASE 
            WHEN f.catalog_momentum_index >= 75.0 THEN 'Elite Velocity (Top Tier)'
            WHEN f.catalog_momentum_index >= 60.0 THEN 'High Momentum (Active Campaign)'
            WHEN f.catalog_momentum_index >= 45.0 THEN 'Moderate Momentum (Steady Catalog)'
            ELSE 'Low Momentum / Dormant (Hiatus)'
        END AS momentum_tier
    FROM fact_artist_snapshot f
    JOIN dim_artist a ON f.artist_key = a.artist_key
    WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_artist_snapshot)
    ORDER BY momentum_rank ASC;
    """
    print("\n" + "=" * 95)
    print("🏆 SECTION 5.1 — Q7: CATALOG MOMENTUM LEADERBOARD & PERFORMANCE TIERS")
    print("=" * 95)
    df_q7 = con.execute(q7).df()
    print(df_q7.to_string(index=False))

    q8 = """
    WITH snapshot_momentum AS (
        SELECT 
            a.artist_name,
            f.snapshot_date,
            f.total_tracks,
            f.catalog_momentum_index,
            LAG(f.catalog_momentum_index, 1) OVER (PARTITION BY f.artist_key ORDER BY f.snapshot_date ASC) AS prev_momentum
        FROM fact_artist_snapshot f
        JOIN dim_artist a ON f.artist_key = a.artist_key
    )
    SELECT 
        artist_name,
        snapshot_date,
        prev_momentum,
        catalog_momentum_index AS current_momentum,
        ROUND(catalog_momentum_index - prev_momentum, 2) AS momentum_delta,
        CASE 
            WHEN prev_momentum IS NULL THEN 'Initial Observation'
            WHEN catalog_momentum_index > prev_momentum THEN 'Surging (+)'
            WHEN catalog_momentum_index < prev_momentum THEN 'Decelerating (-)'
            ELSE 'Stable (0.0)'
        END AS trajectory_direction
    FROM snapshot_momentum
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM fact_artist_snapshot)
    ORDER BY COALESCE(catalog_momentum_index - prev_momentum, 0) DESC, current_momentum DESC;
    """
    print("\n" + "=" * 95)
    print("🚀 SECTION 5.2 — Q8: MOMENTUM TRAJECTORY & DELTA (SNAPSHOT-OVER-SNAPSHOT CHANGE)")
    print("=" * 95)
    df_q8 = con.execute(q8).df()
    print(df_q8.to_string(index=False))
    print("=" * 95)

    print("=" * 95)


def main():
    print("🚀 Initializing DuckDB Gold Analytics Engine...")
    con = get_analytics_connection()
    print("✓ Loaded 5 Gold Views (dim_date, dim_artist, dim_album, dim_track, fact_artist_snapshot)")
    
    # Run Sanity Check
    run_sanity_check(con)

    # Run Section 2
    run_section_2(con)

    # Run Section 3
    run_section_3(con)

    # Run Section 4
    run_section_4(con)

    # Run Section 5
    run_section_5(con)


if __name__ == "__main__":
    main()




