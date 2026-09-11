-- =============================================================================
-- sql/setup_marts.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Establishes the 4 Curated Data Mart views in DuckDB over Gold Views.
--   These marts provide purpose-built dimensional slices powering business
--   intelligence dashboards, executive reports, and the Day 9 Streamlit App.
--
-- Prerequisites:
--   - sql/setup_gold_views.sql must be executed first to create the base views:
--     (dim_date, dim_artist, dim_album, dim_track, fact_artist_snapshot).
--
-- Marts Created:
--   1. mart_artist_activity     - Release volume (12m), album/single mix & strategy
--   2. mart_catalog_growth      - Track expansion velocity & lifecycle classification
--   3. mart_release_seasonality - Historical monthly release distribution & single share
--   4. mart_artist_momentum     - Momentum ranking, tiers, 3 pillars & trajectory delta
-- =============================================================================


-- =============================================================================
-- 1. mart_artist_activity
-- =============================================================================
CREATE OR REPLACE VIEW mart_artist_activity AS
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS max_date 
    FROM fact_artist_snapshot
),
artist_releases AS (
    SELECT 
        f.snapshot_date,
        f.artist_key,
        a.artist_name,
        f.recent_releases_12m,
        f.total_albums,
        f.total_singles,
        (f.total_albums + f.total_singles) AS total_releases,
        ROUND((f.total_singles * 100.0) / NULLIF(f.total_albums + f.total_singles, 0), 1) AS single_pct,
        ROUND((f.total_albums * 100.0) / NULLIF(f.total_albums + f.total_singles, 0), 1) AS album_pct,
        ROUND(f.total_singles * 1.0 / NULLIF(f.total_albums, 0), 2) AS single_to_album_ratio,
        f.total_tracks,
        f.catalog_momentum_index
    FROM fact_artist_snapshot f
    JOIN dim_artist a ON f.artist_key = a.artist_key
    WHERE f.snapshot_date = (SELECT max_date FROM latest_snapshot)
)
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
    CASE 
        WHEN single_to_album_ratio >= 5.0 THEN 'Single-Dominant (Streaming-First)'
        WHEN single_to_album_ratio >= 2.0 THEN 'Balanced Hybrid'
        ELSE 'Album-Focused (Traditional)'
    END AS catalog_strategy,
    DENSE_RANK() OVER (ORDER BY recent_releases_12m DESC, total_releases DESC) AS activity_rank,
    CASE 
        WHEN recent_releases_12m >= 10 THEN 'Hyper-Active (10+ drops/yr)'
        WHEN recent_releases_12m >= 5 THEN 'Active Campaign (5-9 drops/yr)'
        WHEN recent_releases_12m >= 1 THEN 'Moderate Maintenance (1-4 drops/yr)'
        ELSE 'Dormant / Hiatus (0 drops/yr)'
    END AS activity_tier
FROM artist_releases
ORDER BY activity_rank ASC, total_tracks DESC;


-- =============================================================================
-- 2. mart_catalog_growth
-- =============================================================================
CREATE OR REPLACE VIEW mart_catalog_growth AS
WITH track_history AS (
    SELECT 
        f.snapshot_date,
        f.artist_key,
        a.artist_name,
        f.total_tracks AS current_tracks,
        LAG(f.total_tracks, 1) OVER (
            PARTITION BY f.artist_key 
            ORDER BY f.snapshot_date ASC
        ) AS prev_tracks,
        f.catalog_growth_pct
    FROM fact_artist_snapshot f
    JOIN dim_artist a ON f.artist_key = a.artist_key
)
SELECT 
    snapshot_date,
    artist_key,
    artist_name,
    prev_tracks,
    current_tracks,
    COALESCE(current_tracks - prev_tracks, 0) AS net_tracks_added,
    catalog_growth_pct,
    CASE 
        WHEN prev_tracks IS NULL THEN 'Baseline Snapshot (Initial)'
        WHEN current_tracks > prev_tracks THEN 'Catalog Expansion (Active Growth)'
        WHEN current_tracks = prev_tracks THEN 'Static Catalog (Zero Change)'
        ELSE 'Catalog Reduction (Delisting)'
    END AS growth_classification,
    DENSE_RANK() OVER (
        PARTITION BY snapshot_date 
        ORDER BY COALESCE(current_tracks - prev_tracks, 0) DESC, current_tracks DESC
    ) AS expansion_rank
FROM track_history
ORDER BY snapshot_date DESC, expansion_rank ASC;


-- =============================================================================
-- 3. mart_release_seasonality
-- =============================================================================
CREATE OR REPLACE VIEW mart_release_seasonality AS
WITH monthly_aggregation AS (
    SELECT 
        release_month,
        monthname(make_date(2024, release_month, 1)) AS month_name,
        COUNT(*) AS total_releases,
        COUNT(CASE WHEN album_type = 'album' THEN 1 END) AS studio_albums,
        COUNT(CASE WHEN album_type = 'single' THEN 1 END) AS singles,
        COUNT(CASE WHEN album_type = 'compilation' THEN 1 END) AS compilations,
        ROUND((COUNT(CASE WHEN album_type = 'single' THEN 1 END) * 100.0) / NULLIF(COUNT(*), 0), 1) AS single_share_pct,
        ROUND((COUNT(CASE WHEN album_type = 'album' THEN 1 END) * 100.0) / NULLIF(COUNT(*), 0), 1) AS album_share_pct
    FROM dim_album
    WHERE release_month IS NOT NULL
    GROUP BY release_month
)
SELECT 
    release_month,
    month_name,
    total_releases,
    studio_albums,
    singles,
    compilations,
    single_share_pct,
    album_share_pct,
    DENSE_RANK() OVER (ORDER BY total_releases DESC) AS volume_rank,
    CASE 
        WHEN release_month IN (6, 7) THEN 'Summer Single Surge (Streaming Peak)'
        WHEN release_month IN (10, 11) THEN 'Q4 Holiday & Grammy Window (LP Heavy)'
        WHEN release_month IN (5) THEN 'Late Spring Festival Pre-Release'
        WHEN release_month IN (1, 2) THEN 'Q1 Industry Cooldown & Singles Reset'
        ELSE 'Mid-Season Catalog Drops'
    END AS industry_season_archetype
FROM monthly_aggregation
ORDER BY release_month ASC;


-- =============================================================================
-- 4. mart_artist_momentum
-- =============================================================================
CREATE OR REPLACE VIEW mart_artist_momentum AS
WITH snapshot_lag AS (
    SELECT 
        f.snapshot_date,
        f.artist_key,
        a.artist_name,
        f.catalog_momentum_index,
        f.total_tracks,
        f.recent_releases_12m,
        f.median_release_cadence_days,
        f.catalog_growth_pct,
        LAG(f.catalog_momentum_index, 1) OVER (
            PARTITION BY f.artist_key 
            ORDER BY f.snapshot_date ASC
        ) AS prev_momentum
    FROM fact_artist_snapshot f
    JOIN dim_artist a ON f.artist_key = a.artist_key
),
latest_snapshot AS (
    SELECT MAX(snapshot_date) AS max_date 
    FROM fact_artist_snapshot
)
SELECT 
    snapshot_date,
    artist_key,
    artist_name,
    catalog_momentum_index,
    DENSE_RANK() OVER (ORDER BY catalog_momentum_index DESC) AS momentum_rank,
    CASE 
        WHEN catalog_momentum_index >= 75.0 THEN 'Elite Velocity (Top Tier)'
        WHEN catalog_momentum_index >= 60.0 THEN 'High Momentum (Active Campaign)'
        WHEN catalog_momentum_index >= 45.0 THEN 'Moderate Momentum (Steady Catalog)'
        ELSE 'Low Momentum / Dormant (Hiatus)'
    END AS momentum_tier,
    -- Underlying 3 Pillars
    recent_releases_12m,
    median_release_cadence_days,
    catalog_growth_pct,
    total_tracks,
    -- Trajectory Tracking
    prev_momentum,
    ROUND(catalog_momentum_index - prev_momentum, 2) AS momentum_delta,
    CASE 
        WHEN prev_momentum IS NULL THEN 'Initial Observation'
        WHEN catalog_momentum_index > prev_momentum THEN 'Surging (+)'
        WHEN catalog_momentum_index < prev_momentum THEN 'Decelerating (-)'
        ELSE 'Stable (0.0)'
    END AS trajectory_direction
FROM snapshot_lag
WHERE snapshot_date = (SELECT max_date FROM latest_snapshot)
ORDER BY momentum_rank ASC;
