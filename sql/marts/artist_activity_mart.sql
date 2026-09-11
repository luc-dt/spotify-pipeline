-- =============================================================================
-- sql/marts/artist_activity_mart.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Curated Data Mart synthesizing artist rolling 12-month release volume,
--   production mix (singles vs. studio albums), and catalog publishing strategy.
--
-- Powering:
--   - Day 7 Business Analytics (Q1 & Q2)
--   - Day 9 Streamlit Intelligence App: Page 1 (Executive Overview) & Page 4 (Trends)
--
-- Underlying Gold Views:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist (latest snapshot date)
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
