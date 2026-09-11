-- =============================================================================
-- sql/marts/catalog_growth_mart.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Curated Data Mart tracking historical catalog growth velocity, absolute
--   net track additions, and lifecycle growth classification across snapshots.
--
-- Powering:
--   - Day 7 Business Analytics (Q3 & Q4)
--   - Day 9 Streamlit Intelligence App: Page 1 (Overview) & Page 2 (Artist 360)
--
-- Underlying Gold Views:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist per snapshot_date
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
