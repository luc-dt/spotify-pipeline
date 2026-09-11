-- =============================================================================
-- sql/marts/artist_momentum_mart.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Curated Data Mart unifying catalog momentum scoring, underlying driver pillars,
--   executive performance tiers, and snapshot-over-snapshot trajectory direction.
--
-- Powering:
--   - Day 7 Business Analytics (Q7 & Q8)
--   - Day 9 Streamlit Intelligence App: Page 1 (Overview) & Page 2 (Artist 360 & Momentum)
--
-- Underlying Gold Views:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist (latest snapshot date)
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
