-- =============================================================================
-- sql/marts/release_seasonality_mart.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Curated Data Mart analyzing historical monthly release seasonality,
--   production format preferences (singles vs. LPs), and industry timing patterns.
--
-- Powering:
--   - Day 7 Business Analytics (Q5 & Q6)
--   - Day 9 Streamlit Intelligence App: Page 4 (Catalog Trends & Seasonality)
--
-- Underlying Gold Views:
--   - dim_album (al)
--
-- Grain: One row per calendar month (1 to 12)
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
