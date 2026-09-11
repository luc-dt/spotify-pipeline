-- =============================================================================
-- sql/analytics_queries.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Production ANSI SQL analytics queries for the Spotify Music Intelligence Platform.
--   Executed against DuckDB views over the Kimball Gold Layer Parquet warehouse.
--
-- Sections:
--   §1. Sanity Check & Fact Table Integrity
--   §2. Artist Activity & Output Mix (Q1, Q2)
--   §3. Catalog Growth Dynamics (Q3, Q4)
--   §4. Release Seasonality & Timing Patterns (Q5, Q6)
--   §5. Catalog Momentum Trajectory & Performance (Q7, Q8)
-- =============================================================================


-- =============================================================================
-- SECTION 1: SANITY CHECK & FACT INTEGRITY
-- =============================================================================
-- Business Purpose:
--   Validates warehouse health, snapshot dates, cohort size, and referential
--   integrity before executing downstream business intelligence queries.
--
-- Tables:
--   - fact_artist_snapshot (Periodic Snapshot Fact)
--   - dim_artist (Conformed Dimension)
--
-- Grain: One row per snapshot_date
-- =============================================================================

SELECT 
    f.snapshot_date,
    COUNT(*) AS total_fact_rows,
    COUNT(DISTINCT f.artist_key) AS distinct_artists,
    COUNT(DISTINCT a.artist_name) AS resolved_artist_names,
    SUM(f.total_tracks) AS cohort_total_tracks,
    SUM(f.total_albums) AS cohort_total_albums,
    SUM(f.total_singles) AS cohort_total_singles,
    ROUND(AVG(f.catalog_momentum_index), 2) AS cohort_avg_momentum,
    -- Referential Integrity Audit: must be 0 orphans
    COUNT(*) - COUNT(a.artist_key) AS orphan_artist_count
FROM fact_artist_snapshot f
LEFT JOIN dim_artist a ON f.artist_key = a.artist_key
GROUP BY f.snapshot_date
ORDER BY f.snapshot_date ASC;


-- =============================================================================
-- SECTION 2: ARTIST ACTIVITY & OUTPUT MIX (Q1, Q2)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1: TOP ACTIVE ARTISTS (ROLLING 12 MONTHS)
-- -----------------------------------------------------------------------------
-- Business Question:
--   Which artists published the most creative drops (albums + singles) in the
--   rolling 12 months preceding the latest snapshot date?
--
-- Why It Matters:
--   Identifies active commercial engines driving modern playlist placements,
--   touring promotions, and listener engagement vs. dormant catalog artists.
--
-- Tables:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist (latest snapshot date)
-- -----------------------------------------------------------------------------

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


-- -----------------------------------------------------------------------------
-- Q2: ALBUM VS SINGLE DISTRIBUTION MIX & CATALOG STRATEGY
-- -----------------------------------------------------------------------------
-- Business Question:
--   What is each artist's release strategy mix (album-heavy vs single-heavy),
--   and what is the ratio of singles to studio albums across their lifetime discography?
--
-- Why It Matters:
--   Streaming economics heavily favor single-driven publishing cadences.
--   Classifies artists into Streaming-First (Single-Dominant), Balanced Hybrid,
--   or Traditional Album-Centric release strategies.
--
-- Tables:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist (latest snapshot date)
-- -----------------------------------------------------------------------------

SELECT 
    a.artist_name,
    f.total_albums,
    f.total_singles,
    (f.total_albums + f.total_singles) AS total_catalog_releases,
    ROUND((f.total_singles * 100.0) / NULLIF(f.total_albums + f.total_singles, 0), 2) AS single_pct,
    ROUND((f.total_albums * 100.0) / NULLIF(f.total_albums + f.total_singles, 0), 2) AS album_pct,
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


-- =============================================================================
-- SECTION 3: CATALOG GROWTH DYNAMICS (Q3, Q4)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q3: CATALOG GROWTH VELOCITY (RELATIVE PERCENTAGE EXPANSION)
-- -----------------------------------------------------------------------------
-- Business Question:
--   Which artists expanded their track catalog the fastest snapshot-over-snapshot,
--   and how do growth classifications distribute across the cohort?
--
-- Why It Matters:
--   Uncovers sudden promotional expansion pulses (e.g. deluxe drops, live albums,
--   EP bundles) and distinguishes active catalog growth from static catalogs.
--
-- Tables:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist per snapshot date
-- -----------------------------------------------------------------------------

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


-- -----------------------------------------------------------------------------
-- Q4: ABSOLUTE TRACK GROWTH & NET VOLUME ADDED
-- -----------------------------------------------------------------------------
-- Business Question:
--   What is the absolute net volume of new tracks added per artist between
--   snapshots, and which artist contributed the highest raw inventory increase?
--
-- Why It Matters:
--   Percentage growth can be deceptive (Simpson's Paradox): small catalogs show
--   large percentage spikes with few songs. Absolute volume tracks physical inventory additions.
--
-- Tables:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist (latest snapshot date)
-- -----------------------------------------------------------------------------

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


-- =============================================================================
-- SECTION 4: RELEASE SEASONALITY & TIMING PATTERNS (Q5, Q6)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q5: MONTHLY RELEASE DISTRIBUTION ACROSS HISTORICAL DISCOGRAPHY
-- -----------------------------------------------------------------------------
-- Business Question:
--   Across all catalog albums and singles, what are the peak calendar months
--   for commercial drops, and how does seasonality differ between studio albums vs. singles?
--
-- Why It Matters:
--   Commercial music releases follow distinct seasonal cycles. Q4 (Oct-Nov)
--   often peaks for major studio albums ahead of holiday sales & Grammy cutoffs,
--   while summer months (May-Jul) historically peak for standalone singles.
--
-- Tables:
--   - dim_album (Direct dimensional navigation)
--
-- Grain: One row per calendar month (1 to 12)
-- -----------------------------------------------------------------------------

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


-- -----------------------------------------------------------------------------
-- Q6: ARTIST-SPECIFIC RELEASE SEASONALITY & CADENCE CONCENTRATION
-- -----------------------------------------------------------------------------
-- Business Question:
--   For each artist, what is their primary calendar month of choice to drop projects,
--   and what percentage of their total catalog was dropped in that peak month?
--
-- Why It Matters:
--   A&R executives and rival labels study release patterns to prevent clashing
--   with an artist's preferred release window. High concentration reveals seasonal artists.
--
-- Tables:
--   - dim_album (al)
--   - dim_artist (a)
--
-- Grain: One row per artist (their #1 peak month)
-- -----------------------------------------------------------------------------

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


-- =============================================================================
-- SECTION 5: CATALOG MOMENTUM TRAJECTORY & PERFORMANCE (Q7, Q8)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q7: CATALOG MOMENTUM LEADERBOARD & PERFORMANCE TIERS
-- -----------------------------------------------------------------------------
-- Business Question:
--   How do artists rank on overall commercial catalog momentum as of the latest
--   snapshot, and how do they classify into operational momentum tiers?
--
-- Why It Matters:
--   Serves as the master ranking KPI for streaming platform algorithms and
--   label executives to benchmark artist career velocity on a standardized 0–100 scale.
--
-- Tables:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist (latest snapshot date)
-- -----------------------------------------------------------------------------

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


-- -----------------------------------------------------------------------------
-- Q8: MOMENTUM TRAJECTORY & DELTA (SNAPSHOT-OVER-SNAPSHOT CHANGE)
-- -----------------------------------------------------------------------------
-- Business Question:
--   How did each artist's momentum index evolve from snapshot to snapshot,
--   and which artists are accelerating vs. decelerating?
--
-- Why It Matters:
--   Momentum delta detects early signals of artists ramping up for promotional
--   cycles or cooling off after campaign conclusion.
--
-- Tables:
--   - fact_artist_snapshot (f)
--   - dim_artist (a)
--
-- Grain: One row per artist (latest snapshot date)
-- -----------------------------------------------------------------------------

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





