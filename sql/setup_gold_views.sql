-- =============================================================================
-- sql/setup_gold_views.sql
-- -----------------------------------------------------------------------------
-- Purpose:
--   Establishes DuckDB views directly querying Gold Layer Parquet files.
--   Provides a virtual semantic layer for zero-copy, sub-second OLAP querying
--   without requiring an external database cluster or data duplication.
--
-- Views Created:
--   1. dim_date             - Gregorian calendar dimension (1950–2030)
--   2. dim_artist           - Conformed SCD Type 1 artist dimension
--   3. dim_album            - Conformed album dimension with release dates & types
--   4. dim_track            - Conformed track audio features & metadata
--   5. fact_artist_snapshot - Periodic snapshot fact table with Hive partitioning
-- =============================================================================

-- 1. dim_date: Calendar Dimension
CREATE OR REPLACE VIEW dim_date AS
SELECT * 
FROM read_parquet('data/gold/dim_date/*.parquet');

-- 2. dim_artist: Conformed Artist Dimension
CREATE OR REPLACE VIEW dim_artist AS
SELECT * 
FROM read_parquet('data/gold/dim_artist/*.parquet');

-- 3. dim_album: Conformed Album Dimension
CREATE OR REPLACE VIEW dim_album AS
SELECT * 
FROM read_parquet('data/gold/dim_album/*.parquet');

-- 4. dim_track: Conformed Track Dimension
CREATE OR REPLACE VIEW dim_track AS
SELECT * 
FROM read_parquet('data/gold/dim_track/*.parquet');

-- 5. fact_artist_snapshot: Periodic Snapshot Fact Table (Hive Partitioned)
CREATE OR REPLACE VIEW fact_artist_snapshot AS
SELECT * 
FROM read_parquet('data/gold/fact_artist_snapshot/**/*.parquet', hive_partitioning=true);
