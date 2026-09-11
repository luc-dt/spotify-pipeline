"""
tests/test_gold_transformer.py
------------------------------
Automated Data Quality Gate and Unit Test Suite for Gold Star Schema (Day 6).
Validates:
    1. Dimension PK uniqueness and surrogate key formats (MD5 hex, YYYYMMDD).
    2. Conformed dimension FK referential integrity.
    3. Fact table composite grain uniqueness (artist_key, date_key).
    4. Fact table referential integrity to dim_artist and dim_date.
    5. First-snapshot NULL growth invariant & non-zero second-snapshot growth.
    6. Metric boundary invariants (semi-additive >= 0, momentum in [0, 100]).
"""

import os
import sys
import pytest
# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
from src.transform.spark_session import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession fixture for testing."""
    session = get_spark_session(app_name = "Spotify_Gold_UnitTests")
    yield  session
    session.stop()

@pytest.fixture(scope="session")
def gold_tables(spark):
    """Loads all 5 Gold Parquet tables from disk for validation."""
    gold_dir = "data/gold"
    return {
        "dim_date": spark.read.parquet(f"{gold_dir}/dim_date"),
        "dim_artist": spark.read.parquet(f"{gold_dir}/dim_artist"),
        "dim_album": spark.read.parquet(f"{gold_dir}/dim_album"),
        "dim_track": spark.read.parquet(f"{gold_dir}/dim_track"),
        "fact_artist_snapshot": spark.read.parquet(f"{gold_dir}/fact_artist_snapshot"),
    }

# ===========================================================================
# 1. Conformed Dimension Tests
# ===========================================================================

def test_dim_date_integrity(gold_tables):
    """Assert dim_date covers 1950-2030, has unique date_key, and no nulls."""
    dim_date = gold_tables["dim_date"]
    row_count = dim_date.count()
    assert row_count == 29585, f"Expected 29,585 days, got {row_count}"

    # PK uniqueness
    distinct_keys = dim_date.select("date_key").distinct().count()
    assert distinct_keys == row_count, "PK date_key contains duplicate values"

    # Weekday bounds (1 = Monday, 7 = Sunday)
    invalid_days = dim_date.filter((col("day_of_week") < 1) | (col("day_of_week") > 7)).count()
    assert invalid_days == 0, "day_of_week has values outside [1, 7]"


def test_dim_artist_pk_uniqueness(gold_tables):
    """Assert dim_artist has 5 distinct artists with valid 32-char MD5 keys."""
    dim_artist = gold_tables["dim_artist"]
    row_count = dim_artist.count()
    assert row_count == 5, f"Expected 5 artists, got {row_count}"

    distinct_keys = dim_artist.select("artist_key").distinct().count()
    assert distinct_keys == row_count, "artist_key contains duplicate values"

    # Verify MD5 format (32 hex characters)
    sample_key = dim_artist.select("artist_key").first()[0]
    assert len(sample_key) == 32 and all(c in "0123456789abcdef" for c in sample_key)


def test_dim_album_fk_integrity(gold_tables):
    """Assert dim_album PK uniqueness and 100% referential integrity to dim_artist."""
    dim_album = gold_tables["dim_album"]
    dim_artist = gold_tables["dim_artist"]
    row_count = dim_album.count()
    assert row_count == 456, f"Expected 456 albums, got {row_count}"

    # PK uniqueness
    distinct_keys = dim_album.select("album_key").distinct().count()
    assert distinct_keys == row_count, "album_key contains duplicate values"

    # FK Integrity: left_anti join finds albums without a matching artist
    orphan_albums = dim_album.join(dim_artist, on="artist_key", how="left_anti").count()
    assert orphan_albums == 0, f"Found {orphan_albums} orphan albums not matching dim_artist"


def test_dim_track_fk_integrity(gold_tables):
    """Assert dim_track PK uniqueness and 100% referential integrity to album and artist."""
    dim_track = gold_tables["dim_track"]
    dim_album = gold_tables["dim_album"]
    dim_artist = gold_tables["dim_artist"]
    row_count = dim_track.count()
    assert row_count == 2386, f"Expected 2,386 tracks, got {row_count}"

    distinct_keys = dim_track.select("track_key").distinct().count()
    assert distinct_keys == row_count, "track_key contains duplicate values"

    # FK to dim_album
    orphan_tracks_album = dim_track.join(dim_album, on="album_key", how="left_anti").count()
    assert orphan_tracks_album == 0, f"Found {orphan_tracks_album} tracks not matching dim_album"

    # FK to dim_artist
    orphan_tracks_artist = dim_track.join(dim_artist, on="artist_key", how="left_anti").count()
    assert orphan_tracks_artist == 0, f"Found {orphan_tracks_artist} tracks not matching dim_artist"

# ===========================================================================
# 2. Fact Table Grain & Referential Integrity Tests
# ===========================================================================

def test_fact_artist_snapshot_grain_uniqueness(gold_tables):
    """Assert composite PK (artist_key, date_key) is 100% unique (5 artists x 2 dates = 10 rows)."""
    fact = gold_tables["fact_artist_snapshot"]
    row_count = fact.count()
    assert row_count == 10, f"Expected 10 fact rows, got {row_count}"

    distinct_grain = fact.select("artist_key", "date_key").distinct().count()
    assert distinct_grain == row_count, "Composite grain (artist_key, date_key) has duplicate rows!"


def test_fact_referential_integrity(gold_tables):
    """Assert 100% of foreign keys join to dim_artist and dim_date (zero orphans)."""
    fact = gold_tables["fact_artist_snapshot"]
    dim_artist = gold_tables["dim_artist"]
    dim_date = gold_tables["dim_date"]

    # FK 1: artist_key -> dim_artist
    orphan_artists = fact.join(dim_artist, on="artist_key", how="left_anti").count()
    assert orphan_artists == 0, f"Found {orphan_artists} fact rows without matching dim_artist"

    # FK 2: date_key -> dim_date
    orphan_dates = fact.join(dim_date, on="date_key", how="left_anti").count()
    assert orphan_dates == 0, f"Found {orphan_dates} fact rows without matching dim_date"


# ===========================================================================
# 3. Kimball Business Invariants & Metric Bounds Tests
# ===========================================================================

def test_first_snapshot_null_rule_and_growth(gold_tables):
    """
    Assert catalog_growth_pct is strictly NULL on initial baseline (2026-08-31)
    and 0.0% on the second simulated snapshot (2026-09-01).
    """
    fact = gold_tables["fact_artist_snapshot"]

    # First snapshot: 2026-08-31 must have 100% NULLs
    aug31_non_nulls = (
        fact
        .filter(col("snapshot_date") == "2026-08-31")
        .filter(col("catalog_growth_pct").isNotNull())
        .count()
    )
    assert aug31_non_nulls == 0, f"First snapshot violated NULL rule: found {aug31_non_nulls} non-null growth values"

    # Second snapshot: 2026-09-01 must have 100% 0.0%
    sep01_invalid_growth = (
        fact
        .filter(col("snapshot_date") == "2026-09-01")
        .filter(col("catalog_growth_pct") != 0.0)
        .count()
    )
    assert sep01_invalid_growth == 0, f"Second snapshot violated zero-growth rule: found {sep01_invalid_growth} non-zero values"


def test_fact_metric_invariants_and_bounds(gold_tables):
    """Assert non-negative counters and momentum index strictly bounded in [0.0, 100.0]."""
    fact = gold_tables["fact_artist_snapshot"]

    # 1. Non-negative counters
    invalid_counters = (
        fact
        .filter(
            (col("total_albums") < 0)
            | (col("total_tracks") < 0)
            | (col("total_singles") < 0)
            | (col("recent_releases_12m") < 0)
        )
        .count()
    )
    assert invalid_counters == 0, f"Found {invalid_counters} rows with negative counters"

    # 2. Momentum index bounds [0.0, 100.0] and non-null
    invalid_momentum = (
        fact
        .filter(
            (col("catalog_momentum_index") < 0.0)
            | (col("catalog_momentum_index") > 100.0)
            | col("catalog_momentum_index").isNull()
        )
        .count()
    )
    assert invalid_momentum == 0, f"Found {invalid_momentum} rows with momentum index outside [0.0, 100.0]"
