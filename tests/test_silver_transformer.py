"""
tests/test_silver_transformer.py
--------------------------------
Unit tests for Silver layer transformations and deduplication.
Tests deterministic window deduplication, date normalization,
duration derivations, and defensive sentinel handling.
"""

import os
import sys
import pytest
from datetime import datetime, date

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    ArrayType,
    IntegerType,
    LongType,
    BooleanType,
)

from src.transform.spark_session import get_spark_session
from src.transform.silver_transformer import SilverTransformer


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession fixture for testing."""
    session = get_spark_session(app_name="Spotify_Silver_UnitTests")
    yield session
    session.stop()


@pytest.fixture
def transformer(spark):
    return SilverTransformer(spark=spark)


# ===========================================================================
# 1. ARTISTS TESTS
# ===========================================================================
def test_transform_artists_deduplication(spark, transformer):
    """
    Test artist transformation:
    - Deterministic Window deduplication keeps ONLY the latest record.
    - Trims IDs and names.
    - Preserves genres as an Array.
    - Preserves and trims Spotify URI.
    """
    schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_name", StringType(), True),
        StructField("spotify_uri", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("genres", ArrayType(StringType()), True),
        StructField("extracted_at", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("snapshot_date", StringType(), True),
    ])

    data = [
        (
            "  art_001  ",
            "Coldplay (Old)",
            "spotify:artist:001",
            "http://old_image.jpg",
            ["pop", "rock"],
            datetime(2026, 8, 31, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 8, 31, 10, 5, 0),
            "2026-08-31",
        ),
        (
            "art_001",
            "Coldplay",
            "spotify:artist:001",
            "http://new_image.jpg",
            ["pop", "rock", "alternative"],
            datetime(2026, 9, 1, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 9, 1, 10, 5, 0),
            "2026-09-01",
        ),
    ]

    bronze_df = spark.createDataFrame(data, schema=schema)
    silver_df = transformer.transform_artists(bronze_df)
    results = silver_df.collect()

    assert silver_df.count() == 1, "Deduplication failed to collapse duplicate artist"
    winner = results[0]
    assert winner.artist_id == "art_001"
    assert winner.artist_name == "Coldplay"
    assert winner.extracted_at == datetime(2026, 9, 1, 10, 0, 0)
    assert winner.snapshot_date == "2026-09-01"
    assert isinstance(winner.genres, list)
    assert winner.genres == ["pop", "rock", "alternative"]
    assert winner.spotify_uri == "spotify:artist:001"
    assert "silver_transformed_at" in silver_df.columns
    assert winner.silver_transformed_at is not None


# ===========================================================================
# 2. ALBUMS TESTS
# ===========================================================================
def test_transform_albums(spark, transformer):
    """
    Test album transformation:
    - Normalization of mixed date strings (YYYY, YYYY-MM, YYYY-MM-DD) into DateType.
    - Defensive handling of malformed/sentinel dates ("N/A").
    - Derived release_year, release_month, and release_decade.
    - Preserved release_date_precision and lowercased album_type.
    - Deterministic deduplication on album_id.
    """
    schema = StructType([
        StructField("album_id", StringType(), False),
        StructField("album_name", StringType(), True),
        StructField("album_type", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("release_date_precision", StringType(), True),
        StructField("total_tracks", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("spotify_uri", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("extracted_at", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("snapshot_date", StringType(), True),
    ])

    data = [
        # 1. Year only (Thriller 1982) - Old version
        (
            "  alb_001  ",
            "Thriller (Old)",
            "ALBUM",
            "1982",
            "YEAR",
            9,
            "art_001",
            "Michael Jackson",
            "spotify:album:001",
            "http://thriller_old.jpg",
            datetime(2026, 8, 31, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 8, 31, 10, 5, 0),
            "2026-08-31",
        ),
        # 1b. Duplicate of alb_001 - Newer version (winner!)
        (
            "alb_001",
            "Thriller",
            "ALBUM",
            "1982",
            "year",
            9,
            "art_001",
            "Michael Jackson",
            "spotify:album:001",
            "http://thriller_new.jpg",
            datetime(2026, 9, 1, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 9, 1, 10, 5, 0),
            "2026-09-01",
        ),
        # 2. Month precision (Folklore 2020-07)
        (
            "alb_002",
            "Folklore",
            "Single",
            "2020-07",
            "month",
            1,
            "art_002",
            "Taylor Swift",
            "spotify:album:002",
            "http://folklore.jpg",
            datetime(2026, 9, 1, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 9, 1, 10, 5, 0),
            "2026-09-01",
        ),
        # 3. Full day precision (TTPD 2024-04-19)
        (
            "alb_003",
            "The Tortured Poets Department",
            "Album",
            "2024-04-19",
            "day",
            16,
            "art_002",
            "Taylor Swift",
            "spotify:album:003",
            "http://ttpd.jpg",
            datetime(2026, 9, 1, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 9, 1, 10, 5, 0),
            "2026-09-01",
        ),
        # 4. Sentinel date edge case ("N/A") -> Tests your defensive null logic!
        (
            "alb_004",
            "Unknown Album",
            "Album",
            "N/A",
            "year",
            5,
            "art_003",
            "Unknown Artist",
            "spotify:album:004",
            "http://unknown.jpg",
            datetime(2026, 9, 1, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 9, 1, 10, 5, 0),
            "2026-09-01",
        ),
    ]

    bronze_df = spark.createDataFrame(data, schema=schema)
    silver_df = transformer.transform_albums(bronze_df)
    results = {row.album_id: row for row in silver_df.collect()}

    # Assert 5 input rows -> 4 unique albums (deduped alb_001)
    assert silver_df.count() == 4, "Deduplication failed to collapse alb_001"

    # Thriller: Year-only
    thriller = results["alb_001"]
    assert thriller.album_name == "Thriller"
    assert thriller.album_type == "album"
    assert thriller.release_date == date(1982, 1, 1)
    assert thriller.release_year == 1982
    assert thriller.release_month == 1
    assert thriller.release_decade == "1980s"
    assert thriller.release_date_precision == "year"

    # Folklore: Month precision
    folklore = results["alb_002"]
    assert folklore.album_type == "single"
    assert folklore.release_date == date(2020, 7, 1)
    assert folklore.release_year == 2020
    assert folklore.release_month == 7
    assert folklore.release_decade == "2020s"

    # TTPD: Full day precision
    ttpd = results["alb_003"]
    assert ttpd.release_date == date(2024, 4, 19)
    assert ttpd.release_year == 2024
    assert ttpd.release_month == 4
    assert ttpd.release_decade == "2020s"

    # Defensive check: "N/A" date safely converted to None
    unknown = results["alb_004"]
    assert unknown.release_date is None
    assert unknown.release_year is None
    assert unknown.release_decade is None


# ===========================================================================
# 3. TRACKS TESTS
# ===========================================================================
def test_transform_tracks(spark, transformer):
    """
    Test tracks transformation:
    - Derives duration_min and duration_sec with proper precision.
    - Casts explicit to boolean, track/disc numbers to int.
    - Deterministically deduplicates on track_id keeping latest extracted_at.
    - Trims string identifiers and verifies silver_transformed_at lineage.
    """
    schema = StructType([
        StructField("track_id", StringType(), False),
        StructField("track_name", StringType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("track_number", IntegerType(), True),
        StructField("disc_number", IntegerType(), True),
        StructField("album_id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("spotify_uri", StringType(), True),
        StructField("extracted_at", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("snapshot_date", StringType(), True),
    ])

    data = [
        # 1. Old version of track_001
        (
            "  trk_001  ",
            "Blank Space (Old)",
            231826,
            False,
            2,
            1,
            "alb_001",
            "art_001",
            "spotify:track:001",
            datetime(2026, 8, 31, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 8, 31, 10, 5, 0),
            "2026-08-31",
        ),
        # 1b. Newer version of track_001 (winner!)
        (
            "trk_001",
            "Blank Space",
            231826,
            False,
            2,
            1,
            "alb_001",
            "art_001",
            "spotify:track:001",
            datetime(2026, 9, 1, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 9, 1, 10, 5, 0),
            "2026-09-01",
        ),
        # 2. Distinct track with explicit = True
        (
            "trk_002",
            "Cardigan",
            180000,
            True,
            1,
            1,
            "alb_002",
            "art_001",
            "spotify:track:002",
            datetime(2026, 9, 1, 10, 0, 0),
            "spotify-web-api",
            datetime(2026, 9, 1, 10, 5, 0),
            "2026-09-01",
        ),
    ]

    bronze_df = spark.createDataFrame(data, schema=schema)
    silver_df = transformer.transform_tracks(bronze_df)
    results = {row.track_id: row for row in silver_df.collect()}

    assert silver_df.count() == 2, "Deduplication failed to collapse duplicate trk_001"

    blank_space = results["trk_001"]
    assert blank_space.track_name == "Blank Space"
    assert blank_space.duration_ms == 231826
    assert blank_space.duration_min == 3.86
    assert blank_space.duration_sec == 231.8
    assert blank_space.explicit is False
    assert blank_space.track_number == 2
    assert blank_space.disc_number == 1
    assert blank_space.snapshot_date == "2026-09-01"

    cardigan = results["trk_002"]
    assert cardigan.duration_min == 3.0
    assert cardigan.duration_sec == 180.0
    assert cardigan.explicit is True
    assert "silver_transformed_at" in silver_df.columns
