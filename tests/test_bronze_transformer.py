"""tests/test_bronze_transformer.py
--------------------------------
Unit and integration tests for PySpark Bronze layer transformations.
Tests schema definitions, null coalescing, whitespace trimming,
lineage metadata injection, and Parquet serialization/deserialization.
"""

import os
import sys
import pytest

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.transform.spark_session import get_spark_session
from src.transform.schemas import (
    RAW_ARTISTS_SCHEMA,
    RAW_ALBUMS_SCHEMA,
    RAW_TRACKS_SCHEMA,
    ENTITY_SCHEMAS,
)
from src.transform.bronze_transformer import BronzeTransformer


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession fixture for testing."""
    session = get_spark_session(app_name="Spotify_Bronze_UnitTests")
    yield session
    session.stop()


@pytest.fixture
def transformer(spark, tmp_path):
    """Fixture providing a BronzeTransformer with a temporary output directory."""
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    raw_dir.mkdir()
    bronze_dir.mkdir()
    return BronzeTransformer(
        spark=spark,
        raw_base_dir=str(raw_dir),
        bronze_base_dir=str(bronze_dir),
    )


class TestBronzeSchemas:
    """Test suite for explicit StructType schema contracts."""

    def test_schema_entities_present(self):
        """Verify all 3 required entity schemas exist."""
        assert "artists" in ENTITY_SCHEMAS
        assert "albums" in ENTITY_SCHEMAS
        assert "tracks" in ENTITY_SCHEMAS

    def test_primary_keys_non_nullable(self):
        """Verify primary and critical foreign keys cannot be null in schema contract."""
        artist_id_field = next(f for f in RAW_ARTISTS_SCHEMA.fields if f.name == "artist_id")
        album_id_field = next(f for f in RAW_ALBUMS_SCHEMA.fields if f.name == "album_id")
        track_id_field = next(f for f in RAW_TRACKS_SCHEMA.fields if f.name == "track_id")

        assert artist_id_field.nullable is False
        assert album_id_field.nullable is False
        assert track_id_field.nullable is False


class TestBronzeTransformations:
    """Test suite for DataFrame transformations."""

    def test_transform_artists_trims_and_adds_lineage(self, spark, transformer):
        """Verify artist names are trimmed and lineage columns are attached."""
        data = [
            (
                "  06HL4z0CvFAxyc27GXpf02  ",
                "  Taylor Swift  ",
                "spotify:artist:06HL4z0CvFAxyc27GXpf02",
                "http://image.url",
                ["pop"],
                "2026-08-31T07:21:27.241259+00:00",
                "2026-08-31",
            )
        ]
        raw_df = spark.createDataFrame(data, schema=RAW_ARTISTS_SCHEMA)

        bronze_df = transformer.transform_artists(raw_df)
        row = bronze_df.collect()[0]

        assert row.artist_id == "06HL4z0CvFAxyc27GXpf02"
        assert row.artist_name == "Taylor Swift"
        assert row.source == "spotify-web-api"
        assert row.ingestion_timestamp is not None
        assert "genres" in bronze_df.columns

    def test_transform_tracks_coalesces_defaults(self, spark, transformer):
        """Verify tracks transformation handles null disc_number and explicit fields."""
        data = [
            (
                "track_123",
                "Blank Space",
                231000,
                None,  # explicit is null -> should coalesce to False
                1,
                None,  # disc_number is null -> should coalesce to 1
                "album_456",
                "artist_789",
                "spotify:track:track_123",
                "2026-08-31T07:21:31.724799+00:00",
                "2026-08-31",
            )
        ]
        raw_df = spark.createDataFrame(data, schema=RAW_TRACKS_SCHEMA)

        bronze_df = transformer.transform_tracks(raw_df)
        row = bronze_df.collect()[0]

        assert row.explicit is False
        assert row.disc_number == 1
        assert row.source == "spotify-web-api"
        assert row.ingestion_timestamp is not None


class TestBronzeParquetIO:
    """Test suite for Parquet serialization and partition management."""

    def test_write_and_read_parquet(self, spark, transformer):
        """Verify Parquet write produces readable partitioned Snappy dataset."""
        data = [
            (
                "track_abc",
                "Style",
                210000,
                False,
                3,
                1,
                "album_1",
                "artist_1",
                "uri_1",
                "2026-08-31T00:00:00+00:00",
                "2026-08-31",
            )
        ]
        raw_df = spark.createDataFrame(data, schema=RAW_TRACKS_SCHEMA)
        bronze_df = transformer.transform_tracks(raw_df)

        output_path = transformer.write_bronze_parquet(bronze_df, "tracks")

        # Read back from Parquet output
        read_df = spark.read.parquet(output_path)
        assert read_df.count() == 1
        assert "snapshot_date" in read_df.columns
        assert read_df.collect()[0].track_name == "Style"
