import os
import sys
import pytest
from pyspark.sql.types import TimestampType, ArrayType, LongType, BooleanType

# Ensure root workspace is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.transform.spark_session import get_spark_session
from src.transform.schemas import (
    ENTITY_SCHEMAS,
    RAW_ARTISTS_SCHEMA,
    RAW_ALBUMS_SCHEMA,
    RAW_TRACKS_SCHEMA,
)
from src.transform.bronze_transformer import BronzeTransformer


@pytest.fixture(scope="session")
def spark():
    """Session-scoped PySpark test fixture."""
    session = get_spark_session(app_name="Spotify_Bronze_UnitTests", master="local[1]")
    yield session
    session.stop()


@pytest.fixture
def transformer(spark, tmp_path):
    """Provides BronzeTransformer pointing to a temporary test directory."""
    return BronzeTransformer(
        spark=spark,
        raw_base_dir="data/raw",
        bronze_base_dir=str(tmp_path / "bronze"),
    )


class TestBronzeTransformations:
    """Unit test suite for PySpark Bronze layer schemas and transformations."""

    def test_schemas_contract_and_types(self):
        """Test that schemas contain exact required field names and key data types."""
        # 1. Artists Contract
        expected_artist_fields = {
            "artist_id", "artist_name", "spotify_uri", "image_url",
            "genres", "extracted_at", "snapshot_date"
        }
        assert set(RAW_ARTISTS_SCHEMA.fieldNames()) == expected_artist_fields
        assert isinstance(RAW_ARTISTS_SCHEMA["genres"].dataType, ArrayType)

        # 2. Albums Contract
        expected_album_fields = {
            "album_id", "album_name", "album_type", "release_date",
            "release_date_precision", "total_tracks", "artist_id",
            "artist_name", "spotify_uri", "image_url", "extracted_at", "snapshot_date"
        }
        assert set(RAW_ALBUMS_SCHEMA.fieldNames()) == expected_album_fields

        # 3. Tracks Contract
        expected_track_fields = {
            "track_id", "track_name", "duration_ms", "explicit",
            "track_number", "disc_number", "album_id", "artist_id",
            "spotify_uri", "extracted_at", "snapshot_date"
        }
        assert set(RAW_TRACKS_SCHEMA.fieldNames()) == expected_track_fields
        assert isinstance(RAW_TRACKS_SCHEMA["duration_ms"].dataType, LongType)
        assert isinstance(RAW_TRACKS_SCHEMA["explicit"].dataType, BooleanType)

    def test_transform_artists_enrichment(self, spark, transformer):
        """Test transform_artists attaches source, ingestion_timestamp, and casts types."""
        test_data = [
            (" 06HL4z0CvFAxyc27GXpf02 ", " Taylor Swift ", "spotify:artist:06HL4z0CvFAxyc27GXpf02", None, ["pop"], "2026-08-31T07:21:27.000000+00:00", "2026-08-31")
        ]
        raw_df = spark.createDataFrame(test_data, RAW_ARTISTS_SCHEMA)
        bronze_df = transformer.transform_to_bronze("artists", raw_df)

        row = bronze_df.first()
        assert row["artist_id"] == "06HL4z0CvFAxyc27GXpf02"  # Trimmed
        assert row["artist_name"] == "Taylor Swift"          # Trimmed
        assert row["source"] == "spotify-web-api"
        assert row["ingestion_timestamp"] is not None
        assert isinstance(bronze_df.schema["extracted_at"].dataType, TimestampType)

    def test_transform_tracks_defaults(self, spark, transformer):
        """Test transform_tracks handles null disc_number and explicit defaults."""
        test_data = [
            ("track_1", "Song A", 200000, None, 1, None, "alb_1", "art_1", "spotify:track:1", "2026-08-31T07:21:27.000000+00:00", "2026-08-31")
        ]
        raw_df = spark.createDataFrame(test_data, RAW_TRACKS_SCHEMA)
        bronze_df = transformer.transform_to_bronze("tracks", raw_df)

        row = bronze_df.first()
        assert row["disc_number"] == 1       # Coalesced default
        assert row["explicit"] is False      # Coalesced default
        assert row["source"] == "spotify-web-api"

    def test_parquet_write_and_physical_partition(self, spark, transformer):
        """Test writing Parquet and verify both physical partition directory and logical read-back."""
        test_data = [
            ("alb_100", "Album Test", "album", "2026-01-01", "day", 10, "art_1", "Artist 1", None, None, "2026-08-31T07:21:27.000000+00:00", "2026-08-31")
        ]
        raw_df = spark.createDataFrame(test_data, RAW_ALBUMS_SCHEMA)
        bronze_df = transformer.transform_to_bronze("albums", raw_df)

        out_path = transformer.write_bronze_parquet(bronze_df, "albums")

        # 1. Physical directory verification
        physical_partition = os.path.join(out_path, "snapshot_date=2026-08-31")
        assert os.path.isdir(physical_partition), f"Physical partition folder missing: {physical_partition}"

        parquet_files = [f for f in os.listdir(physical_partition) if f.endswith(".parquet")]
        assert len(parquet_files) > 0, "No .parquet file found in physical partition"

        # 2. Logical read-back verification
        read_df = spark.read.parquet(out_path)
        assert read_df.count() == 1
        assert read_df.first()["album_id"] == "alb_100"
        assert str(read_df.first()["snapshot_date"]) == "2026-08-31"

    def test_idempotent_rerun_no_duplicates(self, spark, transformer):
        """Test that re-running the Bronze transformation for the same partition does not duplicate data."""
        test_data = [
            ("alb_100", "Album Test", "album", "2026-01-01", "day", 10, "art_1", "Artist 1", None, None, "2026-08-31T07:21:27.000000+00:00", "2026-08-31")
        ]
        raw_df = spark.createDataFrame(test_data, RAW_ALBUMS_SCHEMA)
        bronze_df = transformer.transform_to_bronze("albums", raw_df)

        # Run 1
        out_path = transformer.write_bronze_parquet(bronze_df, "albums")
        assert spark.read.parquet(out_path).count() == 1

        # Run 2 (Same partition / input)
        transformer.write_bronze_parquet(bronze_df, "albums")
        
        # Verify: Exactly 1 record exists (0 duplicate row accumulation)
        read_df_after = spark.read.parquet(out_path)
        assert read_df_after.count() == 1, "Idempotency failed: Row count doubled on re-run!"
