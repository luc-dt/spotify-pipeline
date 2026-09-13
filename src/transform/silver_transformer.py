"""
Transform Bronze Parquet datasets into clean, deduplicated,
and standardized Silver entities
"""
import os
import sys
import time
from typing import Optional, Dict, Any
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    row_number,
    trim,
    current_timestamp,
    when,
    concat,
    lit,
    coalesce,
    array,
    length,
    substring,
    to_date,
    lower,
    floor,
    round as spark_round,
)

from src.transform.spark_session import get_spark_session

class SilverTransformer:
    """
    Transforms Bronze Parquet datasets into clean, typed, deduplicated Silver entities.
    """

    def __init__(
        self, 
        spark: Optional[SparkSession] = None,
        bronze_base_dir: str = "data/bronze",
        silver_base_dir: str = "data/silver",
    ): 
        self.spark = spark or get_spark_session(app_name="Spotify_Silver_Transformer")
        self.bronze_base_dir = bronze_base_dir
        self.silver_base_dir = silver_base_dir
    
    def read_bronze_parquet(self, entity: str, snapshot_date: Optional[str] = None) -> DataFrame:
        """
        Reads Bronze Parquet data using partition pruning when snapshot_date is supplied.
        Avoids os.path.exists() to ensure cloud object store (S3/ADLS/GCS) compatibility.
        """
        path = os.path.join(self.bronze_base_dir, entity)
        df = self.spark.read.parquet(path)
        if snapshot_date:
            df = df.filter(col("snapshot_date") == lit(snapshot_date))
        return df

    def transform_artists(self, df: DataFrame) -> DataFrame:
        """
        Cleans and deterministically deduplicates Bronze artists.
        - Trims string identifiers FIRST so keys match.
        - Preserves and standardizes Spotify URI structure.
        - Preserves genres as array (coalescing null to empty list).
        - Partitions by artist_id (within snapshot).
        - Orders by extracted_at DESC, ingestion_timestamp DESC.
        - Keeps row_number == 1 (latest extraction).
        - Stamps silver_transformed_at audit lineage
        """
        # 1. Clean, Sanitize, and Validate Fields First
        cleaned_df = (
            df
            .withColumn("artist_id", trim(col("artist_id")))
            .withColumn("artist_name", trim(col("artist_name")))
            .withColumn("spotify_uri", trim(col("spotify_uri")))
            .withColumn("genres", coalesce(col("genres"), array().cast("array<string>")))
            .withColumn("silver_transformed_at", current_timestamp())
        )

        # 2. Define deduplication window
        window_spec = (
            Window
            .partitionBy("artist_id")
            .orderBy(col("extracted_at").desc(), col("ingestion_timestamp").desc())
        )

        # 3. Keep latest record per artist
        return (
            cleaned_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .select(
                "artist_id",
                "artist_name",
                "spotify_uri",
                "image_url",
                "genres",
                "extracted_at",
                "snapshot_date",
                "source",
                "silver_transformed_at",
            )
        )

    def transform_albums(self, df: DataFrame) -> DataFrame:
        """
        Transforms Bronze albums into clean, conformed Silver albums:
        - Trims string identifiers and names FIRST.
        - Standardized mixed release_date string (YYYY, YYYY-MM, YYYY-MM-DD) into DateType.
        - Derives release_year, release_month, and release_decade.
        - Preserves source release_date_precision.
        - Normalizes album_type to lowercase.
        - Deterministically deduplicates on album_id keeping latest extracted_at.
        - Stamps silver_transformed_at audit lineage.
        """
        # 1. Clean strings and pad mixed dates.
        raw_date = trim(col("release_date"))
        date_str = when(raw_date.isin("", "N/A", "null"), None).otherwise(raw_date)

        standardized_date_str = (
            when(length(date_str) == 4, concat(date_str, lit("-01-01")))
            .when(length(date_str) == 7, concat(date_str, lit("-01")))
            .otherwise(date_str)
        )

        release_year = when(length(date_str) >= 4, substring(date_str, 1, 4).cast("int")).otherwise(None)
        release_month = (
            when(length(date_str) >= 7, substring(date_str, 6, 2).cast("int"))
            .when(length(date_str) >= 4, lit(1))
            .otherwise(None)
        )
        release_decade = when(
                release_year.isNotNull(), 
                concat((floor(release_year / 10) * 10).cast("string"), lit("s"))
        ).otherwise(None)
        total_tracks_clean = when(trim(col("total_tracks")).isin("", "null"), None).otherwise(trim(col("total_tracks")))
        cleaned_df = (
            df
            .withColumn("album_id", trim(col("album_id")))
            .withColumn("album_name", trim(col("album_name")))
            .withColumn("artist_id", trim(col("artist_id")))
            .withColumn("artist_name", trim(col("artist_name")))
            .withColumn("spotify_uri", trim(col("spotify_uri")))
            .withColumn("album_type", lower(trim(col("album_type"))))
            .withColumn("release_date", to_date(standardized_date_str, "yyyy-MM-dd"))
            .withColumn("release_year", release_year)
            .withColumn("release_month", release_month)
            .withColumn("release_decade", release_decade)
            .withColumn("release_date_precision", lower(trim(col("release_date_precision"))))
            .withColumn("total_tracks", total_tracks_clean.cast("int"))
            .withColumn("silver_transformed_at", current_timestamp())
        )

        # 2. Define deduplication window
        window_spec = (
            Window
            .partitionBy("album_id")
            .orderBy(col("extracted_at").desc(), col("ingestion_timestamp").desc())
        )

        # 3. Keep latest record per album and project conformed schema
        return (
            cleaned_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .select(
                "album_id",
                "album_name",
                "album_type",
                "release_date",
                "release_year",
                "release_month",
                "release_decade",
                "release_date_precision",
                "total_tracks",
                "artist_id",
                "artist_name",
                "spotify_uri",
                "image_url",
                "extracted_at",
                "snapshot_date",
                "source",
                "silver_transformed_at",
            )
        ) 
    
    def transform_tracks(self, df: DataFrame) -> DataFrame:
        """
        Transform Bronze tracks into clean, conformed Silver tracks:
        - Trims string identifiers and names FIRST.
        - Derives duration_min and duration_sec from duration_ms.
        - Casts explicit to BooleanType.
        - Casts disc_number and track_number to IntegerType.
        - Deterministically deduplicates on track_id keeping latest extracted_at.
        - Stamps silver_transformed_at audit lineage.
        """
        # 1. Clean, derive duration metrics, and cast types.
        duration_ms = trim(col("duration_ms").cast("string")).cast("long")
        duration_min = spark_round(duration_ms / 60000.0, 2)
        duration_sec = spark_round(duration_ms / 1000.0, 1)

        cleaned_df = (
            df
            .withColumn("track_id", trim(col("track_id")))
            .withColumn("track_name", trim(col("track_name")))
            .withColumn("album_id", trim(col("album_id")))
            .withColumn("artist_id", trim(col("artist_id")))
            .withColumn("spotify_uri", trim(col("spotify_uri")))
            .withColumn("duration_ms", duration_ms)
            .withColumn("duration_min", duration_min)
            .withColumn("duration_sec", duration_sec)
            .withColumn("explicit", trim(col("explicit").cast("string")).cast("boolean"))
            .withColumn("track_number", trim(col("track_number").cast("string")).cast("int"))
            .withColumn("disc_number",  trim(col("disc_number").cast("string")).cast("int"))
            .withColumn("silver_transformed_at", current_timestamp())
        )

        # 2. Define deduplication windown on cleaned track_id
        window_spec = (
            Window
            .partitionBy("track_id")
            .orderBy(col("extracted_at").desc(), col("ingestion_timestamp").desc())
        )

        # 3. Keep latest record per track and project conformed schema
        return (
            cleaned_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .select(
                "track_id",
                "track_name",
                "duration_ms",
                "duration_min",
                "duration_sec",
                "explicit",
                "track_number",
                "disc_number",
                "album_id",
                "artist_id",
                "spotify_uri",
                "extracted_at",
                "snapshot_date",
                "source",
                "silver_transformed_at",
            )
        )

    def write_silver_parquet(self, df: DataFrame, entity: str) -> str:
        """
        Writes conformed Silver DataFrame to Snappy Parquet partitioned by snapshot_date.
        Relies on dynamic partition overwrite to update individual partitions safely.
        """
        output_path = os.path.join(self.silver_base_dir, entity)
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(output_path)
        )
        return output_path

    def transform_to_silver(self, entity: str, bronze_df: DataFrame) -> DataFrame:
        """Routes Bronze DataFrame to the corresponding Silver entity transformation."""
        transformers = {
            "artists": self.transform_artists,
            "albums": self.transform_albums,
            "tracks": self.transform_tracks,
        }
        transformer_fn = transformers.get(entity)
        if not transformer_fn:
            raise ValueError(f"Unknown entity: '{entity}'")
        return transformer_fn(bronze_df)

    def run_snapshot(self, snapshot_date: str = "2026-08-31") -> Dict[str, Any]:
        """Orchestrates end-to-end Silver transformation for all 3 entities."""
        start_time = time.time()
        entities = ["artists", "albums", "tracks"]
        summary = {}

        print("=" * 70)
        print("🥈 PYSPARK SILVER TRANSFORMATION ENGINE")
        print(f"📅 Snapshot Date : {snapshot_date}")
        print(f"📁 Bronze Source : {self.bronze_base_dir}")
        print(f"📁 Silver Output : {self.silver_base_dir}")
        print("=" * 70)

        for entity in entities:
            print(f"\n⚙️  Transforming '{entity}' to Silver...")

            # 1. Read Bronze with Catalyst partition pruning
            bronze_df = self.read_bronze_parquet(entity, snapshot_date)
            bronze_count = bronze_df.count()

            # 2. Clean, Standardize, and Deduplicate
            silver_df = self.transform_to_silver(entity, bronze_df)
            silver_count = silver_df.count()

            # 3. Write conformed Snappy Parquet
            out_path = self.write_silver_parquet(silver_df, entity)

            print(f"   ✓ Bronze rows read : {bronze_count:,}")
            print(f"   ✓ Conformed Silver : {silver_count:,}")
            print(f"   ✓ Written to       : {out_path}/snapshot_date={snapshot_date}/")

            summary[entity] = {
                "bronze_records": bronze_count,
                "silver_records": silver_count,
                "output_path": out_path,
            }

        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print(f"🏁 Silver Transformation Complete in {elapsed:.2f}s!")
        print("=" * 70)
        return summary


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="PySpark Silver Layer Transformer")
    parser.add_argument("snapshot_date_pos", nargs="?", default=None, help="Snapshot date (YYYY-MM-DD)")
    parser.add_argument("--snapshot-date", "-d", type=str, default=None, help="Snapshot date (YYYY-MM-DD)")
    args = parser.parse_args()

    target_date = args.snapshot_date or args.snapshot_date_pos or "2026-08-31"

    transformer = SilverTransformer()
    transformer.run_snapshot(snapshot_date=target_date)
    transformer.spark.stop()
