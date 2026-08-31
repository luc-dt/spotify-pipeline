import os
import sys
import time
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    to_timestamp,
    trim,
    coalesce,
)

from src.transform.spark_session import get_spark_session
from src.transform.schemas import ENTITY_SCHEMAS

class BronzeTransformer:
    """
    Ingests raw JSON payloads, enforces StrucType schemas,
    attaches audit lingeage metadata, and persists Snappy Parquet to Bronze layer. 
    """
    def __init__(self, 
            spark: Optional[SparkSession] = None, 
            raw_base_dir: str = "data/raw",
            bronze_base_dir: str = "data/bronze",
    ):
        self.spark = spark or get_spark_session(app_name="Spotify_Bronze_Ingestion")
        self.raw_base_dir = raw_base_dir
        self.bronze_base_dir = bronze_base_dir

    def read_raw_json(self, entity: str, snapshot_date: str) -> DataFrame:
        """Reads raw JSON array using multiline=True and explicit StructType schema."""
        schema = ENTITY_SCHEMAS.get(entity)
        if not schema:
            raise ValueError(f"No schema defined for entity: '{entity}'")
        
        file_path = os.path.join(self.raw_base_dir, entity, f"{entity}_{snapshot_date}.json")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Raw file not found: {file_path}")

        return (
            self.spark.read
            .schema(schema)
            .option("multiLine", True)
            .json(file_path)
        )

    def transform_artists(self, df: DataFrame) -> DataFrame:
        """Transform raw artists into structured Bronze DataFrame."""
        return (
            df
            .withColumn("artist_id", trim(col("artist_id")))
            .withColumn("artist_name", trim(col("artist_name")))
            .withColumn("extracted_at", to_timestamp(col("extracted_at")))
            .withColumn("source", lit("spotify-web-api"))
            .withColumn("ingestion_timestamp", current_timestamp())
            .select(
                "artist_id",
                "artist_name",
                "spotify_uri",
                "image_url",
                "genres",
                "extracted_at",
                "snapshot_date",
                "source",
                "ingestion_timestamp",
            )
        )
    def transform_albums(self, df: DataFrame) -> DataFrame:
        """Transforms raw albums into structured Bronze DataFrame."""
        return (
            df
            .withColumn("album_id", trim(col("album_id")))
            .withColumn("album_name", trim(col("album_name")))
            .withColumn("artist_id", trim(col("artist_id")))
            .withColumn("extracted_at", to_timestamp(col("extracted_at")))
            .withColumn("source", lit("spotify-web-api"))
            .withColumn("ingestion_timestamp", current_timestamp())
            .select(
                "album_id",
                "album_name",
                "album_type",
                "release_date",
                "release_date_precision",
                "total_tracks",
                "artist_id",
                "artist_name",
                "spotify_uri",
                "image_url",
                "extracted_at",
                "snapshot_date",
                "source",
                "ingestion_timestamp",
            )
        )

    def transform_tracks(self, df: DataFrame) -> DataFrame:
        """Transforms raw tracks into structured Bronze DataFrame."""
        return (
            df
            .withColumn("track_id", trim(col("track_id")))
            .withColumn("track_name", trim(col("track_name")))
            .withColumn("album_id", trim(col("album_id")))
            .withColumn("artist_id", trim(col("artist_id")))
            .withColumn("disc_number", coalesce(col("disc_number"), lit(1)))
            .withColumn("explicit", coalesce(col("explicit"), lit(False)))
            .withColumn("extracted_at", to_timestamp(col("extracted_at")))
            .withColumn("source", lit("spotify-web-api"))
            .withColumn("ingestion_timestamp", current_timestamp())
            .select(
                "track_id",
                "track_name",
                "duration_ms",
                "explicit",
                "track_number",
                "disc_number",
                "album_id",
                "artist_id",
                "spotify_uri",
                "extracted_at",
                "snapshot_date",
                "source",
                "ingestion_timestamp",
            )
        )

    def transform_to_bronze(self, entity: str, raw_df: DataFrame) -> DataFrame:
        """Routes raw DataFrame to the appropriate entity transformation"""
        transformers= {
            "artists": self.transform_artists,
            "albums": self.transform_albums,
            "tracks": self.transform_tracks,
        }
        transformer_fn = transformers.get(entity)
        if not transformer_fn:
            raise ValueError(f"Unknown entity: '{entity}'")
        
        return transformer_fn(raw_df)

    def run_snapshot(self, snapshot_date: str = "2026-08-31") -> Dict[str, Any]:
        """Orchestrates end-to-end Bronze ingestion for all 3 entities."""
        start_time = time.time()
        entities = ["artists", "albums", "tracks"]
        summary = {}

        print("=" * 70)
        print("🧱 PYSPARK BRONZE TRANSFORMATION ENGINE")
        print(f"📅 Snapshot Date : {snapshot_date}")
        print(f"📁 Raw Source    : {self.raw_base_dir}")
        print(f"📁 Bronze Output : {self.bronze_base_dir}")
        print("=" * 70)

        for entity in entities:
            print(f"\n⚙️  Processing '{entity}'...")
            
            # 1. Read Raw JSON with Schema
            raw_df = self.read_raw_json(entity, snapshot_date)
            raw_count = raw_df.count()
            
            # 2. Transform to Bronze
            bronze_df = self.transform_to_bronze(entity, raw_df)
            
            # 3. Write Snappy Parquet partitioned by snapshot_date
            out_path = self.write_bronze_parquet(bronze_df, entity)
            
            print(f"   ✓ Read {raw_count:,} raw records")
            print(f"   ✓ Wrote Snappy Parquet to: {out_path}/snapshot_date={snapshot_date}/")

            summary[entity] = {
                "records_processed": raw_count,
                "output_path": out_path,
            }

        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print(f"🏁 Bronze Transformation Complete in {elapsed:.2f}s!")
        print("=" * 70)
        return summary

    def write_bronze_parquet(self, df: DataFrame, entity: str) -> str:
        """Writes DataFrame as Snappy-compressed Parquet partitioned by snapshot_date."""
        output_path = os.path.join(self.bronze_base_dir, entity)
        (
            df.write
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(output_path)
        )
        return output_path
    
if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-08-31"

    transformer = BronzeTransformer()
    transformer.run_snapshot(snapshot_date=target_date)
    transformer.spark.stop()

