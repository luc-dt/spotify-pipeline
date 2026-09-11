"""
src/transform/gold_transformer.py
---------------------------------
Kimball Star Schema Transformation Engine (Day 6 - Phase 3).
Transforms conformed Silver entities into 4 conformed dimensions and 1 periodic snapshot fact.
"""

import os
import sys
from typing import Optional, Dict, Any, List
from datetime import date
from pyspark.sql.types import DoubleType, DateType

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    md5,
    to_date,
    date_format,
    year,
    quarter,
    month,
    dayofmonth,
    dayofweek,
    weekofyear,
    when,
    current_timestamp,
    row_number,
    countDistinct,
    date_sub,
    datediff,
    lag,
    percentile_approx,
    round as spark_round,
    least,
    greatest,
)

from src.transform.spark_session import get_spark_session
from pyspark.sql.window import Window

class GoldTransformer:
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        silver_base_dir: str = "data/silver",
        gold_base_dir: str = "data/gold",
    ):
        self.spark = spark or get_spark_session(app_name="Spotify_Gold_Transformer")
        self.silver_base_dir = silver_base_dir
        self.gold_base_dir = gold_base_dir

    def build_dim_date(
        self,
        start_date: str = "1950-01-01",
        end_date: str = "2030-12-31",
    ) -> DataFrame:
        """
        Generates Gregorian calendar dimension from start_date to end_date.
        Persists unpartitioned Snappy Parquet to data/gold/dim_date/.
        """
        print(f"📅 Building 'dim_date' ({start_date} to {end_date})...")

        # 1. Generate full date sequence using native Spark SQL
        df_dates = self.spark.sql(f"""
            SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date
        """)

        # 2. Enrich with Kimball calendar attributes
        dim_date_df = (
            df_dates
            .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
            .withColumn("year", year(col("full_date")).cast("int"))
            .withColumn("quarter", quarter(col("full_date")).cast("int"))
            .withColumn("month", month(col("full_date")).cast("int"))
            .withColumn("month_name", date_format(col("full_date"), "MMMM"))
            .withColumn("week_of_year", weekofyear(col("full_date")).cast("int"))
            .withColumn("day_of_month", dayofmonth(col("full_date")).cast("int"))
            # 'u' format in Spark returns ISO 1=Monday ... 7=Sunday
            .withColumn("day_of_week", ((dayofweek(col("full_date")) + 5) % 7 + 1).cast("int"))
            .withColumn("day_name", date_format(col("full_date"), "EEEE"))
            .withColumn("is_weekend", when(col("day_of_week").isin(6, 7), lit(True)).otherwise(lit(False)))
            .select(
                "date_key",
                "full_date",
                "year",
                "quarter",
                "month",
                "month_name",
                "week_of_year",
                "day_of_month",
                "day_of_week",
                "day_name",
                "is_weekend",
            )
        )

        # 3. Write unpartitioned Snappy Parquet
        out_path = os.path.join(self.gold_base_dir, "dim_date")
        (
            dim_date_df
            .write
            .mode("overwrite")
            .parquet(out_path)
        )

        print(f"  ✓ Saved 'dim_date' ({dim_date_df.count():,} rows) -> {out_path}")
        return dim_date_df

    def build_dim_artist(self) -> DataFrame:
        """
        Builds conformed Type 1 SCD artist dimension from Silver artists.
        Generates deterministic MD5 surrogate key `artist_key`.
        Persists unpartitioned Snappy Parquet to data/gold/dim_artist/.
        """
        print("👤 Building 'dim_artist'...")
        silver_path = os.path.join(self.silver_base_dir, "artists")
        silver_df = self.spark.read.parquet(silver_path)

        # 1. Type 1 SCD: Get latest record per artist across all snapshots
        window_spec = (
            Window
            .partitionBy("artist_id")
            .orderBy(col("snapshot_date").desc(), col("silver_transformed_at").desc())
        )

        dim_artist_df = (
            silver_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .withColumn("artist_key", md5(col("artist_id")))
            .withColumn("gold_transformed_at", current_timestamp())
            .select(
                "artist_key",
                "artist_id",
                "artist_name",
                "genres",
                "spotify_uri",
                "image_url",
                "gold_transformed_at",
            )
        )

        # 2. Write unpartitioned Snappy Parquet
        out_path = os.path.join(self.gold_base_dir, "dim_artist")
        (
            dim_artist_df
            .write
            .mode("overwrite")
            .parquet(out_path)
        )

        print(f"  ✓ Saved 'dim_artist' ({dim_artist_df.count():,} rows) -> {out_path}")
        return dim_artist_df
    
    def build_dim_album(self) -> DataFrame:
        """
        Builds conformed Type 1 SCD album dimension from Silver albums.
        Generates deterministic MD5 surrogate keys `album_key` and FK `artist_key`.
        Persists unpartitioned Snappy Parquet to data/gold/dim_album/.
        """
        print("💿 Building 'dim_album'...")
        silver_path = os.path.join(self.silver_base_dir, "albums")
        silver_df = self.spark.read.parquet(silver_path)
        
        # 1. Type 1 SCD: Get latest record per album across all snapshots
        window_spec = (
            Window
            .partitionBy("album_id")
            .orderBy(col("snapshot_date").desc(), col("silver_transformed_at").desc())
        )

        dim_album_df = (
            silver_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .withColumn("album_key", md5(col("album_id")))
            .withColumn("artist_key", md5(col("artist_id")))
            .withColumn("gold_transformed_at", current_timestamp())
            .select(
                "album_key",
                "album_id",
                "artist_key",
                "artist_id",
                "album_name",
                "album_type",
                "release_date",
                "release_year",
                "release_month",
                "release_decade",
                "release_date_precision",
                "total_tracks",
                "spotify_uri",
                "image_url",
                "gold_transformed_at",
            )
        )

        # 2. Write unpartitioned Snappy Parquet
        out_path = os.path.join(self.gold_base_dir, "dim_album")
        (
            dim_album_df
            .write
            .mode("overwrite")
            .parquet(out_path)
        )

        print(f"  ✓ Saved 'dim_album' ({dim_album_df.count():,} rows) -> {out_path}")

        return dim_album_df

    def build_dim_track(self) -> DataFrame:
        """
        Builds conformed Type 1 SCD track dimension from Silver tracks.
        Generates deterministic MD5 surrogate ket `track_key`, FK `album_key`, and FK `artist_key`.
        Persists unpartitioned Snappy Parquet to data/gol/dim_track/.
        """
        print("🎵 Building 'dim_track'...")
        silver_path = os.path.join(self.silver_base_dir, "tracks")
        silver_df = self.spark.read.parquet(silver_path)

        # 1. Type 1 SCD: Get latest record per track across all snapshots\
        window_spec = (
            Window
            .partitionBy("track_id")
            .orderBy(col("snapshot_date").desc(), col("silver_transformed_at").desc())
        )

        dim_track_df = (
            silver_df
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .withColumn("track_key", md5(col("track_id")))
            .withColumn("album_key", md5(col("album_id")))
            .withColumn("artist_key", md5(col("artist_id")))
            .withColumn("gold_transformed_at", current_timestamp())
            .select(
                "track_key",
                "track_id",
                "album_key",
                "album_id",
                "artist_key",
                "artist_id",
                "track_name",
                "duration_ms",
                "duration_min",
                "duration_sec",
                "explicit",
                "track_number",
                "disc_number",
                "spotify_uri",
                "gold_transformed_at",
            )
        ) 

        # 2. Write unpartitioned Snappy Parquet
        out_path = os.path.join(self.gold_base_dir, "dim_track")
        (
            dim_track_df
            .write
            .mode("overwrite")
            .parquet(out_path)
        )
        print(f"  ✓ Saved 'dim_track' ({dim_track_df.count():,} rows) -> {out_path}")
        return dim_track_df

    def build_snapshot_base(self, snapshot_date: Optional[str] = None) -> DataFrame:
        """
        Compute base catalog measures per (artist_id, snapshot_date):
        total_albums, total_tracks, total_singles, recent_release_12m, and median cadence.
        Use native PySpark window functions (lag + datediff + percentile_approx).
        """
        print("📐 Computing snapshot base aggregations (Native Spark)...")
        artists_df = self.spark.read.parquet(os.path.join(self.silver_base_dir, "artists"))
        albums_df = self.spark.read.parquet(os.path.join(self.silver_base_dir, "albums"))
        tracks_df = self.spark.read.parquet(os.path.join(self.silver_base_dir, "tracks"))

        if snapshot_date:
            artists_df = artists_df.filter(col("snapshot_date") == lit(snapshot_date))
            albums_df = albums_df.filter(col("snapshot_date") == lit(snapshot_date))
            tracks_df = tracks_df.filter(col("snapshot_date") == lit(snapshot_date))

        # 1. Total Tracks per artist per snapshot
        track_counts = (
            tracks_df
            .groupBy("artist_id", "snapshot_date")
            .agg(countDistinct("track_id").alias("total_tracks"))
        )

        # 2. Album counts and 12m velocity per artist per snapshot
        album_counts = (
            albums_df
            .groupBy("artist_id", "snapshot_date")
            .agg(
                countDistinct(
                    when((col("album_type") == "album") & (col("release_date") <= col("snapshot_date")), col("album_id"))
                ).alias("total_albums"),
                countDistinct(
                    when((col("album_type") == "single") & (col("release_date") <= col("snapshot_date")), col("album_id"))
                ).alias("total_singles"),
                countDistinct(
                    when(
                        col("album_type").isin("album", "single")
                        & (col("release_date") >= date_sub(col("snapshot_date"), 365))
                        & (col("release_date") <= col("snapshot_date")),
                        col("album_id")
                    )
                ).alias("recent_releases_12m")
            )
        )

        # 3. Inter-release Cadence using Native Window Functions
        release_dates_df = (
            albums_df
            .filter(col("release_date").isNotNull() & (col("release_date") <= col("snapshot_date")))
            .select("artist_id", "snapshot_date", "release_date")
            .distinct()
        )
        cadence_window = Window.partitionBy("artist_id", "snapshot_date").orderBy(col("release_date").asc())

        intervals_df = (
            release_dates_df
            .withColumn("prev_date", lag("release_date", 1).over(cadence_window))
            .withColumn("gap_days", datediff(col("release_date"), col("prev_date")))
            .filter(col("gap_days").isNotNull())
        )

        cadence_df = (
            intervals_df
            .groupBy("artist_id", "snapshot_date")
            .agg(percentile_approx("gap_days", 0.5, 10000).cast("double").alias("median_release_cadence_days"))
        )

         # 4. Join all measures into single snapshot grid
        base_df = (
            artists_df.select("artist_id", "snapshot_date").distinct()
            .join(album_counts, on=["artist_id", "snapshot_date"], how="left")
            .join(track_counts, on=["artist_id", "snapshot_date"], how="left")
            .join(cadence_df, on=["artist_id", "snapshot_date"], how="left")
            .fillna({
                "total_albums": 0,
                "total_singles": 0,
                "total_tracks": 0,
                "recent_releases_12m": 0,
            })
        )

        return base_df

    def build_fact_artist_snapshot(self, snapshot_date: Optional[str] = None) -> DataFrame:
        """
        Builds periodic snapshot fact table at the grain of (artist_key, date_key).
        Calculates catalog_growth_pct via LAG windowing and calibrated momentum index.
        Persists partitioned Snappy Parquet to data/gold/fact_artist_snapshot/.
        """
        print("🏛️ Building 'fact_artist_snapshot'...")
        base_df = self.build_snapshot_base(snapshot_date=None)

        # 1. Add Kimball Surrogate Keys
        with_keys_df = (
            base_df
            .withColumn("artist_key", md5(col("artist_id")))
            .withColumn("date_key", date_format(col("snapshot_date"), "yyyyMMdd").cast("int"))
        )

        # 2. Inter-snapshot window LAG for catalog growth %
        growth_window = Window.partitionBy("artist_id").orderBy(col("snapshot_date").asc())
        prev_tracks = lag("total_tracks", 1).over(growth_window)

        with_growth_df = (
            with_keys_df
            .withColumn("prev_tracks", prev_tracks)
            .withColumn(
                "catalog_growth_pct",
                when(col("prev_tracks").isNull() | (col("prev_tracks") == 0), lit(None).cast("double"))
                .otherwise(spark_round(((col("total_tracks") - col("prev_tracks")) / col("prev_tracks")) * 100.0, 2))
            )
        )

        # 3. Compute Sub-scores for Momentum Index
        # S_recent: linear up to 10 releases -> 100 pts
        s_recent = least(lit(100.0), col("recent_releases_12m") * 10.0)

        # S_growth: neutral 50.0 on first snapshot, scaled by growth %
        s_growth = (
            when(col("catalog_growth_pct").isNull(), lit(50.0))
            .otherwise(greatest(lit(0.0), least(lit(100.0), lit(50.0) + (col("catalog_growth_pct") * 5.0))))
        )

        # S_cadence: 100 pts for 14d pace (Ed Sheeran), scaled down to 10 pts floor for >= 180d
        cadence_col = col("median_release_cadence_days")
        s_cadence = (
            when(cadence_col.isNull(), lit(50.0))
            .otherwise(
                greatest(
                    lit(10.0),
                    least(lit(100.0), lit(100.0) - ((cadence_col - 14.0) / (180.0 - 14.0)) * 90.0)
                )
            )
        )

        # 4. Composite Momentum Index
        raw_momentum = (0.40 * s_recent) + (0.35 * s_growth) + (0.25 * s_cadence)
        momentum_index = spark_round(greatest(lit(0.0), least(lit(100.0), raw_momentum)), 2)

        fact_df = (
            with_growth_df
            .withColumn("catalog_momentum_index", momentum_index)
            .withColumn("gold_transformed_at", current_timestamp())
            .select(
                "artist_key",
                "date_key",
                "artist_id",
                "snapshot_date",
                "total_albums",
                "total_tracks",
                "total_singles",
                "recent_releases_12m",
                "median_release_cadence_days",
                "catalog_growth_pct",
                "catalog_momentum_index",
                "gold_transformed_at",
            )
        )

        # 5. Filter for target snapshot if specified
        if snapshot_date:
            fact_df = fact_df.filter(col("snapshot_date") == lit(snapshot_date))

        # 6. Write partitioned Snappy Parquet
        out_path = os.path.join(self.gold_base_dir, "fact_artist_snapshot")
        (
            fact_df
            .write
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(out_path)
        )

        print(f"  ✓ Saved 'fact_artist_snapshot' ({fact_df.count():,} rows) -> {out_path}")
        return fact_df
    

if __name__ == "__main__":
    transformer = GoldTransformer()
    transformer.build_dim_date()
    transformer.build_dim_artist()
    transformer.build_dim_album()
    transformer.build_dim_track()

    fact_df = transformer.build_fact_artist_snapshot()
    fact_df.select(
        "snapshot_date", "artist_id", "total_tracks", "catalog_growth_pct", "catalog_momentum_index"
    ).show(10, truncate=False)

    transformer.spark.stop()
