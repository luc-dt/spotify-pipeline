"""
scripts/run_gold.py
-------------------
Master orchestration script for the Gold Layer (Kimball Dimensional Star Schema).

Performs:
    1. Ingestion of conformed Silver Parquet tables (artists, albums, tracks).
    2. Construction of Conformed Dimensions:
       - dim_date (calendar dimension: 1950 - 2030, smart date_key = YYYYMMDD)
       - dim_artist (conformed SCD Type 1, surrogate key artist_key)
       - dim_album (conformed discography, surrogate key album_key, FK artist_key)
       - dim_track (conformed tracks, surrogate key track_key, FKs album_key, artist_key)
    3. Construction of Periodic Snapshot Fact Table:
       - fact_artist_snapshot (grain: artist_key, date_key)
       - Semi-additive metrics (total_albums, total_tracks, total_singles, recent_releases_12m)
       - Non-additive metrics (median_release_cadence_days, catalog_growth_pct, catalog_momentum_index)
    4. Optional synchronization of Gold Parquet tables to Amazon S3.
    5. Formatted audit reporting and validation summary.
"""

import os
import sys
# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import time
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.transform.spark_session import get_spark_session
from src.transform.gold_transformer import GoldTransformer


def sync_gold_to_s3(
    local_gold_dir: str = "data/gold",
    bucket_name: Optional[str] = None,
    s3_prefix: str = "gold",
) -> None:
    """
    Synchronizes local Gold Parquet files to Amazon S3 using boto3.
    """
    import boto3
    from botocore.exceptions import ClientError
    from dotenv import load_dotenv

    load_dotenv()

    bucket = bucket_name or os.getenv("S3_BUCKET", "spotify-music-intelligence-luc")
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-2"
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        print("⚠️ AWS credentials not found in environment. Skipping S3 sync.")
        return

    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region,
    )
    s3 = session.client("s3")

    print("\n" + "=" * 80)
    print("☁️  SYNCHRONIZING GOLD PARQUET TABLES TO AMAZON S3")
    print(f"   Bucket : s3://{bucket}/{s3_prefix}/")
    print(f"   Source : {local_gold_dir}")
    print("=" * 80)

    uploaded_files = 0
    total_bytes = 0

    for root, _, files in os.walk(local_gold_dir):
        for file in files:
            # Skip hidden files or temporary markers
            if file.startswith(".") or file.endswith(".crc"):
                continue

            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, local_gold_dir).replace("\\", "/")
            s3_key = f"{s3_prefix}/{rel_path}"

            file_size = os.path.getsize(local_path)
            s3.upload_file(
                Filename=local_path,
                Bucket=bucket,
                Key=s3_key,
                ExtraArgs={
                    "ContentType": "application/octet-stream",
                    "ServerSideEncryption": "AES256",
                },
            )
            uploaded_files += 1
            total_bytes += file_size

    print(f"   ✓ Successfully synced {uploaded_files} objects ({total_bytes / 1024:.1f} KB) to s3://{bucket}/{s3_prefix}/")


def run_gold_pipeline(
    snapshot_date: Optional[str] = None,
    silver_dir: str = "data/silver",
    gold_dir: str = "data/gold",
    sync_s3: bool = False,
    spark: Optional[SparkSession] = None,
) -> Dict[str, Any]:
    """
    Executes the end-to-end Gold Kimball star schema pipeline.
    """
    start_time = time.time()
    own_spark = False

    if spark is None:
        spark = get_spark_session(app_name="Spotify_Gold_Pipeline")
        own_spark = True

    transformer = GoldTransformer(
        spark=spark,
        silver_base_dir=silver_dir,
        gold_base_dir=gold_dir,
    )

    print("=" * 80)
    print("🌟 EXECUTING GOLD PIPELINE: KIMBALL DIMENSIONAL STAR SCHEMA")
    print(f"   Silver Source : {silver_dir}")
    print(f"   Gold Target   : {gold_dir}")
    if snapshot_date:
        print(f"   Target Date   : {snapshot_date}")
    else:
        print("   Target Date   : ALL SNAPSHOTS")
    print("=" * 80)

    try:
        # 1. Build Dimensions
        print("\n📅 [1/5] Building Conformed 'dim_date' (1950-2030)...")
        dim_date = transformer.build_dim_date()
        date_count = dim_date.count()

        print("\n👤 [2/5] Building Conformed 'dim_artist' (SCD Type 1)...")
        dim_artist = transformer.build_dim_artist()
        artist_count = dim_artist.count()

        print("\n💿 [3/5] Building Conformed 'dim_album'...")
        dim_album = transformer.build_dim_album()
        album_count = dim_album.count()

        print("\n🎵 [4/5] Building Conformed 'dim_track'...")
        dim_track = transformer.build_dim_track()
        track_count = dim_track.count()

        # 2. Build Fact Table
        print("\n📊 [5/5] Building Periodic Snapshot 'fact_artist_snapshot'...")
        fact_df = transformer.build_fact_artist_snapshot(snapshot_date=snapshot_date)
        fact_count = fact_df.count()

        # 3. Formatted Summary
        elapsed = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"🏆 GOLD PIPELINE COMPLETED SUCCESSFULLY IN {elapsed:.2f}s")
        print("=" * 80)

        print("\n📋 Table Inventory & Row Counts:")
        print(f"   • dim_date             : {date_count:,} rows")
        print(f"   • dim_artist           : {artist_count:,} rows")
        print(f"   • dim_album            : {album_count:,} rows")
        print(f"   • dim_track            : {track_count:,} rows")
        print(f"   • fact_artist_snapshot : {fact_count:,} rows")

        print("\n🎯 Sample Fact Records (Momentum & Growth Index):")
        fact_df.select(
            "snapshot_date",
            "artist_id",
            "total_tracks",
            "recent_releases_12m",
            "median_release_cadence_days",
            "catalog_growth_pct",
            "catalog_momentum_index",
        ).show(10, truncate=False)

        # 4. S3 Synchronization
        if sync_s3:
            sync_gold_to_s3(local_gold_dir=gold_dir)

        return {
            "status": "SUCCESS",
            "counts": {
                "dim_date": date_count,
                "dim_artist": artist_count,
                "dim_album": album_count,
                "dim_track": track_count,
                "fact_artist_snapshot": fact_count,
            },
            "elapsed_seconds": elapsed,
        }

    finally:
        if own_spark:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Master runner for Spotify Gold Kimball Star Schema."
    )
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Optional target snapshot date (YYYY-MM-DD). If omitted, processes all available snapshots.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all available snapshots (equivalent to omitting --snapshot-date).",
    )
    parser.add_argument(
        "--silver-dir",
        type=str,
        default="data/silver",
        help="Path to Silver base directory. Default: data/silver",
    )
    parser.add_argument(
        "--gold-dir",
        type=str,
        default="data/gold",
        help="Path to Gold base directory. Default: data/gold",
    )
    parser.add_argument(
        "--sync-s3",
        action="store_true",
        help="Synchronize output Gold Parquet tables to Amazon S3.",
    )

    args = parser.parse_args()

    target_date = None if args.all else args.snapshot_date

    run_gold_pipeline(
        snapshot_date=target_date,
        silver_dir=args.silver_dir,
        gold_dir=args.gold_dir,
        sync_s3=args.sync_s3,
    )


if __name__ == "__main__":
    main()
