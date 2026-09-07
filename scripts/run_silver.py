"""
scripts/run_silver.py
---------------------
Master orchestration script for the Silver Layer and Automated Data Quality Gate.
Performs:
    1. Bronze ingestion with partition pruning.
    2. Silver cleaning, standardization, and detrministic window deduplication
    3. Pre-write Data Quality Gate (Completeness, PK Uniqueness, Nulls, FK Integrity, Value Ranges).
    4. Quarantine routing for orphan records (if <= 5% warning threshold).
    5. Atomic Silver Parquet write with Dynamic Partition Overwrite.
    6. Persistent JSON audit report emission.
"""
import os
import sys
# Add project root to sys.path so 'src' can be imported when run as a standalone script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import time
from typing import Dict, Any, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.transform.spark_session import get_spark_session
from src.transform.silver_transformer import SilverTransformer
from src.quality.data_quality import DataQualityChecker

def run_silver_pipeline(
    snapshot_date: str,
    bronze_dir: str = "data/bronze",
    silver_dir: str = "data/silver",
    quarantine_dir: str = "data/quarantine",
    report_dir: str = "data/quality/reports",
    spark: SparkSession = None,
) -> Dict[str, Any]:
    """
    Executes the end-to-end Silver pipeline with DQ gate enforcement for a snapshot.
    """
    start_time = time.time()
    own_spark = False 

    if spark is None:
        spark = get_spark_session(app_name=f"Spotify_Silver_Pipeline_{snapshot_date}")
        own_spark = True 
    
    transformer = SilverTransformer(
        spark = spark,
        bronze_base_dir = bronze_dir,
        silver_base_dir = silver_dir,
    )

    checker = DataQualityChecker(
        snapshot_date = snapshot_date,
        quarantine_base_dir = quarantine_dir,
        report_base_dir = report_dir
    )

    print("=" * 80)
    print(f"🚀 EXECUTING SILVER PIPELINE & DQ GATE — SNAPSHOT: {snapshot_date}")
    print("=" * 80)

    try:
        # -------------------------------------------------------------------
        # 1. READ BRONZE DATASETS
        # -------------------------------------------------------------------
        print("\n📥 [1/5] Ingesting Bronze Parquet partitions...")
        bronze_artists = transformer.read_bronze_parquet("artists", snapshot_date)
        bronze_albums = transformer.read_bronze_parquet("albums", snapshot_date)
        bronze_tracks = transformer.read_bronze_parquet("tracks", snapshot_date)
        b_art_count = bronze_artists.count()
        b_alb_count = bronze_albums.count()
        b_trk_count = bronze_tracks.count()
        print(f"   • Bronze Artists : {b_art_count:,} rows")
        print(f"   • Bronze Albums  : {b_alb_count:,} rows")
        print(f"   • Bronze Tracks  : {b_trk_count:,} rows")

        # -------------------------------------------------------------------
        # 2. TRANSFORM TO CANDIDATE SILVER
        # -------------------------------------------------------------------
        print("\n⚙️  [2/5] Applying Silver transformations & Window deduplication...")
        candidate_artists = transformer.transform_artists(bronze_artists).cache()
        candidate_albums = transformer.transform_albums(bronze_albums).cache()
        candidate_tracks = transformer.transform_tracks(bronze_tracks).cache()
        print(f"   • Candidate Artists : {candidate_artists.count():,} rows")
        print(f"   • Candidate Albums  : {candidate_albums.count():,} rows")
        print(f"   • Candidate Tracks  : {candidate_tracks.count():,} rows")

        # -------------------------------------------------------------------
        # 3. EVALUATE AUTOMATED DATA QUALITY GATE
        # -------------------------------------------------------------------
        print("\n🛡️  [3/5] Evaluating Data Quality Rules...")
        # --- ARTISTS CHECKS ---
        checker.check_completeness(candidate_artists, entity="artists", min_expected=1)
        checker.check_uniqueness(candidate_artists, entity="artists", primary_keys=["artist_id"])
        checker.check_critical_nulls(candidate_artists, entity="artists", critical_columns=["artist_id", "artist_name"])

         # --- ALBUMS CHECKS ---
        checker.check_completeness(candidate_albums, entity="albums", min_expected=1)
        checker.check_uniqueness(candidate_albums, entity="albums", primary_keys=["album_id"])
        checker.check_critical_nulls(candidate_albums, entity="albums", critical_columns=["album_id", "album_name", "artist_id"])
        fk_alb_res = checker.check_referential_integrity(
            child_df=candidate_albums,
            parent_df=candidate_artists,
            foreign_key="artist_id",
            child_entity="albums",
            parent_entity="artists",
            max_orphan_threshold_pct=5.0,
        )

        # --- TRACKS CHECKS ---
        checker.check_completeness(candidate_tracks, entity="tracks", min_expected=1)
        checker.check_uniqueness(candidate_tracks, entity="tracks", primary_keys=["track_id"])
        checker.check_critical_nulls(candidate_tracks, entity="tracks", critical_columns=["track_id", "track_name", "album_id", "artist_id"])
        fk_trk_alb_res = checker.check_referential_integrity(
            child_df=candidate_tracks,
            parent_df=candidate_albums,
            foreign_key="album_id",
            child_entity="tracks",
            parent_entity="albums",
            max_orphan_threshold_pct=5.0,
        )
        fk_trk_art_res = checker.check_referential_integrity(
            child_df=candidate_tracks,
            parent_df=candidate_artists,
            foreign_key="artist_id",
            child_entity="tracks",
            parent_entity="artists",
            max_orphan_threshold_pct=5.0,
        )
        checker.check_value_ranges(
            candidate_tracks,
            entity="tracks",
            range_conditions={
                "duration_ms": (1, 7200000),  # >0 and <2 hours
                "track_number": (1, None),
                "disc_number": (1, None),
            },
        )

        # -------------------------------------------------------------------
        # 4. REPORT & QUARANTINE ROUTING
        # -------------------------------------------------------------------
        report = checker.generate_report()
        report_path = checker.save_report(report)
        overall_status = report["overall_status"]

        print(f"\n📊 DQ Gate Result: {overall_status}")
        print(f"   • Total Rules Evaluated : {report['summary']['total_checks']}")
        print(f"   • Passed : {report['summary']['passed']}")
        print(f"   • Warned : {report['summary']['warnings']}")
        print(f"   • Failed : {report['summary']['failed']}")
        print(f"   • Report : {report_path}")

        if overall_status == "FAIL":
            print("\n❌ PIPELINE HALTED: One or more critical DQ checks failed!")
            for r in report["results"]:
                if r.get("status") == "FAIL":
                    print(f"   - [{r['entity'].upper()}] {r['rule']}: {r['message']}")
            raise RuntimeError(f"Silver Pipeline aborted for snapshot {snapshot_date} due to DQ failure.")

        # Quarantine handling for warnings (if orphan records exist)
        clean_albums = candidate_albums
        if fk_alb_res["orphan_count"] > 0:
            orphan_albums = candidate_albums.join(
                candidate_artists,
                candidate_albums["artist_id"] == candidate_artists["artist_id"],
                how="left_anti",
            )
            q_path = checker.quarantine_records(
                orphan_albums,
                entity="albums",
                rule_name="referential_integrity",
                reason="orphan_artist_id",
            )
            print(f"   ⚠️  Quarantined {fk_alb_res['orphan_count']} orphan albums -> {q_path}")
            clean_albums = candidate_albums.join(
                candidate_artists.select("artist_id"),
                on="artist_id",
                how="left_semi",
            )
        clean_tracks = candidate_tracks
        if fk_trk_alb_res["orphan_count"] > 0:
            orphan_tracks_alb = candidate_tracks.join(
                candidate_albums,
                candidate_tracks["album_id"] == candidate_albums["album_id"],
                how="left_anti",
            )
            q_path = checker.quarantine_records(
                orphan_tracks_alb,
                entity="tracks",
                rule_name="referential_integrity",
                reason="orphan_album_id",
            )
            print(f"   ⚠️  Quarantined {fk_trk_alb_res['orphan_count']} orphan tracks (missing album) -> {q_path}")
            clean_tracks = clean_tracks.join(
                candidate_albums.select("album_id"),
                on="album_id",
                how="left_semi",
            )
        # -------------------------------------------------------------------
        # 5. PERSIST TRUSTED SILVER PARQUET
        # -------------------------------------------------------------------
        print("\n💾 [4/5] Writing Conformed Silver Parquet with Dynamic Partition Overwrite...")
        art_path = transformer.write_silver_parquet(candidate_artists, "artists")
        alb_path = transformer.write_silver_parquet(clean_albums, "albums")
        trk_path = transformer.write_silver_parquet(clean_tracks, "tracks")

        
        final_art_count = candidate_artists.count()
        final_alb_count = clean_albums.count()
        final_trk_count = clean_tracks.count()

        print(f"   ✓ Silver Artists : {final_art_count:,} rows -> {art_path}/snapshot_date={snapshot_date}/")
        print(f"   ✓ Silver Albums  : {final_alb_count:,} rows -> {alb_path}/snapshot_date={snapshot_date}/")
        print(f"   ✓ Silver Tracks  : {final_trk_count:,} rows -> {trk_path}/snapshot_date={snapshot_date}/")

        elapsed = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"🏁 [5/5] Pipeline Succeeded in {elapsed:.2f}s!")
        print("=" * 80)

        return {
            "snapshot_date": snapshot_date,
            "overall_status": overall_status,
            "counts": {
                "artists": final_art_count,
                "albums": final_alb_count,
                "tracks": final_trk_count,
            },
            "dq_report": report_path,
        }
    finally:
        # Unpersist cached DataFrames, release the cached data
        candidate_artists.unpersist()
        candidate_albums.unpersist()
        candidate_tracks.unpersist()
        if own_spark:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Run Spotify Silver Pipeline with DQ Gate.")
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default="2026-08-31",
        help="Target snapshot date (YYYY-MM-DD). Default: 2026-08-31",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run pipeline across all available snapshot dates (2026-08-31 and 2026-09-01).",
    )
    args = parser.parse_args()
    if args.all:
        snapshots = ["2026-08-31", "2026-09-01"]
        spark = get_spark_session(app_name="Spotify_Silver_Pipeline_Batch")
        try:
            for snap in snapshots:
                run_silver_pipeline(snapshot_date=snap, spark=spark)
        finally:
            spark.stop()
    else:
        run_silver_pipeline(snapshot_date=args.snapshot_date)
if __name__ == "__main__":
    main()