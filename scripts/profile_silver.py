"""
scripts/profile_silver.py
Silver Data Profiling & Metric Calibration Engine (Day 6 - Phase 2).

Step 1: Core Foundation & Mathematical Helpers
- Imports & PySpark session interface
- Safe date normalization (handling Python date vs datetime vs string)
- Statistical median calculator
"""

import os
import sys

# Add project root to sys.path so 'src' can be resolved
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from src.transform.spark_session import get_spark_session

def to_date_obj(d: Any) -> Optional[date]:
    """Safety coerces string, datetime, or date into a standard Python date."""
    if d is None:
        return None
    
    if isinstance(d, datetime):
        return d.date()

    if isinstance(d, date):
        return d

    if isinstance(d, str):
        try:
            return datetime.strptime(d[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
    return None

def calculate_median(value: List[float]) -> Optional[float]:
    """Calculates exact statistical median (odd/even length handling)."""
    if not value:
        return None

    sorted_vals = sorted(value)
    n = len(sorted_vals)
    mid = n // 2 

    if n % 2 == 1:
        return float(sorted_vals[mid])
    else:
        return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0)

def compute_cadence_for_artist(release_dates: List[Any]) -> Optional[float]:
    """
    Compute the median calendar days between successive release across an artist's discography.
    If fewer than 2 distinct valid release dates exist, returns None.
    """
    date_objs = [to_date_obj(d) for d in release_dates]
    valid_dates = sorted(list(set(d for d in date_objs if d is not None)))

    if len(valid_dates) < 2:
        return None
    intervals = [
        (valid_dates[i + 1] - valid_dates[i]).days
        for i in range(len(valid_dates) - 1)
    ]
    return calculate_median(intervals)

def profile_snapshot(
    spark: SparkSession,
    silver_base_dir: str,
    snapshot_date: str,
    prev_track_counts: Optional[Dict[str, int]] = None
) -> Dict[str, Any]:
    """
    Profiles an individual snapshot partition in the Silver layer
    """
    # 1. Read Silver DataFrames with partition pruning
    artists_df = spark.read.parquet(f"{silver_base_dir}/artists").filter(col("snapshot_date") == lit(snapshot_date))
    albums_df = spark.read.parquet(f"{silver_base_dir}/albums").filter(col("snapshot_date") == lit(snapshot_date))
    tracks_df = spark.read.parquet(f"{silver_base_dir}/tracks").filter(col("snapshot_date") == lit(snapshot_date))

    artists_list = artists_df.select("artist_id", "artist_name").collect()
    albums_list = albums_df.select("album_id", "album_type", "artist_id", "release_date").collect()
    tracks_list = tracks_df.select("track_id", "artist_id").collect()

    # Define snapshot date and rolling 12m lookback cutoff
    snapshot_dt = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
    lookback_cutoff = snapshot_dt - timedelta(days=365)

    artist_profiles = []

    for a in sorted(artists_list, key=lambda x: x["artist_name"]):
        aid = a["artist_id"]
        aname = a["artist_name"]

        # Filter albums and tracks for this specific artist
        artist_albums = [alb for alb in albums_list if alb["artist_id"] == aid]
        artist_tracks = [trk for trk in tracks_list if trk["artist_id"] == aid]

        # 1. Catalog Counts
        total_albums = len([alb for alb in artist_albums if alb["album_type"] == "album"])
        total_singles = len([alb for alb in artist_albums if alb["album_type"] == "single"])
        total_compilations = len([alb for alb in artist_albums if alb["album_type"] == "compilation"])
        total_tracks = len(artist_tracks)

        # 2. Rolling 12m Creative Drops (Excluding Compilations)
        recent_12m_albums = []
        for alb in artist_albums:
            rel_dt = to_date_obj(alb["release_date"])
            if (
                alb["album_type"] in ("album", "single")
                and rel_dt is not None
                and lookback_cutoff <= rel_dt <= snapshot_dt
            ):
                recent_12m_albums.append(alb)
        recent_releases_12m = len(recent_12m_albums)

        # 3. Inter-Release Cadence (Lifetime up to snapshot_dt)
        all_release_dates = [to_date_obj(alb["release_date"]) for alb in artist_albums if alb["release_date"] is not None]
        historical_release_dates = [d for d in all_release_dates if d is not None and d <= snapshot_dt]
        median_cadence_days = compute_cadence_for_artist(historical_release_dates)

        # 4. Track Growth % (Strictly NULL on first snapshot)
        if prev_track_counts and aid in prev_track_counts:
            prev_cnt = prev_track_counts[aid]
            growth_pct = round(((total_tracks - prev_cnt) / prev_cnt) * 100.0, 2) if prev_cnt > 0 else None
        else:
            growth_pct = None

                # Calibrated Scoring Functions:
        # S_recent: 10 releases = 100.0
        s_recent = min(100.0, recent_releases_12m * 10.0)

        # S_growth: baseline 50.0 if NULL (initial snapshot)
        if growth_pct is None:
            s_growth = 50.0
        else:
            s_growth = max(0.0, min(100.0, 50.0 + (growth_pct * 5.0)))

        # S_cadence: Recalibrated (14 days = 100.0, 180 days = 10.0)
        if median_cadence_days is None:
            s_cadence = 50.0
        else:
            raw_s_cadence = 100.0 - ((median_cadence_days - 14.0) / (180.0 - 14.0)) * 90.0
            s_cadence = max(10.0, min(100.0, raw_s_cadence))


        # Composite Momentum Index [0.0, 100.0]
        momentum_index = round(0.40 * s_recent + 0.35 * s_growth + 0.25 * s_cadence, 2)

        artist_profiles.append({
            "artist_id": aid,
            "artist_name": aname,
            "total_albums": total_albums,
            "total_singles": total_singles,
            "total_compilations": total_compilations,
            "total_tracks": total_tracks,
            "recent_releases_12m": recent_releases_12m,
            "median_cadence_days": round(median_cadence_days, 1) if median_cadence_days is not None else None,
            "growth_pct": growth_pct,
            "s_recent": round(s_recent, 1),
            "s_growth": round(s_growth, 1),
            "s_cadence": round(s_cadence, 1),
            "momentum_index": momentum_index,
        })

    # Return snapshot summary dictionary
    return {
        "snapshot_date": snapshot_date,
        "artist_profiles": artist_profiles,
        "total_artists": len(artists_list),
        "total_albums_all": len(albums_list),
        "total_tracks_all": len(tracks_list),
    }

def print_profiling_results(profile_results: List[Dict[str, Any]]):
    """Prints formatted executive profiling tables to the terminal."""
    print("\n" + "=" * 118)
    print("📊 SILVER DATA PROFILING & KIMBALL METRIC CALIBRATION REPORT (DAY 6 - PHASE 2)")
    print("=" * 118)

    for res in profile_results:
        snap = res["snapshot_date"]
        print(f"\n📅 SNAPSHOT: {snap} | Artists: {res['total_artists']} | Albums: {res['total_albums_all']} | Tracks: {res['total_tracks_all']}")
        print("-" * 118)
        print(f"{'Artist Name':<18} | {'Albums':<6} | {'Singles':<7} | {'Tracks':<6} | {'Recent 12m':<10} | {'Cadence(d)':<10} | {'Growth %':<8} | {'S_rec':<5} | {'S_gro':<5} | {'S_cad':<5} | {'Momentum':<8}")
        print("-" * 118)

        for p in res["artist_profiles"]:
            cad_str = str(p["median_cadence_days"]) if p["median_cadence_days"] is not None else "NULL"
            gro_str = f"{p['growth_pct']:+.1f}%" if p["growth_pct"] is not None else "NULL"
            print(
                f"{p['artist_name']:<18} | "
                f"{p['total_albums']:<6} | "
                f"{p['total_singles']:<7} | "
                f"{p['total_tracks']:<6} | "
                f"{p['recent_releases_12m']:<10} | "
                f"{cad_str:<10} | "
                f"{gro_str:<8} | "
                f"{p['s_recent']:<5} | "
                f"{p['s_growth']:<5} | "
                f"{p['s_cadence']:<5} | "
                f"{p['momentum_index']:<8}"
            )
        print("-" * 118)


def summarize_distributions(profile_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Computes cohort summary stats (min, max, median, mean) across all profiles."""
    summary = {}
    for res in profile_results:
        snap = res["snapshot_date"]
        profiles = res["artist_profiles"]

        recents = [p["recent_releases_12m"] for p in profiles]
        cadences = [p["median_cadence_days"] for p in profiles if p["median_cadence_days"] is not None]
        growths = [p["growth_pct"] for p in profiles if p["growth_pct"] is not None]
        momentums = [p["momentum_index"] for p in profiles]

        summary[snap] = {
            "recent_releases_12m": {
                "min": min(recents) if recents else 0,
                "max": max(recents) if recents else 0,
                "median": calculate_median(recents),
                "mean": round(sum(recents) / len(recents), 2) if recents else 0,
            },
            "median_cadence_days": {
                "min": min(cadences) if cadences else None,
                "max": max(cadences) if cadences else None,
                "median": calculate_median(cadences),
                "mean": round(sum(cadences) / len(cadences), 2) if cadences else None,
            },
            "growth_pct": {
                "min": min(growths) if growths else None,
                "max": max(growths) if growths else None,
                "median": calculate_median(growths),
                "mean": round(sum(growths) / len(growths), 2) if growths else None,
            },
            "momentum_index": {
                "min": min(momentums) if momentums else 0,
                "max": max(momentums) if momentums else 0,
                "median": calculate_median(momentums),
                "mean": round(sum(momentums) / len(momentums), 2) if momentums else 0,
            }
        }
    return summary


def main():
    silver_dir = "data/silver"
    snapshots = ["2026-08-31", "2026-09-01"]

    print("🚀 Initializing PySpark Session for Silver Profiling...")
    spark = get_spark_session(app_name="Spotify_Silver_Profiling")

    results = []
    prev_track_counts = None

    for snap in snapshots:
        print(f"🔍 Profiling snapshot partition: {snap}...")
        snap_profile = profile_snapshot(spark, silver_dir, snap, prev_track_counts)
        results.append(snap_profile)

        # Cache current track counts to compute growth on next snapshot
        prev_track_counts = {
            p["artist_id"]: p["total_tracks"]
            for p in snap_profile["artist_profiles"]
        }

    # Print executive tables and summary
    print_profiling_results(results)

    summary = summarize_distributions(results)
    print("\n📈 EMPIRICAL DISTRIBUTION SUMMARY ACROSS COHORT:")
    print(json.dumps(summary, indent=2))

    # Persist JSON report for pipeline auditability
    report_dir = "data/quality/reports"
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, "silver_profiling_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "snapshots": results}, f, indent=2, default=str)
    print(f"\n💾 Profiling report successfully persisted to: {report_path}")

    spark.stop()
    print("✅ Profiling completed successfully.")


if __name__ == "__main__":
    main()
