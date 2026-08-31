import os
import sys
from src.transform.spark_session import get_spark_session


def get_dir_size_bytes(directory: str) -> int:
    """Calculates total file size in bytes for a directory."""
    total = 0
    for root, _, files in os.walk(directory):
        for f in files:
            if not f.startswith("."):
                total += os.path.getsize(os.path.join(root, f))
    return total

def verify_bronze_layer(snapshot_date: str = "2026-08-31"):
    spark = get_spark_session(app_name="Spotify_Bronze_Verification")
    entities = {
        "artists": 8,
        "albums": 741,
        "tracks": 2500,
    }

    print("=" * 75)
    print("🔍 STEP 5 & 7: BRONZE PARQUET VALIDATION & COMPRESSION BENCHMARK")
    print(f"📅 Target Snapshot: {snapshot_date}")
    print("=" * 75)

    total_raw_bytes = 0
    total_bronze_bytes = 0

    print(f"\n{'Entity':<10} | {'Expected':<8} | {'Parquet Rows':<12} | {'Raw JSON':<10} | {'Bronze Parquet':<14} | {'Savings %':<10}")
    print("-" * 75)

    for entity, expected_count in entities.items():
        # 1. Measure Raw JSON Size
        raw_file = f"data/raw/{entity}/{entity}_{snapshot_date}.json"
        raw_size = os.path.getsize(raw_file) if os.path.exists(raw_file) else 0
        total_raw_bytes += raw_size

        # 2. Read Parquet and Verify Rows
        bronze_path = f"data/bronze/{entity}/snapshot_date={snapshot_date}"
        bronze_df = spark.read.parquet(bronze_path)
        actual_count = bronze_df.count()

        # 3. Measure Bronze Parquet Size
        bronze_size = get_dir_size_bytes(bronze_path)
        total_bronze_bytes += bronze_size
        savings = ((raw_size - bronze_size) / raw_size * 100) if raw_size > 0 else 0
        
        assert actual_count == expected_count, f"Mismatch in {entity}: expected {expected_count}, got {actual_count}"
        
        print(f"{entity.capitalize():<10} | {expected_count:<8} | {actual_count:<12} | {raw_size/1024:>7.1f} KB | {bronze_size/1024:>11.1f} KB | {savings:>8.1f}%")
    
    total_savings = ((total_raw_bytes - total_bronze_bytes) / total_raw_bytes * 100)
    print("-" * 75)
    print(f"{'TOTAL':<10} | {'3,249':<8} | {'3,249':<12} | {total_raw_bytes/1024:>7.1f} KB | {total_bronze_bytes/1024:>11.1f} KB | {total_savings:>8.1f}%")
    print("=" * 75)
    print("✅ All record counts verified!")
    print("✅ Parquet read-back verified!")
    print(f"🎉 Raw JSON → Bronze Parquet reduced storage by {total_savings:.1f}% in this snapshot. ")
    
    spark.stop()
if __name__ == "__main__":
    verify_bronze_layer()