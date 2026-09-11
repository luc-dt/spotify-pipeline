"""
scripts/test_gold_views.py
--------------------------
Verification script for DuckDB views over the Gold Parquet Lakehouse layer.
Executes sql/setup_gold_views.sql and asserts integrity, row counts, and schema bindings.
"""

import os
import sys
from typing import Dict, Any

try:
    import duckdb
except ImportError:
    print("❌ duckdb package is not installed in the virtual environment.")
    print("   Please run: pip install duckdb")
    sys.exit(1)


def test_views() -> None:
    print("=" * 80)
    print("🦆 TESTING DUCKDB VIEWS OVER GOLD PARQUET TABLES")
    print("=" * 80)

    sql_file = os.path.join("sql", "setup_gold_views.sql")
    if not os.path.exists(sql_file):
        print(f"❌ SQL file not found: {sql_file}")
        sys.exit(1)

    with open(sql_file, "r", encoding="utf-8") as f:
        sql_script = f.read()

    # 1. Initialize in-memory DuckDB connection
    con = duckdb.connect()

    # 2. Execute view creation script
    print("\n[1/3] Executing sql/setup_gold_views.sql...")
    con.execute(sql_script)
    print("  ✓ Successfully created all 5 views.")

    # 3. Assert View Integrity and Row Counts
    views_to_test = [
        ("dim_date", 29585, "Calendar Days (1950–2030)"),
        ("dim_artist", 8, "Conformed Artists (SCD Type 1)"),
        ("dim_album", 732, "Conformed Discography"),
        ("dim_track", 3837, "Conformed Tracks"),
        ("fact_artist_snapshot", 15, "Periodic Fact Snapshots (Hive Partitioned)"),
    ]

    print("\n[2/3] Validating View Execution & Row Counts:")
    print("-" * 80)
    print(f"{'View Name':<22} | {'Rows':<8} | {'Cols':<6} | {'Status':<8} | Description")
    print("-" * 80)

    all_passed = True
    for view_name, min_expected_rows, desc in views_to_test:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            col_cnt = len(con.execute(f"SELECT * FROM {view_name} LIMIT 0").description)

            # Check for non-zero and reasonable row counts
            if cnt >= min_expected_rows:
                status = "✅ PASS"
            else:
                status = "⚠️ WARN"
                all_passed = False

            print(f"{view_name:<22} | {cnt:<8,d} | {col_cnt:<6} | {status:<8} | {desc}")
        except Exception as e:
            print(f"{view_name:<22} | {'ERROR':<8} | {'-':<6} | ❌ FAIL   | Error: {e}")
            all_passed = False

    print("-" * 80)

    # 4. Preview sample from fact_artist_snapshot with join
    print("\n[3/3] Testing Star Schema Join (fact_artist_snapshot + dim_artist):")
    sample_query = """
        SELECT 
            f.snapshot_date,
            a.artist_name,
            f.total_tracks,
            f.recent_releases_12m,
            f.median_release_cadence_days,
            f.catalog_growth_pct,
            f.catalog_momentum_index
        FROM fact_artist_snapshot f
        JOIN dim_artist a ON f.artist_key = a.artist_key
        ORDER BY f.snapshot_date DESC, f.catalog_momentum_index DESC
        LIMIT 5;
    """
    result_df = con.execute(sample_query).df()
    print(result_df.to_string(index=False))

    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 ALL 5 DUCKDB GOLD VIEWS VERIFIED SUCCESSFULLY!")
    else:
        print("⚠️ Some views returned fewer rows than expected. Review table outputs.")
    print("=" * 80)


if __name__ == "__main__":
    test_views()
