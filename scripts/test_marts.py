"""
scripts/test_marts.py
---------------------
Verification script for the 4 Curated Data Marts in DuckDB.
Executes sql/setup_gold_views.sql and sql/setup_marts.sql, then asserts
integrity, row counts, schema bindings, and previews sample mart records.
"""

import os
import sys

try:
    import duckdb
except ImportError:
    print("❌ duckdb package is not installed in the virtual environment.")
    print("   Please run: pip install duckdb")
    sys.exit(1)


def test_marts() -> None:
    print("=" * 95)
    print("🏪 TESTING CURATED DATA MARTS OVER GOLD PARQUET TABLES")
    print("=" * 95)

    gold_sql_file = os.path.join("sql", "setup_gold_views.sql")
    marts_sql_file = os.path.join("sql", "setup_marts.sql")

    for fpath in [gold_sql_file, marts_sql_file]:
        if not os.path.exists(fpath):
            print(f"❌ SQL file not found: {fpath}")
            sys.exit(1)

    with open(gold_sql_file, "r", encoding="utf-8") as f:
        gold_sql = f.read()

    with open(marts_sql_file, "r", encoding="utf-8") as f:
        marts_sql = f.read()

    # 1. Initialize in-memory DuckDB connection
    con = duckdb.connect()

    # 2. Execute Gold Views and Marts scripts
    print("\n[1/3] Setting Up Gold Semantic Views and Analytical Marts...")
    con.execute(gold_sql)
    print("  ✓ Successfully created 5 base Gold views.")
    con.execute(marts_sql)
    print("  ✓ Successfully created 4 Curated Data Mart views.")

    # 3. Assert Mart Integrity and Row Counts
    marts_to_test = [
        ("mart_artist_activity", 7, 13, "Artist 12m Activity, Production Mix & Strategy"),
        ("mart_catalog_growth", 15, 9, "Historical Track Expansion & Lifecycle Classification"),
        ("mart_release_seasonality", 12, 10, "Historical Monthly Seasonality & LP/Single Shares"),
        ("mart_artist_momentum", 7, 13, "Momentum Scores, Tiers, 3 Pillars & Trajectory"),
    ]

    print("\n[2/3] Validating Mart Execution, Row Counts & Column Schemas:")
    print("-" * 95)
    print(f"{'Mart Name':<28} | {'Rows':<6} | {'Cols':<6} | {'Status':<8} | Description")
    print("-" * 95)

    all_passed = True
    for mart_name, min_expected_rows, expected_cols, desc in marts_to_test:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {mart_name}").fetchone()[0]
            col_cnt = len(con.execute(f"SELECT * FROM {mart_name} LIMIT 0").description)

            if cnt >= min_expected_rows and col_cnt == expected_cols:
                status = "✅ PASS"
            else:
                status = "⚠️ WARN"
                all_passed = False

            print(f"{mart_name:<28} | {cnt:<6,d} | {col_cnt:<6} | {status:<8} | {desc}")
        except Exception as e:
            print(f"{mart_name:<28} | {'ERR':<6} | {'-':<6} | ❌ FAIL   | Error: {e}")
            all_passed = False

    print("-" * 95)

    # 4. Preview Data from each Mart
    print("\n[3/3] Previewing Curated Mart Outputs:")

    print("\n--- 1. mart_artist_activity (Top 5) ---")
    df_act = con.execute("""
        SELECT artist_name, recent_releases_12m, single_to_album_ratio, catalog_strategy, activity_tier, activity_rank
        FROM mart_artist_activity
        LIMIT 5;
    """).df()
    print(df_act.to_string(index=False))

    print("\n--- 2. mart_catalog_growth (Snapshot 2026-09-01) ---")
    df_growth = con.execute("""
        SELECT artist_name, snapshot_date, prev_tracks, current_tracks, net_tracks_added, catalog_growth_pct, growth_classification
        FROM mart_catalog_growth
        WHERE snapshot_date = '2026-09-01'
        LIMIT 5;
    """).df()
    print(df_growth.to_string(index=False))

    print("\n--- 3. mart_release_seasonality (Top 5 Release Months) ---")
    df_season = con.execute("""
        SELECT release_month, month_name, total_releases, studio_albums, singles, single_share_pct, industry_season_archetype
        FROM mart_release_seasonality
        ORDER BY total_releases DESC
        LIMIT 5;
    """).df()
    print(df_season.to_string(index=False))

    print("\n--- 4. mart_artist_momentum (All 7 Artists) ---")
    df_mom = con.execute("""
        SELECT artist_name, catalog_momentum_index, momentum_tier, recent_releases_12m, median_release_cadence_days, momentum_delta, trajectory_direction
        FROM mart_artist_momentum;
    """).df()
    print(df_mom.to_string(index=False))

    print("\n" + "=" * 95)
    if all_passed:
        print("🎉 ALL 4 CURATED DATA MARTS VERIFIED SUCCESSFULLY!")
        print("   Ready to power downstream Day 9 Streamlit UI and executive reporting.")
    else:
        print("⚠️ Some marts failed validation. Review table definitions.")
    print("=" * 95)


if __name__ == "__main__":
    test_marts()
