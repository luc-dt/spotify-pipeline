"""
scripts/audit_medallion_layers.py
---------------------------------
Comprehensive audit script checking all Medallion data layers:
1. Local partitions in Raw, Bronze, Silver, Quality, and Gold.
2. DuckDB validation of the Kimball Gold warehouse.
3. Validation of all 4 Curated Marts for Day 9.
4. AWS S3 Lakehouse inventory check (if credentials configured).
"""

import os
import sys
from pathlib import Path

# Ensure UTF-8 output encoding on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def audit_local_storage():
    print("=" * 80)
    print("📂 [1/3] LOCAL MEDALLION STORAGE INVENTORY AUDIT")
    print("=" * 80)

    layers = [
        ("Raw Artists", "data/raw/artists"),
        ("Raw Albums", "data/raw/albums"),
        ("Raw Tracks", "data/raw/tracks"),
        ("Bronze Artists", "data/bronze/artists"),
        ("Bronze Albums", "data/bronze/albums"),
        ("Bronze Tracks", "data/bronze/tracks"),
        ("Silver Artists", "data/silver/artists"),
        ("Silver Albums", "data/silver/albums"),
        ("Silver Tracks", "data/silver/tracks"),
        ("Quality Reports", "data/quality/reports"),
        ("Gold Fact Table", "data/gold/fact_artist_snapshot"),
    ]

    for label, rel_path in layers:
        full_path = PROJECT_ROOT / rel_path
        if full_path.exists():
            items = sorted([
                i for i in os.listdir(full_path)
                if not i.startswith(".") and i != "_SUCCESS" and not i.endswith(".crc")
            ])
            print(f"  • {label:20} : {items}")
        else:
            print(f"  • {label:20} : [NOT FOUND]")


def audit_gold_duckdb():
    print("\n" + "=" * 80)
    print("🦆 [2/3] DUCKDB GOLD WAREHOUSE & ANALYTICAL MARTS VALIDATION")
    print("=" * 80)

    try:
        import duckdb
        con = duckdb.connect()

        # 1. Fact Table Snapshot Inventory
        fact_path = PROJECT_ROOT / "data" / "gold" / "fact_artist_snapshot" / "**" / "*.parquet"
        df = con.sql(f"""
            SELECT 
                snapshot_date, 
                COUNT(*) AS total_artists,
                SUM(total_albums) AS total_albums,
                SUM(total_tracks) AS total_tracks,
                ROUND(AVG(catalog_momentum_index), 2) AS avg_momentum
            FROM read_parquet('{fact_path.as_posix()}')
            GROUP BY snapshot_date
            ORDER BY snapshot_date
        """).df()

        print("📊 'fact_artist_snapshot' Row Breakdown:")
        print(df.to_string(index=False))

        # 2. Conformed Dimensions Inventory
        dims = ["dim_date", "dim_artist", "dim_album", "dim_track"]
        print("\n🏛️ Conformed Dimensions Inventory:")
        for dim in dims:
            p = PROJECT_ROOT / "data" / "gold" / dim / "*.parquet"
            try:
                cnt = con.sql(f"SELECT COUNT(*) FROM read_parquet('{p.as_posix()}')").fetchone()[0]
                print(f"  ✓ {dim:15} : {cnt:,} rows")
            except Exception as e:
                print(f"  ✗ {dim:15} : Error ({e})")

        # 3. Curated Marts Check
        views_sql = PROJECT_ROOT / "sql" / "setup_gold_views.sql"
        marts_sql = PROJECT_ROOT / "sql" / "setup_marts.sql"
        if views_sql.exists() and marts_sql.exists():
            with open(views_sql, "r", encoding="utf-8") as f:
                con.execute(f.read())
            with open(marts_sql, "r", encoding="utf-8") as f:
                con.execute(f.read())

            marts = [
                "mart_artist_activity",
                "mart_catalog_growth",
                "mart_release_seasonality",
                "mart_artist_momentum",
            ]
            print("\n🏪 Curated Analytical Marts (Day 9 Readiness):")
            for mart in marts:
                cnt = con.execute(f"SELECT COUNT(*) FROM {mart}").fetchone()[0]
                print(f"  ✓ {mart:26} : {cnt:,} active records")
        else:
            print("⚠️ SQL DDL scripts not found in sql/ directory.")

    except Exception as e:
        print(f"❌ Error during DuckDB validation: {e}")


def audit_aws_s3():
    print("\n" + "=" * 80)
    print("☁️ [3/3] AWS S3 CLOUD LAKEHOUSE AUDIT")
    print("=" * 80)

    try:
        import boto3
        from dotenv import load_dotenv

        load_dotenv()
        bucket_name = os.getenv("S3_BUCKET", "spotify-music-intelligence-luc")
        s3 = boto3.client("s3")

        print(f"Connected to S3 Bucket: 's3://{bucket_name}/'")

        prefixes = [
            ("Raw Partitions", "raw/"),
            ("Bronze Artists", "bronze/artists/"),
            ("Silver Artists", "silver/artists/"),
            ("Gold Fact Table", "gold/fact_artist_snapshot/"),
        ]

        for label, prefix in prefixes:
            resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
            common_prefixes = resp.get("CommonPrefixes", [])
            subfolders = [p["Prefix"].replace(prefix, "").strip("/") for p in common_prefixes]
            print(f"  • {label:20} : {subfolders}")

    except Exception as e:
        print(f"⚠️ S3 audit skipped or credentials not configured: {e}")
        print("  Run 'aws s3 ls s3://spotify-music-intelligence-luc/' via AWS CLI to inspect manually.")

    print("\n" + "=" * 80)
    print("🏁 FULL MEDALLION AUDIT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    audit_local_storage()
    audit_gold_duckdb()
    audit_aws_s3()
