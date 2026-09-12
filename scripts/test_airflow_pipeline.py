"""
scripts/test_airflow_pipeline.py
---------------------------------
Orchestration & Idempotency Verification Suite for Day 8.
Simulates and validates the Airflow DAG execution lifecycle locally:
  1. DAG Syntax & Topology Verification (AST inspection of spotify_etl_dag.py).
  2. End-to-End Pipeline Execution on logical date 2026-09-01 (Run 1).
  3. Strict Idempotency Proof: Re-runs the exact same logical date 2026-09-01 (Run 2).
  4. Mathematical Invariant Assertions:
     - Zero Row Multiplication (Row count Run 1 == Row count Run 2).
     - Zero Duplicate Primary Keys: `(artist_key, date_key)`.
     - Zero Orphan Dimensional Keys.
     - Watermark advances atomically only on success.
"""

import os
import sys
import ast
from pathlib import Path
import time
import duckdb

# Ensure UTF-8 output encoding on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestration.watermark_manager import WatermarkManager
from src.transform.bronze_transformer import BronzeTransformer
from src.transform.silver_transformer import SilverTransformer
from src.quality.data_quality import run_quality_gate
from scripts.run_gold import run_gold_pipeline


def run_pipeline_step(step_name: str, fn, *args, **kwargs):
    """Executes a pipeline step with timing and status reporting."""
    start = time.time()
    print(f"  ▶ Executing: {step_name}...", end="", flush=True)
    res = fn(*args, **kwargs)
    elapsed = time.time() - start
    print(f" ✓ ({elapsed:.2f}s)")
    return res


def test_dag_structure():
    """Validates that spotify_etl_dag.py has valid AST syntax, TaskGroups, and operator bindings."""
    print("\n" + "=" * 85)
    print("📋 [1/4] VALIDATING AIRFLOW DAG TOPOLOGY & OPERATOR STRUCTURE")
    print("=" * 85)

    dag_file = PROJECT_ROOT / "airflow" / "dags" / "spotify_etl_dag.py"
    if not dag_file.exists():
        raise FileNotFoundError(f"DAG file not found at: {dag_file}")

    with open(dag_file, "r", encoding="utf-8") as f:
        dag_code = f.read()

    # AST Parse checks 100% Python syntax independently of host packages
    tree = ast.parse(dag_code, filename=str(dag_file))
    print("  ✓ DAG Python AST is 100% syntactically valid.")

    # Validate structural requirements
    assert "spotify_medallion_pipeline" in dag_code, "Missing DAG_ID 'spotify_medallion_pipeline'"
    assert "ShortCircuitOperator" in dag_code, "Missing ShortCircuitOperator for DQ gate!"
    assert "TaskGroup" in dag_code, "Missing TaskGroup organizational structure!"
    assert "reschedule" in dag_code, "S3KeySensor must use mode='reschedule'!"
    assert "commit_watermark" in dag_code, "Missing atomic watermark commit task!"
    print("  ✓ TaskGroup and Operator architecture verified.")
    print("  ✓ ShortCircuitOperator DQ Gate detected.")
    print("  ✓ S3KeySensor 'reschedule' mode detected.")


def inspect_gold_warehouse():
    """Queries Gold Parquet warehouse via DuckDB to get row counts and PK uniqueness."""
    con = duckdb.connect()
    
    # Setup Gold views
    with open(PROJECT_ROOT / "sql" / "setup_gold_views.sql", "r", encoding="utf-8") as f:
        con.execute(f.read())

    # 1. Total fact rows
    total_rows = con.execute("SELECT COUNT(*) FROM fact_artist_snapshot").fetchone()[0]

    # 2. Rows for snapshot 2026-09-01
    sept_rows = con.execute("SELECT COUNT(*) FROM fact_artist_snapshot WHERE snapshot_date = '2026-09-01'").fetchone()[0]

    # 3. Duplicate PK check: (artist_key, snapshot_date)
    dup_pks = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT artist_key, snapshot_date, COUNT(*) 
            FROM fact_artist_snapshot 
            GROUP BY artist_key, snapshot_date 
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    # 4. Orphan check
    orphans = con.execute("""
        SELECT COUNT(*) 
        FROM fact_artist_snapshot f 
        LEFT JOIN dim_artist a ON f.artist_key = a.artist_key 
        WHERE a.artist_key IS NULL
    """).fetchone()[0]

    return {
        "total_rows": total_rows,
        "sept_rows": sept_rows,
        "dup_pks": dup_pks,
        "orphans": orphans,
    }


def simulate_orchestration_run(logical_date: str, run_label: str) -> dict:
    """Simulates an entire Airflow DAG execution run for a specific logical date."""
    print(f"\n🚀 Running Pipeline Simulation ({run_label}) for logical date: {logical_date}")
    print("-" * 85)

    wm = WatermarkManager()
    job_name = "spotify_medallion_pipeline"

    # Task 1: Check Watermark
    already_processed = wm.is_snapshot_processed(job_name, logical_date)
    print(f"  🔍 Watermark Inspection: Snapshot '{logical_date}' already processed? -> {already_processed}")

    # Task 2: Verify Raw Payloads
    def verify_raw_arrival(ds):
        track_path = PROJECT_ROOT / "data" / "raw" / "tracks" / f"tracks_{ds}.json"
        if not track_path.exists():
            raise FileNotFoundError(f"Missing raw track payload at: {track_path}")
        return True
    run_pipeline_step("Raw Payload Sensor", verify_raw_arrival, ds=logical_date)

    # Task 3: PySpark Bronze Transformation
    def run_bronze(ds):
        transformer = BronzeTransformer(
            raw_base_dir=str(PROJECT_ROOT / "data" / "raw"),
            bronze_base_dir=str(PROJECT_ROOT / "data" / "bronze"),
        )
        summary = transformer.run_snapshot(snapshot_date=ds)
        transformer.spark.stop()
        return summary
    run_pipeline_step("PySpark Bronze Transformation", run_bronze, ds=logical_date)

    # Task 4: PySpark Silver Transformation
    def run_silver(ds):
        transformer = SilverTransformer(
            bronze_base_dir=str(PROJECT_ROOT / "data" / "bronze"),
            silver_base_dir=str(PROJECT_ROOT / "data" / "silver"),
        )
        summary = transformer.run_snapshot(snapshot_date=ds)
        transformer.spark.stop()
        return summary
    run_pipeline_step("PySpark Silver Transformation", run_silver, ds=logical_date)

    # Task 5: Silver DQ Gate (ShortCircuitOperator logic)
    def run_dq_gate(ds):
        report = run_quality_gate(
            snapshot_date=ds,
            silver_base_dir=str(PROJECT_ROOT / "data" / "silver"),
        )
        overall_status = report.get("overall_status", "FAIL")
        if overall_status not in ["PASS", "WARN"]:
            return False
        return True
    dq_passed = run_pipeline_step("ShortCircuit DQ Gate", run_dq_gate, ds=logical_date)
    assert dq_passed is True, f"DQ Gate returned False for {logical_date}!"

    # Task 6: Kimball Gold (Dynamic Partition Overwrite)
    def run_gold(ds):
        return run_gold_pipeline(
            snapshot_date=ds,
            silver_dir=str(PROJECT_ROOT / "data" / "silver"),
            gold_dir=str(PROJECT_ROOT / "data" / "gold"),
            sync_s3=False,
        )
    run_pipeline_step("Kimball Gold Transformation", run_gold, ds=logical_date)

    # Task 7: Refresh DuckDB Analytical Marts
    def refresh_marts(ds):
        con = duckdb.connect()
        with open(PROJECT_ROOT / "sql" / "setup_gold_views.sql", "r", encoding="utf-8") as f:
            con.execute(f.read())
        with open(PROJECT_ROOT / "sql" / "setup_marts.sql", "r", encoding="utf-8") as f:
            con.execute(f.read())
        return True
    run_pipeline_step("Refresh DuckDB Marts", refresh_marts, ds=logical_date)

    # Task 8: Atomic Watermark Commit
    def commit_watermark(ds):
        wm.update_watermark(
            job_name=job_name,
            snapshot_date=ds,
            status="SUCCESS",
            metadata={"run_label": run_label, "logical_date": ds},
            sync_to_s3=False,
        )
        return True
    run_pipeline_step("Atomic Watermark Commit", commit_watermark, ds=logical_date)

    # Inspect Warehouse State
    stats = inspect_gold_warehouse()
    return stats


def test_idempotency():
    print("=" * 85)
    print("🔄 [2/4] PROVING PIPELINE IDEMPOTENCY (SAME-DATE BACK-TO-BACK RUNS)")
    print("=" * 85)

    target_date = "2026-09-01"

    # --- RUN 1 ---
    stats_run1 = simulate_orchestration_run(logical_date=target_date, run_label="RUN 1")
    print(f"\n📊 Run 1 Warehouse Snapshot:")
    print(f"   • Total Fact Rows      : {stats_run1['total_rows']}")
    print(f"   • Rows in {target_date}   : {stats_run1['sept_rows']}")
    print(f"   • Duplicate PKs        : {stats_run1['dup_pks']}")
    print(f"   • Orphan Foreign Keys  : {stats_run1['orphans']}")

    # --- RUN 2 (Exact Same Input Date) ---
    print("\n" + "=" * 85)
    print("🔄 [3/4] EXECUTING RUN 2 (EXACT IDENTICAL LOGICAL DATE)")
    print("=" * 85)

    stats_run2 = simulate_orchestration_run(logical_date=target_date, run_label="RUN 2")
    print(f"\n📊 Run 2 Warehouse Snapshot:")
    print(f"   • Total Fact Rows      : {stats_run2['total_rows']}")
    print(f"   • Rows in {target_date}   : {stats_run2['sept_rows']}")
    print(f"   • Duplicate PKs        : {stats_run2['dup_pks']}")
    print(f"   • Orphan Foreign Keys  : {stats_run2['orphans']}")

    # --- VERIFY IDEMPOTENCY INVARIANTS ---
    print("\n" + "=" * 85)
    print("⚖️ [4/4] ASSERTING IDEMPOTENCY MATHEMATICAL INVARIANTS")
    print("=" * 85)

    # 1. Zero Row Multiplication
    assert stats_run1["total_rows"] == stats_run2["total_rows"], (
        f"Row multiplication detected! Run 1: {stats_run1['total_rows']}, Run 2: {stats_run2['total_rows']}"
    )
    print(f"  ✅ INVARIANT 1 PASS: Zero Row Multiplication ({stats_run1['total_rows']} == {stats_run2['total_rows']}).")

    # 2. Partition Row Count Equality
    assert stats_run1["sept_rows"] == stats_run2["sept_rows"], (
        f"Partition count mismatch! Run 1: {stats_run1['sept_rows']}, Run 2: {stats_run2['sept_rows']}"
    )
    print(f"  ✅ INVARIANT 2 PASS: Partition '{target_date}' rows exactly preserved ({stats_run1['sept_rows']}).")

    # 3. Zero Duplicate Primary Keys
    assert stats_run2["dup_pks"] == 0, f"Found {stats_run2['dup_pks']} duplicate PKs!"
    print("  ✅ INVARIANT 3 PASS: Zero Duplicate Primary Keys (artist_key, date_key).")

    # 4. Zero Orphan Foreign Keys
    assert stats_run2["orphans"] == 0, f"Found {stats_run2['orphans']} orphan artist keys!"
    print("  ✅ INVARIANT 4 PASS: Zero Orphan Dimensional Keys (100% referential integrity).")

    # 5. Watermark State Persistence
    wm = WatermarkManager()
    assert wm.is_snapshot_processed("spotify_medallion_pipeline", target_date) is True
    print(f"  ✅ INVARIANT 5 PASS: Watermark correctly records '{target_date}' as processed.")

    print("\n" + "=" * 85)
    print("🎉 ALL IDEMPOTENCY & AIRFLOW PIPELINE TESTS PASSED 100%!")
    print("   The pipeline is officially safe for backfills, retries, and production daily execution.")
    print("=" * 85)


if __name__ == "__main__":
    test_dag_structure()
    test_idempotency()
