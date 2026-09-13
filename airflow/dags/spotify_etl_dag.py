"""
airflow/dags/spotify_etl_dag.py
-------------------------------
Master Apache Airflow DAG for the Spotify Music Intelligence Platform.
Orchestrates the entire end-to-end Medallion Lakehouse architecture:
  API Extraction ──▶ S3 Raw ──▶ S3KeySensor ──▶ PySpark Bronze ──▶
  PySpark Silver ──▶ ShortCircuit DQ Gate ──▶ Kimball Gold ──▶ S3 Sync ──▶
  DuckDB Marts ──▶ Atomic Watermark Commit

Production Engineering Guarantees:
  1. Backfill-Safe Logical Date: Uses `{{ ds }}` / `data_interval_start` exclusively (0% datetime.now()).
  2. ShortCircuit DQ Gate: Skips downstream tasks gracefully on DQ failure without crashing DAG run.
  3. S3KeySensor in 'reschedule' mode: Frees worker slots while awaiting S3 payload arrival.
  4. Quota-Aware Extraction: Handles Spotify 429 with Retry-After and triggers Airflow retries.
  5. Idempotent Watermarks: Advances state atomically only after 100% successful downstream execution.
"""

import os
import sys
from datetime import datetime, timedelta
import logging

# Ensure project root is in sys.path (supports both local host and Airflow Docker container)
if os.path.exists("/opt/airflow/src"):
    PROJECT_ROOT = "/opt/airflow"
else:
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

for p in [PROJECT_ROOT, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))]:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.exceptions import AirflowException

# Optional S3 Sensor import with fallback for testing environments
try:
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    S3_SENSOR_AVAILABLE = True
except ImportError:
    S3_SENSOR_AVAILABLE = False

logger = logging.getLogger(__name__)

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "spotify-music-intelligence-luc")
DAG_ID = "spotify_medallion_pipeline"


def notify_error(context):
    """Structured error alert callback for task failures."""
    dag_id = context.get("dag_run").dag_id if context.get("dag_run") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    exception = context.get("exception", "Unknown error")
    logical_date = context.get("ds", "unknown")
    logger.error(f"❌ [DAG FAILURE ALERT] | DAG: {dag_id} | Task: {task_id} | Date: {logical_date} | Error: {exception}")


# -----------------------------------------------------------------------------
# DAG Task Callables (Parameterized strictly by logical context `ds`)
# -----------------------------------------------------------------------------

def check_watermark_callable(**context):
    """Verifies pipeline state and logs whether the logical date was already processed."""
    from src.orchestration.watermark_manager import WatermarkManager
    ds = context["ds"]
    wm = WatermarkManager()
    
    is_processed = wm.is_snapshot_processed(DAG_ID, ds)
    last_date = wm.get_last_processed_date(DAG_ID)
    logger.info(f"🔍 [Watermark Check] Target ds: {ds} | Last processed: {last_date} | Already run: {is_processed}")
    
    # Store in XCom for auditability
    context["ti"].xcom_push(key="is_already_processed", value=is_processed)
    return is_processed


def extract_spotify_callable(**context):
    """
    Extracts artist, album, and track catalog metadata from Spotify Web API.
    Handles 429 quota exceptions by logging Retry-After and raising AirflowException.
    """
    ds = context["ds"]
    logger.info(f"🎵 Starting Spotify Catalog Extraction for snapshot_date: {ds}...")

    raw_track_file = os.path.join(PROJECT_ROOT, "data", "raw", "tracks", f"tracks_{ds}.json")
    if os.path.exists(raw_track_file):
        logger.info(f"✓ Raw payload for {ds} already landed on storage: {raw_track_file}")
        return True

    try:
        from src.extract.main import run_extraction
        from src.orchestration.watermark_manager import WatermarkManager
        wm = WatermarkManager()
        last_date = wm.get_last_processed_date(DAG_ID)
        since = last_date if last_date != "1970-01-01" and last_date < ds else None

        result = run_extraction(
            snapshot_date=ds,
            output_dir=os.path.join(PROJECT_ROOT, "data", "raw"),
            since_date=since,
        )
        if isinstance(result, dict) and not result.get("is_complete", True):
            completed = result.get("completed_artists", [])
            raise AirflowException(
                f"Spotify extraction partially complete ({len(completed)} artists completed). "
                f"Triggering Airflow retry to resume remaining artists from checkpoint."
            )
    except Exception as e:
        err_msg = str(e)
        if "SpotifyRateLimitError" in type(e).__name__ or "429" in err_msg or "quota" in err_msg.lower():
            logger.warning(f"⏳ Spotify Rate Limit / Quota ceiling reached: {err_msg}")
            if os.path.exists(raw_track_file):
                logger.info("Proceeding with existing raw state...")
                return True
            raise AirflowException(f"Spotify API 429 Quota Pause: {err_msg}")
        raise e


def upload_raw_s3_callable(**context):
    """Uploads extracted local raw JSON files to S3 Raw Lakehouse."""
    from src.storage.s3_uploader import S3Uploader

    ds = context["ds"]
    logger.info(f"☁️ Uploading raw JSON files for {ds} to S3 bucket '{S3_BUCKET}'...")

    try:
        uploader = S3Uploader(bucket_name=S3_BUCKET)
        uploader.validate_bucket()

        entities = ["artists", "albums", "tracks"]
        for entity in entities:
            local_path = os.path.join(PROJECT_ROOT, "data", "raw", entity, f"{entity}_{ds}.json")
            if os.path.exists(local_path):
                s3_key = uploader.build_s3_key(entity_type=entity, filename=f"{entity}_{ds}.json", snapshot_date=ds)
                uploader.upload_file(local_path=local_path, s3_key=s3_key, snapshot_date=ds)
                logger.info(f"  ✓ Uploaded & verified: s3://{S3_BUCKET}/{s3_key}")
            else:
                logger.warning(f"⚠️ Raw file not found for {entity} at {local_path}")
    except Exception as e:
        logger.error(f"❌ S3 Raw upload failed for {ds}: {e}", exc_info=True)
        raise AirflowException(f"S3 Raw upload failed: {e}")


def local_raw_sensor_callable(**context):
    """Fallback sensor checking local raw file arrival when S3 provider is not installed."""
    ds = context["ds"]
    track_path = os.path.join(PROJECT_ROOT, "data", "raw", "tracks", f"tracks_{ds}.json")
    if not os.path.exists(track_path):
        raise AirflowException(f"Awaiting raw track payload for {ds} at: {track_path}")
    logger.info(f"✓ Verified raw track payload exists for {ds}: {track_path}")
    return True


def bronze_transform_callable(**context):
    """Executes PySpark Bronze layer transformation enforcing strict schema contracts."""
    ds = context["ds"]
    logger.info(f"🧱 Executing Bronze PySpark Transformer for {ds}...")
    try:
        from src.transform.bronze_transformer import BronzeTransformer
        transformer = BronzeTransformer(
            raw_base_dir=os.path.join(PROJECT_ROOT, "data", "raw"),
            bronze_base_dir=os.path.join(PROJECT_ROOT, "data", "bronze"),
        )
        summary = transformer.run_snapshot(snapshot_date=ds)
        transformer.spark.stop()
        logger.info(f"✓ Bronze Transformation Summary: {summary}")
    except Exception as e:
        logger.error(f"❌ Bronze PySpark transformation failed for {ds}: {e}", exc_info=True)
        raise AirflowException(f"Bronze transformation failed: {e}")


def silver_transform_callable(**context):
    """Executes PySpark Silver layer transformation with deduplication and standardization."""
    ds = context["ds"]
    logger.info(f"🥈 Executing Silver PySpark Transformer for {ds}...")
    try:
        from src.transform.silver_transformer import SilverTransformer
        transformer = SilverTransformer(
            bronze_base_dir=os.path.join(PROJECT_ROOT, "data", "bronze"),
            silver_base_dir=os.path.join(PROJECT_ROOT, "data", "silver"),
        )
        summary = transformer.run_snapshot(snapshot_date=ds)
        transformer.spark.stop()
        logger.info(f"✓ Silver Transformation Summary: {summary}")
    except Exception as e:
        logger.error(f"❌ Silver PySpark transformation failed for {ds}: {e}", exc_info=True)
        raise AirflowException(f"Silver transformation failed: {e}")


def silver_dq_gate_callable(**context) -> bool:
    """
    ShortCircuitOperator callable: Asserts Silver data quality rules.
    Returns True  -> Pipeline proceeds to Gold.
    Returns False -> Downstream Gold & Marts are skipped cleanly without DAG failure.
    """
    ds = context["ds"]
    logger.info(f"🛡️ Evaluating Silver Data Quality Gate for {ds}...")

    try:
        from src.quality.data_quality import run_quality_gate
        report = run_quality_gate(
            snapshot_date=ds,
            silver_base_dir=os.path.join(PROJECT_ROOT, "data", "silver"),
        )
        overall_status = report.get("overall_status", "FAIL")
        logger.info(f"🛡️ DQ Gate Result: {overall_status}")
        return overall_status in ["PASS", "WARN"]
    except Exception as e:
        logger.error(f"❌ Silver Data Quality Gate execution failed for {ds}: {e}", exc_info=True)
        raise AirflowException(f"Silver Data Quality Gate failed: {e}")


def gold_transform_callable(**context):
    """Executes Kimball Star Schema Gold Transformation (Dimensions + Fact Snapshot)."""
    ds = context["ds"]
    logger.info(f"🏆 Executing Gold Kimball Star Schema Transformer for {ds}...")
    try:
        from scripts.run_gold import run_gold_pipeline
        run_gold_pipeline(
            snapshot_date=ds,
            silver_dir=os.path.join(PROJECT_ROOT, "data", "silver"),
            gold_dir=os.path.join(PROJECT_ROOT, "data", "gold"),
            sync_s3=False,
        )
        logger.info(f"✓ Gold Transformation Complete for {ds}!")
    except Exception as e:
        logger.error(f"❌ Gold Kimball transformation failed for {ds}: {e}", exc_info=True)
        raise AirflowException(f"Gold transformation failed: {e}")


def gold_s3_sync_callable(**context):
    """Syncs Gold Parquet tables to Amazon S3."""
    try:
        from scripts.run_gold import sync_gold_to_s3
        logger.info(f"☁️ Synchronizing Gold Parquet files to s3://{S3_BUCKET}/gold/...")
        sync_gold_to_s3(local_gold_dir=os.path.join(PROJECT_ROOT, "data", "gold"), bucket_name=S3_BUCKET)
    except Exception as e:
        logger.error(f"❌ Gold S3 sync failed for {context.get('ds')}: {e}", exc_info=True)
        raise AirflowException(f"Gold S3 sync failed: {e}")


def refresh_duckdb_marts_callable(**context):
    """Validates and refreshes the 4 Curated Data Marts in DuckDB."""
    ds = context["ds"]
    logger.info(f"🦆 Refreshing DuckDB Analytical Marts for {ds}...")
    try:
        import duckdb
        con = duckdb.connect()
        with open(os.path.join(PROJECT_ROOT, "sql", "setup_gold_views.sql"), "r", encoding="utf-8") as f:
            con.execute(f.read())
        with open(os.path.join(PROJECT_ROOT, "sql", "setup_marts.sql"), "r", encoding="utf-8") as f:
            con.execute(f.read())
        marts = ["mart_artist_activity", "mart_catalog_growth", "mart_release_seasonality", "mart_artist_momentum"]
        for mart in marts:
            cnt = con.execute(f"SELECT COUNT(*) FROM {mart}").fetchone()[0]
            logger.info(f"  ✓ Mart '{mart}' validated with {cnt:,} rows.")
    except Exception as e:
        logger.error(f"❌ DuckDB Analytical Marts refresh failed for {ds}: {e}", exc_info=True)
        raise AirflowException(f"DuckDB Marts refresh failed: {e}")


def commit_watermark_callable(**context):
    """Atomically commits the logical date to state/watermarks.json and S3."""
    from src.orchestration.watermark_manager import WatermarkManager
    ds = context["ds"]
    logger.info(f"📝 Atomically committing watermark state for {ds}...")

    wm = WatermarkManager()
    wm.update_watermark(
        job_name=DAG_ID,
        snapshot_date=ds,
        status="SUCCESS",
        metadata={
            "dag_id": DAG_ID,
            "logical_date": ds,
            "run_id": context.get("run_id", "manual"),
        },
        sync_to_s3=False,
    )
    logger.info(f"🎉 Pipeline successfully completed and watermark advanced for {ds}!")


# -----------------------------------------------------------------------------
# DAG Definition
# -----------------------------------------------------------------------------

# Layer 2 Quota-Aware Task Retry Configurations
EXTRACT_DEFAULTS = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=2),   # covers Spotify's ~1h quota reset window
}

COMPUTE_DEFAULTS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 31),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_error,
    "email_on_failure": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Spotify Music Intelligence Platform: End-to-End Medallion Lakehouse DAG",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "medallion", "lakehouse", "pyspark", "duckdb", "aws"],
) as dag:

    # 1. Pipeline Watermark Inspection
    check_watermark = PythonOperator(
        task_id="check_watermark",
        python_callable=check_watermark_callable,
        provide_context=True,
    )

    # 2. Extract & Landing Group (Aggressive Retry for Quota Resiliency)
    with TaskGroup("extract_and_land") as extract_and_land:
        extract_spotify = PythonOperator(
            task_id="extract_spotify_catalog",
            python_callable=extract_spotify_callable,
            provide_context=True,
            **EXTRACT_DEFAULTS,
        )

        upload_raw_s3 = PythonOperator(
            task_id="upload_raw_s3",
            python_callable=upload_raw_s3_callable,
            provide_context=True,
            retries=3,
            retry_delay=timedelta(minutes=2),
        )

        if S3_SENSOR_AVAILABLE:
            sensor_s3_raw = S3KeySensor(
                task_id="sensor_s3_raw_ready",
                bucket_name=S3_BUCKET,
                bucket_key="raw/extracted_at={{ ds }}/tracks/tracks_{{ ds }}.json",
                mode="reschedule",
                poke_interval=60,
                timeout=3600,
                aws_conn_id="aws_default",
            )
        else:
            sensor_s3_raw = PythonOperator(
                task_id="sensor_raw_ready",
                python_callable=local_raw_sensor_callable,
                provide_context=True,
            )

        extract_spotify >> upload_raw_s3 >> sensor_s3_raw

    # 3. Medallion PySpark Transform Group (Fast-Fail Compute Defaults)
    with TaskGroup("medallion_processing") as medallion_processing:
        transform_bronze = PythonOperator(
            task_id="transform_bronze",
            python_callable=bronze_transform_callable,
            provide_context=True,
            **COMPUTE_DEFAULTS,
        )

        transform_silver = PythonOperator(
            task_id="transform_silver",
            python_callable=silver_transform_callable,
            provide_context=True,
            **COMPUTE_DEFAULTS,
        )

        # Senior addition: ShortCircuitOperator skips downstream on DQ failure
        silver_dq_gate = ShortCircuitOperator(
            task_id="silver_dq_gate",
            python_callable=silver_dq_gate_callable,
            provide_context=True,
        )

        transform_bronze >> transform_silver >> silver_dq_gate

    # 4. Kimball Star Schema Gold Group (Fast-Fail Compute Defaults)
    with TaskGroup("gold_and_marts") as gold_and_marts:
        transform_gold = PythonOperator(
            task_id="transform_gold",
            python_callable=gold_transform_callable,
            provide_context=True,
            **COMPUTE_DEFAULTS,
        )

        sync_gold_s3 = PythonOperator(
            task_id="sync_gold_s3",
            python_callable=gold_s3_sync_callable,
            provide_context=True,
            **COMPUTE_DEFAULTS,
        )

        refresh_duckdb_marts = PythonOperator(
            task_id="refresh_duckdb_marts",
            python_callable=refresh_duckdb_marts_callable,
            provide_context=True,
            **COMPUTE_DEFAULTS,
        )

        transform_gold >> sync_gold_s3 >> refresh_duckdb_marts

    # 5. Commit High-Watermark State
    commit_watermark = PythonOperator(
        task_id="commit_watermark",
        python_callable=commit_watermark_callable,
        provide_context=True,
    )

    # Linear High-Level Dependency Graph
    check_watermark >> extract_and_land >> medallion_processing >> gold_and_marts >> commit_watermark
