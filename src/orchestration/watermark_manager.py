"""
src/orchestration/watermark_manager.py
---------------------------------------
Atomic Watermark Manager for Incremental Loading and Idempotency Control.
Tracks processed snapshot dates and execution metadata both locally and in AWS S3.

Guarantees:
  1. Atomic File Swaps: Writes to a temporary `.tmp` file before renaming with
     `Path.replace()`, preventing corrupted state files on power/process interruption.
  2. Idempotent Invariant: Records processed snapshot history to prevent duplicate
     runs on identical logical dates unless explicitly forced.
  3. Hybrid Dual-Store: Automatically syncs state between local JSON and S3 bucket
     `s3://spotify-music-intelligence-luc/state/watermarks.json`.
"""

import json
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class WatermarkManager:
    """
    Manages high-watermark state for batch ETL pipelines.
    Guarantees atomic file updates and dual local/S3 synchronization.
    """

    def __init__(
        self,
        local_path: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_key: str = "state/watermarks.json",
        auto_sync_s3: bool = False,
    ):
        # 1. Resolve project root and default local state path
        if local_path:
            self.local_path = Path(local_path).resolve()
        else:
            # Default to state/watermarks.json at repository root
            project_root = Path(__file__).resolve().parent.parent.parent
            self.local_path = project_root / "state" / "watermarks.json"

        self.s3_bucket = s3_bucket or os.getenv("S3_BUCKET", "spotify-music-intelligence-luc")
        self.s3_key = s3_key
        self.auto_sync_s3 = auto_sync_s3

        # S3 client lazy initialization
        self._s3_client = None

    @property
    def s3_client(self):
        if self._s3_client is None and BOTO3_AVAILABLE:
            try:
                self._s3_client = boto3.client("s3")
            except Exception:
                self._s3_client = None
        return self._s3_client

    def _is_cloud_env(self) -> bool:
        """Detect if executing inside an AWS environment (Lambda, Glue, ECS)."""
        return (
            "GLUE_COMMAND_CRITERIA" in os.environ
            or "AWS_EXECUTION_ENV" in os.environ
            or "JOB_NAME" in os.environ
        )

    # -------------------------------------------------------------------------
    # State Inspection Methods
    # -------------------------------------------------------------------------

    def get_watermark(self, job_name: str, default: str = "1970-01-01 00:00:00") -> str:
        """Returns the last processed timestamp string for a job."""
        state = self._read_state()
        return state.get(job_name, {}).get("last_processed_timestamp", default)

    def get_last_processed_date(self, job_name: str, default: str = "1970-01-01") -> str:
        """Returns the last processed snapshot partition date (YYYY-MM-DD)."""
        state = self._read_state()
        return state.get(job_name, {}).get("last_processed_date", default)

    def is_snapshot_processed(self, job_name: str, snapshot_date: str) -> bool:
        """
        Checks if a given snapshot date has already been successfully processed.
        Used by the pipeline to skip redundant execution and ensure idempotency.
        """
        state = self._read_state()
        job_data = state.get(job_name, {})
        processed_list = job_data.get("processed_snapshots", [])
        return snapshot_date in processed_list

    def get_processed_snapshots(self, job_name: str) -> List[str]:
        """Returns sorted list of all historical snapshot dates processed."""
        state = self._read_state()
        return sorted(state.get(job_name, {}).get("processed_snapshots", []))

    # -------------------------------------------------------------------------
    # State Mutation Methods (Atomic)
    # -------------------------------------------------------------------------

    def update_watermark(
        self,
        job_name: str,
        snapshot_date: str,
        status: str = "SUCCESS",
        metadata: Optional[Dict[str, Any]] = None,
        sync_to_s3: Optional[bool] = None,
    ) -> None:
        """
        Atomically records a successful pipeline run for a logical snapshot date.
        """
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        state = self._read_state()

        if job_name not in state:
            state[job_name] = {
                "last_processed_date": snapshot_date,
                "last_processed_timestamp": now_utc,
                "status": status,
                "processed_snapshots": [snapshot_date],
                "metadata": metadata or {},
            }
        else:
            job_state = state[job_name]
            job_state["last_processed_date"] = snapshot_date
            job_state["last_processed_timestamp"] = now_utc
            job_state["status"] = status
            if metadata:
                job_state["metadata"] = metadata

            history = set(job_state.get("processed_snapshots", []))
            history.add(snapshot_date)
            job_state["processed_snapshots"] = sorted(list(history))

        # Write locally with atomic swap
        self._write_state(state)
        print(f"[{job_name}] ✅ Atomically updated watermark to: {snapshot_date} (Status: {status})")

        # Optionally sync to S3
        should_sync = sync_to_s3 if sync_to_s3 is not None else (self.auto_sync_s3 or self._is_cloud_env())
        if should_sync:
            self.sync_to_s3()

    def reset_watermark(self, job_name: Optional[str] = None) -> None:
        """Resets state for a specific job or all jobs."""
        state = self._read_state()
        if job_name:
            if job_name in state:
                del state[job_name]
                self._write_state(state)
                print(f"[{job_name}] 🔄 Reset watermark state.")
        else:
            self._write_state({})
            print("🔄 Reset entire watermark state.")

    # -------------------------------------------------------------------------
    # Low-Level Atomic Read / Write Operations
    # -------------------------------------------------------------------------

    def _read_state(self) -> Dict[str, Any]:
        """Reads watermark JSON state from local disk (or fallback from S3)."""
        if self.local_path.exists():
            try:
                with open(self.local_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ Warning reading local state file ({e}), attempting S3 fallback...")

        # If local doesn't exist or is corrupted, attempt read from S3
        if self.s3_client and (self._is_cloud_env() or self.auto_sync_s3):
            try:
                resp = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
                content = resp["Body"].read().decode("utf-8")
                state = json.loads(content)
                # Cache locally atomically
                self._write_state(state)
                return state
            except ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    print(f"⚠️ S3 state read error: {e}")

        return {}

    def _write_state(self, state: Dict[str, Any]) -> None:
        """
        ATOMIC WRITE GUARANTEE:
        1. Writes to temporary file: watermarks.json.tmp
        2. Atomically replaces target file: watermarks.json
        """
        payload = json.dumps(state, indent=2)
        
        # Ensure parent directory exists (e.g. state/)
        self.local_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to temporary file in the same directory (required for atomic filesystem swap)
        tmp_path = self.local_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)

        # Atomic replace
        tmp_path.replace(self.local_path)

    def sync_to_s3(self) -> bool:
        """Pushes current local watermark state to Amazon S3."""
        if not self.s3_client:
            return False

        if not self.local_path.exists():
            return False

        try:
            with open(self.local_path, "r", encoding="utf-8") as f:
                payload = f.read()

            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=self.s3_key,
                Body=payload.encode("utf-8"),
                ContentType="application/json",
            )
            print(f"☁️ Synced watermark state to s3://{self.s3_bucket}/{self.s3_key}")
            return True
        except Exception as e:
            print(f"⚠️ S3 state sync warning: {e}")
            return False


if __name__ == "__main__":
    print("=" * 70)
    print("🧪 TESTING ATOMIC WATERMARK MANAGER LOCALLY")
    print("=" * 70)

    wm = WatermarkManager()

    job = "spotify_medallion_pipeline"
    print(f"Initial State: {wm.get_watermark(job)}")
    print(f"Is 2026-09-01 Processed?: {wm.is_snapshot_processed(job, '2026-09-01')}")

    # 1. Simulate First Run
    print("\n[1/3] Simulating Run 1 for 2026-08-31...")
    wm.update_watermark(
        job_name=job,
        snapshot_date="2026-08-31",
        metadata={"total_tracks": 3837, "total_artists": 8},
    )

    # 2. Verify Atomic State
    print("\n[2/3] Validating State Read-back:")
    assert wm.is_snapshot_processed(job, "2026-08-31") is True, "Snapshot 2026-08-31 not found in state!"
    print(f"  ✓ Last Processed Date : {wm.get_last_processed_date(job)}")
    print(f"  ✓ Processed Snapshots : {wm.get_processed_snapshots(job)}")

    # 3. Simulate Incremental Next Date
    print("\n[3/3] Simulating Run 2 for 2026-09-01...")
    wm.update_watermark(
        job_name=job,
        snapshot_date="2026-09-01",
        metadata={"total_tracks": 3367, "total_artists": 7, "growth_detected": True},
    )

    assert wm.is_snapshot_processed(job, "2026-09-01") is True, "Snapshot 2026-09-01 not found in state!"
    assert len(wm.get_processed_snapshots(job)) == 2, "Expected 2 snapshots in history!"
    print(f"  ✓ Processed Snapshots : {wm.get_processed_snapshots(job)}")

    print("\n" + "=" * 70)
    print("🎉 ATOMIC WATERMARK MANAGER PASSED ALL LOCAL ASSERTIONS!")
    print("=" * 70)
