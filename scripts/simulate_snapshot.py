"""
scripts/simulate_snapshot.py
----------------------------
Synthetic Snapshot Generator for API Quota Resilience (Day 6).

Context:
    When upstream third-party APIs (Spotify Development Mode) enforce strict 24-hour
    quota ceilings (~2,400 calls/day), downstream data engineering development cannot
    remain blocked for multi-day quota reset cycles.

Purpose:
    Propagates a clean baseline cohort (e.g., 2026-08-31 5-artist extraction) into a
    subsequent snapshot partition (2026-09-01) by updating the `snapshot_date` watermark.
    
    By preserving identical track counts across consecutive days without fabricating
    synthetic track IDs, this validates the critical "zero-growth" (0.00%) edge-case path
    in downstream PySpark window functions (`LAG()`), while the first snapshot validates
    the `NULL` initial baseline rule.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List

# Ensure project root is known for path-traversal validation
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _safe_resolve_path(user_path: str, *, label: str = "path") -> Path:
    """Resolve *user_path* and ensure it stays within the project root."""
    resolved = (_PROJECT_ROOT / user_path).resolve()
    if not resolved.is_relative_to(_PROJECT_ROOT):
        print(f"❌ Security: {label} '{user_path}' resolves outside project root. Aborting.")
        sys.exit(1)
    return resolved

def simulate_snapshot(
    source_date: str = "2026-08-31",
    target_date: str = "2026-09-01",
    raw_dir: str = "data/raw",
    entities: List[str] = None
) -> None:

    # Sanitize raw_dir against path traversal (SonarCloud pythonsecurity:S8707)
    safe_raw = _safe_resolve_path(raw_dir, label="raw-dir")

    if entities is None:
        entities = ["artists", "albums", "tracks"]
    
    print("=" * 60)
    print(f"🧪 Simulating Snapshot: {source_date} ➔ {target_date}")
    print("=" * 60)

    for entity in entities:
        src_path = str(safe_raw / entity / f"{entity}_{source_date}.json")
        dst_path = str(safe_raw / entity / f"{entity}_{target_date}.json")

        if not os.path.exists(src_path):
            print(f"❌ Source file not found: {src_path}")
            sys.exit(1)

        # 1. Read source JSON
        with open(src_path, "r", encoding="utf-8") as f:
            records = json.load(f)

        # 2. Update snapshot_date watermark
        for item in records:
            item["snapshot_date"] = target_date

        # 3. Ensure target directory exists and write
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        with open(dst_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii = False)

        print(f"  ✓ {entity:<8}: {len(records):>5} records written -> {dst_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate a snapshot partition from a prior clean run.")
    parser.add_argument("--source-date", type=str, default="2026-08-31", help="Source snapshot date (YYYY-MM-DD)")
    parser.add_argument("--target-date", type=str, default="2026-09-01", help="Target snapshot date (YYYY-MM-DD)")
    parser.add_argument("--raw-dir", type=str, default="data/raw", help="Base raw data directory")

    args = parser.parse_args()

    simulate_snapshot(
        source_date = args.source_date,
        target_date = args.target_date,
        raw_dir = args.raw_dir,
    )


