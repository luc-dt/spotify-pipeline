"""
src/quality/data_quality.py
----------------------------
Automated Data Quality Gate.
Evaluates Silver candidate DataFrames against 5 core data quality rules:
  Rule 1: Completeness (Row count >= min_expected)
  Rule 2: Primary Key Uniqueness (Zero duplicates)
  Rule 3: Critical Column Null Checks (Non-nullable IDs and names)
  Rule 4: Referential Integrity (Left-Anti joins for orphan detection)
  Rule 5: Value / Range Validation (Sensible metrics and bounds)
Returns structured evaluation results and generates audit-ready reports.
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    sum as spark_sum,
    count as spark_count,
    current_timestamp,
)


class DataQualityChecker:
    """
    Evaluates Candidate Silver DataFrames against data quality rules, tracks validation results, 
    and determines whether data is safe to promote to Trusted Silver.

    NOTE ON PERFORMANCE:
    For large datasets in production, callers should call .cache() on the candidate
    DataFrame before running multiple DQ checks to prevent Spark from recomputing
    the upstream DAG for each action.
    """
    def __init__(
        self,
        snapshot_date: str = "2026-08-31",
        quarantine_base_dir: str = "data/quarantine",
        report_base_dir: str = "data/quality/reports",
    ):
        self.snapshot_date = snapshot_date
        self.quarantine_base_dir = quarantine_base_dir
        self.report_base_dir = report_base_dir
        self.results: List[Dict[str, Any]] = []

    def check_completeness(
        self,
        df: DataFrame,
        entity: str,
        min_expected: int = 1
    ) -> Dict[str, Any]:
        """
        Rule 1: Completeness.
        Verifies that an entity DataFrame is not empty and contains at least min_expected rows.
        Severity: FAIL if row_count < min_expected.
        """
        row_count = df.count()
        passed = row_count >= min_expected

        result = {
            "rule": "completeness",
            "entity": entity,
            "snapshot_date": self.snapshot_date,
            "status": "PASS" if passed else "FAIL",
            "row_count": row_count,
            "min_expected": min_expected,
            "message": (
                f"Entity '{entity}' has {row_count:,} rows (>= {min_expected:,})."
                if passed
                else f"Entity '{entity}' FAILED completeness! Found {row_count:,} rows, expected at least {min_expected:,}."
            ),
        }

        self.results.append(result)
        return result

    def check_uniqueness(
        self, 
        df: DataFrame,
        entity: str,
        primary_keys: List[str]
    ) -> Dict[str, Any]:
        """
        Rule 2: Primary Key Uniqueness.
        Verifies that the primary key (or composite keys) has zero duplicate records.
        Severity: FAIL if duplicate_count > 0.
        """
        total_rows = df.count()
        distinct_keys = df.select(primary_keys).distinct().count()
        duplicate_count = total_rows - distinct_keys
        passed = duplicate_count == 0

        duplicate_pct = (
            round((duplicate_count / total_rows) * 100.0, 2)
            if total_rows > 0 else 0.0
        )

        result = {
            "rule": "uniqueness",
            "entity": entity,
            "snapshot_date": self.snapshot_date,
            "primary_keys": primary_keys,
            "status": "PASS" if passed else "FAIL",
            "total_rows": total_rows,
            "distinct_keys": distinct_keys,
            "duplicate_count": duplicate_count,
            "duplicate_rate_pct": duplicate_pct,
            "message": (
                f"Entity '{entity}' passed PK uniqueness ({distinct_keys:,} unique keys, 0 duplicates)."
                if passed
                else f"Entity '{entity}' FAILED PK uniqueness! Found {duplicate_count:,} duplicate keys ({duplicate_pct}%) on {primary_keys}."
            ),
        }

        self.results.append(result)
        return result
    
    def check_critical_nulls(
        self,
        df: DataFrame,
        entity: str,
        critical_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Rule 3: Critical Column Null Checks.
        Verifies that essential columns (IDs, names, FKs) contain no NULL or blank/empty values.
        Single-pass aggregation across all specified columns.
        Severity: FAIL if any critical column has null/blank values.
        """
        agg_exprs = [
            spark_count(lit(1)).alias("_total_rows")
        ] + [
            spark_sum(
                when(col(c).isNull() | (trim(col(c).cast("string")) == ""), 1).otherwise(0)
            ).alias(c)
            for c in critical_columns
        ]

        row = df.select(agg_exprs).collect()[0]
        total_rows = row["_total_rows"] or 0
        null_counts = {c: int(row[c] or 0) for c in critical_columns}
        total_violations = sum(null_counts.values())
        passed = total_violations == 0

        failed_cols = [c for c, count in null_counts.items() if count > 0]

        result = {
            "rule": "critical_nulls",
            "entity": entity,
            "snapshot_date": self.snapshot_date,
            "critical_columns": critical_columns,
            "status": "PASS" if passed else "FAIL",
            "total_rows": total_rows,
            "null_counts": null_counts,
            "total_violations": total_violations,
            "message": (
                f"Entity '{entity}' passed critical null check on columns {critical_columns}."
                if passed
                else f"Entity '{entity}' FAILED critical null check! Columns with missing/empty values: {failed_cols} (Total violations: {total_violations:,})."
            ),
        }

        self.results.append(result)
        return result

    def check_referential_integrity(
        self,
        child_df: DataFrame,
        parent_df: DataFrame,
        foreign_key: str,
        child_entity: str,
        parent_entity: str,
        parent_key: Optional[str] = None,
        max_orphan_threshold_pct: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Rule 4: Referential Integrity via Left-Anti Join.
        Identifies orphan records in child_df that have no matching parent in parent_df.
        
        Severity thresholds:
          - orphan_count == 0                       -> PASS
          - 0 < orphan_rate_pct <= max_threshold    -> WARN
          - orphan_rate_pct > max_threshold         -> FAIL
        """
        parent_pk = parent_key or foreign_key

        # Left-anti join returns strictly the orphan child records
        orphans_df = child_df.join(
            parent_df,
            child_df[foreign_key] == parent_df[parent_pk],
            how="left_anti",
        )

        orphan_count = orphans_df.count()
        total_child_rows = child_df.count()

        orphan_rate_pct = (
            round((orphan_count / total_child_rows) * 100.0, 2)
            if total_child_rows > 0
            else 0.0
        )

        if orphan_count == 0:
            status = "PASS"
        elif orphan_rate_pct <= max_orphan_threshold_pct:
            status = "WARN"
        else:
            status = "FAIL"

        result = {
            "rule": "referential_integrity",
            "entity": child_entity,
            "parent_entity": parent_entity,
            "snapshot_date": self.snapshot_date,
            "foreign_key": foreign_key,
            "status": status,
            "total_rows": total_child_rows,
            "orphan_count": orphan_count,
            "orphan_rate_pct": orphan_rate_pct,
            "threshold_pct": max_orphan_threshold_pct,
            "message": (
                f"Referential integrity PASSED: All {total_child_rows:,} '{child_entity}' records have valid '{parent_entity}' parents."
                if orphan_count == 0
                else f"Referential integrity {status}: {orphan_count:,} '{child_entity}' records ({orphan_rate_pct}%) have no matching '{parent_entity}' on key '{foreign_key}'."
            ),
        }

        self.results.append(result)
        return result

    def check_value_ranges(
        self,
        df: DataFrame,
        entity: str,
        range_conditions: Optional[Dict[str, tuple]] = None,
        numeric_bounds: Optional[Dict[str, tuple]] = None,
        allowed_values: Optional[Dict[str, List[Any]]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Rule 5: Value / Range Validation.
        Verifies numeric metrics stay within physically and logically sensible bounds,
        and optionally verifies categorical values against allowed sets.
        Format for range_conditions:
            {"column_name": (min_value, max_value)}
            Use None for unbounded sides, e.g. {"track_number": (1, None)}.
            
        Single-pass aggregation across all specified columns.
        Severity: FAIL if any values fall outside bounds.
        """
        conditions = range_conditions or numeric_bounds or {}
        agg_exprs = [spark_count(lit(1)).alias("_total_rows")]
        col_conditions = {}

        for col_name, bounds in conditions.items():
            min_val, max_val = bounds
            col_expr = col(col_name)
            invalid_cond = None

            if min_val is not None and max_val is not None:
                invalid_cond = (col_expr < min_val) | (col_expr > max_val)
            elif min_val is not None:
                invalid_cond = col_expr < min_val
            elif max_val is not None:
                invalid_cond = col_expr > max_val

            if invalid_cond is not None:
                # Only check non-null values
                full_cond = col_expr.isNotNull() & invalid_cond
                col_conditions[col_name] = full_cond
                agg_exprs.append(
                    spark_sum(when(full_cond, 1).otherwise(0)).alias(col_name)
                )

        if allowed_values:
            for col_name, valid_vals in allowed_values.items():
                col_expr = col(col_name)
                invalid_cond = col_expr.isNotNull() & (~col_expr.isin(valid_vals))
                col_conditions[col_name] = invalid_cond
                agg_exprs.append(
                    spark_sum(when(invalid_cond, 1).otherwise(0)).alias(f"_cat_{col_name}")
                )

        row = df.select(agg_exprs).collect()[0]
        total_rows = row["_total_rows"] or 0
        violations_by_col = {}
        for c in col_conditions:
            key = f"_cat_{c}" if allowed_values and c in allowed_values and f"_cat_{c}" in row else c
            violations_by_col[c] = int(row[key] or 0)

        total_violations = sum(violations_by_col.values())
        passed = total_violations == 0

        failed_cols = [c for c, count in violations_by_col.items() if count > 0]

        result = {
            "rule": "value_ranges",
            "entity": entity,
            "snapshot_date": self.snapshot_date,
            "range_conditions": {k: list(v) for k, v in conditions.items()},
            "allowed_values": allowed_values or {},
            "status": "PASS" if passed else "FAIL",
            "total_rows": total_rows,
            "violations_by_col": violations_by_col,
            "total_violations": total_violations,
            "message": (
                f"Entity '{entity}' passed value/range validation."
                if passed
                else f"Entity '{entity}' FAILED value/range validation! Violating columns: {failed_cols} (Total violations: {total_violations:,})."
            ),
        }

        self.results.append(result)
        return result

    def quarantine_records(
        self,
        df: DataFrame,
        entity: str,
        rule_name: str,
        reason: str,
    ) -> Optional[str]:
        """
        Step 7: Quarantine Routing.
        Stamps invalid records with DQ audit lineage and writes to data/quarantine/{entity}/.
        Only writes if there are actual rows to quarantine.
        """
        if df.limit(1).count() == 0:
            return None

        quarantined_df = (
            df
            .withColumn("dq_rule", lit(rule_name))
            .withColumn("dq_reason", lit(reason))
            .withColumn("snapshot_date", lit(self.snapshot_date))
            .withColumn("quarantined_at", current_timestamp())
        )

        output_path = os.path.join(self.quarantine_base_dir, entity)
        (
            quarantined_df.write
            .mode("append")
            .partitionBy("snapshot_date")
            .parquet(output_path)
        )
        return output_path

    def generate_report(self) -> Dict[str, Any]:
        """
        Step 8: Consolidates all rule results into an audit-ready summary report.
        Determines overall status:
          - Any FAIL  -> overall FAIL
          - Any WARN  -> overall WARN
          - All PASS  -> overall PASS
        """
        total_checks = len(self.results)
        failed_checks = [r for r in self.results if r.get("status") == "FAIL"]
        warning_checks = [r for r in self.results if r.get("status") == "WARN"]
        passed_checks = [r for r in self.results if r.get("status") == "PASS"]

        if failed_checks:
            overall_status = "FAIL"
        elif warning_checks:
            overall_status = "WARN"
        else:
            overall_status = "PASS"

        return {
            "snapshot_date": self.snapshot_date,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "overall_status": overall_status,
            "summary": {
                "total_checks": total_checks,
                "passed": len(passed_checks),
                "warnings": len(warning_checks),
                "failed": len(failed_checks),
                "success_rate_pct": (
                    round((len(passed_checks) / total_checks) * 100.0, 2)
                    if total_checks > 0
                    else 100.0
                ),
            },
            "results": self.results,
        }

    def save_report(self, report: Optional[Dict[str, Any]] = None) -> str:
        """
        Serializes and saves the DQ report to JSON on disk.
        """
        report_data = report or self.generate_report()
        report_dir = os.path.join(self.report_base_dir, f"snapshot_date={self.snapshot_date}")
        os.makedirs(report_dir, exist_ok=True)
        report_path = os.path.join(report_dir, "dq_report.json")

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)

        return report_path


def run_quality_gate(
    snapshot_date: str = "2026-08-31",
    silver_base_dir: str = "data/silver",
    spark: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Evaluates conformed Silver Parquet tables against the 5 core Data Quality rules.
    Returns structured report with 'overall_status': PASS, WARN, or FAIL.
    """
    from src.transform.spark_session import get_spark_session
    own_spark = False
    if spark is None:
        spark = get_spark_session(app_name=f"Spotify_DQ_Gate_{snapshot_date}")
        own_spark = True

    try:
        checker = DataQualityChecker(snapshot_date=snapshot_date)
        art_path = os.path.join(silver_base_dir, "artists")
        alb_path = os.path.join(silver_base_dir, "albums")
        trk_path = os.path.join(silver_base_dir, "tracks")

        # Partition pruning read
        art_df = spark.read.parquet(art_path).filter(col("snapshot_date") == snapshot_date).cache()
        alb_df = spark.read.parquet(alb_path).filter(col("snapshot_date") == snapshot_date).cache()
        trk_df = spark.read.parquet(trk_path).filter(col("snapshot_date") == snapshot_date).cache()

        # Rule 1: Completeness
        checker.check_completeness(art_df, "artists", min_expected=1)
        checker.check_completeness(alb_df, "albums", min_expected=1)
        checker.check_completeness(trk_df, "tracks", min_expected=1)

        # Rule 2: Uniqueness
        checker.check_uniqueness(art_df, "artists", ["artist_id"])
        checker.check_uniqueness(alb_df, "albums", ["album_id"])
        checker.check_uniqueness(trk_df, "tracks", ["track_id"])

        # Rule 3: Critical Column Nulls
        checker.check_critical_nulls(art_df, "artists", ["artist_id", "artist_name"])
        checker.check_critical_nulls(alb_df, "albums", ["album_id", "album_name", "artist_id"])
        checker.check_critical_nulls(trk_df, "tracks", ["track_id", "track_name", "album_id", "artist_id"])

        # Rule 4: Referential Integrity
        checker.check_referential_integrity(
            child_df=alb_df,
            parent_df=art_df,
            foreign_key="artist_id",
            child_entity="albums",
            parent_entity="artists",
            max_orphan_threshold_pct=5.0,
        )
        checker.check_referential_integrity(
            child_df=trk_df,
            parent_df=alb_df,
            foreign_key="album_id",
            child_entity="tracks",
            parent_entity="albums",
            max_orphan_threshold_pct=5.0,
        )
        checker.check_referential_integrity(
            child_df=trk_df,
            parent_df=art_df,
            foreign_key="artist_id",
            child_entity="tracks",
            parent_entity="artists",
            max_orphan_threshold_pct=5.0,
        )

        # Rule 5: Value Validation
        checker.check_value_ranges(
            trk_df, "tracks",
            range_conditions={
                "duration_ms": (1, 7200000),
                "track_number": (1, None),
                "disc_number": (1, None),
            },
        )

        report = checker.generate_report()
        checker.save_report(report)
        return report
    finally:
        if own_spark:
            spark.stop()


# Reusable engine alias
DataQualityEngine = DataQualityChecker


