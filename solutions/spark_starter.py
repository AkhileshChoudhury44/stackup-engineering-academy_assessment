"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
File: starter_files/spark_starter.py
Pillar: Big Data Processing — Task 3.1
=============================================================

Tasks Covered:
  - Task 3.1a: Explicit schema definition and wildcard loading (~100k rows)
  - Task 3.1b: Validation, deduplication, temporal enrichment, and logging
  - Task 3.1c: 5 aggregated output tables for executive analytics
  - Task 3.1d: Optimized Parquet writes with partitioning and coalescence
  - Task 3.1e: Pipeline telemetry and throughput benchmarking
"""

import os
import sys
import time
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS_DIR = os.path.join(BASE_DIR, "datasets", "events_stream")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "spark")


# ==============================================================================
# STEP 1 — Initialise Spark (Task 3.1)
# ==============================================================================

def get_spark_session() -> SparkSession:
    """
    Create and return an optimized local SparkSession.
    - Application Name: PresightEventsProcessing
    - Master: local[*] utilizing all host cores
    - Driver Memory: 4g to handle in-memory aggregations and cache
    - Shuffle Partitions: 8 (tuned for local hardware to eliminate empty shuffle tasks)
    - Timezone: UTC to preserve timestamp integrity during JSON parsing
    """
    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessing")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# STEP 2 — 3.1a: Load and Parse Events
# ==============================================================================

def load_events(spark: SparkSession, events_dir: str):
    """
    Load all 12 monthly JSONL files using explicit StructType schema.
    Payload parsed via MapType(StringType, StringType) for schema agility.
    """
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("user_id", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("payload", MapType(StringType(), StringType()), True)
    ])

    wildcard_path = os.path.join(events_dir, "events_2025_*.jsonl")
    df = spark.read.schema(event_schema).json(wildcard_path)

    raw_count = df.count()
    print(f"Total raw events loaded (wildcard path): {raw_count:,}")
    return df


# ==============================================================================
# STEP 3 — 3.1b: Validate and Clean
# ==============================================================================

def validate_events(df):
    """
    Validate, deduplicate, and enrich raw events.
    - Drops rows where event_id or user_id is null.
    - Deduplicates by event_id, keeping earliest occurrence by timestamp.
    - Derives event_date, event_hour, and event_month.
    - Logs row counts before and after each drop step.
    - Caches clean DataFrame to avoid redundant re-reading across 5 aggregations.
    """
    raw_count = df.count()
    print(f"\n[Validation] Starting raw event validation on {raw_count:,} records...")

    # 1. Drop rows where event_id or user_id is null
    df_valid = df.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
    valid_count = df_valid.count()
    print(f"[Validation] Rows dropped due to null event_id/user_id: {raw_count - valid_count:,}")

    # 2. Deduplicate event_ids (keep earliest occurrence by timestamp)
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc())
    df_dedup = (
        df_valid
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 3. Add derived temporal columns
    df_clean = (
        df_dedup
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .withColumn("event_hour", F.hour(F.col("timestamp")))
        .withColumn("event_month", F.date_format(F.col("timestamp"), "yyyy-MM"))
    ).cache()

    # Materialize in-memory cache once
    clean_count = df_clean.count()
    print(f"[Validation] Rows dropped due to duplicate event_id: {valid_count - clean_count:,}")
    print(f"[Validation] Final clean events retained in cache: {clean_count:,}\n")

    return df_clean


# ==============================================================================
# STEP 4 — 3.1c: Five Aggregated Output Tables
# ==============================================================================

def project_activity_summary(df):
    """
    Table 1: project_activity_summary
    Aggregates project-level metrics. Excludes null/empty project_ids (e.g. platform logins).
    """
    return (
        df
        .filter(F.col("project_id").isNotNull() & (F.trim(F.col("project_id")) != ""))
        .groupBy("project_id")
        .agg(
            F.count("*").alias("total_events"),
            F.count(F.when(F.col("event_type") == "escalation_raised", 1)).alias("escalation_count"),
            F.count(F.when(F.col("event_type") == "task_completed", 1)).alias("task_completions"),
            F.count(F.when(F.col("event_type") == "document_uploaded", 1)).alias("document_uploads"),
            F.max("timestamp").alias("last_event_timestamp"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("unique_event_types")
        )
        .sort(F.col("total_events").desc())
    )


def user_activity_summary(df):
    """
    Table 2: user_activity_summary
    Profiles individual user engagement, session counts, and distinct project footprints.
    """
    return (
        df
        .groupBy("user_id")
        .agg(
            F.count(F.when(F.col("event_type") == "login", 1)).alias("login_count"),
            F.count(F.when(F.col("event_type") == "logout", 1)).alias("logout_count"),
            F.count(F.when(~F.col("event_type").isin("login", "logout"), 1)).alias("actions_taken"),
            F.countDistinct(
                F.when(F.col("project_id").isNotNull() & (F.trim(F.col("project_id")) != ""), F.col("project_id"))
            ).alias("projects_touched"),
            F.min("timestamp").alias("first_active"),
            F.max("timestamp").alias("last_active"),
            F.countDistinct("event_date").alias("active_days")
        )
        .sort(F.col("actions_taken").desc())
    )


def escalation_log(df):
    """
    Table 3: escalation_log
    Left-joins escalation_raised with earliest matching escalation_resolved event per project.
    Extracts nested payload keys, sets resolution status, and handles unresolved NULL values.
    """
    raised = (
        df
        .filter(F.col("event_type") == "escalation_raised")
        .select(
            F.col("event_id"),
            F.col("project_id"),
            F.col("user_id").alias("raised_by"),
            F.col("timestamp").alias("raised_at"),
            F.coalesce(F.element_at(F.col("payload"), "severity"), F.lit("Medium")).alias("severity")
        )
    )

    resolved = (
        df
        .filter(F.col("event_type") == "escalation_resolved")
        .select(
            F.col("project_id").alias("res_project_id"),
            F.col("timestamp").alias("resolved_at"),
            F.element_at(F.col("payload"), "resolved_by").alias("resolved_by")
        )
    )

    # Left join on project_id and chronological precedence
    order_window = Window.partitionBy("event_id").orderBy(F.col("resolved_at").asc())

    joined = (
        raised
        .join(
            resolved,
            (raised.project_id == resolved.res_project_id) & (resolved.resolved_at >= raised.raised_at),
            how="left"
        )
        .withColumn("_rn", F.row_number().over(order_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "res_project_id")
    )

    result = (
        joined
        .withColumn("resolved", F.col("resolved_at").isNotNull())
        .withColumn(
            "resolution_time_hours",
            F.when(
                F.col("resolved"),
                F.round((F.unix_timestamp("resolved_at") - F.unix_timestamp("raised_at")) / 3600.0, 2)
            ).otherwise(F.lit(None).cast("double"))
        )
        .select(
            "event_id",
            "project_id",
            "raised_by",
            "raised_at",
            "severity",
            "resolved",
            "resolved_by",
            "resolved_at",
            "resolution_time_hours"
        )
    )
    return result


def daily_event_volume(df):
    """
    Table 4: daily_event_volume
    Computes daily volume per event_type and running cumulative totals over time.
    """
    daily = (
        df
        .groupBy("event_date", "event_type")
        .agg(F.count("*").alias("event_count"))
    )

    cum_window = (
        Window
        .partitionBy("event_type")
        .orderBy(F.col("event_date").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        daily
        .withColumn("cumulative_count", F.sum("event_count").over(cum_window))
        .sort(F.col("event_date").asc(), F.col("event_count").desc())
    )


def peak_usage_analysis(df):
    """
    Table 5: peak_usage_analysis
    Identifies top 20 operational hours ranked by total event throughput.
    """
    return (
        df
        .groupBy("event_date", "event_hour")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("event_type").alias("event_types_per_hour")
        )
        .sort(F.col("total_events").desc())
        .limit(20)
    )


# ==============================================================================
# STEP 5 — 3.1d: Write outputs as Parquet
# ==============================================================================

def write_parquet(df, name: str, output_dir: str):
    """
    Write DataFrame to Parquet format in overwrite mode.
    - Partitions daily_event_volume by event_date.
    - Partitions escalation_log by severity.
    - Coalesces unpartitioned tables to 1 file to prevent partition fragmentation.
    - Returns written record count.
    """
    path = os.path.join(output_dir, name)
    writer = df.write.mode("overwrite")

    if name == "daily_event_volume":
        writer = writer.partitionBy("event_date")
    elif name == "escalation_log" and "severity" in df.columns:
        writer = writer.partitionBy("severity")
    else:
        df = df.coalesce(1)
        writer = df.write.mode("overwrite")

    writer.parquet(path)
    count = df.count()
    print(f"[Parquet Writer] Successfully persisted {count:,} rows to: {path}")
    return count


# ==============================================================================
# STEP 6 — 3.1e: Pipeline Execution & Performance Baseline
# ==============================================================================

def run_pipeline():
  os.makedirs(OUTPUT_DIR, exist_ok=True)
  pipeline_start = time.time()

  spark = get_spark_session()

  # 1. Ingestion
  t0 = time.time()
  raw = load_events(spark, EVENTS_DIR)
  t_load = time.time() - t0

  # 2. Validation & Cleaning
  t0 = time.time()
  clean = validate_events(raw)
  total_clean_rows = clean.count()
  t_clean = time.time() - t0

  # 3. Individual Aggregations and Writes
  t0 = time.time()
  proj_summary = project_activity_summary(clean)
  c1 = write_parquet(proj_summary, "project_activity_summary", OUTPUT_DIR)
  t_proj = time.time() - t0

  t0 = time.time()
  user_summary = user_activity_summary(clean)
  c2 = write_parquet(user_summary, "user_activity_summary", OUTPUT_DIR)
  t_user = time.time() - t0

  t0 = time.time()
  esc_log = escalation_log(clean)
  c3 = write_parquet(esc_log, "escalation_log", OUTPUT_DIR)
  t_esc = time.time() - t0

  t0 = time.time()
  daily_vol = daily_event_volume(clean)
  c4 = write_parquet(daily_vol, "daily_event_volume", OUTPUT_DIR)
  t_daily = time.time() - t0

  t0 = time.time()
  peak_usage = peak_usage_analysis(clean)
  c5 = write_parquet(peak_usage, "peak_usage_analysis", OUTPUT_DIR)
  t_peak = time.time() - t0

  clean.unpersist()

  # Performance summary
  total_time = time.time() - pipeline_start
  throughput = total_clean_rows / total_time if total_time > 0 else 0

  print("\n" + "=" * 60)
  print(f"Total Time: {total_time:.2f}s | Throughput: {throughput:,.1f} rows/s")
  print(f" - project_activity_summary: {t_proj:.2f}s ({c1} rows)")
  print(f" - user_activity_summary:    {t_user:.2f}s ({c2} rows)")
  print(f" - escalation_log:           {t_esc:.2f}s ({c3} rows)")
  print(f" - daily_event_volume:       {t_daily:.2f}s ({c4} rows)")
  print(f" - peak_usage_analysis:      {t_peak:.2f}s ({c5} rows)")
  print("=" * 60)

  spark.stop()
  print("Spark pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
