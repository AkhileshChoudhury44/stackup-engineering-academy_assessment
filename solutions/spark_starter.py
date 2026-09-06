"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: spark_starter.py
Pillar: Big Data Processing — Task 3.1
=============================================================
"""

import os
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
# STEP 1 — Initialise Spark
# ==============================================================================

def get_spark_session() -> SparkSession:
    """
    Create and return a local SparkSession.
    # TUNING EXPLANATION: 'spark.sql.shuffle.partitions' is explicitly tuned down to 8 
    # to optimize resource utilization on local single-node development host hardware.
    # Scale this up dynamically (e.g., 200+) when deploying to distributed enterprise cloud cluster instances.
    """
    spark = (
        SparkSession.builder
        .appName("PresightEventsProcessing")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==============================================================================
# STEP 2 — Load events
# ==============================================================================

def load_events(spark: SparkSession, events_dir: str):
    """
    Load all JSONL files from the events_stream directory with an explicit schema.
    """
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("payload", MapType(StringType(), StringType()), True)
    ])

    wildcard_path = os.path.join(events_dir, "events_*.jsonl")
    df = spark.read.schema(event_schema).json(wildcard_path)
    print(f"Loaded raw events count: {df.count():,}")
    return df


# ==============================================================================
# STEP 3 — Validate and clean
# ==============================================================================

def validate_events(df):
    """
    Validate and clean the raw events DataFrame.
    # DATA METRIC CACHING: Caching the clean DataFrame breaks the lineage chain 
    # and optimizes the downstream read throughput across all 5 separate aggregation jobs.
    # Note: For long-running clusters, make sure to add clean.unpersist() before spark.stop().
    """
    raw_count = df.count()

    # Drop rows where event_id or user_id is null
    df_valid = df.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
    null_dropped = raw_count - df_valid.count()
    print(f"Rows dropped due to null event_id/user_id: {null_dropped:,}")

    # Drop duplicate event_ids (keep first occurrence by timestamp)
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc())
    df_dedup = (
        df_valid
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    dup_dropped = df_valid.count() - df_dedup.count()
    print(f"Rows dropped due to duplicate event_id: {dup_dropped:,}")

    # Add temporal columns
    df_clean = (
        df_dedup
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .withColumn("event_hour", F.hour(F.col("timestamp")))
        .withColumn("event_month", F.date_format(F.col("timestamp"), "yyyy-MM"))
    ).cache()

    print(f"Clean events count: {df_clean.count():,}")
    return df_clean


# ==============================================================================
# STEP 4 — Aggregations
# ==============================================================================

def project_activity_summary(df):
    """
    Produce a per-project activity summary.
    """
    return (
        df
        .filter(F.col("project_id").isNotNull() & (F.col("project_id") != ""))
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
    Produce a per-user activity summary.
    """
    return (
        df
        .groupBy("user_id")
        .agg(
            F.count(F.when(F.col("event_type") == "login", 1)).alias("login_count"),
            F.count(F.when(F.col("event_type") == "logout", 1)).alias("logout_count"),
            F.count(F.when(~F.col("event_type").isin("login", "logout"), 1)).alias("actions_taken"),
            F.countDistinct(F.when(F.col("project_id").isNotNull() & (F.col("project_id") != ""), F.col("project_id"))).alias("projects_touched"),
            F.min("timestamp").alias("first_active"),
            F.max("timestamp").alias("last_active"),
            F.countDistinct("event_date").alias("active_days")
        )
        .sort(F.col("actions_taken").desc())
    )


def escalation_log(df):
    """
    Build a resolved escalation log by joining escalation_raised
    and escalation_resolved events.
    # PERFORMANCE WARNING: This join relies on chronological inequalities to match resolutions.
    # For large datasets, look into applying a broadcast join configuration if the resolved 
    # dimension volume remains small, preventing costly cross-node dataset shuffling.
    """
    raised = (
        df
        .filter(F.col("event_type") == "escalation_raised")
        .select(
            F.col("event_id"),
            F.col("project_id"),
            F.col("user_id").alias("raised_by"),
            F.col("timestamp").alias("raised_at"),
            F.col("payload")["severity"].alias("severity")
        )
    )

    resolved = (
        df
        .filter(F.col("event_type") == "escalation_resolved")
        .select(
            F.col("project_id").alias("res_project_id"),
            F.col("timestamp").alias("resolved_at"),
            F.col("payload")["resolved_by"].alias("resolved_by")
        )
    )

    # Left join on project_id matching subsequent resolution
    joined = (
        raised
        .join(
            resolved,
            (raised.project_id == resolved.res_project_id) & (resolved.resolved_at >= raised.raised_at),
            how="left"
        )
        .withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy("event_id").orderBy(F.col("resolved_at").asc()))
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn", "res_project_id")
        .withColumn("resolved", F.col("resolved_at").isNotNull())
        .withColumn(
            "resolution_time_hours",
            F.when(
                F.col("resolved"),
                (F.unix_timestamp("resolved_at") - F.unix_timestamp("raised_at")) / 3600.0
            ).otherwise(F.lit(None))
        )
        .withColumn("severity", F.coalesce(F.col("severity"), F.lit("Medium")))
    )
    return joined


def daily_event_volume(df):
    """
    Produce a daily event volume breakdown by event type with cumulative counts.
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
    Table 5: Top 20 operational hours by event volume.
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
# STEP 5 — Write outputs
# ==============================================================================

def write_parquet(df, name: str, output_dir: str):
    """
    Write a DataFrame to Parquet.
    # ARCHITECTURAL DESIGN NOTE: coalesce(1) forces the output into a single clean CSV/Parquet 
    # file rather than generating hundreds of tiny partition snippets.
    # CRITICAL: Discard or modify this condition if aggregation rows exceed 10 million tracks.
    """
    path = os.path.join(output_dir, name)
    writer = df.write.mode("overwrite")

    if name == "daily_event_volume":
        writer = writer.partitionBy("event_date")
    elif name == "escalation_log":
        writer = writer.partitionBy("severity")
    else:
        df = df.coalesce(1)
        writer = df.write.mode("overwrite")

    writer.parquet(path)
    written_count = df.count()
    print(f"Written {written_count:,} rows to {path}")
    return written_count


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_time = time.time()

    spark = get_spark_session()

    t0 = time.time()
    raw = load_events(spark, EVENTS_DIR)
    t_load = time.time() - t0

    t0 = time.time()
    clean = validate_events(raw)
    total_clean_rows = clean.count()
    t_clean = time.time() - t0

    # Execute aggregations with per-task timing
    timings = {}

    t0 = time.time()
    proj_summary = project_activity_summary(clean)
    write_parquet(proj_summary, "project_activity_summary", OUTPUT_DIR)
    timings["project_activity_summary"] = time.time() - t0

    t0 = time.time()
    user_summary = user_activity_summary(clean)
    write_parquet(user_summary, "user_activity_summary", OUTPUT_DIR)
    timings["user_activity_summary"] = time.time() - t0

    t0 = time.time()
    esc_log = escalation_log(clean)
    write_parquet(esc_log, "escalation_log", OUTPUT_DIR)
    timings["escalation_log"] = time.time() - t0

    t0 = time.time()
    daily_vol = daily_event_volume(clean)
    write_parquet(daily_vol, "daily_event_volume", OUTPUT_DIR)
    timings["daily_event_volume"] = time.time() - t0

    t0 = time.time()
    peak_usage = peak_usage_analysis(clean)
    write_parquet(peak_usage, "peak_usage_analysis", OUTPUT_DIR)
    timings["peak_usage_analysis"] = time.time() - t0

    total_time = time.time() - start_time
    throughput = total_clean_rows / total_time if total_time > 0 else 0

    print("\n" + "=" * 60)
    print("SPARK EXECUTION PERFORMANCE REPORT")
    print("=" * 60)
    print(f"Total Rows Processed:        {total_clean_rows:,}")
    print(f"Total Execution Time:        {total_time:.2f} seconds")
    print(f"Throughput:                  {throughput:,.1f} events/second")
    print("Per-stage timings:")
    print(f"  - Ingestion:               {t_load:.2f}s")
    print(f"  - Cleaning/Deduplication:  {t_clean:.2f}s")
    for name, dur in timings.items():
        print(f"  - {name:<26} {dur:.2f}s")
    print("=" * 60)

    spark.stop()
    print("Spark pipeline complete.")


if __name__ == "__main__":
    run_pipeline()