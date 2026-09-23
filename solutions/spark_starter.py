"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
File: starter_files/spark_starter.py
Pillar: Big Data Processing — Task 3.1
=============================================================
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
# STEP 1 — Initialise Spark Session
# ==============================================================================

def get_spark_session() -> SparkSession:
    """
    Create and return an optimized local SparkSession.
    - Sets shuffle partitions to 8 to avoid hundreds of empty local shuffle tasks.
    - Binds local driver memory to 4GB.
    - Configures UTC timezone to prevent timestamp drift during JSON date parsing.
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
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ==============================================================================
# STEP 2 — 3.1a: Load and Parse Events
# ==============================================================================

def load_events(spark: SparkSession, events_dir: str):
    """
    Load all JSONL files from events_stream using an explicit schema.
    Uses MapType(StringType, StringType) for flexible JSON payload parsing.
    """
    event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("user_id", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("payload", MapType(StringType(), StringType()), True)
    ])

    wildcard_path = os.path.join(events_dir, "events_*.jsonl")
    df = spark.read.schema(event_schema).json(wildcard_path)
    return df


# ==============================================================================
# STEP 3 — 3.1b: Validate and Clean
# ==============================================================================

def validate_events(df):
    """
    Validate, deduplicate, and enrich raw events.
    - Drops records where event_id or user_id is null.
    - Deduplicates by event_id keeping the earliest occurrence by timestamp.
    - Derives event_date, event_hour, and event_month.
    - Caches the clean dataset in memory to accelerate downstream aggregation jobs.
    """
    raw_count = df.count()
    print(f"Total raw events loaded: {raw_count:,}")

    # 1. Drop rows where event_id or user_id is null
    df_valid = df.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull())
    valid_count = df_valid.count()
    print(f"Rows dropped due to null event_id/user_id: {raw_count - valid_count:,}")

    # 2. Deduplicate event_ids (keep earliest occurrence by timestamp)
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("timestamp").asc())
    df_dedup = (
        df_valid
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 3. Add temporal partition and analytical columns
    df_clean = (
        df_dedup
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .withColumn("event_hour", F.hour(F.col("timestamp")))
        .withColumn("event_month", F.date_format(F.col("timestamp"), "yyyy-MM"))
    ).cache()

    # Materialize cache once
    clean_count = df_clean.count()
    print(f"Rows dropped due to duplicate event_id: {valid_count - clean_count:,}")
    print(f"Total clean events retained: {clean_count:,}")

    return df_clean, clean_count


# ==============================================================================
# STEP 4 — 3.1c: Five Aggregated Output Tables
# ==============================================================================

def project_activity_summary(df):
    """
    Table 1: Project Activity Summary.
    Aggregates operational activity per project (excludes null project_ids/logins).
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
    Table 2: User Activity Summary.
    Measures individual user engagement, active window, and unique projects touched.
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
    Table 3: Escalation Resolution Log.
    Left-joins escalation_raised with subsequent escalation_resolved events on project_id.
    Calculates resolution_time_hours and extracts nested payload attributes.
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

    # Left join to pair each raised escalation with the earliest resolution after it
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
                F.round((F.unix_timestamp("resolved_at") - F.unix_timestamp("raised_at")) / 3600.0, 2)
            ).otherwise(F.lit(None))
        )
        .select(
            "event_id", "project_id", "raised_by", "raised_at",
            "severity", "resolved", "resolved_at", "resolved_by",
            "resolution_time_hours"
        )
    )
    return joined


def daily_event_volume(df):
    """
    Table 4: Daily Event Volume & Running Cumulative Totals.
    Calculates cumulative event counts partitioned by event_type ordered by date.
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
    Table 5: Peak Operational Usage Windows.
    Top 20 operational hours ranked by total throughput.
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
# STEP 5 — 3.1d: Write Outputs to Parquet
# ==============================================================================

def write_parquet(df, name: str, output_dir: str):
    """
    Write DataFrame to Parquet format in overwrite mode.
    - Partitions daily_event_volume by event_date.
    - Partitions escalation_log by severity.
    - Coalesces small tables to 1 file to prevent partition file sprawl.
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
    count = df.count()
    print(f"Successfully written {count:,} rows to: {path}")
    return count


# ==============================================================================
# PIPELINE ENTRY POINT & PERFORMANCE BENCHMARK (3.1e)
# ==============================================================================

def run_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_total = time.time()

    print("\n" + "=" * 70)
    print("TASK 3.1: APACHE SPARK BIG DATA EVENTS PROCESSING PIPELINE")
    print("=" * 70)

    spark = get_spark_session()

    # 1. Ingestion
    t0 = time.time()
    raw = load_events(spark, EVENTS_DIR)
    t_load = time.time() - t0

    # 2. Validation & Deduplication
    t0 = time.time()
    clean, total_clean_rows = validate_events(raw)
    t_clean = time.time() - t0

    # 3. Aggregations & Outputs
    timings = {}
    row_counts = {}

    stages = [
        ("project_activity_summary", project_activity_summary),
        ("user_activity_summary", user_activity_summary),
        ("escalation_log", escalation_log),
        ("daily_event_volume", daily_event_volume),
        ("peak_usage_analysis", peak_usage_analysis),
    ]

    for name, func in stages:
        t0 = time.time()
        res_df = func(clean)
        row_counts[name] = write_parquet(res_df, name, OUTPUT_DIR)
        timings[name] = time.time() - t0

    # Clean cached memory
    clean.unpersist()

    total_time = time.time() - start_total
    events_per_sec = total_clean_rows / total_time if total_time > 0 else 0

    # 4. Step 3.1e Performance Baseline Output
    print("\n" + "=" * 70)
    print("SPARK EXECUTION PERFORMANCE REPORT (TASK 3.1e)")
    print("=" * 70)
    print(f"Total Events Ingested & Cleaned : {total_clean_rows:,}")
    print(f"Total Execution Time            : {total_time:.2f} seconds")
    print(f"Processing Throughput           : {events_per_sec:,.1f} events/second")
    print("-" * 70)
    print(f"{'Pipeline Stage':<30} {'Runtime (s)':<15} {'Output Rows':<15}")
    print("-" * 70)
    print(f"{'Ingestion (JSON parsing)':<30} {t_load:<15.2f} {'--':<15}")
    print(f"{'Cleaning & Deduplication':<30} {t_clean:<15.2f} {total_clean_rows:<15,}")
    for name in timings:
        print(f"{name:<30} {timings[name]:<15.2f} {row_counts[name]:<15,}")
    print("=" * 70)

    spark.stop()
    print("Pipeline completed successfully.\n")


if __name__ == "__main__":
    run_pipeline()
