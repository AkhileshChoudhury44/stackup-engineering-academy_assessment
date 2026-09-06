"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: spark_starter.py
Pillar: Big Data Processing — Task 3.1
=============================================================

SCENARIO
--------
The events_stream/ folder contains a growing log of platform events emitted
by the project management system (status changes, logins, escalations,
document uploads, meetings, budget updates, task completions).

New event files will be added monthly (events_2025_01.jsonl, events_2025_02.jsonl, ...).
Your pipeline must process ALL files in the folder, not just one.

Your job is to use PySpark to process the events at scale and produce four
aggregated output tables that feed the executive analytics dashboard.

TASKS
-----
  Task 3.1a → Load and parse all JSONL files from events_stream/
  Task 3.1b → Clean and validate the event schema
  Task 3.1c → Produce four aggregated output DataFrames (see below)
  Task 3.1d → Write outputs in Parquet format to outputs/spark/

OUTPUT TABLES REQUIRED
----------------------
  1. project_activity_summary
       project_id | total_events | escalation_count | task_completions |
       last_event_timestamp | unique_users

  2. user_activity_summary
       user_id | login_count | actions_taken | projects_touched | last_active

  3. escalation_log
       event_id | project_id | raised_by | raised_at | severity |
       resolved | resolved_by | resolved_at | resolution_time_hours

  4. daily_event_volume
       event_date | event_type | event_count

HOW TO RUN
----------
  spark-submit starter_files/spark_starter.py

  Or in a notebook / local Spark session:
  exec(open('starter_files/spark_starter.py').read())
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)
import os

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

    TODO:
      - Set the app name to "PresightEventsProcessing"
      - Configure for local mode with all available cores
      - Set log level to WARN to reduce noise
    """
    # YOUR CODE HERE
    spark = None

    return spark


# ==============================================================================
# STEP 2 — Load events
# ==============================================================================

def load_events(spark: SparkSession, events_dir: str):
    """
    Load all JSONL files from the events_stream directory.

    Schema:
      event_id    STRING
      event_type  STRING
      project_id  STRING (nullable)
      user_id     STRING
      timestamp   TIMESTAMP
      payload     MAP<STRING, STRING>  ← the payload is a nested JSON object

    TODO:
      - Define the schema explicitly (do not use inferSchema)
      - Load all .jsonl files in events_dir using a wildcard path
      - Parse the timestamp field correctly
      - Cast the payload field appropriately
      - Return the raw DataFrame
    """
    # YOUR CODE HERE
    return None


# ==============================================================================
# STEP 3 — Validate and clean
# ==============================================================================

def validate_events(df):
    """
    Validate and clean the raw events DataFrame.

    TODO:
      - Drop rows where event_id or user_id is null
      - Drop duplicate event_ids (keep first occurrence)
      - Add a column: event_date (date only, derived from timestamp)
      - Add a column: event_hour (hour of day, derived from timestamp)
      - Log the count of rows dropped at each step
      - Return the cleaned DataFrame
    """
    # YOUR CODE HERE
    return df


# ==============================================================================
# STEP 4 — Aggregations
# ==============================================================================

def project_activity_summary(df):
    """
    Produce a per-project activity summary.

    Required columns:
      project_id, total_events, escalation_count, task_completions,
      last_event_timestamp, unique_users

    TODO:
      - Filter out rows where project_id is null (login events)
      - Use groupBy + agg to compute each metric
      - For escalation_count: count rows where event_type = 'escalation_raised'
      - For task_completions: count rows where event_type = 'task_completed'
      - Sort by total_events descending
    """
    # YOUR CODE HERE
    return None


def user_activity_summary(df):
    """
    Produce a per-user activity summary.

    Required columns:
      user_id, login_count, actions_taken, projects_touched, last_active

    TODO:
      - actions_taken = all events EXCLUDING logins
      - projects_touched = count of distinct project_ids per user (excluding nulls)
      - last_active = max timestamp per user
    """
    # YOUR CODE HERE
    return None


def escalation_log(df):
    """
    Build a resolved escalation log by joining escalation_raised
    and escalation_resolved events.

    Required columns:
      event_id, project_id, raised_by, raised_at, severity,
      resolved, resolved_by, resolved_at, resolution_time_hours

    TODO:
      - Filter for escalation_raised events → extract severity from payload
      - Filter for escalation_resolved events → extract resolved_by and resolution from payload
      - Join on project_id (a project can only have one open escalation at a time)
      - Compute resolution_time_hours = (resolved_at - raised_at) in hours
      - Set resolved = True/False based on whether a matching resolved event exists
    """
    # YOUR CODE HERE
    return None


def daily_event_volume(df):
    """
    Produce a daily event volume breakdown by event type.

    Required columns:
      event_date, event_type, event_count

    TODO:
      - Group by event_date and event_type
      - Sort by event_date asc, event_count desc
    """
    # YOUR CODE HERE
    return None


# ==============================================================================
# STEP 5 — Write outputs
# ==============================================================================

def write_parquet(df, name: str, output_dir: str):
    """
    Write a DataFrame to Parquet.

    TODO:
      - Write to output_dir/name/
      - Use overwrite mode
      - Partition project_activity_summary and daily_event_volume by event_date
        (for the tables that have that column)
      - Print row count after writing
    """
    path = os.path.join(output_dir, name)
    # YOUR CODE HERE
    pass


# ==============================================================================
# PIPELINE ENTRY POINT
# ==============================================================================

def run_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = get_spark_session()

    raw         = load_events(spark, EVENTS_DIR)
    clean       = validate_events(raw)

    proj_summary  = project_activity_summary(clean)
    user_summary  = user_activity_summary(clean)
    esc_log       = escalation_log(clean)
    daily_vol     = daily_event_volume(clean)

    write_parquet(proj_summary, "project_activity_summary", OUTPUT_DIR)
    write_parquet(user_summary, "user_activity_summary",    OUTPUT_DIR)
    write_parquet(esc_log,      "escalation_log",           OUTPUT_DIR)
    write_parquet(daily_vol,    "daily_event_volume",       OUTPUT_DIR)

    spark.stop()
    print("Spark pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
