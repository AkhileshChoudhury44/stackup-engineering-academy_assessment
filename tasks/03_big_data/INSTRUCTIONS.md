# Pillar 3 — Big Data Processing

> **Time allocation:** ~4 hours  
> **Starter files:** `starter_files/spark_starter.py`, `starter_files/kafka_starter.py`, `starter_files/airflow_dag_starter.py`

---

## Context

The project management platform now emits a continuous stream of user events. With **100,000 events across 12 monthly files** (`events_2025_01.jsonl` through `events_2025_12.jsonl`), the batch ETL approach from Pillar 2 is no longer sufficient. You build the big data layer: distributed processing with Spark, real-time streaming with Kafka, and orchestrated scheduling with Airflow.

> **Volume note:** Each monthly file contains ~8,300 events. The total dataset is ~20 MB. This is large enough that you'll see real performance differences between optimised and naive Spark code.

---

## Task 3.1 — Apache Spark: Process events at scale

**File:** `starter_files/spark_starter.py`

**Dataset:** All 12 files in `datasets/events_stream/events_2025_*.jsonl`

---

**3.1a — Load and parse**

- Define the schema explicitly using `StructType` — do not use `inferSchema` on 100K rows
- Load **all 12 monthly files** using a wildcard path: `datasets/events_stream/events_*.jsonl`
- The `payload` field is a nested JSON object — use `MapType(StringType, StringType)` to keep it flexible
- Parse `timestamp` correctly as `TimestampType`
- Print the row count after loading (should be ~100,000)

---

**3.1b — Validate and clean**

- Drop rows where `event_id` or `user_id` is null
- Remove duplicate `event_id` values (keep first occurrence by timestamp)
- Add `event_date` (date only) and `event_hour` (hour of day) derived columns
- Add `event_month` for partitioning later
- Log row counts before and after each drop operation

---

**3.1c — Five aggregated output tables**

Implement each aggregation. Outputs feed the executive analytics dashboard.

---

**Table 1: project_activity_summary**

For each project:
- `total_events` — all events linked to this project
- `escalation_count` — events where `event_type = 'escalation_raised'`
- `task_completions` — events where `event_type = 'task_completed'`
- `document_uploads` — events where `event_type = 'document_uploaded'`
- `last_event_timestamp` — most recent timestamp
- `unique_users` — distinct users who triggered events
- `unique_event_types` — distinct event types touching this project

Exclude null project_ids (logins).

---

**Table 2: user_activity_summary**

For each user:
- `login_count` — events where `event_type = 'login'`
- `logout_count` — events where `event_type = 'logout'`
- `actions_taken` — all events excluding login/logout
- `projects_touched` — distinct projects (excluding nulls)
- `first_active` — earliest timestamp
- `last_active` — latest timestamp
- `active_days` — distinct event_date count

---

**Table 3: escalation_log**

Build a complete escalation log by joining `escalation_raised` and `escalation_resolved` events.

- Extract `severity` from raised events' payload
- Extract `resolved_by` from resolved events' payload
- Compute `resolution_time_hours` = (resolved_at − raised_at) / 3600
- Set `resolved = True/False` based on existence of matching resolution
- For unresolved escalations, set `resolved_at` and `resolved_by` to NULL

**Hint:** Use a left join between raised and resolved DataFrames on `project_id`, ordered by timestamp.

---

**Table 4: daily_event_volume**

- Group by `event_date` and `event_type`
- Count events per group
- Add `cumulative_count` per event type (running total)
- Sort by `event_date` ascending, `event_count` descending

**Hint:** Use `Window.partitionBy("event_type").orderBy("event_date")` with `sum().over(...)` for cumulative.

---

**Table 5: peak_usage_analysis**

For each `event_date` × `event_hour` combination:
- `total_events` — count
- `unique_users` — distinct users active
- `event_types_per_hour` — distinct event types

Sort by `total_events` descending and return top 20 hours.

This helps identify peak usage windows.

---

**3.1d — Write outputs as Parquet**

- Write each table to `outputs/spark/<table_name>/`
- Use overwrite mode
- Partition `daily_event_volume` by `event_date`
- Partition `escalation_log` by `severity`
- Print row count after each write
- Add `coalesce(1)` for small output tables to avoid hundreds of part files

---

**3.1e — Performance baseline**

Add a section at the end of your pipeline that prints:
- Total execution time (use `time.time()`)
- Per-aggregation timing
- Total rows processed
- A simple "events processed per second" metric

This proves your pipeline runs at reasonable speed for the data volume.

---

## Task 3.2 — Apache Kafka: Real-time streaming

**File:** `starter_files/kafka_starter.py`

**Prerequisites:** Kafka running (`docker compose up -d`)

---

**3.2a — Setup**

Create two topics:
- `presight.project.events` — 3 partitions
- `presight.escalations.critical` — 1 partition

---

**3.2b — Producer**

Build a Kafka producer that:
- Reads events from `datasets/events_stream/events_2025_01.jsonl` (8,333 events)
- Adds `produced_at` timestamp to each message
- Sends to `presight.project.events` using `event_type` as message key
- Sleeps 50ms between messages to simulate throughput
- Logs every 100th message sent
- Tracks total sent count

---

**3.2c — Consumer**

Build a consumer that:
- Subscribes to `presight.project.events`
- Logs every 100th consumed message (don't log all 8,333)
- Accumulates count per `event_type`

---

**3.2d — Escalation forwarding**

Within the consumer:
- If `event_type = 'escalation_raised'` AND `payload.severity = 'Critical'`
  → Forward to `presight.escalations.critical`
- Log forwarded count

---

**3.2e — Summary output**

After consuming, write `outputs/kafka/summary.json`:
- Run timestamp
- Total messages consumed
- Count per event_type
- Critical escalations forwarded
- Throughput (messages/second)

---

## Task 3.3 — Apache Airflow: Orchestrate the pipeline

**File:** `starter_files/airflow_dag_starter.py`

---

**3.3a — DAG configuration**

- `dag_id`: `presight_etl_pipeline`
- Schedule: daily at **06:00 UAE time** (Asia/Dubai = UTC+4)
- `catchup = False`
- `max_active_runs = 1`
- `retries = 2`, `retry_delay = 5 minutes`
- Tags: `['presight', 'etl', 'assessment']`

---

**3.3b — Tasks**

| Task ID | Function | Notes |
|---|---|---|
| `extract_projects` | `load_projects()` | Push row count to XCom |
| `extract_employees` | `load_employees()` | Push row count to XCom |
| `extract_transactions` | `load_transactions()` | Push row count to XCom |
| `validate_data_quality` | `run_data_quality_checks()` | **DQ gate** — fail on critical issues |
| `transform_and_enrich` | All transform functions | Push clean row counts |
| `load_to_output` | `write_outputs()` | Log paths written |
| `generate_pipeline_report` | Custom | Pull XComs, write report |

---

**3.3c — Dependencies**

```
start
  ├── extract_projects ──┐
  ├── extract_employees ─┼──→ validate_dq → transform → load → report → end
  └── extract_transactions ┘
```

Extract tasks run in **parallel**. DQ gate must complete before downstream tasks.

---

**3.3d — DQ gate behaviour**

`validate_data_quality` must:
- Reload all three datasets
- Run `run_data_quality_checks()` on each
- Raise `ValueError` with descriptive message if completeness < 80% on a key column
- If gate fails, downstream tasks must NOT execute

---

**3.3e — Pipeline report via XCom**

`generate_pipeline_report` must pull XCom values and write `outputs/pipeline_report_{execution_date}.txt`:
- Raw vs. clean row counts per dataset
- DQ results summary
- Files written

---

## Completion checklist

- [ ] Spark pipeline runs end-to-end on all 12 monthly files
- [ ] Five Parquet output tables in `outputs/spark/`
- [ ] `daily_event_volume` partitioned by `event_date`, `escalation_log` by `severity`
- [ ] Pipeline timing metrics printed
- [ ] Kafka topics created, messages produced and consumed
- [ ] Critical escalations forwarded to `presight.escalations.critical`
- [ ] `outputs/kafka/summary.json` written with throughput stats
- [ ] Airflow DAG appears in UI with correct schedule
- [ ] DQ gate correctly blocks downstream tasks on failure
- [ ] Pipeline report written via XCom on successful run
