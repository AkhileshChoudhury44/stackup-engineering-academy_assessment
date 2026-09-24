"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: airflow_dag_starter.py
Pillar: Big Data Processing — Task 3.3
=============================================================

SCENARIO
--------
The ETL pipeline you built in Task 2.2 currently runs manually.
The data team needs it scheduled to run automatically every day at 6:00 AM UAE time,
with proper dependency management, retries, and alerting.

You will build an Airflow DAG that orchestrates the full pipeline
including data quality gate — if DQ checks fail, downstream tasks must not run.

DAG STRUCTURE REQUIRED
-----------------------
  start
    │
    ├── extract_projects
    ├── extract_employees
    └── extract_transactions
          │
          ▼
      validate_data_quality  ← DQ gate: fails DAG if checks don't pass
          │
          ▼
      transform_and_enrich
          │
          ▼
      load_to_output
          │
          ▼
      generate_pipeline_report
          │
          ▼
        end

TASKS
-----
  Task 3.3a → Define the DAG with correct schedule and timezone
  Task 3.3b → Implement each task as a PythonOperator
  Task 3.3c → Wire up task dependencies correctly
  Task 3.3d → Add retry logic and failure alerting (email or log)
  Task 3.3e → Use XCom to pass row counts between tasks for the final report

HOW TO RUN
----------
  Ensure Airflow is running (docker-compose up -d)
  Copy this file to your Airflow dags/ folder, or mount via docker-compose.
  The DAG will appear in the Airflow UI as: presight_etl_pipeline

  Trigger manually for testing:
    airflow dags trigger presight_etl_pipeline
"""
"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Starter File: airflow_dag_starter.py
Pillar: Big Data Processing — Task 3.3
=============================================================
"""

import os
import logging
from datetime import datetime, timedelta

import pendulum
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# ── Logging Configuration ──────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ── Paths ──────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

DATASET_DIR = os.path.join(
    BASE_DIR,
    "datasets"
)

OUTPUT_DIR = os.path.join(
    BASE_DIR,
    "outputs"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==============================================================================
# STEP 1 — Task 3.3a: DAG Configuration & Timezone Setup
# ==============================================================================

# UAE timezone
local_tz = pendulum.timezone("Asia/Dubai")


default_args = {
    "owner": "presight_de_team",
    "depends_on_past": False,
    "start_date": datetime(
        2025,
        1,
        1,
        tzinfo=local_tz
    ),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ==============================================================================
# Helper Utilities
# ==============================================================================

def _load_raw_dataset(entity: str) -> pd.DataFrame:
    """
    Load entity dataset from the datasets directory.

    Supported formats:
        .csv
        .json
        .parquet
    """

    # Direct filename matching
    for ext, reader in [
        (".csv", pd.read_csv),
        (".json", pd.read_json),
        (".parquet", pd.read_parquet)
    ]:

        file_path = os.path.join(
            DATASET_DIR,
            f"{entity}{ext}"
        )

        if os.path.exists(file_path):
            logger.info(
                "Loading %s dataset from %s",
                entity,
                file_path
            )

            return reader(file_path)

    # Recursive search
    for root, _, files in os.walk(DATASET_DIR):

        for file_name in files:

            if not file_name.startswith(entity):
                continue

            full_path = os.path.join(
                root,
                file_name
            )

            if file_name.endswith(".csv"):
                return pd.read_csv(full_path)

            elif file_name.endswith(".json"):
                return pd.read_json(full_path)

            elif file_name.endswith(".parquet"):
                return pd.read_parquet(full_path)

    # Do not create fake/mock data.
    raise FileNotFoundError(
        f"Dataset '{entity}' was not found under {DATASET_DIR}"
    )


# ==============================================================================
# STEP 2 — Task 3.3b: Extraction Tasks
# ==============================================================================

def extract_projects(**context):
    """
    Extract project records and push raw row count to XCom.
    """

    ti = context["ti"]

    df = _load_raw_dataset("projects")

    row_count = int(len(df))

    logger.info(
        "Extracted projects dataset: %d records.",
        row_count
    )

    ti.xcom_push(
        key="raw_projects_count",
        value=row_count
    )

    return row_count


def extract_employees(**context):
    """
    Extract employee records and push raw row count to XCom.
    """

    ti = context["ti"]

    df = _load_raw_dataset("employees")

    row_count = int(len(df))

    logger.info(
        "Extracted employees dataset: %d records.",
        row_count
    )

    ti.xcom_push(
        key="raw_employees_count",
        value=row_count
    )

    return row_count


def extract_transactions(**context):
    """
    Extract transaction records and push raw row count to XCom.
    """

    ti = context["ti"]

    df = _load_raw_dataset("transactions")

    row_count = int(len(df))

    logger.info(
        "Extracted transactions dataset: %d records.",
        row_count
    )

    ti.xcom_push(
        key="raw_transactions_count",
        value=row_count
    )

    return row_count


# ==============================================================================
# STEP 3 — Task 3.3d: Data Quality Gate
# ==============================================================================

def validate_data_quality(**context):
    """
    Data Quality Gate.

    The pipeline fails if completeness of any critical key
    column is below 80%.
    """

    ti = context["ti"]

    # Reload datasets
    df_proj = _load_raw_dataset("projects")
    df_emp = _load_raw_dataset("employees")
    df_tx = _load_raw_dataset("transactions")

    datasets = {
        "projects": {
            "df": df_proj,
            "key_col": "project_id"
        },
        "employees": {
            "df": df_emp,
            "key_col": "employee_id"
        },
        "transactions": {
            "df": df_tx,
            "key_col": "transaction_id"
        }
    }

    dq_metrics = {}

    for name, config in datasets.items():

        df = config["df"]
        key = config["key_col"]

        # Check required key exists
        if key not in df.columns:

            raise ValueError(
                f"CRITICAL DQ FAILURE: Required column "
                f"'{key}' is missing from dataset '{name}'."
            )

        total_rows = len(df)

        # Empty dataset
        if total_rows == 0:

            raise ValueError(
                f"CRITICAL DQ FAILURE: "
                f"Dataset '{name}' contains 0 rows."
            )

        # Completeness
        non_null_count = int(
            df[key].notnull().sum()
        )

        completeness = (
            non_null_count / total_rows
        ) * 100.0

        dq_metrics[name] = {
            "key_column": key,
            "total_rows": total_rows,
            "non_null_rows": non_null_count,
            "completeness_pct": round(
                completeness,
                2
            )
        }

        logger.info(
            "DQ check - %s.%s completeness: %.2f%%",
            name,
            key,
            completeness
        )

        # Hard DQ gate
        if completeness < 80.0:

            raise ValueError(
                f"CRITICAL DQ GATE FAILURE: "
                f"Dataset '{name}' key column '{key}' "
                f"has completeness of "
                f"{completeness:.2f}%, below 80.0%."
            )

    # Push DQ results to XCom
    ti.xcom_push(
        key="dq_metrics",
        value=dq_metrics
    )

    logger.info(
        "Data Quality Gate PASSED: %s",
        dq_metrics
    )

    return dq_metrics


# ==============================================================================
# STEP 4 — Transformation & Enrichment
# ==============================================================================

def transform_and_enrich(**context):
    """
    Transform and enrich datasets.

    Clean row counts are pushed to XCom.
    """

    ti = context["ti"]

    # Load datasets
    projects = _load_raw_dataset("projects")
    employees = _load_raw_dataset("employees")
    transactions = _load_raw_dataset("transactions")

    # Basic transformation
    clean_projects = projects.dropna()
    clean_employees = employees.dropna()
    clean_transactions = transactions.dropna()

    clean_counts = {
        "projects": int(len(clean_projects)),
        "employees": int(len(clean_employees)),
        "transactions": int(len(clean_transactions))
    }

    logger.info(
        "Transformation completed: %s",
        clean_counts
    )

    # Push clean row counts to XCom
    ti.xcom_push(
        key="clean_counts",
        value=clean_counts
    )

    # Save transformed datasets temporarily for load task
    # This avoids passing DataFrames through XCom.
    temp_dir = os.path.join(
        OUTPUT_DIR,
        "_staging"
    )

    os.makedirs(
        temp_dir,
        exist_ok=True
    )

    clean_projects.to_parquet(
        os.path.join(
            temp_dir,
            "projects_clean.parquet"
        ),
        index=False
    )

    clean_employees.to_parquet(
        os.path.join(
            temp_dir,
            "employees_clean.parquet"
        ),
        index=False
    )

    clean_transactions.to_parquet(
        os.path.join(
            temp_dir,
            "transactions_clean.parquet"
        ),
        index=False
    )

    logger.info(
        "Transformed datasets staged under %s",
        temp_dir
    )

    return clean_counts


# ==============================================================================
# STEP 5 — Load to Output
# ==============================================================================

def load_to_output(**context):
    """
    Load transformed datasets into final output Parquet files.
    """

    ti = context["ti"]

    staging_dir = os.path.join(
        OUTPUT_DIR,
        "_staging"
    )

    # Read transformed datasets
    projects_clean = pd.read_parquet(
        os.path.join(
            staging_dir,
            "projects_clean.parquet"
        )
    )

    employees_clean = pd.read_parquet(
        os.path.join(
            staging_dir,
            "employees_clean.parquet"
        )
    )

    transactions_clean = pd.read_parquet(
        os.path.join(
            staging_dir,
            "transactions_clean.parquet"
        )
    )

    # Final output files
    output_files = [
        os.path.join(
            OUTPUT_DIR,
            "dim_project.parquet"
        ),
        os.path.join(
            OUTPUT_DIR,
            "dim_employee.parquet"
        ),
        os.path.join(
            OUTPUT_DIR,
            "fact_transactions.parquet"
        )
    ]

    # Write actual transformed data
    projects_clean.to_parquet(
        output_files[0],
        index=False
    )

    employees_clean.to_parquet(
        output_files[1],
        index=False
    )

    transactions_clean.to_parquet(
        output_files[2],
        index=False
    )

    # Log written files
    for path in output_files:

        logger.info(
            "Target file persisted to: %s",
            path
        )

    # Push output paths to XCom
    ti.xcom_push(
        key="written_files",
        value=output_files
    )

    return output_files


# ==============================================================================
# STEP 6 — Task 3.3e: Pipeline Report
# ==============================================================================

def generate_pipeline_report(**context):
    """
    Generate pipeline execution report using XCom values.
    """

    ti = context["ti"]

    execution_date = context["execution_date"]

    # ------------------------------------------------------------------
    # Pull raw row counts
    # ------------------------------------------------------------------

    raw_proj = ti.xcom_pull(
        task_ids="extract_projects",
        key="raw_projects_count"
    ) or 0

    raw_emp = ti.xcom_pull(
        task_ids="extract_employees",
        key="raw_employees_count"
    ) or 0

    raw_tx = ti.xcom_pull(
        task_ids="extract_transactions",
        key="raw_transactions_count"
    ) or 0

    # ------------------------------------------------------------------
    # Pull clean row counts
    # ------------------------------------------------------------------

    clean_counts = ti.xcom_pull(
        task_ids="transform_and_enrich",
        key="clean_counts"
    ) or {}

    # ------------------------------------------------------------------
    # Pull DQ results
    # ------------------------------------------------------------------

    dq_results = ti.xcom_pull(
        task_ids="validate_data_quality",
        key="dq_metrics"
    ) or {}

    # ------------------------------------------------------------------
    # Pull output files
    # ------------------------------------------------------------------

    files_written = ti.xcom_pull(
        task_ids="load_to_output",
        key="written_files"
    ) or []

    # ------------------------------------------------------------------
    # Report path
    # ------------------------------------------------------------------

    report_path = os.path.join(
        OUTPUT_DIR,
        f"pipeline_report_{execution_date.date()}.txt"
    )

    # ------------------------------------------------------------------
    # Report content
    # ------------------------------------------------------------------

    lines = [
        "=" * 74,
        "PRESIGHT DATA PLATFORM — DAILY ETL ORCHESTRATION REPORT",
        "=" * 74,
        f"Execution Timestamp : {execution_date}",
        "Airflow DAG ID      : presight_etl_pipeline",
        "Schedule Target     : 06:00 UAE Time (Asia/Dubai)",
        "-" * 74,
        "1. RAW VS. CLEAN ROW COUNTS PER DATASET",
        "-" * 74,
        f"{'Dataset':<20} {'Raw Count':<15} "
        f"{'Clean Count':<15} {'Filtered':<15}",
        f"{'projects':<20} "
        f"{raw_proj:<15} "
        f"{clean_counts.get('projects', 0):<15} "
        f"{raw_proj - clean_counts.get('projects', 0):<15}",
        f"{'employees':<20} "
        f"{raw_emp:<15} "
        f"{clean_counts.get('employees', 0):<15} "
        f"{raw_emp - clean_counts.get('employees', 0):<15}",
        f"{'transactions':<20} "
        f"{raw_tx:<15} "
        f"{clean_counts.get('transactions', 0):<15} "
        f"{raw_tx - clean_counts.get('transactions', 0):<15}",
        "",
        "-" * 74,
        "2. DATA QUALITY GATE RESULTS",
        "-" * 74,
        "Critical completeness threshold: >= 80%",
    ]

    for name, stats in dq_results.items():

        status = (
            "PASSED"
            if stats["completeness_pct"] >= 80.0
            else "FAILED"
        )

        lines.append(
            f" - {name:<14} | "
            f"Key: {stats['key_column']:<16} | "
            f"Completeness: "
            f"{stats['completeness_pct']:>6.2f}% | "
            f"Status: [{status}]"
        )

    lines.extend([
        "",
        "-" * 74,
        "3. OUTPUT SINK FILES WRITTEN",
        "-" * 74,
    ])

    for file_path in files_written:

        lines.append(
            f" - {file_path}"
        )

    lines.extend([
        "",
        "-" * 74,
        "4. COMPLETION CHECKLIST",
        "-" * 74,
        " [X] Parallel extraction completed",
        " [X] Data quality gate passed",
        " [X] Transformation completed",
        " [X] Output Parquet files written",
        " [X] XCom metrics collected",
        " [X] Pipeline report generated",
        "=" * 74
    ])

    report_content = "\n".join(lines)

    # Write report
    with open(
        report_path,
        "w",
        encoding="utf-8"
    ) as report_file:

        report_file.write(
            report_content
        )

    logger.info(
        "Pipeline report written successfully to: %s",
        report_path
    )

    return report_path


# ==============================================================================
# STEP 7 — DAG Declaration & Dependency Wiring
# ==============================================================================

with DAG(
    dag_id="presight_etl_pipeline",

    default_args=default_args,

    description=(
        "Orchestrated daily batch ETL "
        "with DQ gate and XCom reporting"
    ),

    # 06:00 UAE / Asia-Dubai
    schedule_interval="0 6 * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "presight",
        "etl",
        "assessment"
    ],

) as dag:

    # ------------------------------------------------------------------
    # Boundary tasks
    # ------------------------------------------------------------------

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    # ------------------------------------------------------------------
    # Extraction tasks
    # ------------------------------------------------------------------

    t_ext_projects = PythonOperator(
        task_id="extract_projects",
        python_callable=extract_projects,
    )

    t_ext_employees = PythonOperator(
        task_id="extract_employees",
        python_callable=extract_employees,
    )

    t_ext_transactions = PythonOperator(
        task_id="extract_transactions",
        python_callable=extract_transactions,
    )

    # ------------------------------------------------------------------
    # DQ gate
    # ------------------------------------------------------------------

    t_validate_dq = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    t_transform = PythonOperator(
        task_id="transform_and_enrich",
        python_callable=transform_and_enrich,
    )

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    t_load = PythonOperator(
        task_id="load_to_output",
        python_callable=load_to_output,
    )

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    t_report = PythonOperator(
        task_id="generate_pipeline_report",
        python_callable=generate_pipeline_report,
    )

    # ==========================================================================
    # TASK 3.3c — Dependency Graph
    # ==========================================================================

    start >> [
        t_ext_projects,
        t_ext_employees,
        t_ext_transactions
    ]

    [
        t_ext_projects,
        t_ext_employees,
        t_ext_transactions
    ] >> t_validate_dq

    t_validate_dq >> t_transform

    t_transform >> t_load

    t_load >> t_report

    t_report >> end
