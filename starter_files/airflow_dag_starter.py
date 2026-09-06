"""Task 3.3 - Daily 06:00 Asia/Dubai ETL DAG with parallel extracts and a DQ gate."""
import json
import logging
import os
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Environment paths: works in Docker (/opt/airflow) and local host
ROOT = os.environ.get("REPO_ROOT", "/opt/airflow")
DATA_DIR = os.path.join(ROOT, "datasets")
OUTPUT_DIR = os.path.join(ROOT, "outputs")

# Timezone definition
uae_tz = pendulum.timezone("Asia/Dubai")
DAG_START_DATE = datetime(2025, 1, 1, tzinfo=uae_tz)


def _read_csv(name):
    """
    Utility helper to read a standard CSV file from the raw datasets directory.

    Args:
        name (str): Name of the CSV file (e.g., 'projects.csv').

    Returns:
        pd.DataFrame: Loaded pandas DataFrame.
    """
    return pd.read_csv(os.path.join(DATA_DIR, name))


def _read_transactions_df():
    """
    Utility helper to parse and normalize transaction data from JSON.
    Handles both standard lists and nested dictionary structures.

    Returns:
        pd.DataFrame: Flattened and normalized transaction records.
    """
    path = os.path.join(DATA_DIR, "transactions.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "transactions" in data:
        return pd.json_normalize(data["transactions"])
    return pd.json_normalize(data)


def extract_projects(**context):
    """
    Extracts raw project metadata from the local directory and pushes the 
    total row volume to Airflow XCom for audit tracking.
    """
    rows = len(_read_csv("projects.csv"))
    context["ti"].xcom_push(key="projects_raw_count", value=rows)
    return rows


def extract_employees(**context):
    """
    Task Step: Extracts raw employee data, counts total rows, 
    and logs the volume to XCom for pipeline metrics.
    """
    rows = len(_read_csv("employees.csv"))
    context["ti"].xcom_push(key="employees_raw_count", value=rows)
    return rows


def extract_transactions(**context):
    """
    Task Step: Flattens raw transaction JSON data, tracks ingestion volume, 
    and logs the raw count to XCom.
    """
    df = _read_transactions_df()
    rows = len(df)
    context["ti"].xcom_push(key="transactions_raw_count", value=rows)
    return rows


def validate_data_quality(**context):
    """DQ gate: fail the DAG when any required key is less than 80% complete."""
    datasets = {
        "projects": (_read_csv("projects.csv"), ["project_id"]),
        "employees": (_read_csv("employees.csv"), ["employee_id"]),
        "transactions": (_read_transactions_df(), ["transaction_id"]),
    }
    results = {}
    for name, (df, keys) in datasets.items():
        results[name] = {key: round(float(df[key].notna().mean()), 4) for key in keys}
        bad = [key for key, completeness in results[name].items() if completeness < 0.80]
        if bad:
            raise ValueError(f"DQ gate failed for {name}: key completeness below 80%: {bad}")

    context["ti"].xcom_push(key="dq_results", value=results)
    return results


def transform_and_enrich(**context):
    """
    Applies column transformations, parses timestamps, imputes financial 
    nulls, and flushes results to the clean output directory.
    """
    projects = _read_csv("projects.csv")
    employees = _read_csv("employees.csv")
    transactions = _read_transactions_df()

    projects["status"] = projects["status"].astype(str).str.strip().str.title()
    employees["hire_date"] = pd.to_datetime(employees["hire_date"], errors="coerce")
    transactions["amount"] = pd.to_numeric(transactions["amount"], errors="coerce").fillna(0.0)

    ti = context["ti"]
    for name, df in {
        "projects": projects,
        "employees": employees,
        "transactions": transactions,
    }.items():
        ti.xcom_push(key=f"{name}_clean_count", value=len(df))

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    projects.to_csv(os.path.join(OUTPUT_DIR, "projects_clean.csv"), index=False)
    employees.to_csv(os.path.join(OUTPUT_DIR, "employees_clean.csv"), index=False)
    transactions.to_csv(os.path.join(OUTPUT_DIR, "transactions_clean.csv"), index=False)


def load_to_output(**context):
    """
    Task Step: Validates the presence of fully transformed target CSVs 
    and logs the verified file paths to XCom for downstream reporting.
    """
    files = [
        os.path.join(OUTPUT_DIR, f"{name}_clean.csv")
        for name in ("projects", "employees", "transactions")
    ]
    context["ti"].xcom_push(key="output_files", value=files)
    logger.info("Outputs: %s", files)


def generate_pipeline_report(**context):
    """
    Task Step: Compiles an end-to-end pipeline execution report.
    Pulls historical data from XCom (raw counts, clean counts, and DQ results) 
    and outputs a dated summary text file to the outputs folder.
    """
    ti = context["ti"]
    date_val = context.get("logical_date") or context.get("execution_date") or datetime.now(tz=uae_tz)

    lines = [f"Presight ETL report - {date_val.isoformat()}", ""]
    extract_task = {
        "projects": "extract_projects",
        "employees": "extract_employees",
        "transactions": "extract_transactions",
    }
    for name in ("projects", "employees", "transactions"):
        lines.append(
            f"{name}: raw={ti.xcom_pull(task_ids=extract_task[name], key=f'{name}_raw_count')} "
            f"clean={ti.xcom_pull(task_ids='transform_and_enrich', key=f'{name}_clean_count')}"
        )

    lines += [
        "",
        "DQ results:",
        json.dumps(ti.xcom_pull(task_ids="validate_data_quality", key="dq_results"), indent=2),
        "",
        "Files:",
        *(ti.xcom_pull(task_ids="load_to_output", key="output_files") or []),
    ]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_filename = f"pipeline_report_{date_val.strftime('%Y-%m-%d')}.txt"
    report_path = os.path.join(OUTPUT_DIR, report_filename)
    with open(report_path, "w", encoding="utf-8") as target:
        target.write("\n".join(lines))


default_args = {
    "owner": "Akhilesh Choudhury",
    "start_date": DAG_START_DATE,
    "depends_on_past": False,
    "retries": 2,           # Allows recovery from brief infrastructure drops
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False, # Switch to True for production environments
}

with DAG(
    dag_id="presight_etl_pipeline",
    start_date=DAG_START_DATE,
    schedule_interval="0 2 * * *",  # 06:00 UAE Time (UTC+4) is 02:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["presight", "etl", "assessment"],
) as dag:
    dag.doc_md = """
    ### Presight Data Engineering Production Pipeline
    * **Orchestration Frequency:** Runs daily at 06:00 Asia/Dubai time.
    * **Parallel Operations:** Extracts projects, employees, and transactions concurrently.
    * **Data Quality Gate:** Enforces an explicit 80% completeness threshold check on key fields before writing clean data.
    """
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    projects = PythonOperator(
        task_id="extract_projects",
        python_callable=extract_projects,
    )

    employees = PythonOperator(
        task_id="extract_employees",
        python_callable=extract_employees,
    )

    transactions = PythonOperator(
        task_id="extract_transactions",
        python_callable=extract_transactions,
    )

    dq = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    transform = PythonOperator(
        task_id="transform_and_enrich",
        python_callable=transform_and_enrich,
    )

    load = PythonOperator(
        task_id="load_to_output",
        python_callable=load_to_output,
    )

    report = PythonOperator(
        task_id="generate_pipeline_report",
        python_callable=generate_pipeline_report,
    )

    start >> [projects, employees, transactions] >> dq >> transform >> load >> report >> end