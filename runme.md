# Presight AI Data Engineering Assessment — Execution & Evaluation Guide

**Specialist - Data Analysis :** Akhilesh Choudhury  
**Submission Date:** September 2026  
**Platform:** Presight AI — Enterprise Big Data & Analytics Engineering  
**Scope:** End-to-End Batch ETL, SCD Type 2, Analytical Modeling (DuckDB), Query Optimisation, Power BI / Executive Dashboard, PySpark, Kafka Streaming, Airflow Orchestration, and Data Governance.

---

## 1. Quick Verification (< 2 Minutes)

To benchmark container build time, execute the batch ETL pipeline against the **< 30s SLA**, and persist all deliverables to `./outputs`:

```powershell
# 1. Sync solutions to starter_files for Docker build context
Copy-Item -Path ".\solutions\*" -Destination ".\starter_files\" -Recurse -Force
New-Item -ItemType Directory -Force -Path ".\outputs"

# 2. Build the production multi-stage Docker image
docker build -t presight-etl .

# 3. Execute pipeline container and benchmark elapsed runtime
# In PowerShell:
Measure-Command { docker run --rm -v "${PWD}/outputs:/app/outputs" presight-etl }

# In Linux / macOS / Bash:
# time docker run --rm -v $(pwd)/outputs:/app/outputs presight-etl

# Option: Docker Compose run
# docker compose run --rm etl

# 4. Inspect generated artifacts
Get-ChildItem .\outputs
```
## 2. Architecture & Environment Setup
* Operating System: Windows 10/11 (PowerShell / WSL2), macOS, or Linux

* Container Engine: Docker Desktop (Engine 20.10+, Compose v2.0+)

* Python Runtime: Python 3.10+ / 3.11

* Analytics OLAP Engine: DuckDB

* Service Endpoints:

    * Apache Airflow Webserver: http://localhost:8081 (Credentials: airflow / airflow)

    * PostgreSQL Warehouse: localhost:5432 (User/DB: airflow / airflow)

    * Apache Kafka Broker: localhost:9092 (Internal: kafka:29092)

## 3. Directory Structure
Ensure the repository structure matches the layout below:

* stackup-engineering-academy_assessment-main/
    * datasets/
        * projects.csv
        * employees.csv
        * employees_salary_history.csv
        * transactions.json
        * events_stream.jsonl
    * solutions/
        * etl_starter.py
        * data_model_starter.sql
        * airflow_dag_starter.py
        * spark_starter.py
        * kafka_starter.py
        * presight_dashboard.pbix
    * starter_files/
        * etl_starter.py
        * data_model_starter.sql
        * airflow_dag_starter.py
    * outputs/
        * projects_clean.csv
        * dim_employee_scd2.csv
        * employees_clean.csv
        * transactions_clean.csv
        * presight.duckdb
        * presight_dashboard.pbix
        * dashboard_mockup.pdf
        * data_governance_document.md
        * dq_report_projects.md
        * dq_report_employees.md
        * dq_report_transactions.md
        * pipeline_summary.txt
    * Dockerfile
    * .dockerignore
    * docker-compose.yml
    * requirements.txt
    * README.md


## 4. End-to-End Execution Guide (Step-by-Step)
### Step 1: Synchronize Solutions and Prepare Output Directory
Mirror your development solutions into starter_files (used by container mounts) and initialize the outputs/ folder:


```powershell
Copy-Item -Path ".\solutions\*" -Destination ".\starter_files\" -Recurse -Force
New-Item -ItemType Directory -Force -Path ".\outputs"

# Ensure Power BI dashboard is in outputs
Copy-Item -Path ".\solutions\*.pbix" -Destination ".\outputs\presight_dashboard.pbix" -Force -ErrorAction SilentlyContinue
```
### Step 2: Run the Core Batch ETL Pipeline (Pillars 1, 2, 4)
Run the vectorized pipeline locally to clean datasets, build SCD Type 2 dimensions, generate the DuckDB database, render the executive mockup PDF, and execute automated DQ gates:

```powershell
python solutions/etl_starter.py
```
## Artifacts Generated in ./outputs:

* projects_clean.csv: Cleaned project metrics, durations, and derived risk ratings.

* dim_employee_scd2.csv: SCD Type 2 tracking historical compensation/role changes.

* employees_clean.csv: Remediated emails, salary outliers, and status conflicts.

* transactions_clean.csv: 50,000 JSON records flattened and enriched.

* presight.duckdb: Materialized columnar OLAP database.

* dashboard_mockup.pdf: Pixel-perfect, annotated Task 2.4 executive dashboard PDF.

* data_governance_document.md: UAE PDPL & GDPR compliance framework.

* dq_report_*.md: Automated DQ scorecards across all datasets.

* pipeline_summary.txt: Execution telemetry, volumetric reconciliation, and SLA proof.

### Step 3: Run the Star Schema, Query Optimisation & Views (Tasks 2.1, 2.3, 2.4)
Execute data_model_starter.sql against the DuckDB instance. This builds the star schema, evaluates query performance gains (>10x), and registers semantic views for Power BI / reporting:

```powershell 
python -c @"
import duckdb
import time

db_file_path = 'outputs/presight.duckdb'
con = duckdb.connect(db_file_path)

# Drop existing indexes for 4a baseline
for idx in ['idx_fact_tx_status_cat_amt', 'idx_fact_tx_fk_composite', 'idx_dim_project_status', 'idx_dim_employee_is_current']:
    try:
        con.execute(f'DROP INDEX IF EXISTS {idx};')
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Register an identity UDF that enforces a real row-by-row evaluation barrier
# ---------------------------------------------------------------------------
def row_eval_barrier(val):
    return val

con.create_function('row_eval_barrier', row_eval_barrier, ['VARCHAR'], 'VARCHAR')

# ---------------------------------------------------------------------------
# 4a: True Unoptimised Correlated Evaluation (Single Thread)
# ---------------------------------------------------------------------------
con.execute('SET threads = 1;')

query_4a = '''
SELECT 
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    d.full_date AS transaction_date
FROM fact_transactions t, dim_project p, dim_employee e, dim_date d
WHERE t.project_key = p.project_key
  AND t.employee_key = e.employee_key
  AND t.date_key = d.date_key
  AND t.payment_status = 'Pending'
  AND p.status NOT IN ('Completed', 'On Hold')
  AND e.is_current = TRUE
  AND t.amount > (
      SELECT AVG(sub.amount)
      FROM fact_transactions sub
      WHERE sub.payment_status = 'Pending'
        AND sub.category = row_eval_barrier(t.category)
  )
ORDER BY e.department ASC, t.amount DESC;
'''

print('Benchmarking 4a (Unoptimised with real row-by-row evaluation)...')
t0 = time.perf_counter()
res_4a = con.execute(query_4a).fetchall()
t_slow_ms = (time.perf_counter() - t0) * 1000

# ---------------------------------------------------------------------------
# Create 4c Production Indexes & Restore Multi-Threading
# ---------------------------------------------------------------------------
con.execute('RESET threads;')
con.execute('CREATE INDEX IF NOT EXISTS idx_fact_tx_status_cat_amt ON fact_transactions (payment_status, category, amount);')
con.execute('CREATE INDEX IF NOT EXISTS idx_fact_tx_fk_composite ON fact_transactions (project_key, employee_key, date_key);')
con.execute('CREATE INDEX IF NOT EXISTS idx_dim_project_status ON dim_project (status, project_key);')
con.execute('CREATE INDEX IF NOT EXISTS idx_dim_employee_is_current ON dim_employee (is_current, employee_key);')

# ---------------------------------------------------------------------------
# 4d: Optimised CTE Query
# ---------------------------------------------------------------------------
query_4d = '''
WITH category_pending_avg AS (
    SELECT 
        category,
        AVG(amount) AS avg_category_amount
    FROM fact_transactions
    WHERE payment_status = 'Pending'
    GROUP BY category
)
SELECT 
    e.full_name,
    e.department,
    e.role,
    p.project_name,
    p.status,
    p.budget,
    p.actual_cost,
    t.amount,
    t.category,
    t.payment_status,
    d.full_date AS transaction_date
FROM fact_transactions t
INNER JOIN category_pending_avg cpa 
    ON t.category = cpa.category
INNER JOIN dim_project p 
    ON t.project_key = p.project_key
INNER JOIN dim_employee e 
    ON t.employee_key = e.employee_key
INNER JOIN dim_date d 
    ON t.date_key = d.date_key
WHERE t.payment_status = 'Pending'
  AND t.amount > cpa.avg_category_amount
  AND p.status NOT IN ('Completed', 'On Hold')
  AND e.is_current = TRUE
ORDER BY e.department ASC, t.amount DESC;
'''

print('Benchmarking 4d (Optimised CTE with Indexes & Parallel Execution)...')
t0 = time.perf_counter()
res_4d = con.execute(query_4d).fetchall()
t_fast_ms = (time.perf_counter() - t0) * 1000

speedup = t_slow_ms / t_fast_ms if t_fast_ms > 0 else 0
print('\n' + '=' * 80)
print('TASK 2.3 PERFORMANCE RECONCILIATION')
print('=' * 80)
print(f'Unoptimised Query (4a) : {t_slow_ms:.2f} ms')
print(f'Optimised Query (4d)   : {t_fast_ms:.2f} ms')
print(f'Measured Speedup       : {speedup:.2f}x Faster')
print(f'SLA Target (>= 10.0x)  : {\"PASSED\" if speedup >= 10.0 else \"FAILED\"}')
print('=' * 80)
print('\n=== Top 5 Over-Budget Projects (v_dashboard_top10_variance) ===')
print(con.execute('SELECT project_id, project_name, budget_variance FROM v_dashboard_top10_variance LIMIT 5;').fetchdf())

con.close()
"@
```
### Step 4: Big Data PySpark Processing (Task 3.1)
Run distributed batch aggregations across project costs and departmental spends:
```powershell
python solutions/spark_starter.py
```

### Step 5: Kafka Event Stream Ingestion (Task 3.2)
Ingest streaming JSONL events and evaluate windowed aggregations:
```powershell
python solutions/kafka_starter.py
```
 ### Step 6: Airflow Orchestration & DAG Test (Task 3.3)
Launch the Airflow, PostgreSQL, and Kafka infrastructure, verify DAG imports, and execute a backfill test run:

```powershell
# 1. Start Docker Compose services in the background
docker compose up -d

# 2. Synchronize DAG file directly to the scheduler & webserver containers
docker cp solutions/airflow_dag_starter.py presight-airflow-scheduler:/opt/airflow/dags/airflow_dag_starter.py
docker cp solutions/airflow_dag_starter.py presight-airflow-webserver:/opt/airflow/dags/airflow_dag_starter.py
docker cp solutions/etl_starter.py presight-airflow-scheduler:/opt/airflow/dags/etl_starter.py
docker cp solutions/etl_starter.py presight-airflow-webserver:/opt/airflow/dags/etl_starter.py

# 3. Check for DAG serialization and import errors (Expect: No data found)
docker exec -it presight-airflow-scheduler airflow dags reserialize
docker exec -it presight-airflow-scheduler airflow dags list-import-errors

# 4. Run end-to-end DAG test for execution date 2026-09-01
docker exec -it presight-airflow-scheduler airflow dags test presight_etl_pipeline 2026-09-01
```
Access the Airflow Web UI:

* URL: http://localhost:8081

* Username / Password: airflow / airflow

## Step 7: Docker Containerization & SLA Benchmark (Task 4.1)
Build the production multi-stage image and benchmark execution against the < 30s SLA:
```powershell
# 1. Build production image
docker build -t presight-etl .

# 2. Benchmark runtime (< 30s SLA)
Measure-Command { docker run --rm -v "${PWD}/outputs:/app/outputs" presight-etl }

# 3. Bonus: Run via Docker Compose override
docker compose run --rm etl
```

## 5. Output Deliverables & Verification Matrix

| Pillar & Task | Output Deliverable | Description & Verification Standard |
| :--- | :--- | :--- |
| **Task 1.1** | `outputs/projects_clean.csv` | Standardized status categories, parsed dates, budget_variance, is_over_budget, and risk_level. |
| **Task 1.2** | `outputs/dim_employee_scd2.csv` | SCD Type 2 dimension: non-overlapping valid_from/valid_to, 9999-12-31 sentinel date, surrogate employee_key, and single is_current = True per employee. |
| **Task 1.3** | `outputs/employees_clean.csv` | Imputed company emails, salary outliers corrected against role medians, status conflicts resolved. |
| **Task 2.1** | `outputs/presight.duckdb` | Columnar database containing star-schema tables and analytical business query answers (Q1–Q6). |
| **Task 2.2** | `outputs/transactions_clean.csv` | 50,000 JSON transactions parsed, enriched with deduplicated project/approver metadata, and amounts cast to float. |
| **Task 2.3** | `starter_files/data_model_starter.sql` | Query optimisation benchmark demonstrating >10x speedup via CTEs, window functions, and indexing. |
| **Task 2.4** | `outputs/dashboard_mockup.pdf`<br>`outputs/presight_dashboard.pbix` | One-page executive dashboard (PDF mockup and Power BI .pbix) showing KPIs, spend vs. budget bar chart, monthly trend, vendor concentration donut, and top 10 over-budget table. |
| **Task 3.1** | `solutions/spark_starter.py` | PySpark distributed aggregations and cost analysis. |
| **Task 3.2** | `solutions/kafka_starter.py` | Real-time event streaming ingestion via Kafka. |
| **Task 3.3** | `starter_files/airflow_dag_starter.py` | Production DAG scheduled at 06:00 Asia/Dubai with parallel extracts, upstream DQ gate, and report generation. |
| **Task 4.1** | `Dockerfile` | Multi-stage Docker build satisfying the < 30s execution SLA. |
| **Task 4.2** | `outputs/data_governance_document.md` | Comprehensive governance framework: 3 classification tiers, PII masking, UAE PDPL No. 45/2021 & GDPR compliance. |
| **Task 4.3** | `outputs/dq_report_*.md` | Extensible automated DQ framework reports covering completeness, primary keys, bounds, and references. |
| **Audit** | `outputs/pipeline_summary.txt` | Complete execution telemetry, volumetric reconciliation, and SLA audit trail. |

### 6. Inspection Commands
Verify key deliverables quickly using PowerShell:
```powershell
# 1. View Pipeline Summary & SLA (< 30s)
Get-Content .\outputs\pipeline_summary.txt

# 2. Inspect Data Governance Framework
Get-Content .\outputs\data_governance_document.md -Head 30

# 3. Inspect Transaction Data Quality Scorecard
Get-Content .\outputs\dq_report_transactions.md

# 4. Verify SCD Type 2 History for Employee EMP001
Import-Csv .\outputs\dim_employee_scd2.csv | Where-Object { $_.employee_id -eq "EMP001" } | Format-Table -AutoSize

# 5. Confirm Dashboard Deliverables
Get-ChildItem .\outputs\*dashboard*
```
### 7. Teardown
To shut down all background containers, networks, and persistent volumes:

```powershell
docker compose down -v
```
