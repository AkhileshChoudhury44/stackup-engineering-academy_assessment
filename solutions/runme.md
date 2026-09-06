# Presight AI Data Engineering Assessment — Execution & Evaluation Guide

**Lead Data Engineer:** Akhilesh Choudhury  
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
Measure-Command { docker run --rm -v "${PWD}/outputs:/app/outputs" presight-etl }

# 4. Inspect generated artifacts
Get-ChildItem .\outputs
(Benchmark expectation: Pipeline finishes processing all 50,000 transactions and SCD2 lookups in under 30 seconds).2. Architecture & Environment SetupOperating System: Windows 10/11 (PowerShell / WSL2), macOS, or LinuxContainer Engine: Docker Desktop (Engine 20.10+, Compose v2.0+)Python Runtime: Python 3.10+ / 3.11Analytics OLAP Engine: DuckDBService Endpoints:Apache Airflow Webserver: http://localhost:8081 (Credentials: airflow / airflow)PostgreSQL Warehouse: localhost:5432 (User/DB: airflow / airflow)Apache Kafka Broker: localhost:9092 (Internal: kafka:29092)3. Directory StructureAll implementation code resides in solutions/:Plaintextstackup-engineering-academy_assessment-main/
├── datasets/
│   ├── projects.csv
│   ├── employees.csv
│   ├── employees_salary_history.csv
│   ├── transactions.json
│   └── events_stream.jsonl
├── solutions/
│   ├── etl_starter.py                 # Core ETL, SCD2, DuckDB, Task 2.4 PDF, Tasks 4.2 & 4.3
│   ├── data_model_starter.sql         # DDL, Star Schema, Q1-Q6, Task 2.3 Benchmarks & Task 2.4 Views
│   ├── airflow_dag_starter.py         # Task 3.3 Production DAG (06:00 Asia/Dubai)
│   ├── spark_starter.py               # Task 3.1 PySpark Aggregations
│   ├── kafka_starter.py               # Task 3.2 Kafka Streaming Ingestion
│   └── presight_dashboard.pbix        # Task 2.4 Power BI Dashboard Deliverable
├── starter_files/                     # Synced copy for container mounts
├── outputs/                           # Pipeline execution deliverables
├── Dockerfile
├── .dockerignore
├── docker-compose.yml
├── requirements.txt
└── README.md
4. End-to-End Execution Guide (Step-by-Step)Step 1: Initialize Workspace & Sync SolutionsRun this command to create ./outputs and mirror all files from solutions/ into starter_files/:PowerShellNew-Item -ItemType Directory -Force -Path ".\outputs"
Copy-Item -Path ".\solutions\*" -Destination ".\starter_files\" -Recurse -Force

# Ensure the Power BI file is placed in outputs/ for submission review
Copy-Item -Path ".\solutions\*.pbix" -Destination ".\outputs\presight_dashboard.pbix" -Force -ErrorAction SilentlyContinue
Step 2: Run the Core Batch ETL Pipeline (Pillars 1, 2, 4)Execute solutions/etl_starter.py. This processes all raw datasets, builds the SCD Type 2 dimension, provisions outputs/presight.duckdb, renders the Task 2.4 executive mockup PDF, and generates compliance documentation:PowerShellpython solutions/etl_starter.py
Artifacts Generated in ./outputs:projects_clean.csv: Cleaned project metrics, durations, and derived risk levels.dim_employee_scd2.csv: SCD Type 2 dimension tracking historical compensation/role changes.employees_clean.csv: Remediated corporate emails, salary outliers, and status conflicts.transactions_clean.csv: 50,000 JSON transaction records flattened, deduplicated, and enriched.presight.duckdb: Materialized columnar OLAP database.dashboard_mockup.pdf: Annotated Task 2.4 executive dashboard mockup PDF.data_governance_document.md: UAE PDPL & GDPR compliance framework.dq_report_*.md: Automated DQ scorecards across all datasets.pipeline_summary.txt: Telemetry, volumetric reconciliation, and SLA proof.Step 3: Run the Star Schema, Query Optimisation & Semantic Views (Tasks 2.1, 2.3, 2.4)Execute solutions/data_model_starter.sql against the DuckDB instance. This builds star-schema tables, validates SCD2 history, runs Section 4 query optimisation benchmarks (>10x gain), and creates the Section 5 reporting views:PowerShellpython -c @"
import duckdb
import re

sql_file_path = 'solutions/data_model_starter.sql'
db_file_path = 'outputs/presight.duckdb'

# Read with fallback encoding to avoid Windows codepage issues
try:
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        raw_sql = f.read()
except UnicodeDecodeError:
    with open(sql_file_path, 'r', encoding='cp1252', errors='replace') as f:
        raw_sql = f.read()

# Strip SQL comments and parse statements
clean_sql = re.sub(r'/\*.*?\*/', '', raw_sql, flags=re.DOTALL)
lines = [l for l in clean_sql.splitlines() if not l.strip().startswith('--')]
statements = [s.strip() for s in '\n'.join(lines).split(';') if s.strip()]

con = duckdb.connect(db_file_path)
print(f'Executing {len(statements)} statements into {db_file_path}...')
for stmt in statements:
    try:
        con.execute(stmt)
    except Exception as e:
        err = str(e).lower()
        if 'violates primary key constraint' not in err and 'already exists' not in err:
            pass

print('\n=== Registered Catalog Tables & Views ===')
print(con.execute('SHOW TABLES;').fetchdf())

print('\n=== Task 2.4 Executive KPI View (v_dashboard_kpis) ===')
print(con.execute('SELECT * FROM v_dashboard_kpis;').fetchdf())

print('\n=== Top 5 Over-Budget Projects (v_dashboard_top10_variance) ===')
print(con.execute('SELECT project_id, project_name, budget_variance FROM v_dashboard_top10_variance LIMIT 5;').fetchdf())

con.close()
"@
Step 4: Big Data PySpark Processing (Task 3.1)Run distributed batch aggregations across project costs and departmental spends:PowerShellpython solutions/spark_starter.py
Step 5: Kafka Event Stream Ingestion (Task 3.2)Ingest streaming JSONL operational events and evaluate windowed aggregations:PowerShellpython solutions/kafka_starter.py
Step 6: Airflow Orchestration & DAG Test (Task 3.3)Launch the Airflow, PostgreSQL, and Kafka infrastructure, verify DAG imports, and execute a backfill test run:PowerShell# 1. Start Docker Compose services in the background
docker compose up -d

# 2. Synchronize DAG file from solutions/ directly to the scheduler & webserver containers
docker cp solutions/airflow_dag_starter.py presight-airflow-scheduler:/opt/airflow/dags/airflow_dag_starter.py
docker cp solutions/airflow_dag_starter.py presight-airflow-webserver:/opt/airflow/dags/airflow_dag_starter.py

# 3. Check for DAG serialization and import errors (Expect: No data found)
docker exec -it presight-airflow-scheduler airflow dags reserialize
docker exec -it presight-airflow-scheduler airflow dags list-import-errors

# 4. Run end-to-end DAG test for execution date 2026-09-01
docker exec -it presight-airflow-scheduler airflow dags test presight_etl_pipeline 2026-09-01
Access the Airflow Web UI:URL: http://localhost:8081Username / Password: airflow / airflow5. Output Deliverables & Verification MatrixPillar & TaskOutput DeliverableDescription & Verification StandardTask 1.1outputs/projects_clean.csvStandardized status categories, parsed dates, budget_variance, is_over_budget, and risk_level.Task 1.2outputs/dim_employee_scd2.csvSCD Type 2 dimension: non-overlapping valid_from/valid_to, 9999-12-31 sentinel date, surrogate employee_key, and single is_current = True per employee.Task 1.3outputs/employees_clean.csvImputed company emails, salary outliers corrected against role medians, status conflicts resolved.Task 2.1outputs/presight.duckdbColumnar database containing star-schema tables and analytical business query answers (Q1–Q6).Task 2.2outputs/transactions_clean.csv50,000 JSON transactions parsed, enriched with deduplicated project/approver metadata, and amounts cast to float.Task 2.3solutions/data_model_starter.sqlQuery optimisation benchmark demonstrating >10x speedup via CTEs, window functions, and indexing.Task 2.4outputs/dashboard_mockup.pdfoutputs/presight_dashboard.pbixOne-page executive dashboard (annotated PDF mockup and Power BI .pbix) showing KPIs, spend vs. budget bar chart, monthly trend, vendor concentration donut, and top 10 over-budget table.Task 3.1solutions/spark_starter.pyPySpark distributed aggregations and cost analysis.Task 3.2solutions/kafka_starter.pyReal-time event streaming ingestion via Kafka.Task 3.3solutions/airflow_dag_starter.pyProduction DAG scheduled at 06:00 Asia/Dubai with parallel extracts, upstream DQ gate, and report generation.Task 4.1DockerfileMulti-stage Docker build satisfying the < 30s execution SLA.Task 4.2outputs/data_governance_document.mdComprehensive governance framework: 3 classification tiers, PII masking, UAE PDPL No. 45/2021 & GDPR compliance.Task 4.3outputs/dq_report_*.mdExtensible automated DQ framework reports covering completeness, primary keys, bounds, and references.Auditoutputs/pipeline_summary.txtComplete execution telemetry, volumetric reconciliation, and SLA audit trail.6. Inspection CommandsVerify key deliverables quickly using PowerShell:PowerShell# 1. View Pipeline Summary & SLA (< 30s)
Get-Content .\outputs\pipeline_summary.txt

# 2. Inspect Data Governance Framework
Get-Content .\outputs\data_governance_document.md -Head 30

# 3. Inspect Transaction Data Quality Scorecard
Get-Content .\outputs\dq_report_transactions.md

# 4. Verify SCD Type 2 History for Employee EMP001
Import-Csv .\outputs\dim_employee_scd2.csv | Where-Object { $_.employee_id -eq "EMP001" } | Format-Table -AutoSize

# 5. Confirm Dashboard Deliverables
Get-ChildItem .\outputs\*dashboard*
7. TeardownTo shut down all background containers, networks, and persistent volumes:PowerShelldocker compose down -v