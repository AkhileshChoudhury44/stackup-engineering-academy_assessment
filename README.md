# StackUp Engineering Academy — Data Engineering Assessment

> **Learning Pathway:** Data Analyst / Modeler / BI Specialist → Fullstack Data Engineer

This repository contains the complete data engineering skills assessment. It includes realistically-sized test datasets, task instructions, and starter files aligned to the four learning pathway pillars: **Foundations**, **SQL & Data Visualization**, **Big Data Processing**, and **Infrastructure & Governance**.

---

## 📊 Dataset overview

This assessment uses production-scale data so candidates work with realistic volumes:

| Dataset | File | Rows | Format |
|---|---|---|---|
| Projects | `datasets/projects.csv` | 500 | CSV |
| Employees (current state) | `datasets/employees.csv` | 1,000 | CSV |
| Employee salary history | `datasets/employees_salary_history.csv` | ~1,800 | CSV |
| Transactions | `datasets/transactions.json` | 50,000 | JSON |
| Event stream | `datasets/events_stream/events_2025_*.jsonl` | 100,000 (12 monthly files) | JSONL |



---

## ⚙️ Getting Started



### Prerequisites at a glance

You must install or have access to the following:

| # | Tool | Minimum Version | Purpose | Install method |
|---|---|---|---|---|
| 1 | **Python** | 3.9+ | Core scripting, data processing, ETL | Manual install |
| 2 | **Git** | 2.x+ | Version control and PR submission | Manual install |
| 3 | **Docker Desktop** | 20.x+ | Runs Kafka, Airflow, PostgreSQL locally | Manual install |
| 4 | **Java JDK** | 17 (LTS) | Required runtime for Apache Spark | Manual install |
| 5 | **VS Code** (or equivalent IDE) | Latest | Python + SQL development | Manual install |
| 6 | **Power BI Desktop** *(Windows only)* | Latest | Task 2.4 dashboard | Manual install — alternatives accepted |

These are installed automatically by `pip install -r requirements.txt` (no separate install needed):

| # | Tool | Version | Purpose |
|---|---|---|---|
| 7 | **PySpark** | 3.4+ | Distributed data processing (Task 3.1) |
| 8 | **kafka-python** | 2.0+ | Kafka client (Task 3.2) |
| 9 | **Apache Airflow** | 2.7+ | Workflow orchestration (Task 3.3) |
| 10 | **DuckDB** | 0.9+ | Embedded SQL engine (Tasks 1.2, 2.1, 2.3) |
| 11 | **Pandas** | 2.0+ | Data manipulation |
| 12 | **PyArrow** | 12.0+ | Parquet read/write |

These run inside Docker containers — started via `docker compose up -d`:

| # | Service | Image | Port | Purpose |
|---|---|---|---|---|
| 13 | **Apache Kafka** | confluentinc/cp-kafka | 9092 | Streaming (Task 3.2) |
| 14 | **Zookeeper** | confluentinc/cp-zookeeper | 2181 | Kafka dependency |
| 15 | **Kafka UI** | provectuslabs/kafka-ui | 8080 | Browse Kafka topics |
| 16 | **Apache Airflow** | apache/airflow:2.7.3 | 8081 | Pipeline orchestration UI |
| 17 | **PostgreSQL** | postgres:15 | 5432 | Airflow metadata + optional SQL practice |

**System requirements:**
- RAM: 8 GB minimum (16 GB recommended)
- Disk: 25 GB free
- OS: Windows 10/11, macOS 10.15+, Ubuntu 20.04+

### 📚 Detailed setup guides

For first-time setup, follow the master guide:

→ **[docs/setup/SETUP_GUIDE.md](docs/setup/SETUP_GUIDE.md)**

---

### Quick start (after prerequisites installed)

Below steps are mentioned in detail in SETUP_GUIDE.md hence follow from their to avoid any errors.
Once python venv setup with all the requirment.txt package are done you can start the docker service as mentioned in below step 4 . After that you can start with every pillar one by one.

```bash

# 1. Set up Python environment
python -m venv venv
source venv/bin/activate          # Mac/Linux
# OR: venv\Scripts\activate       # Windows
pip install -r requirements.txt

# 2. Start Docker services
docker-compose up -d

# 3. Start with Pillar 1
cat tasks/01_foundations/INSTRUCTIONS.md
```

---

## 📁 Repository Structure

```
stackup-engineering-academy_assessment/
│
├── datasets/                       # Test datasets (~40 MB total)
│   ├── projects.csv                # 500 rows
│   ├── employees.csv               # 1,000 rows (with DQ issues)
│   ├── employees_salary_history.csv # ~1,800 rows (for SCD2)
│   ├── transactions.json           # 50,000 rows
│   └── events_stream/              # 12 monthly files
│       ├── events_2025_01.jsonl    # ~8,333 events
│       ├── events_2025_02.jsonl
│       └── ... (10 more)
│
│
├── tasks/                          # Assessment instructions
│   ├── 01_foundations/INSTRUCTIONS.md
│   ├── 02_sql_and_viz/INSTRUCTIONS.md
│   ├── 03_big_data/INSTRUCTIONS.md
│   └── 04_infrastructure/INSTRUCTIONS.md
│
├── starter_files/                  # Scaffolded starter code
│   ├── etl_starter.py
│   ├── spark_starter.py
│   ├── kafka_starter.py
│   ├── airflow_dag_starter.py
│   └── data_model_starter.sql
│
├── docs/setup/                     # Setup guides
│   ├── SETUP_GUIDE.md              # Start here
│   ├── PYTHON_SETUP.md
│   ├── GIT_GITHUB_SETUP.md
│   ├── DOCKER_SETUP.md
│   └── POWER_BI_SETUP.md
│
├── solutions/                      # ⭐ YOUR CODE GOES HERE
│   ├── 01_foundations/             # Pillar 1 work
│   │   ├── etl_pipeline.py
│   │   ├── data_model.sql
│   │   └── notebooks/              # Jupyter notebooks (if used)
│   │       └── exploration.ipynb
│   ├── 02_sql_and_viz/             # Pillar 2 work
│   │   ├── queries.sql
│   │   ├── etl_full.py
│   │   └── query_optimization.sql
│   ├── 03_big_data/                # Pillar 3 work
│   │   ├── spark_pipeline.py
│   │   ├── kafka_streaming.py
│   │   └── airflow_dag.py
│   ├── 04_infrastructure/          # Pillar 4 work
│   │   ├── Dockerfile
│   │   ├── data_governance.md
│   │   └── dq_framework.py
│   └── SUBMISSION_NOTES.md         # Your summary, assumptions, decisions
│
├── outputs/                        # Generated artifacts (gitignored)
│                                   # CSVs, reports, dashboards, Parquet files
│
│
├── docker-compose.yml              # Local services (Kafka, Airflow, PostgreSQL)
├── requirements.txt                # Python dependencies
└── README.md
```
## 🎯 Assessment Tasks Overview

1. Each pillar has detailed instructions in its respective `INSTRUCTIONS.md`.
2. Refer to `starter_files` to see how to create the solutions for every section mentioned in `INSTRUCTIONS.md`.

   > **Note:** Do not write your code in these `starter_files`. They are for read-only purposes. All your solutions should be added to the `solutions` folder.

3. All the solutions for every section should be added to the `solutions` folder using the folder structure below:

   ```
   solutions/submissions/<trainee_name>/01_foundations/<python_file.py> or <sql_file.sql>
   solutions/submissions/<trainee_name>/02_sql_and_viz/<python_file.py> or <sql_file.sql>
   solutions/submissions/<trainee_name>/03_big_data/<python_file.py> or <sql_file.sql>
   solutions/submissions/<trainee_name>/04_infrastructure/<python_file.py> or <sql_file.sql>
   ```

4. All output generated as part of your solutions should be placed using the folder structure below:

   ```
   outputs/results/<trainee_name>/01_foundations/<output_files/diagrams/etc>
   outputs/results/<trainee_name>/02_sql_and_viz/<output_files/diagrams/etc>
   outputs/results/<trainee_name>/03_big_data/<output_files/diagrams/etc>
   outputs/results/<trainee_name>/04_infrastructure/<output_files/diagrams/etc>
   ```


### Pillar 1 — Foundations (~3.5 hours)

**Skills:** Python, Pandas, data modelling, SCD2, data quality

| Task | Description | Output |
|---|---|---|
| 1.1 | Clean and transform 500 projects with 7 derived columns | `outputs/projects_clean.csv` |
| 1.2 | Star schema design with `dim_employee` as SCD Type 2 | DDL in starter SQL |
| 1.3 | Find ALL data quality issues across 1,000 employees | `outputs/employees_clean.csv` |

---

### Pillar 2 — SQL & Data Visualization (~4 hours)

**Skills:** SQL, window functions, query optimisation, dashboards

| Task | Description | Output |
|---|---|---|
| 2.1 | Six business questions including SCD2 history queries | SQL queries with explanations |
| 2.2 | Full ETL on 50K transactions, completing in < 30 seconds | `outputs/transactions_clean.csv` |
| 2.3 | **Measurable** query optimisation with EXPLAIN ANALYZE | Before/after benchmarks |
| 2.4 | Power BI dashboard (or annotated mockup) | `.pbix` or PDF |

---

### Pillar 3 — Big Data Processing (~4 hours)

**Skills:** PySpark, Kafka, Airflow

| Task | Description | Output |
|---|---|---|
| 3.1 | Process 100K events across 12 files into 5 aggregated tables | 5 Parquet tables in `outputs/spark/` |
| 3.2 | Real-time Kafka producer/consumer with severity-based forwarding | Topics + `outputs/kafka/summary.json` |
| 3.3 | Airflow DAG with DQ gate, retries, XCom-driven reporting | DAG visible in Airflow UI |

Note: you can refer below commands to run Airflow dag:

step 1:

docker exec -it <airflow-container-name> bash
cd /opt/airflow/dags
rm airflow_dag_starter.py
rm -rf /opt/airflow/dags/__pycache__
exit
docker compose restart

Open UI 
http://localhost:8081 --- credential to logon are shared in setup guide.

*** by the end of above steps you would be able to see airflow UI.

step 2:
docker exec -it airflow-webserver bash
***now you are into airflow container
*** copy your dag file or use below command:
cat <<EOF > /opt/airflow/dags/presight_etl_pipeline.py
<SOLUTION>
EOF

exit

docker compose restart

***Open UI 
http://localhost:8081 --- credential to logon are shared in setup guide.




---

### Pillar 4 — Infrastructure & Governance (~3.5 hours)

**Skills:** Docker, data governance, DQ frameworks

| Task | Description | Output |
|---|---|---|
| 4.1 | Containerise the ETL pipeline | `Dockerfile`, `.dockerignore` |
| 4.2 | Data governance document with PII classification, retention, access | `outputs/data_governance_document.md` |
| 4.3 | Configurable DQ framework with 6+ checks | Framework in `etl_starter.py` + DQ reports |

---

## 📤 Submission Instructions

Once tasks are complete:

## Submission Instructions

1. Create a public repository on GitHub containing your solution.
2. Push all your code and any required files to that repository.
3. Send back the link to your GitHub repository as your submission.


## 📊 Assessment Criteria

| Criterion | Weight |
|---|---|
| **Correctness** — does the code produce expected output? | 35% |
| **Code quality** — readable, modular, well-commented, vectorised | 25% |
| **Design decisions** — modelling and architecture justified? | 20% |
| **Performance** — does it scale to the dataset size? | 10% |
| **Documentation** — assumptions and decisions clear? | 10% |


## 📚 Learning Resources

This assessment is aligned to the StackUp Engineering Academy learning pathway. Course references in the original curriculum cover:

**Foundations**
- Python Essential Training (LinkedIn Learning)
- Pandas Essential Training (LinkedIn Learning)
- Learning Data Modeling (O'Reilly)

**SQL & Data Visualization**
- SQL Essential Training (LinkedIn Learning)
- ETL in Python and SQL (LinkedIn Learning)
- Advanced SQL for Query Tuning (LinkedIn Learning)
- Power BI Essential Training (LinkedIn Learning)

**Big Data Processing**
- Spark Programming in Python (O'Reilly)
- Complete Guide to Apache Kafka (LinkedIn Learning)
- Apache Airflow Essential Training (LinkedIn Learning)

**Infrastructure & Governance**
- Data Engineering on Azure (O'Reilly)
- Docker for Developers (LinkedIn Learning)
- Data Governance (O'Reilly)
- Data Quality: Core Concepts (LinkedIn Learning)

---

*StackUp Engineering Academy — Data Engineering Assessment*
