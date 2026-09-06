# Pillar 4 — Infrastructure & Governance

> **Time allocation:** ~3.5 hours  
> **Starter files:** `starter_files/etl_starter.py` (Task 4.3), new files you create

---

## Context

Your pipeline works on real volumes — 1,000 employees, 500 projects, 50,000 transactions, 100,000 events. Now it needs to be deployable, maintainable, and compliant.

DevOps wants the ETL containerised. Data Governance needs a formal classification document. Data Quality wants an automated check framework they can extend without touching pipeline code.

---

## Task 4.1 — Docker: Containerise the ETL pipeline

Create a `Dockerfile` packaging the ETL from Task 2.2.

**Requirements:**

1. **Base image:** `python:3.11-slim`
2. **Dependencies:** Install from `requirements.txt`
3. **Entry point:** Container automatically executes `etl_starter.py`
4. **Output mounting:** Writes to mounted volume — results accessible on host
5. **Environment variables:** Pipeline reads `DATA_DIR` and `OUTPUT_DIR` from env (with defaults)
6. **Multi-stage build (bonus):** Use a builder stage to reduce final image size

**Files to create:**
- `Dockerfile` — repo root
- `.dockerignore` — exclude `outputs/`, `.git/`, `__pycache__/`, `*.pyc`, `venv/`

**Test:**
```bash
docker build -t presight-etl .
time docker run -v $(pwd)/outputs:/app/outputs presight-etl
```

Measure execution time. The container should process all 50K transactions in under 30 seconds.

**Bonus:** Add `docker-compose.override.yml` so pipeline runs via `docker-compose run etl`.

**Assessment focus:**
- Container builds without errors
- Output files appear in mounted volume after run
- `.dockerignore` is correct
- Environment variable handling is clean
- Execution time is reasonable

---

## Task 4.2 — Data Governance Document

Write `outputs/data_governance_document.md` covering all four datasets (projects, employees, transactions, salary history).

---

**Section 1 — Data inventory**

| Dataset | Source system | Format | Update frequency | Volume estimate | Daily growth |
|---|---|---|---|---|---|
| projects | | | | | |
| employees | | | | | |
| transactions | | | | | |
| employees_salary_history | | | | | |

---

**Section 2 — Data classification**

Classify each column in each dataset:

| Classification | Definition |
|---|---|
| **Public** | Non-sensitive, shareable externally |
| **Internal** | Internal use only, no regulatory requirement |
| **Confidential** | Sensitive business data — restricted access |
| **Personal (PII)** | Personal identifiable information — regulatory requirements apply |

For each PII column, state which regulation applies (GDPR, UAE PDPL, or both).

---

**Section 3 — Data ownership**

| Dataset | Data Owner (role) | Data Steward (role) | Access approver |
|---|---|---|---|

Explain Owner vs. Steward in your own words.

---

**Section 4 — Retention policy**

For each dataset:
- Retention period
- Justification (business need + regulatory)
- Disposal method (delete, anonymise, archive)
- Who enforces the policy

**Special consideration:** `employees_salary_history.csv` contains historical salary changes. UAE labour law and tax regulations may require longer retention than typical employee data. Address this.

---

**Section 5 — Access control**

| Persona | Projects | Employees | Transactions | Salary History |
|---|---|---|---|---|
| Data Engineer | | | | |
| BI Analyst | | | | |
| Finance Team | | | | |
| HR Team | | | | |
| Executive | | | | |

Access levels: `None`, `Read`, `Read + Write`, `Full (including delete)`

**Note:** Salary history requires especially careful access controls — most personas should have NO access. Justify your decisions.

---

**Section 6 — Data lineage**

Diagram (mermaid or ASCII) the flow:
- Source systems → raw datasets → cleaned datasets → warehouse → reporting

Show where transformations happen and where quality checks are applied.

---

**Assessment focus:**
- All columns classified across all 4 datasets
- PII columns explicitly tagged with regulation
- Retention reasoning addresses both business need and regulation
- Access controls reflect the principle of least privilege
- Awareness of UAE PDPL in addition to GDPR

---

## Task 4.3 — Data Quality Framework

**File:** `starter_files/etl_starter.py` → `run_data_quality_checks()`

Build a reusable, configurable framework. The team must extend it by adding rules — not by modifying pipeline code.

**Minimum 6 checks required:**

| Check | Type | Description |
|---|---|---|
| Completeness | Column-level | % non-null per column; flag below threshold |
| Uniqueness | Table-level | PK columns contain no duplicates |
| Validity — numeric | Column-level | Values within expected min/max range |
| Validity — date | Column-level | Dates valid, non-future where expected |
| Consistency | Cross-column | `start_date < end_date`; `actual_cost >= 0`; salary level matches role level |
| Referential integrity | Cross-table | FKs exist in referenced table (e.g. `project_manager_id` exists in employees) |

**Bonus checks:**
- **Distribution check** — flag if > 30% of a column has the same value (data loading error)
- **Freshness check** — flag if max transaction_date older than 30 days
- **Outlier check** — flag if any value is > 3 standard deviations from mean

---

**Framework design requirements:**

Rules and thresholds must be **defined in a config dict or YAML file**, not hardcoded:

```python
DQ_CONFIG = {
    "projects": {
        "completeness_threshold": 0.90,
        "pk_columns": ["project_id"],
        "numeric_ranges": {
            "budget": {"min": 0, "max": 10_000_000},
            "actual_cost": {"min": 0, "max": 10_000_000}
        },
        "date_columns": ["start_date", "end_date"],
        "consistency_rules": [
            {"type": "before", "columns": ["start_date", "end_date"]},
            {"type": "non_negative", "column": "actual_cost"}
        ],
        "foreign_keys": {
            "project_manager_id": ("employees", "employee_id")
        }
    },
    "employees": {
        "completeness_threshold": 0.85,
        "pk_columns": ["employee_id"],
        "numeric_ranges": {
            "salary": {"min": 10000, "max": 100000},
            "years_experience": {"min": 0, "max": 50}
        },
        ...
    }
}
```

**Return format:**

```python
{
    "dataset_name": "projects",
    "checks_run": 6,
    "checks_passed": 4,
    "checks_failed": 2,
    "results": {
        "completeness": {
            "status": "PASS",
            "details": {"project_id": 1.0, "budget": 0.94, ...},
            "failed_columns": []
        },
        "uniqueness": {
            "status": "PASS",
            "details": "project_id: 500 unique / 500 total"
        },
        "validity_numeric": {
            "status": "FAIL",
            "details": "budget: 2 values below minimum (0), actual_cost: 1 value above maximum"
        },
        ...
    }
}
```

Log `WARNING` for every `FAIL`.

**Bonus — Output a markdown report:**

Write `outputs/dq_report_{dataset}.md` for each dataset with check results in a human-readable format.

---

## Completion checklist

- [ ] `Dockerfile` builds successfully
- [ ] Container runs, processes 50K transactions, writes outputs to mounted volume
- [ ] Execution time measured and documented
- [ ] `.dockerignore` excludes outputs, git, cache
- [ ] `outputs/data_governance_document.md` covers all 4 datasets
- [ ] All columns classified with PII regulation noted
- [ ] Retention policy addresses salary history separately
- [ ] Access controls reflect least privilege
- [ ] Data lineage diagram included
- [ ] DQ framework implements minimum 6 checks
- [ ] Framework is configurable (rules in config, not code)
- [ ] Return format matches specification
- [ ] WARN logged for every failing check
- [ ] DQ reports written as markdown per dataset
