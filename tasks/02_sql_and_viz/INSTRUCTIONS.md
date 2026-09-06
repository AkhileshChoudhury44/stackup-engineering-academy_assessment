# Pillar 2 — SQL & Data Visualization

> **Time allocation:** ~4 hours  
> **Starter files:** `starter_files/etl_starter.py` (Task 2.2), `starter_files/data_model_starter.sql` (Tasks 2.1 & 2.3)

---

## Context

With clean data from Pillar 1, you build the analytics layer. Finance and Operations teams need reliable SQL queries for dashboards. The Head of Data wants a Power BI report showing project spend performance.

**Working with 500 projects, 1,000 employees, 50,000 transactions, and ~1,800 salary history records — query design matters now.**

---

## Task 2.1 — SQL: Answer six business questions

**File:** `starter_files/data_model_starter.sql` → Section 3

First, load your cleaned data from Pillar 1 into your warehouse using Section 2.

Each query must:
- Run without errors
- Include a comment explaining your approach
- Return results in the exact column format specified
- Use the `dim_employee` SCD2 table correctly where employee context is needed (current vs. historical)

---

**Q1 — Department budget performance**

Which departments have spent more than 90% of their total allocated budget? Include departments that are over budget.

Required columns: `department`, `total_budget`, `total_actual_cost`, `spend_percentage`, `over_budget`  
Order by: `spend_percentage` descending

---

**Q2 — Project manager workload (with current employee data)**

Which managers are currently overseeing more than three active projects? A manager with too many active projects is a delivery risk.

**Note:** Use `dim_employee` records where `is_current = TRUE`.

Required columns: `full_name`, `email`, `active_project_count`, `combined_budget_responsibility`, `combined_actual_spend`  
Order by: `active_project_count` descending

---

**Q3 — Vendor concentration risk**

Identify vendors who account for more than 5% of total transaction spend. With 50,000 transactions, even a 5% share represents meaningful concentration.

Required columns: `vendor_name`, `total_spend`, `transaction_count`, `percentage_of_total_spend`, `risk_flag` (HIGH if > 10%, MEDIUM if 5–10%, else NORMAL)  
Order by: `percentage_of_total_spend` descending

---

**Q4 — Projects with open financial issues**

Find all projects with pending or disputed transactions totalling more than 50,000 AED. These need finance team attention.

Required columns: `project_id`, `project_name`, `department`, `project_status`, `open_transaction_count`, `open_transaction_value`  
Order by: `open_transaction_value` descending

---

**Q5 — Monthly spend trend with running total**

Show total transaction spend per month, per category, with a running total accumulating within each category over time. This view drives the Finance dashboard.

Required columns: `year_month` (YYYY-MM), `category`, `monthly_spend`, `running_total`, `month_over_month_pct_change`  
Order by: `category`, `year_month` ascending

**Hint:** Use window functions: `SUM() OVER (PARTITION BY category ORDER BY year_month)` for the running total, and `LAG()` for month-over-month change.

---

**Q6 — Employee compensation history analysis**

Using `dim_employee` SCD2 data, identify employees who received the largest single salary increase (in absolute AED terms).

Required columns: `employee_id`, `full_name`, `change_date` (the `valid_from` of the new record), `previous_salary`, `new_salary`, `increase_amount`, `increase_pct`  
Order by: `increase_amount` descending, top 20

**Hint:** Self-join `dim_employee` on `employee_id` matching the previous version's `valid_to` to the next version's `valid_from`.

---

## Task 2.2 — ETL Pipeline: Ingest, transform, and load

**File:** `starter_files/etl_starter.py` → `load_transactions()`, `enrich_transactions()`, `write_outputs()`

Complete the full ETL pipeline.

**1. Load** `datasets/transactions.json` (50,000 rows) into a Pandas DataFrame
- Flatten the JSON into tabular structure
- Parse `transaction_date` as date type
- Decide how to handle null `amount` and null `approved_by` values — **document your decision**

**2. Enrich** transactions:
- Add `project_name` and `department` from projects (via `project_id`)
- Add approver `full_name` from CURRENT employee records (via `approved_by` = `employee_id`)
- Add `is_approved` = True if `approved_by` not null
- Add `amount_aed` = `amount` cast to float, nulls → 0.0
- Add `transaction_year_month` for downstream aggregation

**3. Performance considerations** — with 50,000 transactions:
- Don't materialise the join result if you're only computing aggregates
- Use Pandas merge with explicit `on=` and `how=` parameters
- Consider chunked processing if memory becomes a concern

**4. Write outputs:**
- All three cleaned DataFrames as CSVs in `outputs/`
- `outputs/pipeline_summary.txt` containing:
  - Run timestamp
  - Row counts before/after for each dataset
  - Data quality decisions made
  - Pipeline execution time

**Assessment focus:**
- JSON → tabular conversion is clean
- Joins produce no row duplication
- Null handling decisions documented
- Pipeline handles 50K records in reasonable time (< 30 seconds)

---

## Task 2.3 — Query Optimisation (NOW MEANINGFUL)

**File:** `starter_files/data_model_starter.sql` → Section 4

With 50,000 transactions joined to 500 projects and 1,000 employees, query optimisation has measurable impact. **You should see runtime differences of 10x or more between the slow and optimised queries.**

The slow query is in Section 4 of the SQL file.

**Required deliverables:**

**4a — Benchmark and analyse the original query**

```sql
-- Run with timing
\timing on                            -- PostgreSQL
.timer on                             -- DuckDB / SQLite

-- Run EXPLAIN ANALYZE
EXPLAIN ANALYZE [original query];
```

Paste as comment in your file:
- Total execution time
- The EXPLAIN ANALYZE output
- Identification of:
  - Which join is the bottleneck?
  - Which operations have the highest cost?
  - Are there full table scans where indexes should help?
  - Is the correlated subquery being re-executed per row?

**4b — Rewrite the query**

Apply at least three of the following optimisations:
- Convert implicit `FROM A, B, C` to explicit `JOIN ... ON ...`
- Replace the correlated subquery with a CTE or window function
- Filter rows early (push predicates down)
- Avoid `SELECT *` — pick only needed columns
- Use `EXISTS` instead of `IN` for subqueries against large tables

**4c — Add indexes to support the query**

Write `CREATE INDEX` statements for indexes that would help in production:

```sql
CREATE INDEX idx_<table>_<columns> ON <table> (<columns>);
```

For each index, explain in a comment:
- Which query patterns it accelerates
- Why this column order (for composite indexes)
- Estimated trade-off (write cost vs. read benefit)

**4d — Benchmark the optimised query**

Re-run EXPLAIN ANALYZE on your rewritten query and paste the new output. Show:
- New execution time
- Speedup factor (e.g. "8.3x faster — 420ms → 51ms")
- Confirmation that indexes are being used (look for "Index Scan" not "Seq Scan")

**Assessment focus:**
- Can you read and interpret EXPLAIN ANALYZE output?
- Are your optimisations justified, not random?
- Do indexes target real bottlenecks?
- Is the performance improvement measurable and documented?

---

## Task 2.4 — Dashboard Design

**Tools:** Power BI Desktop, OR a hand-drawn/mockup PDF.

Design or build a one-page executive dashboard for project spend performance.

**Required visuals:**

| Visual | Description | Data source |
|---|---|---|
| KPI cards | Total budget, total actual spend, % over-budget projects, total transactions | From cleaned data |
| Bar chart | Actual spend vs budget by department | Q1 results |
| Line chart | Monthly transaction spend trend by category | Q5 results |
| Table | Top 10 projects by budget variance | From projects_clean |
| Donut chart | Vendor concentration — show top 5 + "Other" | Q3 results |
| Slicer | Region, project status, year | All |

**If building in Power BI:**
- Export as `.pbix` to `outputs/presight_dashboard.pbix`
- Add a title text box with your name and date

**If submitting a mockup:**
- Annotate each visual with what it shows and why you chose that chart type
- Use actual numbers from your dataset analysis
- Export as `outputs/dashboard_mockup.pdf`

---

## Completion checklist

- [ ] Six SQL queries written, each with approach comment
- [ ] ETL pipeline runs end-to-end on 50K transactions in < 30 seconds
- [ ] `outputs/transactions_clean.csv` written
- [ ] `outputs/pipeline_summary.txt` with row counts, decisions, and run time
- [ ] Original query's EXPLAIN ANALYZE pasted with bottleneck analysis
- [ ] Rewritten query achieves measurable speedup (paste before/after timings)
- [ ] Index DDL written with justification per index
- [ ] Dashboard `.pbix` or `dashboard_mockup.pdf` in `outputs/`
- [ ] Q6 correctly uses SCD2 employee history (self-join pattern)
