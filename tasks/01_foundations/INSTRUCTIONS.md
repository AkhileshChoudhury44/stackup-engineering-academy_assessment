# Pillar 1 — Foundations

> **Time allocation:** ~3.5 hours  
> **Starter files:** `starter_files/etl_starter.py` (Tasks 1.1 & 1.3), `starter_files/data_model_starter.sql` (Task 1.2)

---

## Context

You are joining the data engineering team at **Presight**, a technology company running a project management platform used by government and enterprise clients. The team has received four raw data exports from the operational systems:

| File | Size | Description |
|---|---|---|
| `datasets/projects.csv` | 500 rows | Project records — status, budget, dates |
| `datasets/employees.csv` | 1,000 rows | Employee records — contains data quality issues |
| `datasets/transactions.json` | 50,000 rows | Financial transactions linked to projects |
| `datasets/employees_salary_history.csv` | ~1,800 rows | Historical salary/role changes (~60% of employees) |

> **Dataset scale note:** These volumes are realistic for a mid-sized enterprise. Operations that worked on toy datasets (loading everything into memory, nested loops) will not scale. Think about efficiency from the start.

---

## Task 1.1 — Python: Clean and transform projects.csv

**File:** `starter_files/etl_starter.py` → `load_projects()` and `transform_projects()`

**What to do:**

1. Load `datasets/projects.csv` into a Pandas DataFrame with correct data types
2. Parse `start_date` and `end_date` as proper date columns
3. Add four derived columns:
   - `budget_variance` = `actual_cost` − `budget`
   - `is_over_budget` = True if `actual_cost` > `budget`, else False. Handle nulls.
   - `duration_days` = days between `start_date` and `end_date` where both exist
   - `budget_utilisation_pct` = `actual_cost` / `budget` × 100 (handle div-by-zero)
4. Standardise `status` values (strip whitespace, consistent casing)
5. Map statuses to `status_category`: Active, Closed, or Pending
6. Replace null `budget` or `actual_cost` values with 0
7. Add a derived `risk_level` based on combined logic:
   - `High` if priority = Critical OR is_over_budget = True
   - `Medium` if priority = High OR budget_utilisation_pct > 90
   - `Low` otherwise

**Expected output:** `outputs/projects_clean.csv` (~500 rows × 15 columns)

**Assessment focus:**
- Correct dtype handling
- **Vectorised operations** — avoid `.iterrows()`, won't scale
- Sensible null handling
- Readable, commented code

---

## Task 1.2 — Data Modelling: Star schema with SCD Type 2

**File:** `starter_files/data_model_starter.sql` → Section 1

Design the analytics warehouse star schema:

| Table | Type | Notes |
|---|---|---|
| `dim_date` | Dimension | Full date dim — year, quarter, month, month_name, week, day, day_of_week, is_weekend |
| `dim_project` | Dimension | From projects.csv |
| `dim_employee` | **SCD Type 2** | Track salary and role changes over time |
| `dim_vendor` | Dimension | Derived from transactions vendor fields |
| `bridge_employee_project` | Bridge | Many-to-many employees ↔ projects |
| `fact_transactions` | Fact | Central fact with FKs to all dimensions |

**Constraints:**
- Surrogate integer keys on all dimensions
- Preserve source IDs as natural keys
- `fact_transactions` must include: `transaction_key`, `project_key`, `employee_key`, `vendor_key`, `date_key`, `amount`, `category`, `payment_status`
- Comment each table explaining your design decision

---

### SCD Type 2 requirements for `dim_employee`

You have two sources:
- `employees.csv` — current state (1,000 rows)
- `employees_salary_history.csv` — historical changes (~1,800 rows)

Your `dim_employee` must:

1. **Track history** — `valid_from`, `valid_to`, `is_current` columns
2. **Maintain integrity** — `is_current = TRUE` exactly once per `employee_id`
3. **Surrogate key** (`employee_key`) uniquely identifies each version
4. **Order chronologically** — non-overlapping periods per employee
5. **Sentinel end date** for current records (e.g. `9999-12-31`)

**Implementation hints:**
- For employees in the history file: one row per historical record + one current row
- For employees NOT in history: single row with `valid_from = hire_date`, `valid_to = 9999-12-31`, `is_current = TRUE`
- Salary and role values must match the time period

**Required validation queries (include in your SQL file):**

```sql
-- Q1: No employee has more than one current record
SELECT employee_id, COUNT(*) AS current_count
FROM dim_employee
WHERE is_current = TRUE
GROUP BY employee_id
HAVING COUNT(*) > 1;     -- Must return zero rows

-- Q2: Show employees with version history
SELECT employee_id, COUNT(*) AS version_count
FROM dim_employee
GROUP BY employee_id
ORDER BY version_count DESC
LIMIT 10;     -- Expect employees with 2-5 versions

-- Q3: Write a self-join to detect overlapping periods per employee
-- (Your code here)
```

**Bonus:** Add a `change_reason` column populated from the history file.

---

## Task 1.3 — Data Quality: Find and fix issues in employees.csv

**File:** `starter_files/etl_starter.py` → `clean_employees()`

**What to do:**

Multiple data quality issues are sprinkled across the 1,000 employee records — you will NOT find them by inspecting the first 40 rows. Use proper detection logic.

For each issue:
1. Use Pandas aggregations to **detect** how many rows are affected
2. Comment the issue in code
3. Apply a fix and log row counts

**Categories to investigate (find ALL instances of each):**

| Category | Look for |
|---|---|
| Missing values | Required columns with nulls or empty strings |
| Invalid date formats | Dates that won't parse to a date type |
| Implausible dates | Dates outside any reasonable business range |
| Numeric out-of-range | Salary or experience values that don't fit the level |
| Logical inconsistencies | Salary doesn't match level expectations |
| Status conflicts | Records whose state contradicts other fields |

**Required deliverables:**

1. **Detection report** logged at INFO level — counts per issue type
2. **Cleaned DataFrame** → `outputs/employees_clean.csv`
3. **Quality summary** → rows affected per fix type

**Hint — scalable detection pattern:**

```python
# Don't do this — won't scale to 1,000 rows × multiple columns:
for index, row in df.iterrows():
    if row['email'] is None:
        ...

# Do this:
missing_email = df['email'].isna() | (df['email'] == '')
logger.info(f"Missing emails: {missing_email.sum()}")
df.loc[missing_email, 'email'] = 'unknown@presight.ai'
```

**Assessment focus:**
- Did you find ALL issues, not just obvious ones?
- Detection methods are **vectorised** (not row-by-row)
- Each fix is documented and defensible
- Output includes both cleaned data AND a quality report

---

## Completion checklist

- [ ] `outputs/projects_clean.csv` written with all 15 derived columns
- [ ] `outputs/employees_clean.csv` written with quality summary
- [ ] `data_model_starter.sql` Section 1 contains valid DDL for all 6 tables
- [ ] Each table commented with design rationale
- [ ] `dim_employee` correctly implements SCD Type 2
- [ ] Validation queries prove integrity (no duplicate currents, no overlapping periods)
- [ ] All data quality issues detected with row counts logged
- [ ] Code uses vectorised operations — no `.iterrows()`
