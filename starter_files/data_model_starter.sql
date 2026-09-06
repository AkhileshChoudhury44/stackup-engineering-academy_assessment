-- =============================================================
-- StackUp Engineering Academy — Data Engineering Assessment
-- Starter File: data_model_starter.sql
-- Pillars: Foundations (Task 1.2) | SQL & Viz (Tasks 2.1, 2.3)
-- =============================================================
--
-- SCENARIO
-- --------
-- Presight runs a project management platform. You are designing
-- the data warehouse layer that will power analytics dashboards
-- for Finance, Operations, and Executive leadership.
--
-- The source data comes from three operational tables:
--   projects      → datasets/projects.csv
--   employees     → datasets/employees.csv
--   transactions  → datasets/transactions.json
--
-- HOW TO USE
-- ----------
-- This file is divided into four sections.
-- Work through each section in order.
-- Run against your local database (SQLite, PostgreSQL, or DuckDB all work).
-- =============================================================


-- ===========================================================================
-- SECTION 1 — TASK 1.2: Design the data model (Star Schema)
-- ===========================================================================
--
-- Design a star schema for the Presight project analytics warehouse.
--
-- REQUIREMENTS:
--   - One central fact table: fact_transactions
--   - At minimum four dimension tables:
--       dim_project, dim_employee, dim_vendor, dim_date
--   - fact_transactions must include foreign keys to all four dimensions
--   - dim_date must be a proper date dimension (not just a date column)
--     with columns for year, quarter, month, month_name, week, day, is_weekend
--   - Use surrogate keys (integer PKs) on all dimension tables
--   - Preserve the original source system IDs as natural keys
--   - Add a dim_employee_project bridge table to handle the many-to-many
--     relationship between employees and projects (one employee can manage
--     multiple projects; one project can have multiple team members)
--
-- BONUS:
--   - Add a slowly-changing dimension (SCD Type 2) design to dim_employee
--     to track salary changes over time
--
-- DOCUMENT YOUR DECISIONS:
--   Write a comment before each table explaining why you designed it that way.

-- TODO: Create dim_date
-- YOUR CODE HERE


-- TODO: Create dim_project
-- YOUR CODE HERE


-- TODO: Create dim_employee
-- YOUR CODE HERE


-- TODO: Create dim_vendor
-- YOUR CODE HERE


-- TODO: Create bridge_employee_project (many-to-many)
-- YOUR CODE HERE


-- TODO: Create fact_transactions
-- YOUR CODE HERE


-- ===========================================================================
-- SECTION 2 — Load staging data
-- ===========================================================================
--
-- Before answering the business questions, load your cleaned data
-- (output of etl_starter.py) into the tables above.
--
-- If using SQLite: use .import or INSERT statements
-- If using DuckDB: use read_csv_auto() or read_json_auto()
-- If using PostgreSQL: use COPY or \copy

-- Example (DuckDB):
-- INSERT INTO dim_project SELECT * FROM read_csv_auto('outputs/projects_clean.csv');

-- TODO: Load your data here
-- YOUR CODE HERE


-- ===========================================================================
-- SECTION 3 — TASK 2.1: Answer five business questions
-- ===========================================================================
--
-- Write a SQL query to answer each question.
-- For each query, add a comment explaining your approach.
-- Queries must run without errors against your loaded data.

-- ---------------------------------------------------------------------------
-- Q1. BUDGET PERFORMANCE
-- Which departments have spent more than 90% of their total budget across
-- all projects? Show department, total_budget, total_actual_cost,
-- spend_percentage, and whether they are over budget.
-- Order by spend_percentage descending.
-- ---------------------------------------------------------------------------

-- YOUR CODE HERE


-- ---------------------------------------------------------------------------
-- Q2. PROJECT MANAGER WORKLOAD
-- Which project managers are currently managing more than one active project
-- (status = 'In Progress')? Show their full name, email, number of active
-- projects, and the combined budget they are responsible for.
-- ---------------------------------------------------------------------------

-- YOUR CODE HERE


-- ---------------------------------------------------------------------------
-- Q3. VENDOR CONCENTRATION RISK
-- Identify vendors who account for more than 30% of total transaction spend
-- across all projects. Show vendor_name, total_spend, percentage_of_total.
-- This is a risk indicator — flag these vendors in the output.
-- ---------------------------------------------------------------------------

-- YOUR CODE HERE


-- ---------------------------------------------------------------------------
-- Q4. ESCALATION-TO-COMPLETION RATE
-- Using the events data (if loaded), calculate the percentage of projects
-- that had at least one escalation event during their lifecycle.
-- If events data is not available, use the transactions data instead:
-- calculate the percentage of projects with a 'Disputed' transaction.
-- Show project_id, project_name, status, had_issue (True/False).
-- ---------------------------------------------------------------------------

-- YOUR CODE HERE


-- ---------------------------------------------------------------------------
-- Q5. MONTHLY SPEND TREND
-- Show the total transaction amount per month for the past 12 months,
-- broken down by category (Software, Consulting, Cloud Services, etc.)
-- Format the output as: year_month | category | total_amount | running_total
-- (running_total should accumulate within each category across months)
-- ---------------------------------------------------------------------------

-- YOUR CODE HERE


-- ===========================================================================
-- SECTION 4 — TASK 2.3: Query optimisation
-- ===========================================================================
--
-- The following query runs slowly in production. It is used on the finance
-- dashboard and executes hundreds of times per day.
--
-- YOUR TASKS:
--   a) Run EXPLAIN or EXPLAIN ANALYZE on the query (depending on your DB)
--      and paste the output as a comment. Identify the bottleneck.
--
--   b) Rewrite the query to improve performance.
--      You may restructure it, add CTEs, change joins, etc.
--
--   c) Write a comment explaining:
--      - What was wrong with the original query
--      - What you changed and why
--      - What indexes (if any) you would add in production
--
-- NOTE: Load sufficient data to make the performance difference meaningful.
--       If running on a small dataset, describe what you would observe at scale.

-- ORIGINAL QUERY (do not modify this — rewrite it in section 4b below)
-- ---------------------------------------------------------------------------
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
    t.transaction_date
FROM employees e, projects p, transactions t
WHERE e.employee_id = p.project_manager_id
AND   p.project_id  = t.project_id
AND   p.status NOT IN ('Completed', 'On Hold')
AND   t.payment_status = 'Pending'
AND   t.amount > (
        SELECT AVG(amount)
        FROM transactions
        WHERE payment_status = 'Pending'
      )
ORDER BY e.department, t.amount DESC;

-- ---------------------------------------------------------------------------
-- 4a) EXPLAIN output and bottleneck analysis (paste as comment):
-- ---------------------------------------------------------------------------
-- YOUR ANALYSIS HERE


-- ---------------------------------------------------------------------------
-- 4b) REWRITTEN QUERY
-- ---------------------------------------------------------------------------
-- YOUR CODE HERE


-- ---------------------------------------------------------------------------
-- 4c) Explanation of changes and indexing strategy
-- ---------------------------------------------------------------------------
-- YOUR EXPLANATION HERE