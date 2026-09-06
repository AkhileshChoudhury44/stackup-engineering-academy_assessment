# Data Quality Report: `projects`
**Execution Timestamp:** 2026-09-06 21:41:34  
**Total Checks Run:** 7 | **Passed:** 4 | **Failed:** 3

| Check Name | Status | Details |
| :--- | :---: | :--- |
| `completeness` | **FAIL** | Columns below threshold (0.9): ['start_date', 'end_date', 'duration_days'] |
| `uniqueness` | PASS | All 500 records are unique across ['project_id'] |
| `validity_numeric` | PASS | All numeric columns within bounds |
| `validity_date` | **FAIL** | start_date: 65 invalid date values; end_date: 286 invalid date values |
| `consistency` | PASS | Consistency rules satisfied |
| `referential_integrity` | PASS | Referential integrity validated |
| `outliers_actual_cost` | **FAIL** | 11 statistical outliers (>3 std dev) detected in actual_cost |
