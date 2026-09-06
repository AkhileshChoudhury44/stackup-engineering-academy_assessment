# Data Quality Report: `employees`
**Execution Timestamp:** 2026-09-06 21:41:34  
**Total Checks Run:** 6 | **Passed:** 5 | **Failed:** 1

| Check Name | Status | Details |
| :--- | :---: | :--- |
| `completeness` | PASS | {'employee_id': 1.0, 'full_name': 1.0, 'email': 1.0, 'department': 1.0, 'role': 1.0, 'level': 1.0, 'hire_date': 1.0, 'salary': 1.0, 'manager_id': 1.0, 'region': 1.0, 'status': 1.0, 'years_experience': 1.0} |
| `uniqueness` | PASS | All 1000 records are unique across ['employee_id'] |
| `validity_numeric` | PASS | All numeric columns within bounds |
| `validity_date` | PASS | All date columns valid |
| `consistency` | PASS | Consistency rules satisfied |
| `outliers_salary` | **FAIL** | 17 statistical outliers (>3 std dev) detected in salary |
