# Data Quality Report: `transactions`
**Execution Timestamp:** 2026-09-06 21:41:34  
**Total Checks Run:** 7 | **Passed:** 6 | **Failed:** 1

| Check Name | Status | Details |
| :--- | :---: | :--- |
| `completeness` | **FAIL** | Columns below threshold (0.95): ['notes'] |
| `uniqueness` | PASS | All 50000 records are unique across ['transaction_id'] |
| `validity_numeric` | PASS | All numeric columns within bounds |
| `validity_date` | PASS | All date columns valid |
| `consistency` | PASS | Consistency rules satisfied |
| `referential_integrity` | PASS | Referential integrity validated |
| `distribution_category` | PASS | Distribution balanced (top 20.3%) |
