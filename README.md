# CHW Monthly Activity Metrics Model (`chw_activity_monthly`)

**Author**: [Kioko Mutinda]  
**Date**: December 2025  
**Warehouse**: Snowflake  
**Database**: `CHW` | **Schema**: `marts`

---

### Overview

This dbt model aggregates Community Health Worker (CHW) activities into **monthly performance metrics** with a **special business rule**:

> **Activities on or after the 26th of the month are assigned to the NEXT reporting month**  
> (e.g., Jan 28 → February, Dec 31 → January next year)

---

### Key Features

- Handles **26th cutoff rule** via reusable macro `month_assignment()`
- Fully **incremental** with `delete+insert` strategy
- Supports **late-arriving data** and historical corrections
- Filters out invalid records (`NULL chv_id`, `NULL activity_date`, `is_deleted = TRUE`)
- 100% tested with `not_null`, `unique_combination_of_columns`, and data quality checks
- Works across **year boundaries** (Dec 2024 → Jan 2025)

---

### Source

- **Source Table**: `CHW.marts.FCT_CHV_ACTIVITY`
- Referenced via `{{ source('chw', 'fct_chv_activity') }}`
- Defined in `models/staging/sources.yml`

---

### How to Run

```bash
# 1. Install dependencies (run once)
dbt deps

# 2. Build the model
dbt run --select chw_activity_monthly

# 3. Run tests (all should pass)
dbt test --select chw_activity_monthly
```

### Project Structure

models/
├── metrics/
│ ├── chw_activity_monthly.sql
│ └── schema.yml
├── staging/
│ └── sources.yml
macros/
└── month_assignment.sql
└── show_results_after_run.sql ← auto-prints results after run
packages.yml ← includes dbt_utils

### Tests

This model is has **9 automated dbt tests** that run on every `dbt test` command:

These tests guarantee:

- No duplicate CHW-month rows (critical for dashboards)
- No missing or NULL keys
- Data integrity even when late-arriving or corrected records come in

All tests are defined in `models/metrics/schema.yml` and use the `dbt_utils` package.
