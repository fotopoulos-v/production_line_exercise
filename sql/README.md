# SQL Implementation Guide

This folder contains the SQL files needed to replicate the production KPI
calculations inside a Data Warehouse (DWH). The implementation uses PostgreSQL
and is organised into 4 files that must be executed in order.

---

## Files

| File | Description |
|------|-------------|
| `1_tables.sql` | Creates the `production` schema and the `raw_events` table |
| `2_sample_data.sql` | *(Optional)* Inserts sample data for testing and demonstration |
| `3_sessions.sql` | Creates the `sessions` view — builds the full uptime/downtime timeline |
| `4_kpis.sql` | Creates 3 KPI views that answer the business questions |

---

## Prerequisites

- PostgreSQL 12 or higher
- A database where you have `CREATE SCHEMA` and `CREATE TABLE` privileges
- A database client such as psql, DBeaver, or pgAdmin

---

## Installation Steps

### Step 1 — Create the table

Run `1_tables.sql` to create the `production` schema and the `raw_events` table:
```sql
\i 1_tables.sql
```

This creates:
- Schema: `production`
- Table: `production.raw_events`

---

### Step 2 — Load data (Optional)

If you want to use the provided sample data, run `2_sample_data.sql`:
```sql
\i 2_sample_data.sql
```

**Skip this step** if you are loading your own data from an existing source.
Your data must follow this structure:

| Column | Type | Description |
|--------|------|-------------|
| `production_line_id` | VARCHAR(50) | Unique identifier of the production line |
| `status` | VARCHAR(10) | ON, START, or STOP |
| `timestamp` | TIMESTAMP WITHOUT TIME ZONE | Exact timestamp of the status update |

---

### Step 3 — Create the sessions view

Run `3_sessions.sql` to create the `production.sessions` view:
```sql
\i 3_sessions.sql
```

This view builds the complete uptime and downtime timeline per production line.
It is the foundation for all KPI calculations and can be queried directly:
```sql
SELECT * FROM production.sessions;
```

---

### Step 4 — Create the KPI views

Run `4_kpis.sql` to create the 3 KPI views:
```sql
\i 4_kpis.sql
```

---

## Querying the KPIs

### Business Question 1
For production line "gr-np-47", return all uptime sessions with their
start timestamp, stop timestamp and duration:
```sql
SELECT * FROM production.kpi_line_sessions;
```

### Business Question 2
Total uptime and downtime of the whole production floor:
```sql
SELECT * FROM production.kpi_floor_uptime_downtime;
```

### Business Question 3
Which production line had the most downtime and how much was it:
```sql
SELECT * FROM production.kpi_most_downtime_line;
```

---

## Schema Overview
```
production (schema)
│
├── raw_events (table)               ← source data
├── sessions (view)                  ← uptime/downtime timeline
├── kpi_line_sessions (view)         ← Business Question 1
├── kpi_floor_uptime_downtime (view) ← Business Question 2
└── kpi_most_downtime_line (view)    ← Business Question 3
```

---

## Notes

- All SQL files are idempotent — safe to run multiple times without errors
- The `sessions` view automatically detects the observation window from the data
- Incomplete sessions (no START or no STOP) are flagged with `is_complete = FALSE`
- For any questions about the implementation refer to the Python package which
  mirrors this logic and includes inline comments throughout

---

## Integrating with an Existing DWH

If your DWH already collects production line data, you may not need to run
`1_tables.sql` or `2_sample_data.sql` at all. Follow these steps instead:

### Scenario 1 — Your existing table has the same column names

Skip `1_tables.sql` and `2_sample_data.sql` entirely. Simply update the
table reference in `3_sessions.sql` from `production.raw_events` to your
existing table name. For example:
```sql
-- Replace this:
FROM production.raw_events

-- With your existing table:
FROM your_schema.your_table
```

Then run `3_sessions.sql` and `4_kpis.sql` as normal.

---

### Scenario 2 — Your existing table has different column names

If your existing table uses different column names, update the column
references in `3_sessions.sql` to match. For example, if your table uses
`line_id` instead of `production_line_id` and `event_status` instead of
`status`:
```sql
-- Replace this:
SELECT
    production_line_id,
    status,
    timestamp
FROM production.raw_events

-- With your existing column names:
SELECT
    line_id             AS production_line_id,
    event_status        AS status,
    event_timestamp     AS timestamp
FROM your_schema.your_table
```

By aliasing your columns to match the expected names, the rest of the SQL
files will work without any further changes.

---

### Scenario 3 — Your DWH uses a different SQL dialect

The SQL in this package is written for **PostgreSQL 12+**. If your DWH uses
a different dialect, adjustments may be needed:

