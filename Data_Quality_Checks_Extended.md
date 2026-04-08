# Data Quality Checks

For analytics engineers who want clean data without the noise.

---

## Why This Matters

As an analytics engineer joining a new organization, you typically inherit existing data pipelines. Upon executing your first queries, you encounter results that do not align with business expectations. Investigation reveals that source data contains inconsistencies, and transformation logic compounds these issues. What follows is a multi-day debugging process to trace errors that could have been identified within minutes through systematic quality checks.

This guide establishes protocols for preventing such scenarios. It does not rely on proprietary tools or external platforms. It relies on disciplined SQL practices and explicit validation decisions at each processing stage.

---

## Table of Contents
- [Why This Matters](#why-this-matters)
- [Part 1: Incoming Data is Dirty](#part-1-incoming-data-is-dirty)
  - [Schema Drift](#schema-drift)
  - [Schema Evolution Safety](#schema-evolution-safety)
  - [Volume Anomalies](#volume-anomalies)
  - [Data Freshness](#data-freshness)
  - [Duplicate Records](#duplicate-records)
  - [Orphan Records](#orphan-records)
  - [Type Mismatches](#type-mismatches)
  - [Encoding & Character Issues](#encoding--character-issues)
  - [Whitespace & Padding](#whitespace--padding)
  - [Case Standardization](#case-standardization)
  - [Flexible Categorization](#flexible-categorization)
  - [Defensive Value Mapping](#defensive-value-mapping)
  - [Impossible Values](#impossible-values)
  - [Outliers](#outliers)
  - [Missing Values (NULLs)](#missing-values-nulls)
- [Part 2: Adjusting Data for Analysis](#part-2-adjusting-data-for-analysis)
  - [Division by Zero](#division-by-zero)
  - [Aggregation Errors](#aggregation-errors)
  - [Date Arithmetic Errors](#date-arithmetic-errors)
  - [Implicit Cast Failures](#implicit-cast-failures)
  - [Circular References](#circular-references)
  - [Post-Transform Reconciliation](#post-transform-reconciliation)
- [Part 3: Monitoring & Alerting](#part-3-monitoring--alerting)
  - [DWH Architecture for Quality](#dwh-architecture-for-quality)
  - [Data Lineage Mapping](#data-lineage-mapping)
  - [Quarantine Performance Guardrails](#quarantine-performance-guardrails)
  - [Monitoring Methods](#monitoring-methods)
  - [Alerting Channels](#alerting-channels)
  - [Runbook Automation](#runbook-automation)
  - [Checklists](#checklists)
- [Personal Experience](#personal-experience)
- [AI Collaboration Disclosure](#ai-collaboration-disclosure)

---

## Part 1: Incoming Data is Dirty
Problems caused by sources you don't control. Fix or flag before any transformation.

**DWH Approach:** Apply all checks in staging layer (raw data landing zone). Implement as views or ephemeral models that filter/quarantine bad records before persistence. Never allow dirty data into core warehouse tables. Use separate `quarantine` schema for rejected records with full audit trail.  
**Monitoring:** `[Execution Context: SQL is the validation logic. dbt/Airflow/CI systems schedule and report these standard queries.]` Automated tests on staging models, separate `data_quality` schema with rejected records, freshness checks in orchestrator, schema validation via source contracts.

### Schema Drift

**Problem:** Table structure changes without warning.  
**Consequences:** Your query selects columns that no longer exist, or misses new critical fields. Pipeline fails or produces incomplete data.  
**Decision:** Check schema before every transformation.

**DWH Approach:** Staging layer source contracts (dbt 1.5+), `information_schema` polling in ingestion pipeline, versioned schemas for breaking changes. Store contracts in YAML/JSON Schema, integrate into CI/CD workflows.

**Monitoring:** Schema change alerts via webhook to Slack/Teams; dbt source freshness tests; separate `schema_changes` audit table logging all DDL alterations.

```sql
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'your_table'
ORDER BY ordinal_position;
```
### Schema Evolution Safety

**Problem:** `ALTER TABLE ... ADD COLUMN` breaks downstream pipelines or causes full table rewrites when adding non-nullable fields.  
**Consequences:** Pipeline halts on deployment. Backfill scripts lock tables. Reports break during transition windows.  
**Decision:** Always add columns as nullable with defaults, backfill asynchronously, then enforce constraints.  
**DWH Approach:** Staging/Intermediate layer: 3-step evolution pattern. Never add `NOT NULL` without a default or backfill job.  
**Monitoring:** `[Execution Context: CI/CD pipeline runs validation SQL]` `schema_evolution_log` tracking migration steps; pipeline blocking if `ALTER` lacks nullable default or backfill reference.

```
-- STEP 1: Add nullable column (zero-downtime, no table lock)
ALTER TABLE staging.orders ADD COLUMN discount_code VARCHAR(50);

-- STEP 2: Backfill asynchronously (run as separate job)
UPDATE staging.orders 
SET discount_code = 'DEFAULT' 
WHERE discount_code IS NULL AND order_date >= '2024-01-01';

-- STEP 3: Enforce constraint once backfill completes
ALTER TABLE staging.orders ALTER COLUMN discount_code SET NOT NULL;
```

### Volume Anomalies
**Problem:** Row counts change unexpectedly.  
**Consequences:** 50% drop = upstream pipeline broke. 300% spike = duplicate data. Zero rows = silent failure. All lead to wrong business decisions.  
**Decision:** Compare today's count to historical average. Stop if deviation > 20%.  
**DWH Approach:** Staging layer row count validation before `INSERT`. Use `COUNT(*)` in pre-flight CTE; abort load if outside tolerance.  
**Monitoring:** `[Execution Context: Scheduler compares historical SQL outputs]` Time-series table `volume_metrics` (date, table_name, row_count, z_score); BI dashboard with 7-day rolling bands; pager alert on 3-sigma deviation.
```sql
-- Optimized: Single scan, dialect-agnostic duplicate detection
SELECT 
    COUNT(*) as raw_count,
    COUNT(DISTINCT customer_id || '|' || order_date || '|' || amount) as unique_combinations,
    COUNT(*) - COUNT(DISTINCT customer_id || '|' || order_date || '|' || amount) as duplicate_rows
FROM orders;
```

### Data Freshness

**Problem:** Pipeline delivers stale data.  
**Consequences:** Decisions based on yesterday's data in real-time business. Missed opportunities. Wrong actions.  
**Decision:** Check last update time. Stop if > SLA threshold.

**DWH Approach:** Staging layer metadata check before transformation. SLA enforcement via pipeline orchestrator (fail task if `MAX(updated_at)` < threshold).

**Monitoring:** `freshness_alerts` table with SLA breach history; BI dashboard showing lag by source; automated Slack alert with `@channel` for critical tables > 4 hours stale.

```sql
SELECT 
    MAX(updated_at) as last_update,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at)))/3600 as hours_old
FROM your_table;
```

### Duplicate Records

**Problem:** Same entity appears multiple times.  
**Consequences:** Revenue double-counted. Customer counts inflated. Metrics meaningless.  
**Decision:** Check primary key uniqueness before aggregation.

**DWH Approach:** Staging layer: Deduplicate via `ROW_NUMBER()` before persistence, or route duplicates to `quarantine.duplicates` table. Never pass duplicates to mart layer.

**Monitoring:** `duplicate_counts` daily snapshot table; dbt uniqueness tests on all primary keys; BI dashboard showing duplicate rate by source.

```sql
-- Primary key duplicates
SELECT id, COUNT(*) 
FROM your_table 
GROUP BY id 
HAVING COUNT(*) > 1;

-- Semantic duplicates (same email, different IDs)
SELECT email, COUNT(DISTINCT customer_id) 
FROM customers 
GROUP BY email 
HAVING COUNT(DISTINCT customer_id) > 1;

-- Fix: Deduplicate before aggregating
-- Select * will also return rn. So getting back to rule above - always be explicit in select statement about the fileds to return!
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
    FROM your_table
) WHERE rn = 1;
```

### Orphan Records

**Problem:** Foreign keys point to non-existent parent records.  
**Consequences:** Joins silently drop data. Reports undercount. Relationships appear broken when they exist.  
**Decision:** Verify referential integrity before joins.

**DWH Approach:** Staging layer referential integrity checks. Left join to parent; filter orphans to `quarantine.orphans` table with FK violation details.

**Monitoring:** `orphan_rate` metric in data quality dashboard; dbt relationship tests; weekly orphan reconciliation report to source system owners.

```sql
SELECT COUNT(*) as orphan_count
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL 
  AND o.customer_id IS NOT NULL;
```

### Type Mismatches

**Problem:** Data arrives as wrong type (TEXT instead of DECIMAL, timestamps as strings).  
**Consequences:** Implicit casting fails or produces wrong results. Aggregations break. Sorting is lexical instead of numeric.  
**Decision:** Explicitly cast and validate before use.

**DWH Approach:** Staging layer: Regex validation before cast; invalid formats to `quarantine.type_errors`. Cast only after validation.

**Monitoring:** `type_mismatch_log` table capturing failed casts; dbt tests with `try_cast` patterns; source system alerts for schema violations.

```sql
-- Validate: Can this cast to decimal?
SELECT 
    CASE 
        WHEN amount ~ '^[0-9]+\.?[0-9]*$' THEN amount::DECIMAL
        ELSE NULL
    END as amount_clean,
    CASE 
        WHEN amount ~ '^[0-9]+\.?[0-9]*$' THEN 'OK'
        ELSE 'INVALID_TYPE'
    END as flag
FROM orders;
```

### Encoding & Character Issues

**Problem:** Special characters, encoding mismatches, invisible characters (BOM, zero-width spaces).  
**Consequences:** String matching fails. 'Paris' != 'Paris' (with invisible char). Group by produces multiple versions of same value.  
**Decision:** Clean and standardize text on ingestion.

**DWH Approach:** Staging layer: `REGEXP_REPLACE` normalization for all text fields. Store original in `raw_text` column, cleaned in `clean_text`.

**Monitoring:** `encoding_issues` table tracking non-printable character detection; character distribution analysis in BI; alerts on > 1% of records with encoding flags.

```sql
-- Find non-printable characters
SELECT 
    customer_name,
    LENGTH(customer_name) as char_count,
    LENGTH(REGEXP_REPLACE(customer_name, '[\x00-\x1F\x7F]', '', 'g')) as clean_count
FROM customers
WHERE LENGTH(customer_name) != LENGTH(REGEXP_REPLACE(customer_name, '[\x00-\x1F\x7F]', '', 'g'));

-- Standardize: lowercase, trim whitespace, remove special chars
SELECT 
    LOWER(TRIM(REGEXP_REPLACE(customer_name, '[^a-zA-Z0-9\s]', '', 'g'))) as clean_name
FROM customers;
```

### Whitespace & Padding

**Problem:** Leading/trailing spaces, multiple internal spaces, tab characters.  
**Consequences:** 'ABC Corp' and 'ABC Corp ' don't match. Joins fail silently.  
**Decision:** Always TRIM() string fields on ingestion.

**DWH Approach:** Staging layer: `TRIM()` all string fields via macro. Compare raw vs trimmed; flag if different.

**Monitoring:** `padding_detection` view showing records with leading/trailing spaces; automated cleanup reports; dbt tests for trimmed values.

```sql
-- Find padded values
SELECT 
    product_code,
    CONCAT('|', product_code, '|') as visualized
FROM products
WHERE product_code != TRIM(product_code);

-- Clean
SELECT TRIM(BOTH ' ' FROM product_code) as clean_code FROM products;
```

### Case Standardization

**Problem:** Same value in different cases (USA, usa, Usa).  
**Consequences:** Group by splits single category into multiple.  
**Decision:** Standardize case for all categorical fields.

**DWH Approach:** Staging layer: `LOWER()` or `UPPER()` all categorical fields. Maintain `original_case` in metadata if needed for audit.

**Monitoring:** `case_variance` report showing distinct values that normalize to same string; BI dashboard with standardization rate by field.

```sql
-- Check case variations
SELECT country, COUNT(*) 
FROM customers 
GROUP BY country 
ORDER BY LOWER(country);

-- Standardize
SELECT LOWER(country) as standard_country FROM customers;
```

### Flexible Categorization

**Problem:** Hardcoded categorization rules break when new values appear in source data. Maintenance requires hunting through SQL files to add new branches.

**Consequences:** Uncategorized values produce NULLs in reports. Business sees incomplete data. Engineer spends hours tracing why metrics don't match.

**Decision:** Structure CASE statements with explicit fallback that surfaces unmapped values for immediate visibility.

**DWH Approach:** Staging layer: Always include `ELSE 'UNMAPPED: ' || column` to catch new values without silent NULLs. Route flagged records to `quarantine.unmapped_values` table for data steward review. Update mappings weekly, not ad-hoc.

**Monitoring:** `unmapped_value_log` table tracking frequency of new values by source; Slack alert when unmapped rate exceeds 1% of daily volume; BI dashboard showing unmapped value trends.

```sql
-- WRONG: Silent NULLs for new values, hard to detect
SELECT 
    CASE region
        WHEN 'US' THEN 'North America'
        WHEN 'CA' THEN 'North America'
        WHEN 'UK' THEN 'Europe'
    END as region_group
FROM staging.customers;

-- RIGHT: Visible unmapped values, self-documenting
SELECT 
    CASE 
        WHEN region IN ('US', 'CA', 'MX') THEN 'North America'
        WHEN region IN ('UK', 'DE', 'FR', 'ES', 'IT') THEN 'Europe'
        WHEN region IN ('JP', 'CN', 'KR', 'IN') THEN 'Asia-Pacific'
        WHEN region IS NULL THEN 'Unknown'
        ELSE 'UNMAPPED: ' || region  -- Shows exactly what needs fixing
    END as region_group
FROM staging.customers;

-- Quarantine pattern: Isolate unmapped for review
INSERT INTO quarantine.unmapped_values
SELECT 
    'customers' as source_table,
    'region' as column_name,
    region as raw_value,
    'UNMAPPED: ' || region as flag,
    CURRENT_TIMESTAMP as detected_at
FROM staging.customers
WHERE region NOT IN ('US', 'CA', 'MX', 'UK', 'DE', 'FR', 'ES', 'IT', 'JP', 'CN', 'KR', 'IN')
  AND region IS NOT NULL;
```

### Defensive Value Mapping

**Problem:** Mapping tables or CASE statements miss target values that match themselves, creating gaps where clean data becomes NULL.

**Consequences:** 'New York' in source becomes NULL in output because mapper only handled 'ny' and 'nyc'. Valid data appears missing. Reports undercount.

**Decision:** Include target spelling in IN list to prevent self-mapping gaps. Never assume source values are already clean.

**DWH Approach:** Staging layer: All variants—including the desired output—belong in the same WHEN clause. Output standardized value regardless of input form.

**Monitoring:** `mapping_gap_detection` query comparing input distinct values vs mapped output; dbt test ensuring zero NULLs in standardized columns when source is non-NULL.

```sql
-- WRONG: 'New York' becomes NULL because target not in list
SELECT 
    CASE 
        WHEN city = 'ny' THEN 'New York'
        WHEN city = 'nyc' THEN 'New York'
        WHEN city = 'new york' THEN 'New York'
        -- 'New York' falls through to NULL!
    END as city_standardized
FROM staging.locations;

-- RIGHT: Target included, all variants handled
SELECT 
    CASE 
        WHEN city IN ('ny', 'nyc', 'new york', 'New York') THEN 'New York'
        WHEN city IN ('sf', 'san francisco', 'San Francisco') THEN 'San Francisco'
        ELSE 'UNMAPPED: ' || city
    END as city_standardized
FROM staging.locations;
```

### Impossible Values

**Problem:** Data violates business reality.  
**Consequences:** Future dates in order history. Negative ages. 200-year-old customers. Analysis becomes joke.  
**Decision:** Define valid ranges. Reject or flag violations.

**DWH Approach:** Staging layer: `CHECK` constraints on staging tables; business rule validation in CTEs; quarantine violations to `quarantine.business_rules`.

**Monitoring:** `impossible_value_log` with rule violation counts; BI dashboard showing violation rate by rule; automated source system feedback loop.

```sql
SELECT 
    CASE 
        WHEN order_date > CURRENT_DATE THEN 'FUTURE'
        WHEN order_date < '2000-01-01' THEN 'TOO_OLD'
        WHEN amount < 0 THEN 'NEGATIVE'
        WHEN email NOT LIKE '%@%.%' THEN 'INVALID_EMAIL'
        ELSE 'OK'
    END as flag,
    COUNT(*)
FROM orders
GROUP BY flag
HAVING flag != 'OK';
```

### Outliers
**Problem:** Values statistically valid but business-impossible (e.g., $1M order when max is $10K).  
**Consequences:** Means skewed. Aggregations misleading. Fraud or errors hidden.  
**Decision:** Flag values beyond N standard deviations or percentile thresholds.  
**DWH Approach:** Staging layer: Statistical profiling in separate `outlier_detection` CTE; flag but don't remove (business decision); pass flag to mart layer.  
**Monitoring:** `[Execution Context: Scheduler runs anomaly SQL]` `outlier_summary` table (date, column, outlier_count, threshold_used); BI dashboard with outlier trend lines; fraud investigation alerts on extreme outliers.
```sql
-- Flag statistical outliers (beyond 3 standard deviations)
WITH stats AS (
    SELECT 
        AVG(order_amount) as mean_val,
        STDDEV(order_amount) as std_val
    FROM orders
)
SELECT 
    o.order_id,
    o.order_amount,
    CASE 
        WHEN s.std_val = 0 THEN 0
        WHEN ABS(o.order_amount - s.mean_val) > 3 * s.std_val THEN 'OUTLIER'
        ELSE 'OK'
    END as flag
FROM orders o
CROSS JOIN stats s;
```

### Missing Values (NULLs)

**Problem:** Data is absent.  
**Consequences:** Aggregations wrong. Joins drop rows. You cannot distinguish "zero" from "unknown."  
**Decision:** Never silently convert NULL to zero. Preserve numeric output with separate flag column.

**DWH Approach:** Staging layer: Imputation only if business-approved; always add `is_imputed` boolean flag; NULLs preserved in `raw_value` column.

**Monitoring:** `null_rate` time-series by column; dbt `not_null` tests on critical fields; BI dashboard showing NULL trend by source.

```sql
-- WRONG: Silent fabrication, breaks numeric output
SELECT COALESCE(sales, 0) as sales FROM table;

-- RIGHT: Numeric value + separate flag
SELECT 
    COALESCE(sales, 0) as sales_value,
    CASE WHEN sales IS NULL THEN 1 ELSE 0 END as is_imputed,
    CASE WHEN sales IS NULL THEN 'MISSING' ELSE 'OK' END as quality_flag
FROM table;
```

---

## Part 2: Adjusting Data for Analysis
Problems caused by your own transformations. Prevent errors in calculations and aggregations.

**DWH Approach:** Apply in intermediate/mart layer. Use ephemeral models for transformation logic; persist quality checks in `audit` schema. Reconciliation tables comparing source → staging → mart totals.  
**Monitoring:** `[Execution Context: SQL is the validation logic. Test runners/BI platforms schedule and surface these queries.]` Tests on all marts, complex rule checks via SQL wrappers, BI dashboard showing "data health score" by mart.

### Division by Zero

**Problem:** Denominator in calculation equals zero.  
**Consequences:** Query crashes. Dashboard shows error. Report to executives fails on delivery.  
**Decision:** Always guard division with NULLIF or CASE.

**DWH Approach:** Mart layer: `NULLIF()` in all calculated fields; separate `calculation_validation` CTE checking denominators before final SELECT.

**Monitoring:** dbt tests ensuring no NULL denominators; `division_by_zero_incidents` log table; automated report validation before executive delivery.

```sql
-- WRONG
SELECT revenue / orders as aov FROM metrics;

-- RIGHT
SELECT revenue / NULLIF(orders, 0) as aov FROM metrics;
-- Or: CASE WHEN orders = 0 THEN NULL ELSE revenue / orders END
```

### Aggregation Errors

**Problem:** SUM, AVG, COUNT on dirty data produces wrong results.  
**Consequences:** Double-counting from duplicates. Averages skewed by outliers. Counts include NULLs when they shouldn't.  
**Decision:** Clean before aggregating. Verify row uniqueness. Check denominator in averages.

**DWH Approach:** Mart layer: `COUNT(DISTINCT)` for de-duplicated counts; window functions for running totals with validation; separate `aggregation_audit` table.

**Monitoring:** Reconciliation reports (source vs mart totals); dbt tests for expected row counts; BI alerts when mart totals diverge > 0.1% from source.

```sql
-- To inspect specific duplicates (limited output)
SELECT customer_id, order_date, amount, COUNT(*) as occurrences
FROM orders
GROUP BY customer_id, order_date, amount
HAVING COUNT(*) > 1
LIMIT 100;
```

### Date Arithmetic Errors

**Problem:** Calculating intervals, ages, durations with NULL or invalid dates.  
**Consequences:** Negative ages. Division by zero in duration calculations. Wrong cohort analysis.  
**Decision:** Validate dates before arithmetic.

**DWH Approach:** Mart layer: `CASE` guards on all date math; `AGE()` function with NULL checks; separate `date_validation` flags.

**Monitoring:** `negative_date_results` alert table; dbt tests ensuring no future birth dates; BI dashboard showing date quality by calculation type.

```sql
-- Calculate customer age safely
SELECT 
    customer_id,
    birth_date,
    CASE 
        WHEN birth_date IS NULL THEN NULL
        WHEN birth_date > CURRENT_DATE THEN NULL
        ELSE DATE_PART('year', AGE(CURRENT_DATE, birth_date))
    END as age_years,
    CASE 
        WHEN birth_date IS NULL THEN 'MISSING_BIRTH_DATE'
        WHEN birth_date > CURRENT_DATE THEN 'FUTURE_DATE'
        ELSE 'OK'
    END as flag
FROM customers;
```

### Implicit Cast Failures

**Problem:** Database implicitly casts types, producing unexpected results, or you explicitly cast in performance-critical contexts.  
**Consequences:** '2024-01-01' > '2024-1-1' (string comparison vs date). 5 / 2 = 2 (integer division). Worse: casting in JOINs or WHERE clauses disables indexes, causing full table scans on large tables.  
**Decision:** Store data as correct types from ingestion. Cast only in final SELECT or in CTEs/materialized tables, never in JOINs or WHERE clauses on source tables.

**DWH Approach:** Staging layer: Correct types enforced at ingestion. Mart layer: Cast only in final projection, never in JOIN/WHERE. Use `TRY_CAST` for safe conversions.

**Monitoring:** `implicit_cast_detection` query log analysis; query performance monitoring for sequential scans; dbt linting rules banning casts in JOINs.

```sql
-- WRONG: Implicit string comparison (works but risky)
SELECT * FROM orders WHERE order_date > '2024-01-01';  

-- WRONG: Cast in WHERE (disables index, full scan)
SELECT * FROM orders WHERE order_date > '2024-01-01'::DATE;  

-- RIGHT: Ensure column is DATE type, compare to literal
-- (Literal '2024-01-01' is implicitly cast to DATE once, not per-row)
SELECT * FROM orders WHERE order_date > '2024-01-01';

-- RIGHT: Cast only in final output, not in filter/join
SELECT 
    order_id,
    order_date,
    amount::DECIMAL(12,2) as amount_decimal  -- Safe: final projection only
FROM orders
WHERE order_date > '2024-01-01';  -- No cast, index-friendly
```

### Circular References

**Problem:** Self-referencing calculations or recursive CTEs that never terminate.  
**Consequences:** Query hangs. Resources exhausted. Database locks.  
**Decision:** Always set recursion limits. Validate termination conditions.

**DWH Approach:** Mart layer: `MAXRECURSION` setting in CTEs; cycle detection via `visited` array; materialize recursive results to prevent re-computation.

**Monitoring:** `recursive_query_timeout` alerts; query duration monitoring in BI; `infinite_loop_detection` via path length validation.

```sql
-- Safe recursive CTE with limit and cycle detection
-- Note: For large hierarchies, consider using a separate lookup table 
-- or LTREE extension instead of recursive CTEs
WITH RECURSIVE hierarchy AS (
    SELECT employee_id, manager_id, 1 as level,
           ARRAY[employee_id] as visited
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT e.employee_id, e.manager_id, h.level + 1,
           h.visited || e.employee_id
    FROM employees e
    JOIN hierarchy h ON e.manager_id = h.employee_id
    WHERE h.level < 10  -- Prevent deep recursion
      AND NOT e.employee_id = ANY(h.visited)  -- Prevent cycles using array scan
)
SELECT * FROM hierarchy;

-- Alternative for very deep hierarchies: iterative approach with temp table
-- to avoid stack depth issues and enable progress tracking
```

### Post-Transform Reconciliation

**Problem:** Transformation changes totals unexpectedly.  
**Consequences:** Source says $1M revenue, dashboard shows $900K. $100K vanishes. No one trusts data.  
**Decision:** Match source totals after every transformation.

**DWH Approach:** Mart layer: `reconciliation` table comparing source → staging → mart for all numeric fields; automated variance detection [^2^].

**Monitoring:** `reconciliation_variance` dashboard showing $/% differences by table; Slack alerts on > 0.5% variance; weekly reconciliation certification report.

```sql
WITH source AS (SELECT SUM(amount) as total FROM raw_table),
     transformed AS (SELECT SUM(amount) as total FROM clean_table)
SELECT 
    s.total = t.total as match,
    ABS(s.total - t.total) as difference,
    ABS(s.total - t.total) / NULLIF(s.total, 0) * 100 as pct_variance
FROM source s, transformed t;
```

---

## Part 3: Monitoring & Alerting

Systematic observation of data health across the entire pipeline.

### DWH Architecture for Quality

**Three-Layer Quality Architecture:**

| Layer | Purpose | Quality Mechanism |
|-------|---------|-------------------|
| **Staging** | Raw data landing | Schema validation, type checking, quarantine bad records |
| **Intermediate** | Cleaned/transformed | Referential integrity, business rules, reconciliation |
| **Mart** | Business-ready | Metric validation, semantic checks, drift detection |

**Quarantine Pattern (Fail-Fast vs Quarantine):**

Per best practices, implement **Quarantine Strategy** as default: separate invalid records from valid ones, allowing pipeline to continue while isolating problems for review. Use **Fail-Fast** only for critical schema violations or when data quality drops below 80% pass rate.

**Quarantine Schema Structure:**

```sql
-- Main pipeline with validation
CREATE TABLE staging.valid_orders AS
SELECT * FROM raw_orders
WHERE amount > 0 AND customer_id IS NOT NULL;

-- Quarantine table with inverted logic and metadata
CREATE TABLE quarantine.rejected_orders (
    raw_data JSONB,           -- Original record
    rejection_reason VARCHAR(100),  -- Which check failed
    check_name VARCHAR(50),   -- Specific validation rule
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table VARCHAR(50)
);

-- Populate quarantine
INSERT INTO quarantine.rejected_orders
SELECT 
    to_jsonb(raw_orders.*),
    CASE 
        WHEN amount <= 0 THEN 'invalid_amount'
        WHEN customer_id IS NULL THEN 'missing_customer_id'
    END,
    'staging_validation',
    CURRENT_TIMESTAMP,
    'raw_orders'
FROM raw_orders
WHERE amount <= 0 OR customer_id IS NULL;
```

**Audit Schema for Monitoring:**

```sql
-- Centralized quality logging
CREATE SCHEMA audit;

CREATE TABLE audit.data_quality_log (
    log_id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    check_name VARCHAR(100),
    table_name VARCHAR(100),
    check_type VARCHAR(50),     -- 'schema', 'volume', 'freshness', etc.
    status VARCHAR(20),         -- 'PASS', 'FAIL', 'WARNING'
    metric_value NUMERIC,
    threshold NUMERIC,
    details JSONB,
    run_id VARCHAR(100)         -- Pipeline execution ID
);

CREATE TABLE audit.reconciliation_summary (
    reconciliation_date DATE,
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    metric_name VARCHAR(50),    -- 'row_count', 'total_amount', etc.
    source_value NUMERIC,
    target_value NUMERIC,
    absolute_variance NUMERIC,
    percent_variance NUMERIC,
    status VARCHAR(20)
);

CREATE TABLE audit.schema_changes (
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(100),
    change_type VARCHAR(50),    -- 'column_added', 'column_removed', 'type_changed'
    column_name VARCHAR(100),
    old_value TEXT,
    new_value TEXT,
    ddl_statement TEXT
);
```

### Data Lineage Mapping
**Problem:** Quality checks exist in isolation; when a test fails, engineers struggle to trace which upstream source or downstream report is impacted.  
**Consequences:** Slow incident response. Unintended downstream breaks. Difficulty prioritizing fixes based on business impact.  
**Decision:** Maintain a SQL-based metadata registry linking every check to its source column, transformation, and downstream consumers.  
**DWH Approach:** Intermediate/Mart layer: `quality_checks_lineage` table populated via automated SQL queries against `information_schema` and test metadata. Query this table during alerting to attach downstream impact directly to notifications.  
**Monitoring:** `[Execution Context: CI/CD runs lineage SQL]` `quality_lineage_report` view; alerts include "Impacts: mart.revenue_daily, executive_dashboard"; automated dependency graph generation.
```
-- Lineage registry (enforce uniqueness to prevent duplicates on re-runs)
CREATE TABLE IF NOT EXISTS audit.quality_checks_lineage (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    source_schema VARCHAR(50),
    source_table VARCHAR(100),
    source_column VARCHAR(100),
    downstream_schema VARCHAR(50),
    downstream_table VARCHAR(100),
    impact_level VARCHAR(20),  -- 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
    UNIQUE(check_name, source_table, source_column, downstream_table)
);

-- STEP 1: Ingest test metadata & downstream dependencies 
-- (These can be auto-generated from dbt/CI YAML parsers into simple staging tables)
CREATE TEMP TABLE test_catalog AS VALUES
    ('null_check', 'staging', 'orders', 'customer_id', 'HIGH'),
    ('uniqueness_check', 'staging', 'orders', 'order_id', 'CRITICAL'),
    ('volume_check', 'staging', 'orders', '*', 'MEDIUM');

CREATE TEMP TABLE downstream_deps AS VALUES
    ('staging', 'orders', 'mart', 'daily_revenue'),
    ('staging', 'orders', 'mart', 'order_facts');

-- STEP 2: Automated population using information_schema for validation & expansion
INSERT INTO audit.quality_checks_lineage (
    check_name, source_schema, source_table, source_column,
    downstream_schema, downstream_table, impact_level
)
SELECT DISTINCT
    c.check_name,
    c.table_schema AS source_schema,
    c.table_name AS source_table,
    i.column_name AS source_column,
    d.downstream_schema,
    d.downstream_table,
    c.impact_level
FROM test_catalog c
JOIN information_schema.columns i 
  ON c.table_schema = i.table_schema 
  AND c.table_name = i.table_name 
  AND (c.column_name = '*' OR c.column_name = i.column_name)
JOIN downstream_deps d 
  ON c.table_schema = d.source_schema 
  AND c.table_name = d.source_table
ON CONFLICT (check_name, source_table, source_column, downstream_table) DO UPDATE
SET impact_level = EXCLUDED.impact_level;

-- STEP 3: Trace impact on failure (used by alerting pipeline)
SELECT downstream_table, impact_level, check_name
FROM audit.quality_checks_lineage
WHERE source_table = 'orders' AND source_column = 'amount';
```

### Quarantine Performance Guardrails
**Problem:** `JSONB` quarantine tables grow unbounded, causing slow joins, high storage costs, and index bloat over time.  
**Consequences:** Pipeline latency increases. Storage bills spike. Engineers avoid querying quarantine for debugging.  
**Decision:** Partition quarantine tables by ingestion date, index rejection reasons, and automate archival to cold storage.  
**DWH Approach:** Staging layer: Range-partition quarantine tables by `rejected_at` or `load_batch_id`. Create targeted indexes on high-cardinality filter columns. Schedule monthly SQL archival jobs.  
**Monitoring:** `[Execution Context: Scheduler runs maintenance SQL]` `quarantine_size_metrics` table; alerts when partition exceeds threshold; automated `VACUUM`/`REINDEX` tracking.
```sql
-- Partition quarantine by month for query performance & archival
CREATE TABLE quarantine.rejected_orders (
    raw_data JSONB,
    rejection_reason VARCHAR(100),
    check_name VARCHAR(50),
    rejected_at TIMESTAMP NOT NULL,
    source_table VARCHAR(50)
) PARTITION BY RANGE (rejected_at);

-- Create monthly partitions
CREATE TABLE quarantine.rejected_orders_2024_01 PARTITION OF quarantine.rejected_orders
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Index for fast filtering during investigations
CREATE INDEX idx_quarantine_reason ON quarantine.rejected_orders(rejection_reason, rejected_at);

-- Archive old partitions (run monthly via scheduler)
-- ALTER TABLE quarantine.rejected_orders_2023_01 SET TABLESPACE cold_storage;
```

### Monitoring Methods

**1. Table Constraints (Hard Enforcement)**

Postgres-native enforcement at database level. Use for non-negotiable rules on production tables.

```sql
-- Primary key (uniqueness + non-null)
ALTER TABLE customers ADD PRIMARY KEY (customer_id);

-- Foreign key (referential integrity)
ALTER TABLE orders ADD CONSTRAINT fk_customer 
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id);

-- Check constraints (business rules)
ALTER TABLE orders ADD CONSTRAINT chk_positive_amount 
    CHECK (amount >= 0);

-- Unique constraints (alternate keys)
ALTER TABLE customers ADD CONSTRAINT uq_email 
    UNIQUE (email);

-- Not null (completeness)
ALTER TABLE orders ALTER COLUMN order_date SET NOT NULL;
```

**When to use:** Critical production tables where violation must halt pipeline. High overhead; use sparingly on large tables.

**Monitoring:** Constraint violation logs in Postgres; `pg_constraint` catalog queries; immediate pipeline failure on violation.

2. **dbt Tests (Soft Enforcement)**  
Configurable validation without blocking. Standard: uniqueness, not_null, accepted_values, relationships. Custom: SQL-based business rules.  
*Note: dbt parses and executes the exact SQL patterns shown in this guide. It does not introduce new validation paradigms.*
```yaml
# schema.yml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: amount
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
```

**3. Separate QA Tables (Audit Trail)**

Persistent logging of quality metrics over time. Enables trend analysis and SLA reporting.

```sql
-- Daily quality snapshot with trend detection
CREATE TABLE audit.daily_quality_metrics (
    check_date DATE,
    table_name VARCHAR(100),
    check_name VARCHAR(100),
    metric_value NUMERIC,
    threshold NUMERIC,
    status VARCHAR(20),
    details JSONB,
    z_score NUMERIC  -- For anomaly detection
);

-- Insert with anomaly detection
WITH stats AS (
    SELECT 
        AVG(metric_value) as mean_val,
        STDDEV(metric_value) as std_val
    FROM audit.daily_quality_metrics
    WHERE check_date >= CURRENT_DATE - INTERVAL '30 days'
      AND table_name = 'orders'
      AND check_name = 'null_rate'
)
INSERT INTO audit.daily_quality_metrics
SELECT 
    CURRENT_DATE as check_date,
    'orders' as table_name,
    'null_rate' as check_name,
    COUNT(*) FILTER (WHERE customer_id IS NULL) * 1.0 / COUNT(*) as metric_value,
    0.05 as threshold,
    CASE WHEN COUNT(*) FILTER (WHERE customer_id IS NULL) * 1.0 / COUNT(*) > 0.05 
         THEN 'FAIL' ELSE 'PASS' END as status,
    NULL as details,
    (COUNT(*) FILTER (WHERE customer_id IS NULL) * 1.0 / COUNT(*) - s.mean_val) / 
        NULLIF(s.std_val, 0) as z_score
FROM orders, stats s;
```

**When to use:** Compliance requirements, SLA tracking, executive reporting, trend analysis. Required for data governance maturity.

**4. BI Dashboard Monitoring**

Real-time visibility for business stakeholders. Separate from technical alerts.

```sql
-- Materialized view for dashboard performance
CREATE MATERIALIZED VIEW mv_data_health_score AS
SELECT 
    table_name,
    COUNT(*) as total_checks,
    COUNT(*) FILTER (WHERE status = 'PASS') as passed,
    COUNT(*) FILTER (WHERE status = 'FAIL') as failed,
    ROUND(COUNT(*) FILTER (WHERE status = 'PASS') * 100.0 / COUNT(*), 1) as health_score,
    MAX(check_timestamp) as last_check
FROM audit.data_quality_log
WHERE check_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY table_name;

-- Refresh schedule for materialized view
-- Use pg_cron for automated refresh every 15 minutes
SELECT cron.schedule('refresh_health_score', '*/15 * * * *', 
    'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_data_health_score');
```

**Dashboard components:**
- Health score by table (0-100%) with color coding 
- Trend line (7/30/90 days) showing quality degradation
- SLA compliance (freshness, volume) with threshold lines
- Top 5 failing checks with drill-down capability
- Quarantine queue depth and aging
- Reconciliation variance by table

**When to use:** Business stakeholder communication, executive summaries, self-service quality visibility.

5. **Anomaly Detection (Statistical)**  
Pattern detection requiring historical baseline.  
*Note: Packages like Great Expectations or Elementary parse and run the exact SQL shown below. They do not replace the statistical logic.*
```sql
-- Z-score calculation for anomaly detection (safe division)
WITH stats AS (
    SELECT 
        table_name,
        check_name,
        AVG(metric_value) as mean_val,
        STDDEV(metric_value) as std_val
    FROM audit.daily_quality_metrics
    WHERE check_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY table_name, check_name
)
SELECT 
    m.table_name,
    m.check_name,
    m.metric_value,
    s.mean_val,
    CASE 
        WHEN s.std_val = 0 THEN 0
        ELSE (m.metric_value - s.mean_val) / s.std_val
    END as z_score,
    CASE 
        WHEN s.std_val = 0 THEN 'NORMAL'
        WHEN ABS((m.metric_value - s.mean_val) / s.std_val) > 3 
        THEN 'ANOMALY' ELSE 'NORMAL' 
    END as status
FROM audit.daily_quality_metrics m
JOIN stats s ON m.table_name = s.table_name AND m.check_name = s.check_name
WHERE m.check_date = CURRENT_DATE;
```

### Alerting Channels

**Severity Matrix:**

| Severity | Channel | Response Time | Examples |
|----------|---------|---------------|----------|
| **CRITICAL** | PagerDuty/Opsgenie + Slack @channel | 15 minutes | Pipeline failure, freshness > 8hrs, reconciliation > 5% variance |
| **HIGH** | Slack #data-alerts | 1 hour | Schema change, volume anomaly > 50%, constraint violation |
| **MEDIUM** | Slack #data-quality | 4 hours | dbt test failure, outlier spike, NULL rate increase |
| **LOW** | Email digest + BI dashboard | 24 hours | Minor threshold breach, informational drift |

**Alert Content Standards:**

Every alert must include:
- Check name and table
- Expected vs actual value
- First failure timestamp
- Sample bad records (limited to 5)
- Runbook link
- Escalation contact
- dbt compiled SQL (via Elementary one-click copy)

**Slack Integration Example:**

```sql
-- Function to send Slack alert (via webhook)
CREATE OR REPLACE FUNCTION notify_slack(
    severity TEXT,
    check_name TEXT,
    table_name TEXT,
    message TEXT
) RETURNS VOID AS $$
BEGIN
    -- Implementation via pg_net or external script
    PERFORM pg_net.http_post(
        url := 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL',
        body := jsonb_build_object(
            'text', format('[%s] %s on %s: %s', severity, check_name, table_name, message)
        )::text
    );
END;
$$ LANGUAGE plpgsql;

-- Trigger alert on critical check failure
SELECT notify_slack(
    'CRITICAL', 
    'freshness_check', 
    'orders', 
    'Data is 12 hours stale, expected < 4 hours'
);
```

**Alert Fatigue Prevention:**

- Group related failures (don't spam 50 alerts for same root cause)
- Auto-resolve on recovery with notification
- Weekly alert quality review (tune thresholds)
- On-call rotation documented in runbook
- Separate "data quality" channel from "data alerts" to reduce noise

**BI Platform Dashboard Integration:**

For executive visibility, push quality metrics to your BI platform:

```sql
-- Table for BI consumption
CREATE TABLE bi_data_quality_metrics (
    report_date DATE,
    data_source VARCHAR(100),
    quality_dimension VARCHAR(50),  -- 'completeness', 'accuracy', 'timeliness'
    score NUMERIC(5,2),  -- 0-100
    trend VARCHAR(20),   -- 'improving', 'stable', 'degrading'
    last_updated TIMESTAMP
);

-- Populate for BI tools (Tableau, Looker, Metabase)
INSERT INTO bi_data_quality_metrics
SELECT 
    CURRENT_DATE,
    table_name,
    'completeness',
    (1 - null_rate) * 100,
    CASE 
        WHEN null_rate < LAG(null_rate) OVER (ORDER BY check_date) THEN 'improving'
        WHEN null_rate > LAG(null_rate) OVER (ORDER BY check_date) THEN 'degrading'
        ELSE 'stable'
    END,
    CURRENT_TIMESTAMP
FROM audit.daily_quality_metrics
WHERE check_date = CURRENT_DATE;
```
### Runbook Automation
**Problem:** Alerts trigger without clear next steps. Engineers waste time investigating known issues or guessing remediation paths.  
**Consequences:** Extended MTTR. Alert fatigue. Repeated fire-fighting instead of systemic fixes.  
**Decision:** Link every alert severity and check type to a predefined runbook ID with auto-remediation scripts where safe.  
**DWH Approach:** Intermediate layer: `runbook_mappings` table mapping `check_name` + `status` to `runbook_url`, `auto_fix_sql`, and `owner`. Orchestrator appends runbook ID to alert payloads automatically.  
**Monitoring:** `[Execution Context: Scheduler joins alerts to runbook SQL]` `runbook_hit_rate` tracking; weekly review of unused runbooks; auto-execution logs for safe remediations.
```
-- Runbook mapping table
CREATE TABLE audit.runbook_mappings (
    check_name VARCHAR(100) PRIMARY KEY,
    severity VARCHAR(20),
    runbook_id VARCHAR(50),
    runbook_url TEXT,
    auto_remediation_sql TEXT,  -- e.g., "REFRESH MATERIALIZED VIEW..."
    owner_team VARCHAR(100),
    is_auto_executable BOOLEAN DEFAULT FALSE
);

-- Query to enrich alerts with runbook context
SELECT 
    a.check_name,
    a.table_name,
    a.status,
    a.metric_value,
    r.runbook_id,
    r.runbook_url,
    r.auto_remediation_sql,
    r.owner_team
FROM audit.data_quality_log a
LEFT JOIN audit.runbook_mappings r ON a.check_name = r.check_name
WHERE a.status = 'FAIL' AND a.check_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour';
```

---

### Checklists
**Before Every Commit**
- [ ] Schema unchanged (staging source contracts passing)
- [ ] Schema evolution follows 3-step pattern (nullable → backfill → constrain)
- [ ] Volume in expected range (±20% of 30-day average)
- [ ] No duplicates in source (uniqueness tests passing)
- [ ] No orphans in joins (relationship tests passing)
- [ ] Types match expectations (no implicit casts)
- [ ] No encoding issues (regex validation passing)
- [ ] Whitespace trimmed (standardization macros applied)
- [ ] Case standardized (LOWER/UPPER applied)
- [ ] Flexible categorization with UNMAPPED fallback (no silent NULLs)
- [ ] Defensive value mapping includes target spelling (no self-gaps)
- [ ] No impossible values (check constraints passing)
- [ ] Outliers flagged (statistical tests run)
- [ ] NULLs handled with separate flags (not converted to zero)
- [ ] No division by zero (NULLIF guards in place)
- [ ] Aggregations verified for uniqueness (COUNT DISTINCT vs COUNT)
- [ ] Date arithmetic validated (no negative ages/future dates)
- [ ] Source totals reconciled (variance < 0.1%)
- [ ] Quarantine tables reviewed (no unexpected rejects)
- [ ] Check mapped to runbook ID in `audit.runbook_mappings`

**Weekly Review**
- [ ] All freshness checks passing (SLA compliance > 99%)
- [ ] No new orphan records (referential integrity stable)
- [ ] Distribution shapes unchanged (no drift in key metrics)
- [ ] Data health score > 95% for all critical tables
- [ ] Alert volume reviewed (tune thresholds if > 10 false positives/week)
- [ ] Documentation updated with new edge cases found
- [ ] Reconciliation variance trend analyzed
- [ ] Schema change log reviewed (any unplanned alterations?)
- [ ] Quarantine indexes healthy (no bloat, partitions archived)

**Monthly Governance**
- [ ] Data quality metrics reported to stakeholders via BI dashboard
- [ ] SLA breach root cause analysis completed
- [ ] Quarantine table cleanup (archive old rejects > 30 days)
- [ ] Audit table retention (archive > 90 days to cold storage)
- [ ] Constraint performance impact reviewed (drop if causing issues)
- [ ] Monitoring coverage gaps identified
- [ ] Alert runbooks updated and auto-remediation tested
- [ ] Data steward review meeting completed
- [ ] Lineage map audited for orphaned checks or missing downstream impacts

## AI Collaboration Disclosure

"In creating this document, I collaborated with Kimi, Qwen to assist with drafting, structure, and technical editing. I affirm that all AI-generated and co-created content underwent thorough review and evaluation. The final output accurately reflects my understanding, expertise, and intended meaning. While AI assistance was instrumental in the process, I maintain full responsibility for the content, its accuracy, and its presentation. This disclosure is made in the spirit of transparency and to acknowledge the role of AI in the creation process."

