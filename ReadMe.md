# Data Quality Checks

For analytics engineers who want clean data without the noise.

---

## Why This Matters

As an analytics engineer joining a new organization, you typically inherit existing data pipelines. 
Upon executing your first queries, you encounter results that do not align with business expectations. Investigation reveals that source data contains inconsistencies, and transformation logic compounds these issues. What follows is a multi-day debugging process to trace errors that could have been identified within minutes through systematic quality checks.
This guide establishes protocols for preventing such scenarios. It does not rely on proprietary tools or external platforms. It relies on disciplined SQL practices and explicit validation decisions at each processing stage.

## Table of Contents

- [Why This Matters](#why-this-matters)
- [Part 1: Incoming Data is Dirty](#part-1-incoming-data-is-dirty)
  - [Schema Drift](#schema-drift)
  - [Volume Anomalies](#volume-anomalies)
  - [Data Freshness](#data-freshness)
  - [Duplicate Records](#duplicate-records)
  - [Orphan Records](#orphan-records)
  - [Type Mismatches](#type-mismatches)
  - [Encoding & Character Issues](#encoding--character-issues)
  - [Whitespace & Padding](#whitespace--padding)
  - [Case Standardization](#case-standardization)
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
- [Checklists](#checklists)
- [Personal Experience](#personal-experience)
- [AI Collaboration Disclosure](#ai-collaboration-disclosure)
---

## Part 1: Incoming Data is Dirty

Problems caused by sources you don't control. Fix or flag before any transformation.

### Schema Drift

**Problem:** Table structure changes without warning.  
**Consequences:** Your query selects columns that no longer exist, or misses new critical fields. Pipeline fails or produces incomplete data.  
**Decision:** Check schema before every transformation.

```sql
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'your_table'
ORDER BY ordinal_position;
```

### Volume Anomalies

**Problem:** Row counts change unexpectedly.  
**Consequences:** 50% drop = upstream pipeline broke. 300% spike = duplicate data. Zero rows = silent failure. All lead to wrong business decisions.  
**Decision:** Compare today's count to historical average. Stop if deviation > 20%.

```sql
SELECT 
    COUNT(*) as today_count,
    (SELECT AVG(daily_count) 
     FROM (SELECT COUNT(*) as daily_count 
           FROM your_table 
           WHERE created_at > CURRENT_DATE - INTERVAL '30 days'
           GROUP BY DATE(created_at)) sub) as avg_count
FROM your_table
WHERE DATE(created_at) = CURRENT_DATE;
```

### Data Freshness

**Problem:** Pipeline delivers stale data.  
**Consequences:** Decisions based on yesterday's data in real-time business. Missed opportunities. Wrong actions.  
**Decision:** Check last update time. Stop if > SLA threshold.

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
```

### Orphan Records

**Problem:** Foreign keys point to non-existent parent records.  
**Consequences:** Joins silently drop data. Reports undercount. Relationships appear broken when they exist.  
**Decision:** Verify referential integrity before joins.

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

```sql
-- Check: What types are actually stored?
SELECT 
    column_name,
    data_type,
    pg_typeof(column_name) as actual_type
FROM information_schema.columns
WHERE table_name = 'your_table';

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

```sql
-- Check case variations
SELECT country, COUNT(*) 
FROM customers 
GROUP BY country 
ORDER BY LOWER(country);

-- Standardize
SELECT LOWER(country) as standard_country FROM customers;
```

### Impossible Values

**Problem:** Data violates business reality.  
**Consequences:** Future dates in order history. Negative ages. 200-year-old customers. Analysis becomes joke.  
**Decision:** Define valid ranges. Reject or flag violations.

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

### Division by Zero

**Problem:** Denominator in calculation equals zero.  
**Consequences:** Query crashes. Dashboard shows error. Report to executives fails on delivery.  
**Decision:** Always guard division with NULLIF or CASE.

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

```sql
-- Safe recursive CTE with limit
WITH RECURSIVE hierarchy AS (
    SELECT employee_id, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT e.employee_id, e.manager_id, h.level + 1
    FROM employees e
    JOIN hierarchy h ON e.manager_id = h.employee_id
    WHERE h.level < 10  -- Prevent infinite recursion
)
SELECT * FROM hierarchy;
```

### Post-Transform Reconciliation

**Problem:** Transformation changes totals unexpectedly.  
**Consequences:** Source says $1M revenue, dashboard shows $900K. $100K vanishes. No one trusts data.  
**Decision:** Match source totals after every transformation.

```sql
WITH source AS (SELECT SUM(amount) as total FROM raw_table),
     transformed AS (SELECT SUM(amount) as total FROM clean_table)
SELECT 
    s.total = t.total as match,
    ABS(s.total - t.total) as difference
FROM source s, transformed t;
```

---

## Checklists

### Before Every Commit
- [ ] Schema unchanged
- [ ] Volume in expected range
- [ ] No duplicates in source
- [ ] No orphans in joins
- [ ] Types match expectations
- [ ] No encoding issues
- [ ] Whitespace trimmed
- [ ] Case standardized
- [ ] No impossible values
- [ ] Outliers flagged
- [ ] NULLs handled with separate flags (not converted to zero)
- [ ] No division by zero
- [ ] Aggregations verified for uniqueness
- [ ] Date arithmetic validated
- [ ] Source totals reconciled

### Weekly Review
- [ ] All freshness checks passing
- [ ] No new orphan records
- [ ] Distribution shapes unchanged
- [ ] Documentation updated with new edge cases

---

## Personal Experience

<div style="background-color: #f0f0f0; padding: 15px; border-radius: 5px; opacity: 0.7;">

**Template for your war stories:**

**Incident:** [Brief description of what went wrong]

**Problem:** [Specific data quality issue]

**Consequences:** [Business impact, time lost, credibility hit]

**Decision:** [What check now prevents this]

**Query:** [The SQL that would have caught it]

---

*Add your real incidents here. Replace this template with actual failures you've experienced.*

</div>

---

## AI Collaboration Disclosure

"In creating this document, I collaborated with Kimi to assist with drafting, structure, and technical editing. I affirm that all AI-generated and co-created content underwent thorough review and evaluation. The final output accurately reflects my understanding, expertise, and intended meaning. While AI assistance was instrumental in the process, I maintain full responsibility for the content, its accuracy, and its presentation. This disclosure is made in the spirit of transparency and to acknowledge the role of AI in the creation process."
