# Data Quality Checks

A practical guide for keeping your data clean and trustworthy—without the enterprise software sales pitch.

---

## Why This Guide Exists

When you join a new company as an analytics engineer, you usually inherit existing data pipelines. You run your first query, and the numbers look... wrong. 

After digging, you discover the source data is messy, and your transformations made it worse. Now you're spending days debugging problems that could have been caught in minutes.

This guide helps you avoid that scenario. No fancy tools required—just disciplined SQL practices and good validation habits.

---

## How to Use This Guide

Each section follows the same structure:

- **The Problem** — What's going wrong
- **What Happens** — Why you should care
- **The Fix** — The general approach
- **How to Implement** — Four ways to solve it, from simple SQL to full automation
- **Monitoring** — How to track it over time

### Implementation Options Explained

| Option | Best For | Dependencies |
|--------|----------|--------------|
| **SQL-First** | Quick wins, any database, no tooling budget | ✅ Standalone |
| **dbt-Specific** | Teams already using dbt | ⚠️ Needs baseline data (from SQL-First or Database-Native) |
| **Orchestrator-Level** | Airflow/Dagster/Prefect users | ⚠️ Needs baseline data for historical comparison |
| **Database-Native** | Postgres/Snowflake power users | ✅ Standalone for logging; ⚠️ needs Alerting for notifications |

### Monitoring Options Explained

| Option | Best For | Dependencies |
|--------|----------|--------------|
| **Dashboards** | Executive visibility, trend analysis | ⚠️ Needs data from Database-Native tables |
| **Alerting** | Immediate response, waking people up | ⚠️ Needs detection logic from other layers |

---

## Table of Contents

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
- [Division by Zero](#division-by-zero)
- [Aggregation Errors](#aggregation-errors)
- [Date Arithmetic Errors](#date-arithmetic-errors)
- [Implicit Cast Failures](#implicit-cast-failures)
- [Circular References](#circular-references)
- [Post-Transform Reconciliation](#post-transform-reconciliation)

---

## Schema Drift

**The Problem:** Table structure changes without warning—new columns appear, old ones vanish, types shift.

**What Happens:** Your query selects columns that no longer exist, or misses new critical fields. Pipeline fails or produces incomplete data. The 3 AM page you just got? This is why.

**The Fix:** Detect schema changes before they break downstream transforms.

---

### How to Implement

<details>
<summary><strong>SQL-First (Works Everywhere)</strong> — ✅ Standalone</summary>

Query `information_schema` to snapshot current structure. Zero dependencies, works on any SQL database.

```sql
-- Snapshot current schema
SELECT 
    column_name, 
    data_type,
    ordinal_position
FROM information_schema.columns 
WHERE table_name = 'orders'
  AND table_schema = 'staging'
ORDER BY ordinal_position;

-- Compare to expected (hardcoded or stored in a config table)
WITH expected AS (
    SELECT * FROM (VALUES 
        ('order_id', 'integer'),
        ('customer_id', 'integer'), 
        ('amount', 'numeric'),
        ('order_date', 'timestamp')
    ) AS e(column_name, data_type)
),
actual AS (
    SELECT column_name, data_type
    FROM information_schema.columns 
    WHERE table_name = 'orders'
)
SELECT 
    COALESCE(e.column_name, a.column_name) as column_name,
    e.data_type as expected_type,
    a.data_type as actual_type,
    CASE 
        WHEN e.column_name IS NULL THEN 'COLUMN_ADDED'
        WHEN a.column_name IS NULL THEN 'COLUMN_REMOVED'
        WHEN e.data_type != a.data_type THEN 'TYPE_CHANGED'
        ELSE 'UNCHANGED'
    END as change_type
FROM expected e
FULL OUTER JOIN actual a USING (column_name)
WHERE e.column_name IS NULL OR a.column_name IS NULL OR e.data_type != a.data_type;
```

**Best for:** Quick checks, CI/CD validation, any database without additional tooling.
</details>

<details>
<summary><strong>dbt-Specific</strong> — ✅ Standalone (uses dbt's built-in contracts)</summary>

Native dbt source contracts (1.5+) with automatic enforcement. No external dependencies.

```yaml
# sources.yml
sources:
  - name: raw_orders
    database: analytics
    schema: staging
    config:
      contract:
        enforced: true
    tables:
      - name: orders
        columns:
          - name: order_id
            data_type: int
            constraints:
              - type: not_null
          - name: amount
            data_type: decimal
          - name: order_date
            data_type: timestamp
```

Run: `dbt source freshness` or `dbt compile` to validate

**Best for:** Teams already using dbt who want compile-time guarantees.
</details>

<details>
<summary><strong>Orchestrator-Level</strong> — ⚠️ Needs SQL-First logic or YAML config</summary>

Fail the pipeline before dbt runs. Requires schema definition from somewhere—either SQL-First queries or hardcoded config.

```python
# Airflow task - depends on expected_schema definition
def schema_drift_check(**context):
    hook = PostgresHook(postgres_conn_id='warehouse')

    # Get actual schema via SQL-First query
    actual_columns = hook.get_records("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'orders'
    """)
    actual = {col: dtype for col, dtype in actual_columns}

    # Expected schema - could be from YAML, JSON, or hardcoded
    expected = {
        'order_id': 'integer',
        'customer_id': 'integer',
        'amount': 'numeric',
        'order_date': 'timestamp'
    }

    # Compare
    missing = set(expected.keys()) - set(actual.keys())
    added = set(actual.keys()) - set(expected.keys())
    changed = [col for col in expected if col in actual and expected[col] != actual[col]]

    if missing or added or changed:
        raise AirflowFailException(
            f"Schema drift detected! Missing: {missing}, Added: {added}, Changed: {changed}"
        )

    return "Schema valid"
```

**Dependencies:** Requires expected schema definition (YAML, JSON, or SQL-First query to reference table).
</details>

<details>
<summary><strong>Database-Native</strong> — ✅ Standalone for logging; ⚠️ needs Alerting for notifications</summary>

Event triggers for automatic DDL logging. Works alone for audit trails, but needs pairing with Alerting to notify anyone.

```sql
-- Postgres: Auto-log all schema changes
CREATE TABLE audit.schema_changes (
    change_id SERIAL PRIMARY KEY,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(100),
    change_type VARCHAR(50),
    column_name VARCHAR(100),
    old_value TEXT,
    new_value TEXT,
    ddl_statement TEXT,
    user_name VARCHAR(100) DEFAULT CURRENT_USER
);

-- Function to capture DDL
CREATE OR REPLACE FUNCTION log_schema_change()
RETURNS EVENT TRIGGER AS $$
BEGIN
    INSERT INTO audit.schema_changes (table_name, change_type, ddl_statement)
    SELECT 
        objid::regclass::text,
        tg_tag,
        tg_event
    FROM pg_event_trigger_ddl_commands();
END;
$$ LANGUAGE plpgsql;

-- Trigger on any DDL
CREATE EVENT TRIGGER schema_change_trigger 
ON ddl_command_end 
EXECUTE FUNCTION log_schema_change();
```

**Standalone use:** Passive audit logging, compliance trails.
**Paired use:** Query `audit.schema_changes` from Alerting layer to send notifications.
</details>

---

### Monitoring

<details>
<summary><strong>Dashboards</strong> — ⚠️ Needs Database-Native audit table</summary>

Track schema change trends for governance reviews.

```sql
-- BI-friendly view of schema evolution
CREATE VIEW bi_schema_history AS
SELECT 
    DATE_TRUNC('week', change_timestamp) as week,
    table_name,
    change_type,
    COUNT(*) as change_count,
    STRING_AGG(DISTINCT column_name, ', ') as columns_affected
FROM audit.schema_changes
WHERE change_timestamp >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, change_count DESC;
```

**Dependencies:** Requires `audit.schema_changes` table from Database-Native implementation.
</details>

<details>
<summary><strong>Alerting</strong> — ⚠️ Needs Database-Native audit table or SQL-First detection</summary>

Notify when breaking changes occur.

```sql
-- Alert on breaking changes (column drops, type changes)
SELECT notify_slack(
    'HIGH',
    'schema_drift',
    table_name,
    format('%s on %s: column %s', change_type, table_name, column_name)
)
FROM audit.schema_changes
WHERE change_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
  AND change_type IN ('column_dropped', 'type_changed');
```

**Dependencies:** Requires `audit.schema_changes` (Database-Native) or SQL-First comparison logic.
</details>

---

## Schema Evolution Safety

**The Problem:** Adding columns the wrong way breaks pipelines or locks tables.

**What Happens:** `ALTER TABLE ... ADD COLUMN NOT NULL` without a default = full table rewrite. 50M row table? That's 20 minutes of downtime. Deployment fails. Your team learns new curse words.

**The Fix:** Use the 3-step pattern: nullable → backfill → constrain.

---

### How to Implement

<details>
<summary><strong>SQL-First (Works Everywhere)</strong> — ✅ Standalone</summary>

Manual migration script that follows safe patterns. Works on any SQL database.

```sql
-- STEP 1: Add nullable column (zero-downtime)
ALTER TABLE staging.orders 
ADD COLUMN discount_code VARCHAR(50);

-- STEP 2: Backfill asynchronously (run as separate transaction)
-- Do this during low-traffic hours
UPDATE staging.orders 
SET discount_code = 'LEGACY' 
WHERE discount_code IS NULL 
  AND order_date < '2024-01-01';

-- Verify backfill complete
SELECT COUNT(*) as remaining_nulls
FROM staging.orders 
WHERE discount_code IS NULL;

-- STEP 3: Enforce constraint only after 100% backfill
ALTER TABLE staging.orders 
ALTER COLUMN discount_code SET NOT NULL;

-- Optional: Add default for future inserts
ALTER TABLE staging.orders 
ALTER COLUMN discount_code SET DEFAULT 'DIRECT';
```

**Best for:** Manual migrations, databases without sophisticated tooling, understanding the underlying mechanics.
</details>

<details>
<summary><strong>dbt-Specific</strong> — ⚠️ Needs SQL-First migration or external orchestration</summary>

dbt handles the transformation layer, not the DDL layer. Use dbt for post-migration validation only.

```sql
-- models/staging/stg_orders.sql
-- Assumes column was added via SQL-First or Database-Native migration

SELECT 
    order_id,
    customer_id,
    amount,
    order_date,
    -- Handle NULLs gracefully during transition period
    COALESCE(discount_code, 'LEGACY') as discount_code_clean,
    discount_code IS NULL as is_legacy_order
FROM {{ source('raw', 'orders') }}
```

```yaml
# Validate the migration worked
tests:
  - name: stg_orders
    tests:
      - dbt_utils.expression_is_true:
          expression: "discount_code_clean IS NOT NULL"
```

**Dependencies:** Requires column to exist first (added via SQL-First or Database-Native migration).
</details>

<details>
<summary><strong>Orchestrator-Level</strong> — ✅ Standalone for automation</summary>

Automate the 3-step pattern with safety checks. Can orchestrate SQL-First commands.

```python
# Airflow DAG for safe schema evolution
def add_column_safe(**context):
    hook = PostgresHook(postgres_conn_id='warehouse')

    table = context['params']['table']
    column = context['params']['column']
    data_type = context['params']['data_type']
    default_value = context['params']['default']

    # Step 1: Add nullable
    hook.run(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {data_type}")

    # Step 2: Backfill in batches (no table lock)
    batch_size = 10000
    while True:
        result = hook.run(f"""
            UPDATE {table} 
            SET {column} = %s
            WHERE {column} IS NULL 
            AND ctid IN (
                SELECT ctid FROM {table} 
                WHERE {column} IS NULL 
                LIMIT %s
            )
        """, parameters=(default_value, batch_size))

        if result == "UPDATE 0":
            break

    # Step 3: Verify no NULLs remain, then add constraint
    null_count = hook.get_first(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")[0]

    if null_count == 0:
        hook.run(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL")
        return f"Column {column} added safely"
    else:
        raise AirflowFailException(f"Backfill incomplete: {null_count} NULLs remain")
```

**Best for:** Automating safe migrations, enforcing the 3-step pattern across teams.
</details>

<details>
<summary><strong>Database-Native</strong> — ⚠️ Limited support; often paired with SQL-First</summary>

Some databases support online DDL, but most still require the 3-step pattern for complex changes.

```sql
-- Postgres 11+: Adding nullable column is fast (metadata-only)
-- But backfill and constraint still need SQL-First approach

-- What Database-Native CAN do: track migration status
CREATE TABLE audit.schema_migrations (
    migration_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    step_number INT,
    step_name VARCHAR(50),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    rows_affected BIGINT,
    status VARCHAR(20)
);

-- Log each step
INSERT INTO audit.schema_migrations (table_name, column_name, step_number, step_name, status)
VALUES ('orders', 'discount_code', 1, 'add_nullable', 'complete');
```

**Dependencies:** Core migration still uses SQL-First; Database-Native adds audit logging.
</details>

---

### Monitoring

<details>
<summary><strong>Dashboards</strong> — ⚠️ Needs Database-Native audit table</summary>

Track migration progress and team adherence to safe patterns.

```sql
-- View: Are teams following the 3-step pattern?
SELECT 
    table_name,
    column_name,
    MAX(CASE WHEN step_number = 1 THEN completed_at END) as step_1_completed,
    MAX(CASE WHEN step_number = 2 THEN completed_at END) as step_2_completed,
    MAX(CASE WHEN step_number = 3 THEN completed_at END) as step_3_completed,
    CASE 
        WHEN COUNT(*) = 3 THEN 'COMPLIANT'
        ELSE 'INCOMPLETE'
    END as migration_status
FROM audit.schema_migrations
GROUP BY table_name, column_name;
```

**Dependencies:** Requires `audit.schema_migrations` table (Database-Native).
</details>

<details>
<summary><strong>Alerting</strong> — ⚠️ Needs Database-Native audit table</summary>

Alert on unsafe migration attempts.

```sql
-- Detect direct NOT NULL adds (skipping step 1)
SELECT notify_slack(
    'CRITICAL',
    'unsafe_migration',
    table_name,
    'Direct NOT NULL constraint added without backfill pattern'
)
FROM audit.schema_changes
WHERE change_type = 'constraint_added'
  AND ddl_statement LIKE '%NOT NULL%'
  AND NOT EXISTS (
      SELECT 1 FROM audit.schema_migrations m
      WHERE m.table_name = schema_changes.table_name
        AND m.step_number = 2
        AND m.status = 'complete'
  );
```

**Dependencies:** Requires both `audit.schema_changes` and `audit.schema_migrations` tables.
</details>

---

## Volume Anomalies

**The Problem:** Row counts change unexpectedly.

**What Happens:** 50% drop = upstream pipeline broke. 300% spike = duplicate data. Zero rows = silent failure. All lead to wrong business decisions and awkward explanations to your boss.

**The Fix:** Compare today's count to historical average. Stop if deviation > 20%.

---

### How to Implement

<details>
<summary><strong>SQL-First (Works Everywhere)</strong> — ✅ Standalone</summary>

Calculate baseline and detect anomalies in pure SQL. No dependencies.

```sql
WITH today_stats AS (
    SELECT 
        COUNT(*) as row_count,
        COUNT(DISTINCT customer_id || '|' || order_date) as unique_keys
    FROM orders
    WHERE order_date = CURRENT_DATE
),
history_stats AS (
    SELECT 
        AVG(daily_count) as avg_count,
        STDDEV(daily_count) as std_count,
        MIN(daily_count) as min_count,
        MAX(daily_count) as max_count
    FROM (
        SELECT order_date, COUNT(*) as daily_count
        FROM orders
        WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
          AND order_date < CURRENT_DATE
        GROUP BY order_date
    ) daily
)
SELECT 
    t.row_count,
    h.avg_count,
    ROUND((t.row_count - h.avg_count) / h.avg_count * 100, 2) as pct_deviation,
    CASE 
        WHEN t.row_count = 0 THEN 'ZERO_ROWS'
        WHEN ABS(t.row_count - h.avg_count) > 3 * h.std_count THEN 'ANOMALY'
        WHEN ABS(t.row_count - h.avg_count) > 0.2 * h.avg_count THEN 'WARNING'
        ELSE 'OK'
    END as status
FROM today_stats t
CROSS JOIN history_stats h;
```

**Best for:** Quick checks, any database, ad-hoc investigations.
</details>

<details>
<summary><strong>dbt-Specific</strong> — ⚠️ Needs SQL-First logic or Database-Native baseline table</summary>

Use dbt tests, but they need baseline data from somewhere.

```sql
-- models/staging/stg_orders.sql with volume check
{{ config(
    pre_hook="""
        DO $$
        DECLARE
            today_count INT;
            avg_count NUMERIC;
        BEGIN
            SELECT COUNT(*) INTO today_count FROM {{ this }} WHERE order_date = CURRENT_DATE;
            SELECT AVG(daily_count) INTO avg_count 
            FROM (
                SELECT COUNT(*) as daily_count 
                FROM {{ this }} 
                WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY order_date
            ) x;

            IF today_count < 0.8 * avg_count THEN
                RAISE EXCEPTION 'Volume drop detected';
            END IF;
        END $$;
    """
) }}

SELECT * FROM {{ source('raw', 'orders') }}
```

**Or with external baseline table:**
```yaml
models:
  - name: stg_orders
    tests:
      - custom_macro.volume_anomaly:
          baseline_table: ref('daily_volume_stats')
          threshold: 0.2
```

**Dependencies:** Either inline SQL-First logic (slower) or external `daily_volume_stats` table (faster).
</details>

<details>
<summary><strong>Orchestrator-Level</strong> — ⚠️ Needs SQL-First logic or Database-Native baseline table</summary>

Fail the pipeline before bad data propagates.

```python
# Airflow task
def volume_check(**context):
    hook = PostgresHook(postgres_conn_id='warehouse')

    # Option A: Inline SQL-First (standalone but slower)
    result = hook.get_first("""
        WITH stats AS (
            SELECT AVG(daily_count) as avg_count, STDDEV(daily_count) as std_count
            FROM (
                SELECT load_date, COUNT(*) as daily_count
                FROM raw_orders
                WHERE load_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY load_date
            ) x
        )
        SELECT 
            (SELECT COUNT(*) FROM raw_orders WHERE load_date = CURRENT_DATE) as today,
            avg_count,
            std_count
        FROM stats
    """)

    today_count, avg_count, std_count = result
    deviation = abs(today_count - avg_count) / avg_count if avg_count else 0

    if today_count == 0:
        raise AirflowFailException("ZERO ROWS detected")
    elif deviation > 0.5:
        raise AirflowFailException(f"Volume anomaly: {today_count} vs avg {avg_count}")

    return f"Volume check passed: {today_count} rows"
```

**Dependencies:** Inline SQL-First (no deps but slower) OR Database-Native baseline table (faster).
</details>

<details>
<summary><strong>Database-Native</strong> — ✅ Standalone for logging; often paired with Alerting</summary>

Materialized views and automated logging for persistent history.

```sql
-- Baseline table for fast lookups
CREATE MATERIALIZED VIEW mv_volume_baseline AS
SELECT 
    table_name,
    AVG(row_count) as avg_rows,
    STDDEV(row_count) as std_rows,
    MAX(collection_date) as last_updated
FROM audit.daily_volume_stats
WHERE collection_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY table_name;

-- Auto-refresh every hour
SELECT cron.schedule('refresh_volume_baseline', '0 * * * *', 
    'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_volume_baseline');

-- Daily volume tracking
CREATE TABLE audit.daily_volume_stats (
    collection_date DATE,
    table_name VARCHAR(100),
    row_count BIGINT,
    unique_keys BIGINT,
    PRIMARY KEY (collection_date, table_name)
);
```

**Standalone use:** Fast baseline queries, historical trends.
**Paired use:** Feed data to Orchestrator-Level checks or Alerting webhooks.
</details>

---

### Monitoring

<details>
<summary><strong>Dashboards</strong> — ⚠️ Needs Database-Native audit table</summary>

Visualize volume trends and anomaly history.

```sql
CREATE TABLE bi_volume_metrics (
    report_date DATE,
    table_name VARCHAR(100),
    row_count BIGINT,
    expected_range_low NUMERIC,
    expected_range_high NUMERIC,
    z_score NUMERIC,
    status VARCHAR(20)
);

-- Populate from Database-Native source
INSERT INTO bi_volume_metrics
SELECT 
    CURRENT_DATE,
    'orders',
    COUNT(*),
    (SELECT avg_rows - 2*std_rows FROM mv_volume_baseline WHERE table_name = 'orders'),
    (SELECT avg_rows + 2*std_rows FROM mv_volume_baseline WHERE table_name = 'orders'),
    (COUNT(*) - (SELECT avg_rows FROM mv_volume_baseline)) / 
        NULLIF((SELECT std_rows FROM mv_volume_baseline), 0),
    CASE 
        WHEN ABS(COUNT(*) - (SELECT avg_rows FROM mv_volume_baseline)) 
             > 3 * (SELECT std_rows FROM mv_volume_baseline) THEN 'CRITICAL'
        WHEN ABS(COUNT(*) - (SELECT avg_rows FROM mv_volume_baseline)) 
             > 2 * (SELECT std_rows FROM mv_volume_baseline) THEN 'WARNING'
        ELSE 'NORMAL'
    END
FROM orders
WHERE order_date = CURRENT_DATE;
```

**Dependencies:** Requires `mv_volume_baseline` (Database-Native).
</details>

<details>
<summary><strong>Alerting</strong> — ⚠️ Needs detection logic from other layers</summary>

Tiered notifications based on severity.

```sql
-- Alert function
CREATE OR REPLACE FUNCTION notify_volume_alert(
    table_name TEXT,
    actual BIGINT,
    expected NUMERIC,
    severity TEXT
) RETURNS VOID AS $$
BEGIN
    PERFORM pg_net.http_post(
        url := '{{ env_var("SLACK_WEBHOOK_URL") }}',
        body := jsonb_build_object(
            'text', format(
                '[%s] %s volume: %s rows (expected ~%s)',
                severity, table_name, actual, ROUND(expected)
            )
        )::text
    );
END;
$$ LANGUAGE plpgsql;
```

**Dependencies:** Requires actual vs expected counts from SQL-First logic or Database-Native tables.
</details>

---

## Dependency Summary

### Foundation Layer (Can Stand Alone)
- **SQL-First** — Core logic, no dependencies
- **Database-Native** — Storage, constraints, logging

### Integration Layer (Builds on Foundation)
- **dbt-Specific** — Needs SQL-First logic or Database-Native tables
- **Orchestrator-Level** — Needs SQL-First logic or Database-Native tables

### Presentation Layer (Needs Data From Others)
- **Dashboards** — Needs Database-Native audit tables
- **Alerting** — Needs detection logic from SQL-First or Database-Native

### Common Pairings
| If you want... | Pair these |
|----------------|-----------|
| Fast baseline lookups | Database-Native (materialized view) + any implementation |
| Automated quarantine | SQL-First (detection) + Database-Native (table) |
| dbt with fast checks | Database-Native (baseline table) + dbt-Specific (tests) |
| Full automation | SQL-First (logic) + Orchestrator-Level (scheduling) + Alerting (notifications) |

---

*Document generated with user-friendly language and collapsible implementation options.*
