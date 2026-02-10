# Golden Data Layer: Meta Framework Architecture Plan

## Executive Summary

This document defines a **metadata-driven control plane** for establishing Databricks as a golden data layer across multiple source systems. The architecture uses 14 governance tables in a dedicated `prod.meta` schema to provide complete visibility into what data exists, how it moves, how keys relate across systems, who consumes it, and what business rules govern it — all before a single row of business data is ingested.

---

## Schema Layout

```
prod.meta.*       ← 14 governance/control plane tables (no medallion layering)
prod.bronze.*     ← raw landing, per source system (subset — extraction-filtered)
prod.silver.*     ← validated, conformed, per entity
prod.gold.*       ← cross-system resolved, business-ready
```

**Key design decision:** `prod.meta` is treated as gold-tier reference data — tight write access (data stewards only), broad read access. These tables are manually curated, not ingested from external systems, so the medallion pattern does not apply to them.

**Key design decision:** Filtering happens at extraction, not at silver/gold. Bronze contains only data the business has explicitly configured to pull. **Tradeoff accepted:** if the business re-enables a previously excluded group/category, historical data must be backfilled from the source system — it will not exist in bronze. If source system volume or API rate limits later make full extraction impractical, or if storage costs become material, this decision may be revisited to filter at silver instead.

---

## Medallion Layer Responsibilities

Each layer answers a different question along a **trust gradient**:

| Layer | Question | Trust Level | What Happens Here |
|---|---|---|---|
| **Bronze** | "What did the source send us?" | Zero trust — raw, as-received | Data contracts checked, metadata tagged, extraction filters applied |
| **Silver** | "Is this data valid and conforming?" | Verified — enterprise-grade | Type casting, deduplication, quality rules applied, keys standardized, audit columns added |
| **Gold** | "What does this mean to the business?" | Curated — business-ready | Cross-system key resolution via crosswalks, dimensional models, aggregations, business glossary naming |

### Where Meta Tables Fire By Layer

| Meta Table | Bronze | Silver | Gold |
|---|---|---|---|
| `source_systems` | ✅ Origin tracking | Inherited | Inherited |
| `ingestion_pipelines` | ✅ Extract + stage | ✅ Transform | ✅ Load to final |
| `ingestion_pipeline_steps` | ✅ Steps 1-2 | ✅ Step 3 | ✅ Step 4 |
| `data_contracts` | ✅ Schema check on arrival | | |
| `quality_rules` | Basic (not null, format) | ✅ Bulk of validation | Aggregation-level checks |
| `key_registry` | | ✅ Standardize keys | Reference for joins |
| `key_crosswalk` | | | ✅ Cross-system resolution |
| `key_crosswalk_paths` | | | ✅ Multi-hop resolution |
| `business_glossary` | | | ✅ Column/table naming |
| `consumers` | | | ✅ What they query |
| `retention_policies` | ✅ Short retention | ✅ Medium retention | ✅ Long retention |
| `extraction_filters` | ✅ Drives what gets pulled | | |
| `extraction_filter_decisions` | ✅ Audit trail of filter changes | | |
| `pipeline_execution_log` | ✅ Logged | ✅ Logged | ✅ Logged |

---

## The 14 Meta Tables

### Category 1: What Exists (Source Discovery)

#### 1. `meta.source_systems`
**Purpose:** Master registry of all external systems that feed data into the platform.

Captures system identity, connectivity method (REST API, JDBC, SFTP, email, linked server, Delta Sharing), data formats, business ownership, technical ownership, and documentation links. Connection details stored as JSON/VARIANT to accommodate diverse system types without sparse columns.

```sql
CREATE TABLE meta.source_systems (
  source_system_id    INT GENERATED ALWAYS AS IDENTITY,
  system_code         STRING NOT NULL,
  system_name         STRING NOT NULL,
  system_type         STRING NOT NULL,
  connectivity_method STRING NOT NULL,
  connection_details  STRING,
  data_formats        STRING,
  owning_business_unit STRING NOT NULL,
  data_steward        STRING,
  technical_owner     STRING,
  environment         STRING DEFAULT 'PROD',
  documentation_url   STRING,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_source_system PRIMARY KEY (source_system_id)
) USING DELTA;
```

---

### Category 2: How Data Moves (Ingestion)

#### 2. `meta.ingestion_pipelines` (header)
**Purpose:** Pipeline-level metadata — schedule, owner, SLA, target tables. One record per pipeline.

```sql
CREATE TABLE meta.ingestion_pipelines (
  pipeline_id         INT GENERATED ALWAYS AS IDENTITY,
  source_system_id    INT NOT NULL,
  pipeline_code       STRING NOT NULL,
  pipeline_name       STRING NOT NULL,
  description         STRING,
  ingestion_pattern   STRING NOT NULL,
  schedule_type       STRING,
  schedule_expression STRING,
  target_bronze_table STRING,
  target_silver_table STRING,
  target_gold_tables  STRING,
  job_id              STRING,
  managing_owner      STRING NOT NULL,
  sla_minutes         INT,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_pipeline PRIMARY KEY (pipeline_id),
  CONSTRAINT fk_pipeline_source FOREIGN KEY (source_system_id)
    REFERENCES meta.source_systems(source_system_id)
) USING DELTA;
```

#### 3. `meta.ingestion_pipeline_steps` (detail)
**Purpose:** Step-by-step detail of each pipeline. Ordered via `step_sequence` with a unique constraint per pipeline. Each step documents what happens, who owns it, what it reads/writes, and how errors are handled.

Example 4-step flow:
1. `EXTRACT` — Azure Function pulls from source API
2. `STAGE` — Data lands in bronze table
3. `TRANSFORM` — Databricks job task runs MERGE against silver
4. `LOAD` — Target gold tables updated

```sql
CREATE TABLE meta.ingestion_pipeline_steps (
  step_id             INT GENERATED ALWAYS AS IDENTITY,
  pipeline_id         INT NOT NULL,
  step_sequence       INT NOT NULL,
  step_name           STRING NOT NULL,
  step_type           STRING NOT NULL,
  description         STRING NOT NULL,
  executor            STRING,
  executor_owner      STRING,
  input_reference     STRING,
  output_reference    STRING,
  key_columns_used    STRING,
  error_handling      STRING,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_step PRIMARY KEY (step_id),
  CONSTRAINT fk_step_pipeline FOREIGN KEY (pipeline_id)
    REFERENCES meta.ingestion_pipelines(pipeline_id),
  CONSTRAINT uq_pipeline_step UNIQUE (pipeline_id, step_sequence)
) USING DELTA;
```

#### 4. `meta.data_contracts`
**Purpose:** Versioned schema + SLA agreements per source system per pipeline. Defines what the source system promises to deliver (columns, types, volume, freshness) and what happens when that contract is broken.

When a source changes schema, a new contract version is created with `ACTIVE` status; the old version is `DEPRECATED`. Full audit trail of what was promised when.

```sql
CREATE TABLE meta.data_contracts (
  contract_id         INT GENERATED ALWAYS AS IDENTITY,
  source_system_id    INT NOT NULL,
  pipeline_id         INT NOT NULL,
  contract_version    INT NOT NULL DEFAULT 1,
  contract_status     STRING NOT NULL DEFAULT 'ACTIVE',
  schema_definition   STRING NOT NULL,
  delivery_sla_minutes INT,
  freshness_sla_minutes INT,
  volume_expectation  STRING,
  breaking_change_policy STRING,
  owner               STRING NOT NULL,
  effective_date      DATE NOT NULL,
  expiration_date     DATE,
  notes               STRING,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_contract PRIMARY KEY (contract_id),
  CONSTRAINT fk_contract_source FOREIGN KEY (source_system_id)
    REFERENCES meta.source_systems(source_system_id),
  CONSTRAINT fk_contract_pipeline FOREIGN KEY (pipeline_id)
    REFERENCES meta.ingestion_pipelines(pipeline_id),
  CONSTRAINT uq_contract_version UNIQUE (pipeline_id, contract_version)
) USING DELTA;
```

---

### Category 3: How Keys Relate (Identity Resolution)

#### 5. `meta.key_registry`
**Purpose:** Master list of every key that exists across all source systems. Documents key name, aliases, data type, example values, source location, and Databricks location.

```sql
CREATE TABLE meta.key_registry (
  key_id              INT GENERATED ALWAYS AS IDENTITY,
  source_system_id    INT NOT NULL,
  key_name            STRING NOT NULL,
  key_aliases         STRING,
  key_type            STRING NOT NULL,
  data_type           STRING NOT NULL,
  example_values      STRING,
  source_table        STRING,
  source_column       STRING,
  databricks_table    STRING,
  databricks_column   STRING,
  description         STRING,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_key PRIMARY KEY (key_id),
  CONSTRAINT fk_key_source FOREIGN KEY (source_system_id)
    REFERENCES meta.source_systems(source_system_id)
) USING DELTA;
```

#### 6. `meta.key_crosswalk` (direct mappings)
**Purpose:** Maps keys between systems where a direct relationship exists. This is an MDM crosswalk pattern, not a Kimball bridge table. Captures mapping type (1:1, 1:N, N:1, conditional), transformation rules (e.g., `UPPER(TRIM(x))`), confidence level, and validation provenance.

```sql
CREATE TABLE meta.key_crosswalk (
  crosswalk_id        INT GENERATED ALWAYS AS IDENTITY,
  from_key_id         INT NOT NULL,
  to_key_id           INT NOT NULL,
  mapping_type        STRING NOT NULL,
  mapping_confidence  STRING DEFAULT 'EXACT',
  transformation_rule STRING,
  conditions          STRING,
  bidirectional       BOOLEAN DEFAULT TRUE,
  validated_by        STRING,
  validation_date     DATE,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_crosswalk PRIMARY KEY (crosswalk_id),
  CONSTRAINT fk_cw_from FOREIGN KEY (from_key_id) REFERENCES meta.key_registry(key_id),
  CONSTRAINT fk_cw_to FOREIGN KEY (to_key_id) REFERENCES meta.key_registry(key_id)
) USING DELTA;
```

#### 7. `meta.key_crosswalk_paths` (multi-hop)
**Purpose:** Pre-computed paths for systems that require multiple crosswalk hops to connect. Stores ordered array of crosswalk IDs to traverse. Supplemented by a recursive CTE function (`meta.find_key_path`) for dynamic ad-hoc traversal.

```sql
CREATE TABLE meta.key_crosswalk_paths (
  path_id             INT GENERATED ALWAYS AS IDENTITY,
  from_key_id         INT NOT NULL,
  to_key_id           INT NOT NULL,
  hop_count           INT NOT NULL,
  path_crosswalk_ids  STRING NOT NULL,
  path_description    STRING,
  path_reliability    STRING,
  conditions          STRING,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_path PRIMARY KEY (path_id),
  CONSTRAINT fk_path_from FOREIGN KEY (from_key_id) REFERENCES meta.key_registry(key_id),
  CONSTRAINT fk_path_to FOREIGN KEY (to_key_id) REFERENCES meta.key_registry(key_id)
) USING DELTA;
```

---

### Category 4: Is Data Correct (Quality + Contracts)

#### 8. `meta.quality_rules`
**Purpose:** Central registry of all data quality rules. DLT pipelines read this table at runtime to generate expectations dynamically — no hardcoded rules in notebooks. Classified by type (completeness, uniqueness, validity, consistency, timeliness) and severity (expect/warn, expect_or_drop, expect_or_fail).

```sql
CREATE TABLE meta.quality_rules (
  rule_id             INT GENERATED ALWAYS AS IDENTITY,
  rule_code           STRING NOT NULL,
  rule_name           STRING NOT NULL,
  target_table        STRING NOT NULL,
  target_column       STRING,
  rule_expression     STRING NOT NULL,
  rule_type           STRING NOT NULL,
  severity            STRING NOT NULL,
  layer               STRING NOT NULL,
  owner               STRING NOT NULL,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_quality_rule PRIMARY KEY (rule_id)
) USING DELTA;
```

---

### Category 5: Who Depends On It (Consumer Tracking)

#### 9. `meta.consumers`
**Purpose:** Registry of every downstream consumer — dashboards, applications, ML models, exports, API consumers, teams. Captures criticality (P0–P3) for incident prioritization and freshness requirements for SLA management.

```sql
CREATE TABLE meta.consumers (
  consumer_id         INT GENERATED ALWAYS AS IDENTITY,
  consumer_name       STRING NOT NULL,
  consumer_type       STRING NOT NULL,
  consuming_tables    STRING NOT NULL,
  owning_team         STRING NOT NULL,
  contact             STRING NOT NULL,
  access_method       STRING NOT NULL,
  criticality         STRING NOT NULL,
  freshness_requirement STRING,
  notification_channel STRING,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_consumer PRIMARY KEY (consumer_id)
) USING DELTA;
```

---

### Category 6: How Long We Keep It (Retention)

#### 10. `meta.retention_policies`
**Purpose:** Per-table retention rules. Drives automated VACUUM, archival, and purge jobs. A scheduled job reads this table and executes maintenance dynamically — retention is policy-driven, not notebook-driven.

```sql
CREATE TABLE meta.retention_policies (
  policy_id           INT GENERATED ALWAYS AS IDENTITY,
  target_table        STRING NOT NULL,
  layer               STRING NOT NULL,
  retention_days      INT NOT NULL,
  time_travel_days    INT NOT NULL DEFAULT 7,
  log_retention_days  INT NOT NULL DEFAULT 30,
  archive_after_days  INT,
  purge_after_days    INT,
  vacuum_strategy     STRING DEFAULT 'LITE_DAILY_FULL_WEEKLY',
  regulatory_basis    STRING,
  owner               STRING NOT NULL,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_retention PRIMARY KEY (policy_id)
) USING DELTA;
```

---

### Category 7: Business Discovery (Glossary)

#### 11. `meta.business_glossary`
**Purpose:** Maps business terminology to technical objects. When a Finance user searches "Customer Lifetime Value" they find the definition, the exact gold table and column, and who owns the definition. This is what makes the golden layer discoverable by non-engineers.

```sql
CREATE TABLE meta.business_glossary (
  term_id             INT GENERATED ALWAYS AS IDENTITY,
  business_term       STRING NOT NULL,
  definition          STRING NOT NULL,
  calculation_logic   STRING,
  mapped_tables       STRING NOT NULL,
  mapped_columns      STRING NOT NULL,
  domain              STRING NOT NULL,
  owner               STRING NOT NULL,
  synonyms            STRING,
  is_active           BOOLEAN DEFAULT TRUE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_glossary PRIMARY KEY (term_id)
) USING DELTA;
```

---

### Category 8: Business-Driven Extraction Control

#### 12. `meta.extraction_filters`
**Purpose:** Current state of what the business wants pulled. Two filter types: GROUP (applies uniformly across all systems) and CATEGORY (system-specific). Pipelines query this table at runtime to build extraction WHERE clauses.

CDF enabled so changes are automatically trackable.

```sql
CREATE TABLE meta.extraction_filters (
  filter_id           INT GENERATED ALWAYS AS IDENTITY,
  source_system_id    INT NOT NULL,
  filter_type         STRING NOT NULL,
  filter_value        STRING NOT NULL,
  is_enabled          BOOLEAN NOT NULL DEFAULT TRUE,
  rationale           STRING,
  decided_by          STRING NOT NULL,
  effective_date      DATE NOT NULL,
  expiration_date     DATE,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  updated_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_filter PRIMARY KEY (filter_id),
  CONSTRAINT fk_filter_source FOREIGN KEY (source_system_id)
    REFERENCES meta.source_systems(source_system_id)
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

#### 13. `meta.extraction_filter_decisions`
**Purpose:** Full audit trail of every business decision to enable, disable, or modify extraction filters. Captures before/after state as JSON snapshots, rationale, and approver.

```sql
CREATE TABLE meta.extraction_filter_decisions (
  decision_id         INT GENERATED ALWAYS AS IDENTITY,
  filter_id           INT NOT NULL,
  action              STRING NOT NULL,
  previous_state      STRING,
  new_state           STRING NOT NULL,
  rationale           STRING NOT NULL,
  decided_by          STRING NOT NULL,
  approved_by         STRING,
  decision_date       TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_decision PRIMARY KEY (decision_id),
  CONSTRAINT fk_decision_filter FOREIGN KEY (filter_id)
    REFERENCES meta.extraction_filters(filter_id)
) USING DELTA;
```

---

### Category 9: Execution Observability

#### 14. `meta.pipeline_execution_log`
**Purpose:** Every pipeline run gets a detailed record. Captures parameters used at execution time (as JSON snapshots for point-in-time accuracy), row counts by operation, timing, errors, and compute context.

```sql
CREATE TABLE meta.pipeline_execution_log (
  execution_id        STRING NOT NULL DEFAULT uuid(),
  pipeline_id         INT NOT NULL,
  step_id             INT,
  job_id              STRING,
  run_id              STRING,
  execution_type      STRING NOT NULL,
  status              STRING NOT NULL,
  applied_filters     STRING,
  source_query        STRING,
  target_table        STRING,
  rows_extracted      BIGINT,
  rows_inserted       BIGINT,
  rows_updated        BIGINT,
  rows_deleted        BIGINT,
  rows_rejected       BIGINT,
  rows_skipped        BIGINT,
  start_time          TIMESTAMP NOT NULL,
  end_time            TIMESTAMP,
  duration_seconds    INT,
  error_code          STRING,
  error_message       STRING,
  error_stack_trace   STRING,
  executed_by         STRING DEFAULT current_user(),
  compute_resource    STRING,
  notebook_path       STRING,
  created_at          TIMESTAMP DEFAULT current_timestamp(),
  CONSTRAINT pk_execution PRIMARY KEY (execution_id),
  CONSTRAINT fk_exec_pipeline FOREIGN KEY (pipeline_id)
    REFERENCES meta.ingestion_pipelines(pipeline_id)
) USING DELTA;
```

---

## Entity Relationships

```
source_systems (1) ──→ (N) ingestion_pipelines (1) ──→ (N) ingestion_pipeline_steps
source_systems (1) ──→ (N) key_registry (1) ──→ (N) key_crosswalk (from/to)
source_systems (1) ──→ (N) extraction_filters (1) ──→ (N) extraction_filter_decisions
source_systems (1) ──→ (N) data_contracts ←── (N) ingestion_pipelines
key_registry   (1) ──→ (N) key_crosswalk_paths (from/to)
ingestion_pipelines (1) ──→ (N) pipeline_execution_log

quality_rules        → references target tables by string name
consumers            → references consuming tables by string name
retention_policies   → references target tables by string name
business_glossary    → references mapped tables/columns by string name
```

The last four use string references rather than FKs because they reference tables across all catalogs/schemas. Unity Catalog's `information_schema` already serves as the table-of-all-tables.

---

## Summary By Concern

| Concern | Tables |
|---|---|
| **What exists?** | source_systems, key_registry, business_glossary |
| **How does data move?** | ingestion_pipelines, pipeline_steps, data_contracts |
| **How do keys relate?** | key_crosswalk, key_crosswalk_paths |
| **Is data correct?** | quality_rules, data_contracts |
| **Who depends on it?** | consumers |
| **How long do we keep it?** | retention_policies |
| **What does business want pulled?** | extraction_filters, extraction_filter_decisions |
| **What happened when it ran?** | pipeline_execution_log |

---

## Supplementary Functions

### Recursive Key Path Traversal

```sql
CREATE FUNCTION meta.find_key_path(start_key_id INT, end_key_id INT)
RETURNS TABLE
RETURN
WITH RECURSIVE paths AS (
  SELECT from_key_id, to_key_id, 1 AS depth,
         ARRAY(crosswalk_id) AS path,
         ARRAY(from_key_id, to_key_id) AS visited
  FROM meta.key_crosswalk
  WHERE from_key_id = start_key_id AND is_active = TRUE

  UNION ALL

  SELECT p.from_key_id, c.to_key_id, p.depth + 1,
         ARRAY_APPEND(p.path, c.crosswalk_id),
         ARRAY_APPEND(p.visited, c.to_key_id)
  FROM paths p
  JOIN meta.key_crosswalk c ON p.to_key_id = c.from_key_id
  WHERE NOT ARRAY_CONTAINS(p.visited, c.to_key_id)
    AND p.depth < 5
    AND c.is_active = TRUE
)
SELECT * FROM paths WHERE to_key_id = end_key_id;
```

---

## Open Items

1. **Table maintenance strategy** — Which of the 14 tables require manual stewardship vs. can be populated/updated automatically from Unity Catalog's `information_schema`? Needs bucketing exercise.
2. **Delta processing strategies** — Incremental extraction patterns (watermark, change token, hash comparison, source CDC, snapshot diff) to be formalized into a `meta.delta_strategies` table when ready.
3. **Extraction filter enforcement** — Bronze filtering is at extraction. If volume/cost constraints change, may need to revisit to ingest-everything-filter-at-silver model. See tradeoff notes in this document.
