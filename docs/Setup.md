# Golden Data Layer — Local Test Harness Setup Guide

## Prerequisites

| Requirement    | Version                                   | Notes                                               |
| -------------- | ----------------------------------------- | --------------------------------------------------- |
| Docker Desktop | 4.x+                                      | Must support linux/arm64 or linux/amd64             |
| Azure SQL Edge | `mcr.microsoft.com/azure-sql-edge:latest` | SQL Server 15.x compatible. Free developer edition. |
| SQL client     | SSMS, Azure Data Studio, or `sqlcmd`      | Any standard SQL client works                       |

### Start the container

```bash
docker run -d --name golden-sql \
  -e "ACCEPT_EULA=1" \
  -e "MSSQL_SA_PASSWORD=YourStrong!Pass123" \
  -p 1433:1433 \
  mcr.microsoft.com/azure-sql-edge:latest
```

Wait ~15 seconds for the engine to initialize, then verify:

```bash
sqlcmd -S localhost,1433 -U SA -P 'YourStrong!Pass123' -Q "SELECT @@VERSION"
```

---

## File Inventory

| #   | File                          | Lines | Purpose                                                                         |
| --- | ----------------------------- | ----- | ------------------------------------------------------------------------------- |
| 1   | `01_ddl.sql`                  | 1,074 | Database, 5 schemas, 44 tables (14 meta, 6 bronze, 12 silver, 11 gold, 1 audit) |
| 2   | `02_meta_programmability.sql` | 1,140 | 4 functions + 33 procs (CRUD for all 14 meta tables + queries)                  |
| 3   | `03_audit.sql`                | 102   | 2 audit procedures + 1 audit view                                               |
| 4   | `04_silver.sql`               | 1,407 | 11 silver transforms + 1 utility + 4 orchestrators + 1 view (17 objects)        |
| 5   | `06_gold.sql`                 | 895   | 11 gold loads + 2 orchestrators + 4 views (17 objects)                          |
| 6   | `05_seed_data.sql`            | 612   | Meta seed, bronze dimension data, transaction generator, bad rows               |

---

## Execution Order

**Critical:** Files must run in this exact order. Each file depends on objects created by prior files.

```text
01_ddl.sql          ← Creates DB, schemas, all 44 tables
      ↓
02_meta_programmability.sql ← 4 functions + CRUD for all 14 meta tables + query procs
      ↓
03_audit.sql       ← ETL run logging procs + audit view
      ↓
04_silver.sql      ← All silver transforms, orchestrators, quarantine view
      ↓
05_seed_data.sql    ← Meta data + bronze dimension data + transaction generator
      ↓
06_gold.sql        ← Gold load procs, gold views, full-pipeline orchestrator
```

**Why 06 runs after 05:** The gold load procs reference silver tables that need to exist and have data.
The proc _definitions_ don't strictly require data, but running the full pipeline at the end does.

### Running via sqlcmd

```bash
# Default dates
sqlcmd -S localhost,1433 -U SA -P 'YourStrong!Pass123' -i 01_ddl.sql
sqlcmd -S localhost,1433 -U SA -P 'YourStrong!Pass123' -i 02_meta_programmability.sql
sqlcmd -S localhost,1433 -U SA -P 'YourStrong!Pass123' -i 03_audit.sql
sqlcmd -S localhost,1433 -U SA -P 'YourStrong!Pass123' -i 04_silver.sql
sqlcmd -S localhost,1433 -U SA -P 'YourStrong!Pass123' -i 05_seed_data.sql
sqlcmd -S localhost,1433 -U SA -P 'YourStrong!Pass123' -i 06_gold.sql
```

### Running via SSMS / Azure Data Studio

1. Open each file in order (01 → 02 → 03 → 04 → 05 → 06) and execute.
2. To change dates, edit the hardcoded values at the top of `05_seed_data.sql`.

---

## Configuration (05_seed_data.sql)

Three hardcoded dates at the top of `05_seed_data.sql` control all timestamps:

```sql
-- Seed date:       2025-01-01  (meta timestamps, bronze _ingested_at)
-- Txn start date:  2025-01-15  (transaction generator start)
-- Txn end date:    2025-03-31  (transaction generator end)
```

To change: find-and-replace `2025-01-01`, `2025-01-15`, `2025-03-31` in the file.

**Constraint:** `TxnStartDate` should be ≥ `SeedDate` (dimensions must be ingested before transactions reference them).

---

## Running the Full Pipeline

After all 6 files are loaded, run:

```sql
EXEC dbo.usp_run_full_pipeline;
```

This executes silver then gold in dependency order:

```text
Silver Phase 1 (independent):  team → pg → portfolio, entity, asset, WSO sec → WSO pricing
Silver Phase 2 (bridges):      portfolio_entity_ownership, entity_asset_ownership
Silver Phase 3 (composite):    security (cascading WSO match)
Silver Phase 4 (transactions): transaction (FK check against security)
Gold Phase 1 (dimensions):     investment_team, entity, asset
Gold Phase 2 (dependent dims): portfolio_group, portfolio, security
Gold Phase 3 (bridges):        portfolio_entity_bridge, entity_asset_bridge
Gold Phase 4 (facts):          position_transactions_fact → position_fact → position_team_bridge
```

The proc prints a summary table at the end with row counts for all 19 populated tables.

---

## Verification

### Step 1: After 01_ddl.sql — Table Counts

```sql
SELECT s.name AS [schema], COUNT(*) AS table_count
FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('meta','bronze','silver','gold','audit')
GROUP BY s.name ORDER BY s.name;
```

| Schema    | Expected |
| --------- | -------- |
| audit     | 1        |
| bronze    | 6        |
| gold      | 11       |
| meta      | 14       |
| silver    | 12       |
| **Total** | **44**   |

### Step 2: After All SQL Files — Programmability Counts

```sql
SELECT s.name + '.' + o.name AS object_name,
       CASE o.type WHEN 'P' THEN 'PROCEDURE' WHEN 'FN' THEN 'FUNCTION' WHEN 'V' THEN 'VIEW' END AS type
FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type IN ('P','FN','V') AND s.name IN ('meta','bronze','silver','gold','audit','dbo')
ORDER BY s.name, o.type, o.name;
```

| Type              | Expected Count                                                              |
| ----------------- | --------------------------------------------------------------------------- |
| Meta functions    | 4 (fn_row_hash_2, fn_translate_key, fn_is_valid_date, fn_is_valid_decimal)  |
| Meta procedures   | 33 (12 upserts + 11 deactivates + 1 delete + 2 log + 6 queries + 1 utility) |
| Audit procedures  | 2 (start/complete etl_run)                                                  |
| Silver procedures | 16 (1 quarantine_row + 11 transforms + 4 orchestrators)                     |
| Gold procedures   | 12 (11 loads + 1 orchestrator)                                              |
| dbo procedures    | 1 (usp_run_full_pipeline)                                                   |
| Bronze procedures | 1 (usp_generate_transactions — from 05_seed_data.sql)                       |
| Views             | 6 (1 audit + 1 silver + 4 gold)                                             |
| **Total objects** | **75**                                                                      |

### Step 3: After 05_seed_data.sql — Meta + Bronze Row Counts

```sql
-- Meta
SELECT 'meta.source_systems' AS tbl, COUNT(*) AS cnt FROM meta.source_systems
UNION ALL SELECT 'meta.ingestion_pipelines', COUNT(*) FROM meta.ingestion_pipelines
UNION ALL SELECT 'meta.key_registry', COUNT(*) FROM meta.key_registry
UNION ALL SELECT 'meta.key_crosswalk', COUNT(*) FROM meta.key_crosswalk
UNION ALL SELECT 'meta.key_crosswalk_paths', COUNT(*) FROM meta.key_crosswalk_paths
UNION ALL SELECT 'meta.quality_rules', COUNT(*) FROM meta.quality_rules
UNION ALL SELECT 'meta.data_contracts', COUNT(*) FROM meta.data_contracts
UNION ALL SELECT 'meta.consumers', COUNT(*) FROM meta.consumers
UNION ALL SELECT 'meta.retention_policies', COUNT(*) FROM meta.retention_policies
UNION ALL SELECT 'meta.business_glossary', COUNT(*) FROM meta.business_glossary
ORDER BY tbl;
```

| Meta Table          | Expected |
| ------------------- | -------- |
| source_systems      | 6        |
| ingestion_pipelines | 7        |
| key_registry        | 21       |
| key_crosswalk       | 12       |
| key_crosswalk_paths | 12       |
| quality_rules       | 13       |
| data_contracts      | 6        |
| consumers           | 4        |
| retention_policies  | 9        |
| business_glossary   | 4        |

```sql
-- Bronze
SELECT 'src_enterprise_raw' AS tbl, COUNT(*) FROM bronze.src_enterprise_raw
UNION ALL SELECT 'src_entity_mgmt_raw', COUNT(*) FROM bronze.src_entity_mgmt_raw
UNION ALL SELECT 'src_asset_mgmt_raw', COUNT(*) FROM bronze.src_asset_mgmt_raw
UNION ALL SELECT 'src_security_mgmt_raw', COUNT(*) FROM bronze.src_security_mgmt_raw
UNION ALL SELECT 'src_txn_mgmt_raw', COUNT(*) FROM bronze.src_txn_mgmt_raw
UNION ALL SELECT 'src_ws_online_raw', COUNT(*) FROM bronze.src_ws_online_raw;
```

| Bronze Table          | Expected     | Breakdown                                            |
| --------------------- | ------------ | ---------------------------------------------------- |
| src_enterprise_raw    | 17           | 3 teams + 5 funds + 7 portfolios + 2 bad             |
| src_entity_mgmt_raw   | 19           | 5 entities + 6 PE ownership + 7 EA ownership + 1 bad |
| src_asset_mgmt_raw    | 8            | 7 assets + 1 bad                                     |
| src_security_mgmt_raw | 9            | 8 securities + 1 bad                                 |
| src_txn_mgmt_raw      | ~3,000–5,000 | Generated + 1 bad (varies by date range)             |
| src_ws_online_raw     | 11           | 6 securities + 4 pricing + 1 bad                     |

### Step 4: After dbo.usp_run_full_pipeline — Silver + Gold

The pipeline prints its own summary. You can also verify manually:

```sql
-- Silver
SELECT 'investment_team' AS tbl, COUNT(*) AS cnt FROM silver.investment_team
UNION ALL SELECT 'portfolio_group', COUNT(*) FROM silver.portfolio_group
UNION ALL SELECT 'portfolio', COUNT(*) FROM silver.portfolio
UNION ALL SELECT 'entity', COUNT(*) FROM silver.entity
UNION ALL SELECT 'asset', COUNT(*) FROM silver.asset
UNION ALL SELECT 'security', COUNT(*) FROM silver.security
UNION ALL SELECT 'ws_online_security', COUNT(*) FROM silver.ws_online_security
UNION ALL SELECT 'ws_online_pricing', COUNT(*) FROM silver.ws_online_pricing
UNION ALL SELECT 'portfolio_entity_ownership', COUNT(*) FROM silver.portfolio_entity_ownership
UNION ALL SELECT 'entity_asset_ownership', COUNT(*) FROM silver.entity_asset_ownership
UNION ALL SELECT 'transaction', COUNT(*) FROM silver.transaction
UNION ALL SELECT 'quarantine', COUNT(*) FROM silver.quarantine
ORDER BY tbl;
```

| Silver Table               | Expected                                    |
| -------------------------- | ------------------------------------------- |
| investment_team            | 3                                           |
| portfolio_group            | 5                                           |
| portfolio                  | 7                                           |
| entity                     | 5                                           |
| asset                      | 7                                           |
| security                   | 8                                           |
| ws_online_security         | 6                                           |
| ws_online_pricing          | 4                                           |
| portfolio_entity_ownership | 6                                           |
| entity_asset_ownership     | 7                                           |
| transaction                | ~3,000–5,000 (matches generated - 1 bad FK) |
| quarantine                 | 7                                           |

```sql
-- Gold
SELECT 'investment_team_dimension' AS tbl, COUNT(*) AS cnt FROM gold.investment_team_dimension
UNION ALL SELECT 'portfolio_group_dimension', COUNT(*) FROM gold.portfolio_group_dimension
UNION ALL SELECT 'portfolio_dimension', COUNT(*) FROM gold.portfolio_dimension
UNION ALL SELECT 'entity_dimension', COUNT(*) FROM gold.entity_dimension
UNION ALL SELECT 'asset_dimension', COUNT(*) FROM gold.asset_dimension
UNION ALL SELECT 'security_dimension', COUNT(*) FROM gold.security_dimension
UNION ALL SELECT 'portfolio_entity_bridge', COUNT(*) FROM gold.portfolio_entity_bridge
UNION ALL SELECT 'entity_asset_bridge', COUNT(*) FROM gold.entity_asset_bridge
UNION ALL SELECT 'position_transactions_fact', COUNT(*) FROM gold.position_transactions_fact
UNION ALL SELECT 'position_fact', COUNT(*) FROM gold.position_fact
UNION ALL SELECT 'position_team_bridge', COUNT(*) FROM gold.position_team_bridge
ORDER BY tbl;
```

| Gold Table                 | Expected                         |
| -------------------------- | -------------------------------- |
| investment_team_dimension  | 3                                |
| portfolio_group_dimension  | 5                                |
| portfolio_dimension        | 7                                |
| entity_dimension           | 5                                |
| asset_dimension            | 7                                |
| security_dimension         | 8                                |
| portfolio_entity_bridge    | 6                                |
| entity_asset_bridge        | 7                                |
| position_transactions_fact | ~3,000–5,000                     |
| position_fact              | Aggregated (< transaction count) |
| position_team_bridge       | = position_fact count            |

### Step 5: Quarantine Validation

Verify all 7 deliberate bad rows were caught:

```sql
SELECT quarantine_id, source_table, failed_rule, failure_detail, raw_payload
FROM silver.quarantine
ORDER BY quarantine_id;
```

| #   | Source                   | Bad Record    | Rule Triggered        | What's Wrong                                  |
| --- | ------------------------ | ------------- | --------------------- | --------------------------------------------- |
| 1   | silver.investment_team   | ENT-IT-10099  | TEAM_NAME_NOT_EMPTY   | NULL team_name                                |
| 2   | silver.portfolio_group   | ENT-PG-20099  | PG_TEAM_EXISTS        | References nonexistent team ENT-IT-99999      |
| 3   | silver.entity            | SEM-E-20099   | ENTITY_NAME_NOT_EMPTY | Whitespace-only entity_name                   |
| 4   | silver.asset             | SAM-A-40099   | ASSET_TYPE_NOT_EMPTY  | NULL asset_type                               |
| 5   | silver.security          | SSM-SEC-50099 | SEC_TYPE_VALID        | Invalid type "CRYPTO"                         |
| 6   | silver.transaction       | STM-TXN-99999 | TXN_SECURITY_EXISTS   | References nonexistent security STM-SEC-99999 |
| 7   | silver.ws_online_pricing | WSO-SEC-70001 | PRICE_DATE_VALID      | Unparseable date "NOT-A-DATE"                 |

### Step 6: Composite Security Assembly

Verify the WSO matching results:

```sql
SELECT security_enterprise_key, security_name,
       bank_loan_id, cusip, isin, ticker,
       _wso_match_status, _wso_match_key, _wso_match_confidence
FROM silver.security
ORDER BY security_enterprise_key;
```

| Security                      | Match Status | Match Key       | Confidence | Why                                          |
| ----------------------------- | ------------ | --------------- | ---------- | -------------------------------------------- |
| SEC-50001 (Meridian Class A)  | AMBIGUOUS    | CUSIP_AMBIGUOUS | LOW        | CUSIP `59156R100` matches 2 WSO records      |
| SEC-50002 (Meridian Suburban) | MATCHED      | TICKER_TYPE     | MEDIUM     | Matched via ticker `MER.SUB` + type `EQUITY` |
| SEC-50003 (Apex LP Units)     | MATCHED      | ISIN            | HIGH       | Matched via ISIN `US03783A1007`              |
| SEC-50004 (Vertex Series B)   | UNMATCHED    | NULL            | NULL       | No WSO record exists for private equity      |
| SEC-50005 (Coastal Wind)      | MATCHED      | BANK_LOAN_ID    | HIGH       | Matched via `BL-COAST-001`                   |
| SEC-50006 (Summit Tranche A)  | MATCHED      | BANK_LOAN_ID    | HIGH       | Matched via `BL-SUMMIT-001`                  |
| SEC-50007 (Summit Mezz)       | UNMATCHED    | NULL            | NULL       | No identifiers to match                      |
| SEC-50008 (Summit CDS)        | UNMATCHED    | NULL            | NULL       | No identifiers to match                      |

### Step 7: Gold Views

```sql
-- Investment hierarchy: team → fund → portfolio → entity
SELECT * FROM gold.vw_investment_hierarchy;

-- Entity → asset hierarchy with ownership %
SELECT * FROM gold.vw_entity_asset_hierarchy;

-- Fully resolved position detail (all dimension names)
SELECT TOP 10 * FROM gold.vw_position_detail;

-- Positions weighted by team allocation
SELECT TOP 10 * FROM gold.vw_position_by_team;

-- Data quality dashboard
SELECT * FROM silver.vw_quarantine_summary;

-- ETL run history
SELECT * FROM audit.vw_recent_runs;
```

### Step 8: Audit Trail

```sql
SELECT pipeline_code, target_layer, target_table, operation,
       rows_read, rows_inserted, rows_updated, rows_quarantined,
       status, DATEDIFF(SECOND, start_time, end_time) AS duration_sec
FROM audit.etl_run_log
ORDER BY start_time;
```

Every silver and gold proc logs its run here. Expect ~22 rows after a full pipeline execution.

---

## Deliberate Test Scenarios

The seed data is designed to exercise specific edge cases:

| Scenario                    | How to Test                                                               |
| --------------------------- | ------------------------------------------------------------------------- |
| FK validation / quarantine  | 7 bad rows across 6 source tables (see Step 5)                            |
| Composite security assembly | 4 match types + 1 ambiguous + 3 unmatched (see Step 6)                    |
| Idempotent re-run           | Run `dbo.usp_run_full_pipeline` twice — row counts should not change      |
| Hash-based CDC              | Modify a bronze record, re-run silver — only changed rows update          |
| Temporal bridges            | PE/EA ownership records have effective_date/end_date for SCD              |
| WSO orphan                  | WSO-SEC-70006 ("Unrelated Corp") has no matching internal security        |
| WSO ambiguous CUSIP         | CUSIP `59156R100` matches both WSO-SEC-70001 and WSO-SEC-70005            |
| Transaction volume          | ~3,000–5,000 rows with realistic type distribution and randomized amounts |

---

## Architecture Reference

### Schema Layout

```
meta.*          14 tables   Governance: source systems, keys, quality rules, contracts
bronze.*         6 tables   Raw ingestion: 1 table per source system, all NVARCHAR
silver.*        12 tables   Conformed: typed columns, validated, quarantined failures
gold.*          11 tables   Dimensional model: 6 dims, 2 facts, 3 bridges
audit.*          1 table    ETL run log
```

### 6-Source-System Model

| #   | System                 | Code              | Owns                                        |
| --- | ---------------------- | ----------------- | ------------------------------------------- |
| 1   | Enterprise Data        | SRC_ENTERPRISE    | Investment team, portfolio group, portfolio |
| 2   | Entity Management      | SRC_ENTITY_MGMT   | Entity, PE/EA ownership bridges             |
| 3   | Asset Management       | SRC_ASSET_MGMT    | Asset                                       |
| 4   | Security Management    | SRC_SECURITY_MGMT | Security                                    |
| 5   | Transaction Management | SRC_TXN_MGMT      | Transactions (refs all dims)                |
| 6   | Wall Street Online     | SRC_WS_ONLINE     | Public market data, pricing                 |

### Key Translation

Source-native keys translate to enterprise keys via prefix replacement:

```
ENT-IT-10001  →  IT-10001    (investment team)
SEM-E-20001   →  E-20001     (entity)
SAM-A-40001   →  A-40001     (asset)
SSM-SEC-50001 →  SEC-50001   (security)
STM-P-30001   →  P-30001     (portfolio, from txn system)
```

### Pipeline Dependencies

```
                ┌─ PL_ENTERPRISE_DAILY ─→ team → pg → portfolio
                │
                ├─ PL_ENTITY_DAILY ────→ entity → PE ownership → EA ownership
                │
                ├─ PL_ASSET_DAILY ─────→ asset
                │
Phase 1         ├─ PL_MARKET_DAILY ────→ WSO security → WSO pricing
                │
                │                              ↓
Phase 2         ├─ PL_SECURITY_DAILY ──→ security (composite assembly, needs WSO)
                │                              ↓
Phase 3         └─ PL_TXN_DAILY ───────→ transaction (needs all dims + security)
                                               ↓
Phase 4                         gold.usp_run_all_gold (dims → bridges → facts)
```

---

## Meta Tables Without Seed Data

Three meta tables have DDL but no seed data. This is intentional:

| Table                              | Why No Seed                                                            |
| ---------------------------------- | ---------------------------------------------------------------------- |
| `meta.ingestion_pipeline_steps`    | Granular step definitions — add per-implementation                     |
| `meta.extraction_filters`          | Source-specific extraction criteria — add when connecting real sources |
| `meta.extraction_filter_decisions` | Audit trail for filter decisions — populated at runtime                |
| `meta.pipeline_execution_log`      | Populated at runtime by `usp_start_etl_run` / `usp_complete_etl_run`   |

These are structural placeholders for production use. The test harness works without them.

---

## Resetting the Environment

```sql
-- Full teardown
USE master;
GO
DROP DATABASE IF EXISTS GoldenDataLayer;
GO
```

Then re-run all 6 files in order.

For a **data-only reset** (keep schema + procs, clear all rows):

```sql
USE GoldenDataLayer;
-- Gold (child tables first due to FKs)
DELETE FROM gold.position_team_bridge;
DELETE FROM gold.position_fact;
DELETE FROM gold.position_transactions_fact;
DELETE FROM gold.entity_asset_bridge;
DELETE FROM gold.portfolio_entity_bridge;
DELETE FROM gold.security_dimension;
DELETE FROM gold.asset_dimension;
DELETE FROM gold.entity_dimension;
DELETE FROM gold.portfolio_dimension;
DELETE FROM gold.portfolio_group_dimension;
DELETE FROM gold.investment_team_dimension;
-- Silver
DELETE FROM silver.quarantine;
DELETE FROM silver.transaction;
DELETE FROM silver.security;
DELETE FROM silver.ws_online_pricing;
DELETE FROM silver.ws_online_security;
DELETE FROM silver.entity_asset_ownership;
DELETE FROM silver.portfolio_entity_ownership;
DELETE FROM silver.asset;
DELETE FROM silver.entity;
DELETE FROM silver.portfolio;
DELETE FROM silver.portfolio_group;
DELETE FROM silver.investment_team;
-- Bronze
DELETE FROM bronze.src_txn_mgmt_raw;
DELETE FROM bronze.src_ws_online_raw;
DELETE FROM bronze.src_security_mgmt_raw;
DELETE FROM bronze.src_asset_mgmt_raw;
DELETE FROM bronze.src_entity_mgmt_raw;
DELETE FROM bronze.src_enterprise_raw;
-- Audit
DELETE FROM audit.etl_run_log;
-- Then re-run 05_seed_data.sql + dbo.usp_run_full_pipeline
```

---

## Troubleshooting

| Symptom                          | Cause                                 | Fix                                                                        |
| -------------------------------- | ------------------------------------- | -------------------------------------------------------------------------- |
| FK violation on gold load        | Silver tables empty                   | Run `silver.usp_run_all_silver` before gold procs                          |
| 0 rows in silver.security        | WSO security not loaded yet           | Ensure `usp_conform_ws_online_security` runs before `usp_conform_security` |
| Transaction count = 0            | Security not in silver                | Security composite assembly must complete before transaction conform       |
| Quarantine < 7                   | Procs ran in wrong order              | Run `dbo.usp_run_full_pipeline` which handles dependency order             |
| `Login failed for user 'SA'`     | Wrong password or container not ready | Wait 15 sec after `docker run`, verify password                            |
| Identity column errors on re-run | IDENTITY seeds not reset              | Use full teardown (`DROP DATABASE`) or DBCC CHECKIDENT                     |
