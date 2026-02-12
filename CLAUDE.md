# Golden Data Layer — SQL Server Test Environment

## What This Is

A medallion architecture (bronze → silver → gold) data governance framework for a PE / real assets investment firm, implemented as 6 sequential SQL files targeting **SQL Server / Azure SQL Edge**. This is a local test harness — the production target is Databricks with Unity Catalog.

The system models the full PE investment hierarchy: Management Company (GP) → Fund → Portfolio → Entity → Asset → Security, with two fact tables (position transactions and summarized positions) and bridge tables for M:N ownership relationships.

## Project Structure

```
sql/           — 6 sequential SQL files (execute 01 → 06 in order)
docs/          — Architecture docs, design specs, industry research
scripts/       — Deployment/utility scripts
```

## SQL File Execution Order

Files MUST run sequentially. Each file's header lists its dependencies.

| File | Purpose | Objects |
|---|---|---|
| `01_ddl.sql` | Database, schemas, all tables (bronze/silver/gold/meta/audit), nonclustered indexes | 44 tables + indexes |
| `02_meta_programmability.sql` | Meta functions, views, CRUD procs for 14 governance tables | ~40 procs |
| `03_audit.sql` | ETL run log procs (start/complete/cleanup) | 3 procs |
| `04_silver.sql` | 11 bronze→silver transform procs + orchestrators + quarantine | ~16 procs |
| `05_seed_data.sql` | Test data: meta config, bronze records, transaction generator | Seed data |
| `06_gold.sql` | 11 gold load procs + orchestrators + 4 analytical views | ~15 procs |

## Running the Pipeline

```sql
-- After executing all 6 files in order:
EXEC dbo.usp_run_full_pipeline;

-- Or run phases independently:
EXEC silver.usp_run_all_silver;
EXEC gold.usp_run_all_gold;
```

## Architecture Decisions

### 6-Source-System Model
```
enterprise_data               → investment_team, portfolio_group, portfolio
Source_Entity_Management       → entity, portfolio↔entity ownership, entity↔asset ownership
Source_Asset_Management        → asset master data
Source_Security_Management     → security master (composite: internal + WSO)
Source_Transaction_Management  → daily transactions by security
Source_Wall_Street_Online      → public market security data, pricing
```

### Key Translation Pattern
Source-native keys are translated to enterprise keys at the silver layer using prefix replacement rules stored in `meta.key_crosswalk`. Example: `ENT-IT-10001` → `IT-10001`. The `meta.fn_translate_key` function validates the expected prefix before stripping it — if the source key doesn't start with the expected prefix, it returns NULL (which quarantine rules then catch). Gold generates surrogate IDENTITY keys and looks up enterprise keys for joins.

### Security Composite Assembly
`silver.security` is assembled from two inputs: internal records from Source_Security_Management + public market identifiers from WSO (Wall Street Online). Match cascade: bank_loan_id → CUSIP → ISIN → ticker+type. Ambiguous matches are flagged, not auto-enriched.

### Quarantine Pattern
Failed quality rules route rows to `silver.quarantine` (centralized) with rule name, raw payload, and resolution tracking. Rows are never silently dropped — every silver transform proc has explicit quarantine checks for NULL enterprise keys, NULL required fields, invalid types, and FK violations.

### Bridge Tables with Ownership %
- `portfolio_entity_bridge` — which portfolios own what % of which entities
- `entity_asset_bridge` — which entities own what % of which assets
- `position_team_bridge` — allocates positions to investment teams (via security→team lookup)

### Star Schema Surrogate Key Pattern
Gold dimensions use IDENTITY surrogate keys. Dimension-to-dimension references use surrogate INT keys with FK constraints:
- `portfolio_group_dimension.investment_team_key` → FK to `investment_team_dimension`
- `portfolio_dimension.portfolio_group_key` → FK to `portfolio_group_dimension`
- `security_dimension.investment_team_key` → FK to `investment_team_dimension`
- `security_dimension.entity_key` → FK to `entity_dimension`
- `security_dimension.asset_key` → FK to `asset_dimension`

## SQL Server Compatibility Notes

These patterns were discovered through debugging and MUST be maintained:

### ERROR_MESSAGE() Cannot Be Passed Directly to EXEC
SQL Server does not allow function calls as positional EXEC parameters. Always capture first:
```sql
-- WRONG: EXEC audit.usp_complete_etl_run @run_id, 'FAILED', 0, 0, 0, 0, 0, ERROR_MESSAGE();
-- RIGHT:
DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
EXEC audit.usp_complete_etl_run @run_id, 'FAILED', 0, 0, 0, 0, 0, @err_msg;
```

### No SQLCMD Mode Dependencies
File 05 originally used `:setvar` directives. These were removed because SQLCMD mode requires special client config. Dates are hardcoded with comments indicating where to edit.

### Security Composite Dedup
The WSO LEFT JOIN in `usp_conform_security` can fan out rows when multiple WSO records match one internal security. A dedup step (`#sec_final_raw` → ROW_NUMBER → `#sec_final`) was added to prevent PK violations. Dedup prefers MATCHED > AMBIGUOUS > UNMATCHED.

### Security Type Validation (Delimiter-Wrapped CHARINDEX)
`usp_conform_security` uses delimiter-wrapped CHARINDEX for exact-match security type validation:
```sql
WHERE CHARINDEX(',' + s.security_type + ',', ',' + @valid_types + ',') = 0;
```
This prevents substring false positives (e.g., `DEBT` no longer matches `SENIOR_DEBT`).

### DDL Idempotency
All CREATE TABLE statements are preceded by `DROP TABLE IF EXISTS` guards. Tables are dropped in reverse dependency order (facts → bridges → dimensions → silver → bronze → audit → meta) to respect FK constraints. The entire DDL file can be re-executed safely.

### Position Fact Rebuild Transaction Safety
`usp_load_position_fact` wraps its DELETE + INSERT rebuild in an explicit `BEGIN TRANSACTION / COMMIT TRANSACTION` with `ROLLBACK` in the CATCH block. This prevents data loss if the rebuild fails partway through.

## Data Model Summary

### Dimensions (gold)
- `investment_team_dimension` — GP / management company teams
- `portfolio_group_dimension` — Funds (vintage year, strategy, committed capital; FK to investment_team)
- `portfolio_dimension` — Collections of investments within a fund (FK to portfolio_group)
- `entity_dimension` — Legal entities (LLC, LP, SPV, Corp)
- `asset_dimension` — Physical/financial assets (real estate, infrastructure, etc.)
- `security_dimension` — Financial instruments (equity, debt, mezzanine, derivatives; FKs to team, entity, asset)

### Facts (gold)
- `position_transactions_fact` — Individual transactions (append-only, UNIQUE on source_system_transaction_id)
- `position_fact` — Summarized daily positions (full rebuild in explicit transaction)

### Bridges (gold)
- `portfolio_entity_bridge` — M:N with ownership_pct + effective dating (FK to source_systems)
- `entity_asset_bridge` — M:N with ownership_pct + effective dating (FK to source_systems)
- `position_team_bridge` — Allocates positions to teams (currently 1:1, designed for M:N)

### Silver tables
- `investment_team`, `portfolio_group`, `portfolio`, `entity`, `asset` — conformed dimensions
- `position_transaction` — conformed transactions (renamed from `[transaction]` to avoid reserved word)
- `security` — composite assembly from SSM + WSO
- `ws_online_security`, `ws_online_pricing` — WSO market data
- `portfolio_entity_ownership`, `entity_asset_ownership` — conformed bridges (with `_row_hash`)
- `quarantine` — centralized quarantine with status CHECK constraint

### Meta/Governance (14 tables)
source_systems, ingestion_pipelines, ingestion_pipeline_steps, data_contracts, key_registry, key_crosswalk, key_crosswalk_paths, quality_rules, consumers, retention_policies, business_glossary, extraction_filters, extraction_filter_decisions, pipeline_execution_log

### Nonclustered Indexes
All FK reference columns, filter columns, and fact date columns have nonclustered indexes. See the `PART 7: NONCLUSTERED INDEXES` section of `01_ddl.sql`.

## Pipeline Dependency Order

### Silver (4 phases)
1. **Independent sources**: enterprise (team→pg→portfolio), entity, asset, WSO (security→pricing)
2. **Ownership bridges**: portfolio_entity, entity_asset (need portfolio+entity+asset)
3. **Security composite**: needs WSO security data
4. **Transactions**: needs portfolio+entity+security

### Gold (4 phases)
1. **Independent dims**: investment_team, entity, asset
2. **Dependent dims**: portfolio_group (needs team key), portfolio (needs PG key), security (needs team+entity+asset keys)
3. **Bridges**: portfolio_entity, entity_asset (need surrogate keys)
4. **Facts**: position_transactions, position_fact, position_team_bridge

## Coding Conventions

- All procs log via `audit.usp_start_etl_run` / `audit.usp_complete_etl_run`
- Silver transforms: CTE with ROW_NUMBER dedup → temp table → quarantine checks (no silent drops) → MERGE
- Gold loads: MERGE on enterprise key (dims) or composite key (bridges)
- Row change detection via `_row_hash` (HASHBYTES SHA2_256 of ALL business columns — including valuations, dates, currencies)
- Silver audit columns: `_source_system_id`, `_bronze_record_id` (NVARCHAR(36)), `_source_modified_at`, `_conformed_at`, `_conformed_by`, `_row_hash`
- Gold audit: `created_date`, `created_by`, `modified_date`, `modified_by`
- Orchestrators use TRY/CATCH with phase tracking for error diagnostics
- Meta table natural keys have UNIQUE constraints: `system_code`, `pipeline_code`, `rule_code`, `consumer_name`, `business_term`

## Target Production Platform

Databricks with Unity Catalog. See `docs/` for:
- `golden_layer_6_source_architecture.md` — full source system mapping
- `golden_layer_meta_framework.md` — governance table design rationale
- `golden_layer_silver_design.md` — silver layer specs with transformation rules
- `pe_real_assets_data_hierarchy_research.md` — industry hierarchy validation

## Open Items

1. **Quarantine review workflow** — no tooling yet for data steward review of quarantined rows
2. **Late-arriving dimensions** — currently quarantines txns referencing missing portfolios/entities/securities; may need placeholder pattern
3. **Backfill strategy** — no process for re-running historical bronze through silver when rules change
4. **Quarantine retention** — no purge policy defined yet
5. **SCD Type 2** — all gold dimensions use Type 1 (overwrite); entity and security dimensions would benefit from SCD2 for historical state tracking
6. **Date dimension** — no `gold.date_dimension` for fiscal year/quarter analytics
7. **Gold pricing fact** — `silver.ws_online_pricing` has no corresponding gold table for mark-to-market analytics
8. **Currency/FX reference** — no currency reference table or historical FX rates for validation
9. **MERGE audit accuracy** — @@ROWCOUNT after MERGE captures inserts+updates combined; needs OUTPUT $action for exact split
10. **Stale run cleanup** — `audit.usp_cleanup_stale_runs` available but not scheduled; should be called periodically
