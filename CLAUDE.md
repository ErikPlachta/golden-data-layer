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
| `01_ddl.sql` | Database, schemas, all tables (bronze/silver/gold/meta/audit) | 44 tables |
| `02_meta_programmability.sql` | Meta functions, views, CRUD procs for 14 governance tables | ~40 procs |
| `03_audit.sql` | ETL run log procs (start/complete) | 2 procs |
| `04_silver.sql` | 11 bronze→silver transform procs + orchestrators + quarantine | ~16 procs |
| `05_seed_data.sql` | Test data: meta config, bronze records, transaction generator | Seed data |
| `06_gold.sql` | 11 gold load procs + orchestrators + 4 analytical views | ~15 procs |

Total: ~5,800 lines, 75 programmable objects.

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
Source-native keys are translated to enterprise keys at the silver layer using prefix replacement rules stored in `meta.key_crosswalk`. Example: `ENT-IT-10001` → `IT-10001`. Gold generates surrogate IDENTITY keys and looks up enterprise keys for joins.

### Security Composite Assembly
`silver.security` is assembled from two inputs: internal records from Source_Security_Management + public market identifiers from WSO (Wall Street Online). Match cascade: bank_loan_id → CUSIP → ISIN → ticker+type. Ambiguous matches are flagged, not auto-enriched.

### Quarantine Pattern
Failed quality rules route rows to `silver.quarantine` (centralized) with rule name, raw payload, and resolution tracking. Rows are never silently dropped.

### Bridge Tables with Ownership %
- `portfolio_entity_bridge` — which portfolios own what % of which entities
- `entity_asset_bridge` — which entities own what % of which assets
- `position_team_bridge` — allocates positions to investment teams (via security→team lookup)

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

### `transaction` is a Reserved Word
Always bracket: `silver.[transaction]`, never `silver.transaction` in DML.

### No SQLCMD Mode Dependencies
File 05 originally used `:setvar` directives. These were removed because SQLCMD mode requires special client config. Dates are hardcoded with comments indicating where to edit.

### Security Composite Dedup
The WSO LEFT JOIN in `usp_conform_security` can fan out rows when multiple WSO records match one internal security. A dedup step (`#sec_final_raw` → ROW_NUMBER → `#sec_final`) was added to prevent PK violations. Dedup prefers MATCHED > AMBIGUOUS > UNMATCHED.

### CHARINDEX for Type Validation
`usp_conform_security` uses `CHARINDEX(type, @valid_types)` for security type validation. This is substring-based — fragile if new types are substrings of existing ones (e.g., adding `DEBT` would false-match `SENIOR_DEBT`). Known minor issue, not a runtime blocker with current type list.

## Data Model Summary

### Dimensions (gold)
- `investment_team_dimension` — GP / management company teams
- `portfolio_group_dimension` — Funds (vintage year, strategy, committed capital)
- `portfolio_dimension` — Collections of investments within a fund
- `entity_dimension` — Legal entities (LLC, LP, SPV, Corp)
- `asset_dimension` — Physical/financial assets (real estate, infrastructure, etc.)
- `security_dimension` — Financial instruments (equity, debt, mezzanine, derivatives)

### Facts (gold)
- `position_transactions_fact` — Individual transactions (append-only)
- `position_fact` — Summarized daily positions (full rebuild)

### Bridges (gold)
- `portfolio_entity_bridge` — M:N with ownership_pct + effective dating
- `entity_asset_bridge` — M:N with ownership_pct + effective dating
- `position_team_bridge` — Allocates positions to teams (currently 1:1, designed for M:N)

### Meta/Governance (14 tables)
source_systems, ingestion_pipelines, key_registry, key_crosswalk, key_crosswalk_paths, quality_rules, data_contracts, consumers, retention_policies, business_glossary, lineage_catalog, tag_registry, tag_assignments, change_log

## Pipeline Dependency Order

### Silver (4 phases)
1. **Independent sources**: enterprise (team→pg→portfolio), entity, asset, WSO (security→pricing)
2. **Ownership bridges**: portfolio_entity, entity_asset (need portfolio+entity+asset)
3. **Security composite**: needs WSO security data
4. **Transactions**: needs portfolio+entity+security

### Gold (4 phases)
1. **Independent dims**: investment_team, entity, asset
2. **Dependent dims**: portfolio_group, portfolio (needs PG key), security
3. **Bridges**: portfolio_entity, entity_asset (need surrogate keys)
4. **Facts**: position_transactions, position_fact, position_team_bridge

## Coding Conventions

- All procs log via `audit.usp_start_etl_run` / `audit.usp_complete_etl_run`
- Silver transforms: CTE with ROW_NUMBER dedup → temp table → quarantine checks → MERGE
- Gold loads: MERGE on enterprise key (dims) or composite key (bridges)
- Row change detection via `_row_hash` (HASHBYTES SHA2_256 of business columns only)
- Audit columns: `_source_system_id`, `_bronze_record_id`, `_source_modified_at`, `_conformed_at`, `_conformed_by`, `_row_hash`
- Gold audit: `created_date`, `created_by`, `modified_date`, `modified_by`

## Target Production Platform

Databricks with Unity Catalog. See `docs/` for:
- `golden_layer_6_source_architecture.md` — full source system mapping
- `golden_layer_meta_framework.md` — governance table design rationale
- `golden_layer_silver_design.md` — silver layer specs with transformation rules
- `pe_real_assets_data_hierarchy_research.md` — industry hierarchy validation

## Open Items

1. **Quarantine review workflow** — no tooling yet for data steward review of quarantined rows
2. **Late-arriving dimensions** — currently quarantines txns referencing missing securities; may need placeholder pattern
3. **Backfill strategy** — no process for re-running historical bronze through silver when rules change
4. **Quarantine retention** — no purge policy defined yet
5. **CHARINDEX type validation** — should migrate to exact-match pattern for robustness
