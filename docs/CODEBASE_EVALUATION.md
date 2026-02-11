# Codebase Evaluation: Golden Data Layer

**Date**: 2026-02-11
**Scope**: All 6 SQL files, deployment scripts, documentation
**Codebase**: ~5,260 lines SQL, 44 tables, ~75 programmable objects

---

## Executive Summary

This is a well-structured medallion architecture implementation with clear separation of concerns, consistent proc patterns, and comprehensive documentation. However, the analysis reveals **systemic issues in five areas**: missing indexes (every non-PK query is a table scan), referential integrity gaps (silver has zero FKs, gold has inconsistent surrogate key usage), silent data loss in quarantine logic (multiple procs drop rows instead of quarantining them), inaccurate audit logging (all MERGEs misreport insert/update counts), and missing error handling in orchestrators. The documentation also diverges from the actual implementation in several places.

The findings below are organized by severity, then by layer.

---

## P0 — Data Loss / Silent Corruption Risk

### 1. Position fact rebuild has no transaction safety
`06_gold.sql:614-616` — `gold.usp_load_position_fact` DELETEs `position_team_bridge` and `position_fact` before re-inserting, but these operations are not wrapped in an explicit `BEGIN TRAN / ROLLBACK`. If the INSERT fails after the DELETEs succeed, both tables are empty and the CATCH block THROWs — but the deletions have already committed. This is a **data loss path** on any failure during the rebuild.

### 2. Zero nonclustered indexes across 44 tables
`01_ddl.sql` — The DDL creates zero nonclustered indexes. Every query that is not a PK lookup results in a full table scan. Critical missing indexes include:
- Bronze `_record_type` columns (every silver transform filters on these)
- Silver enterprise key reference columns (every gold MERGE joins on these)
- Gold fact dimension key columns (`portfolio_key`, `entity_key`, `security_key`, `as_of_date`)
- Gold bridge table individual key columns (composite PK only helps lead-column lookups)
- `silver.quarantine(source_table, resolution_status)`
- `audit.etl_run_log(pipeline_code, status, start_time)`

### 3. NEWID() as clustered PK on all bronze tables
`01_ddl.sql:356,390,418,441,463,490` — All 6 bronze tables use `UNIQUEIDENTIFIER DEFAULT NEWID()` as the clustered primary key. `NEWID()` generates random GUIDs, causing page splits and fragmentation on the clustered index. For append-heavy bronze tables, `NEWSEQUENTIALID()` or `BIGINT IDENTITY` would eliminate this.

### 4. Silent row drops violate the quarantine contract
The CLAUDE.md states: "Rows are never silently dropped." Multiple procs violate this:

| Proc | File:Line | Silently dropped rows |
|---|---|---|
| `usp_conform_asset` | `04_silver.sql:490-495` | NULL enterprise key, NULL name, NULL type — no quarantine INSERTs exist |
| `usp_conform_ws_online_security` | `04_silver.sql:583-584` | NULL `wso_security_id` — no quarantine INSERTs exist, `@quar` not even declared |
| `usp_conform_portfolio_group` | `04_silver.sql:228-232` | NULL enterprise key, NULL name — filtered in MERGE, never quarantined |
| `usp_conform_portfolio` | `04_silver.sql:320-321` | NULL enterprise key, NULL name — filtered in MERGE, never quarantined |
| `usp_conform_entity` | `04_silver.sql:408` | NULL enterprise key — filtered in MERGE, never quarantined |
| `usp_conform_entity_asset_ownership` | `04_silver.sql:803` | Bad `ownership_pct` (≤0 or >1) — filtered in MERGE, never quarantined |

### 5. Transaction proc missing FK checks for portfolio and entity
`04_silver.sql:1216-1252` — `usp_conform_transaction` quarantines missing securities but does **not** quarantine transactions referencing nonexistent portfolios or entities. Additionally, if `fn_translate_key` returns NULL for portfolio/entity keys (bad prefix), the row passes the security check but will hit a NOT NULL constraint violation at INSERT time, failing the entire batch.

---

## P1 — Data Integrity / Correctness

### 6. Gold dimensions break the star schema surrogate key pattern
- `gold.portfolio_group_dimension` (`01_ddl.sql:811`) stores `investment_team_enterprise_key NVARCHAR(100)` instead of `investment_team_key INT` FK
- `gold.security_dimension` (`01_ddl.sql:901-903`) stores three enterprise keys (`investment_team_enterprise_key`, `entity_enterprise_key`, `asset_enterprise_key`) instead of surrogate INT FKs

This forces NVARCHAR(100) joins in analytical views (e.g., `vw_position_detail` at `06_gold.sql:875-876`) instead of INT joins, prevents FK constraint enforcement, and breaks the pattern where `gold.portfolio_dimension` correctly uses `portfolio_group_key INT` with a proper FK.

### 7. Silver layer has zero foreign keys
No silver table references any other silver table. Orphaned enterprise keys can persist indefinitely. The quality rules in the stored procs catch *some* FK violations (e.g., transaction→security), but miss many others (see P0 #5 above).

### 8. CHARINDEX type validation is substring-based (false positive risk)
`04_silver.sql:986` — `WHERE CHARINDEX(s.security_type, @valid_types) = 0` searches for the type as a substring within the comma-delimited list. Adding a type like `DEBT` would falsely match `SENIOR_DEBT`. Fix: `CHARINDEX(',' + s.security_type + ',', ',' + @valid_types + ',')`.

### 9. All 11 silver MERGE procs misreport audit counts
`04_silver.sql` (lines 158, 261, 344, 434, 529, 611, 726, 828, 923, 1152, 1291) — Every proc sets `@ins = @@ROWCOUNT` after MERGE, which captures inserts + updates combined, then reports it as `@rows_inserted` with `@rows_updated = 0`. The audit log overstates inserts and understates updates. An `OUTPUT $action` clause is needed for accurate counts.

### 10. Hash exclusions cause missed updates on multiple dimensions
Several procs exclude business columns from `_row_hash`, meaning changes to those columns will never trigger an update:

| Proc | File:Line | Excluded columns |
|---|---|---|
| `usp_conform_asset` | `04_silver.sql:478-482` | `asset_short_name`, `asset_legal_name`, `acquisition_date`, `last_valuation_date`, `last_valuation_amount`, `last_valuation_currency` (6 columns) |
| `usp_conform_entity` | `04_silver.sql:383-388` | `incorporation_date` |
| `usp_conform_portfolio_group` | `04_silver.sql:200-207` | `committed_capital_currency`, `portfolio_group_description` |
| `usp_conform_transaction` | `04_silver.sql:1204-1208` | Hashes raw bronze strings instead of conformed values (e.g., `'equity'` vs `'EQUITY'` produces different hashes) |

The asset exclusion is the most severe — valuation amounts and dates change frequently and would be silently ignored.

### 11. Documentation claims 4 meta tables that don't exist
CLAUDE.md lists `lineage_catalog`, `tag_registry`, `tag_assignments`, `change_log` as part of the 14 governance tables. These are not in the DDL. They were replaced by `ingestion_pipeline_steps`, `extraction_filters`, `extraction_filter_decisions`, `pipeline_execution_log` — which serve different purposes. The governance framework lacks lineage and tagging capabilities that the documentation promises.

### 12. Missing UNIQUE constraints on natural keys
No UNIQUE constraint on: `meta.source_systems.system_code`, `meta.ingestion_pipelines.pipeline_code`, `meta.quality_rules.rule_code`, `meta.consumers.consumer_name`, `meta.business_glossary.business_term`, `gold.position_transactions_fact.source_system_transaction_id`. Duplicate natural keys can be inserted, and MERGE statements silently rely on implicit uniqueness.

### 13. `fn_translate_key` does not verify prefix before stripping
`02_meta_programmability.sql:70` — The function blindly strips `LEN(@strip_prefix)` characters and prepends `@add_prefix`. If the source key does not start with the expected prefix, it produces a garbage enterprise key. No `LEFT(@source_key, LEN(@strip_prefix)) = @strip_prefix` guard exists.

### 14. Key crosswalk ambiguity — `key_name` resolved without source system context
`02_meta_programmability.sql:525-526` — `usp_upsert_key_crosswalk` resolves `key_name` to `key_id` without specifying `source_system_id`. If two source systems have keys with the same name, it silently picks an arbitrary match.

### 15. Append-only fact table cannot handle transaction corrections
`06_gold.sql:545` — `usp_load_position_transactions_fact` uses `INSERT WHERE NOT EXISTS` on `source_system_transaction_id`. If a silver transaction is corrected (same ID, different amounts), the gold fact retains the original values with no update path.

### 16. Analytical views use INNER JOINs that silently drop data
`06_gold.sql:870-876` — `vw_position_detail` uses all INNER JOINs. Missing dimension records cause position_fact rows to silently disappear from the view rather than appearing with NULL dimension attributes.

---

## P2 — Design Gaps / Missed Capabilities

### 17. No SCD Type 2 support on any dimension
All gold dimensions use Type 1 (overwrite) via MERGE. No `effective_from`/`effective_to`/`is_current` columns exist. Historical state changes (entity status, security attributes, fund status) are lost on every update.

### 18. No date dimension
Star schemas conventionally include a `date_dimension` for fiscal year, quarter, business day flags, etc. Fact tables join on `as_of_date DATE` directly, limiting analytical flexibility.

### 19. No WSO pricing in gold layer
`silver.ws_online_pricing` has daily market prices but no corresponding gold table or view. Consumers needing mark-to-market or pricing analytics have no gold-layer access.

### 20. No currency/FX rate reference table
The system tracks multiple currencies (`committed_capital_currency`, `last_valuation_currency`, transaction amounts in local/USD) but has no currency reference table or historical FX rate table for validation or conversion.

### 21. No orchestrator-level error handling
`04_silver.sql:1314-1393`, `06_gold.sql:713-743`, `06_gold.sql:749-788` — No orchestrator (`usp_run_all_silver`, `usp_run_all_gold`, `usp_run_full_pipeline`) has TRY/CATCH. A failure in any sub-proc skips all remaining phases with no orchestrator-level audit record. Independent phases (e.g., enterprise vs entity in silver Phase 1) could run in isolation but are executed serially with no fault isolation.

### 22. No mechanism for soft-deletes in gold dimensions
No dimension MERGE uses `WHEN NOT MATCHED BY SOURCE`. Records removed from silver persist in gold dimensions indefinitely.

### 23. Missing `_row_hash` on 3 silver tables
`silver.portfolio_entity_ownership`, `silver.entity_asset_ownership`, `silver.ws_online_pricing` — These tables have no `_row_hash` column, breaking the change detection pattern used by all other silver tables. The pricing proc (`04_silver.sql:906`) uses unconditional UPDATE on every match as a result.

### 24. `silver.quarantine` unused utility proc
`04_silver.sql:37-49` — `silver.usp_quarantine_row` is defined but never called. All 11 procs INSERT directly into `silver.quarantine`. If the quarantine schema changes, all 11 procs need updating instead of just one.

### 25. Audit orphan detection — no mechanism for stale RUNNING status
`03_audit.sql` — If an ETL run crashes without calling `usp_complete_etl_run`, the run stays in RUNNING status forever. No proc detects or cleans up orphaned runs.

### 26. vw_recent_runs is misnamed and incomplete
`03_audit.sql:71-88` — The view returns ALL runs (no temporal filter) despite the name. It also omits the `rows_deleted` column that the underlying table captures.

### 27. Deploy script leaks credentials in process list
`scripts/deploy.sh:41` — Password is passed via `-P "$PASS"` on the command line, which is visible in `ps` output on Linux. Should use environment variable or `-G` (Windows Auth) where possible.

---

## P3 — Naming / Convention / Minor Issues

### 28. Inconsistent audit column naming across layers
- Meta/bronze/silver: `created_at`, `updated_at`
- Gold: `created_date`, `modified_date`
- Audit: `created_at`

### 29. `is_active` vs `is_enabled`
`meta.extraction_filters` uses `is_enabled`; all other tables use `is_active`.

### 30. Inconsistent `is_active` filtering in meta CRUD procs
Some procs check `AND is_active = 1` when resolving FK lookups, others don't. Pattern is inconsistent across `02_meta_programmability.sql` (see procs at lines 228, 302, 377, 454, 525, 880).

### 31. `silver.[transaction]` reserved word
Named with a SQL Server reserved word, requiring brackets everywhere. Renaming to `silver.position_transaction` would eliminate maintenance burden.

### 32. `_bronze_record_id` type mismatch
Bronze PKs are `UNIQUEIDENTIFIER`. Silver stores `_bronze_record_id` as `NVARCHAR(200)` — wastes storage and prevents FK enforcement. Should be `UNIQUEIDENTIFIER` or `NVARCHAR(36)`.

### 33. `DECIMAL(5,4)` for ownership percentages
Maximum value `9.9999` with CHECK `<= 1.0`. Works but allows only 4 decimal places. `DECIMAL(7,6)` would better represent complex fractional interests (e.g., 1/3 = 0.333333).

### 34. Deactivate procs silently succeed on non-existent targets
All `usp_deactivate_*` procs update zero rows without warning when the target doesn't exist, unlike the upsert procs which raise errors on failed FK lookups.

### 35. No TRY/CATCH in any meta CRUD proc
All 29 procs in `02_meta_programmability.sql` lack error handling. Constraint violations, deadlocks, and data truncation propagate as raw SQL Server errors.

### 36. DDL is not idempotent
No `IF NOT EXISTS` guards on CREATE TABLE statements. Re-running `01_ddl.sql` fails on every table. Only the schema creation has idempotent guards.

---

## Seed Data Issues

### 37. Transaction generator references nonexistent ownership path
`05_seed_data.sql:502` — Combo `(STM-P-30005, STM-E-20003, STM-SEC-50004)` generates transactions for a portfolio→entity pair with no ownership bridge entry.

### 38. Crosswalk path IDs reference wrong crosswalk entries
`05_seed_data.sql:205-206` — `path_crosswalk_ids` values `'[8,14]'` and `'[12]'` reference IDENTITY-generated IDs that don't correspond to the actual crosswalk entries they claim to map.

### 39. Key crosswalks incomplete for security foreign references
Security seed data uses `SSM-IT-`, `SSM-E-`, `SSM-A-` prefixes for team/entity/asset references, but no `key_registry` or `key_crosswalk` entries document these translations. The procs hardcode them, making the governance metadata incomplete.

### 40. No edge case test data
Missing from seed data: expired bridges (`end_date` non-null), boundary ownership percentages (0.0001, exactly 1.0), duplicate bronze records (to test dedup), bad ownership_pct values (to test quarantine), WSO pricing for all securities.

---

## Opportunities

### Immediate (before Databricks migration)
1. **Add nonclustered indexes** on FK columns, filter columns, and fact date columns — the single highest-impact change
2. **Fix the position_fact rebuild** to wrap DELETE + INSERT in explicit BEGIN TRAN / ROLLBACK
3. **Add quarantine checks** to the 6 procs that silently drop rows
4. **Fix hash column exclusions** so valuation amounts, dates, and other business columns trigger updates
5. **Add UNIQUE constraints** on natural keys in meta tables and gold fact source IDs
6. **Fix fn_translate_key** to validate prefix before stripping

### Structural (for Databricks migration)
7. **Convert gold dimension parent references** from enterprise keys to surrogate keys (portfolio_group→team, security→team/entity/asset)
8. **Add a date dimension** for fiscal/calendar analytics
9. **Implement SCD Type 2** on entity_dimension and security_dimension at minimum
10. **Add gold pricing fact** from silver.ws_online_pricing
11. **Add orchestrator error handling** with TRY/CATCH, phase-level audit logging, and continue-on-error for independent phases
12. **Use OUTPUT $action** on MERGE statements for accurate insert/update/delete audit counts

### Governance completion
13. **Build lineage_catalog, tag_registry, tag_assignments** tables and CRUD procs (or update documentation to reflect actual scope)
14. **Add quarantine review workflow** — the quarantine table exists but there is no tooling to review, approve, or re-process quarantined rows
15. **Add orphaned run detection** — scheduled check for RUNNING status older than N minutes
