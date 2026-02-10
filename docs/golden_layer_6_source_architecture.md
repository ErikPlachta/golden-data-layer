# Golden Data Layer: Reorganized Architecture Plan (6-Source-System Model)

**Date:** 2026-02-08
**Supersedes:** Original 3-source-system model
**Reference:** pe_real_assets_data_hierarchy_research.md

---

## 1. Source System Registry

| ID | Code | Name | Owns | Connectivity |
|---|---|---|---|---|
| 1 | `SRC_ENTERPRISE` | Enterprise Data | investment_team, portfolio_group, portfolio | JDBC (linked server) |
| 2 | `SRC_ENTITY_MGMT` | Source Entity Management | entity, entity→asset ownership % | REST API |
| 3 | `SRC_ASSET_MGMT` | Source Asset Management | asset master data, types, valuations | REST API |
| 4 | `SRC_SECURITY_MGMT` | Source Security Management | security master (composite) | REST API + internal assembly |
| 5 | `SRC_TXN_MGMT` | Source Transaction Management | transactions (daily, by security_id) | JDBC |
| 6 | `SRC_WS_ONLINE` | Source Wall Street Online | public market security data, pricing | REST API |

### Data Flow

```
enterprise_data ──→ bronze.src_enterprise_raw ──→ silver.enterprise_conformed
                    ──→ gold.investment_team_dimension
                    ──→ gold.portfolio_group_dimension
                    ──→ gold.portfolio_dimension

Source_Entity_Mgmt ──→ bronze.src_entity_mgmt_raw ──→ silver.entity_mgmt_conformed
                       ──→ gold.entity_dimension
                       ──→ gold.portfolio_entity_bridge  (NEW: ownership %)

Source_Asset_Mgmt ──→ bronze.src_asset_mgmt_raw ──→ silver.asset_mgmt_conformed
                      ──→ gold.asset_dimension

Source_Security_Mgmt ──→ bronze.src_security_mgmt_raw ──→ silver.security_mgmt_conformed
                         ──→ gold.security_dimension
                         (consumes silver.ws_online_conformed as input)

Source_Txn_Mgmt ──→ bronze.src_txn_mgmt_raw ──→ silver.txn_mgmt_conformed
                    ──→ gold.position_transactions_fact
                    ──→ gold.position_fact (summarized)
                    ──→ gold.position_team_bridge

Source_WS_Online ──→ bronze.src_ws_online_raw ──→ silver.ws_online_conformed
                     (feeds into Source_Security_Mgmt as external reference)
```

---

## 2. Revised Gold Dimensional Model

### Dimensions

| Table | Source System | Key Column | Notes |
|---|---|---|---|
| `investment_team_dimension` | enterprise_data | investment_team_enterprise_key | GP/management company |
| `portfolio_group_dimension` | enterprise_data | portfolio_group_enterprise_key | Fund (vintage, strategy, capital) |
| `portfolio_dimension` | enterprise_data | portfolio_enterprise_key | FK → portfolio_group |
| `entity_dimension` | Source_Entity_Mgmt | entity_enterprise_key | Portfolio company / legal entity |
| `asset_dimension` | Source_Asset_Mgmt | asset_enterprise_key | Physical/financial asset |
| `security_dimension` | Source_Security_Mgmt | security_enterprise_key | Tradeable instrument (equity, debt, derivative) |

### Bridge Tables

| Table | Resolves | Source | Key Columns |
|---|---|---|---|
| `portfolio_entity_bridge` | **NEW** — Portfolio ↔ Entity M:N with ownership % | Source_Entity_Mgmt | portfolio_key, entity_key, ownership_pct |
| `entity_asset_bridge` | Entity ↔ Asset 1:N with ownership % | Source_Entity_Mgmt | entity_key, asset_key, ownership_pct |
| `position_team_bridge` | Position ↔ Investment Team M:N with allocation % | Derived (gold) | position_fact_key, investment_team_key, allocation_pct |

### Fact Tables

| Table | Grain | Source |
|---|---|---|
| `position_transactions_fact` | 1 row per transaction (portfolio × entity × security × date × txn_id) | Source_Txn_Mgmt |
| `position_fact` | 1 row per position summary (portfolio × entity × security × date) | Derived from position_transactions_fact |

### Relationship Map

```
investment_team_dimension
    └──1:N──→ portfolio_group_dimension (FK: investment_team_enterprise_key)
                  └──1:N──→ portfolio_dimension (FK: portfolio_group_key)
                                └──M:N──→ entity_dimension (via portfolio_entity_bridge + ownership_pct)
                                              └──1:N──→ asset_dimension (via entity_asset_bridge + ownership_pct)
                                                            └──1:N──→ security_dimension (FK: asset_enterprise_key)
                                              └──N:1──← security_dimension (FK: entity_enterprise_key)

position_transactions_fact ──FK──→ portfolio_dimension, entity_dimension, security_dimension
position_fact              ──FK──→ portfolio_dimension, entity_dimension, security_dimension
position_team_bridge       ──FK──→ position_fact, investment_team_dimension
```

---

## 3. New Table DDL (Changes Only)

### 3.1 portfolio_entity_bridge (NEW)

```sql
-- Resolves M:N between portfolio and entity with fractional ownership
-- A portfolio may own 60% of Entity A and 40% of Entity B
CREATE TABLE gold.portfolio_entity_bridge (
    portfolio_key       INT             NOT NULL,
    entity_key          INT             NOT NULL,
    ownership_pct       DECIMAL(5,4)    NOT NULL,  -- 0.0001 to 1.0000
    effective_date      DATE            NOT NULL,
    end_date            DATE            NULL,       -- NULL = current
    source_system_key   INT             NOT NULL,
    created_date        DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by          NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date       DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by         NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_portfolio_entity PRIMARY KEY (portfolio_key, entity_key, effective_date),
    CONSTRAINT fk_pe_portfolio FOREIGN KEY (portfolio_key)
        REFERENCES gold.portfolio_dimension (portfolio_key),
    CONSTRAINT fk_pe_entity FOREIGN KEY (entity_key)
        REFERENCES gold.entity_dimension (entity_key),
    CONSTRAINT ck_pe_ownership CHECK (ownership_pct > 0 AND ownership_pct <= 1.0)
);
```

### 3.2 entity_asset_bridge (NEW)

```sql
-- Resolves 1:N between entity and asset with fractional ownership
-- Entity A owns 100% of Asset X, 50% of Asset Y
CREATE TABLE gold.entity_asset_bridge (
    entity_key          INT             NOT NULL,
    asset_key           INT             NOT NULL,
    ownership_pct       DECIMAL(5,4)    NOT NULL,
    effective_date      DATE            NOT NULL,
    end_date            DATE            NULL,
    source_system_key   INT             NOT NULL,
    created_date        DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by          NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date       DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by         NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_entity_asset PRIMARY KEY (entity_key, asset_key, effective_date),
    CONSTRAINT fk_ea_entity FOREIGN KEY (entity_key)
        REFERENCES gold.entity_dimension (entity_key),
    CONSTRAINT fk_ea_asset FOREIGN KEY (asset_key)
        REFERENCES gold.asset_dimension (asset_key),
    CONSTRAINT ck_ea_ownership CHECK (ownership_pct > 0 AND ownership_pct <= 1.0)
);
```

### 3.3 asset_dimension (UPDATED — add entity FK)

```sql
-- asset_dimension already exists; entity linkage now handled via entity_asset_bridge
-- No structural change needed to asset_dimension itself
-- The bridge table replaces any direct FK from asset → entity
```

### 3.4 security_dimension (UPDATED — add asset FK, BankLoanID, CUSIP, ISIN)

```sql
-- Add public market identifiers to security_dimension
-- These come from Source_Security_Management (composite of internal + WSO)
ALTER TABLE gold.security_dimension ADD
    bank_loan_id        NVARCHAR(50)    NULL,
    cusip               NVARCHAR(9)     NULL,
    isin                NVARCHAR(12)    NULL,
    ticker              NVARCHAR(20)    NULL,
    security_name       NVARCHAR(500)   NULL,
    security_status     NVARCHAR(50)    NOT NULL DEFAULT 'ACTIVE';
```

---

## 4. Revised Pipeline Definitions

| # | Pipeline | Source | Schedule | Target Bronze | Target Silver | Target Gold |
|---|---|---|---|---|---|---|
| 1 | `PL_ENTERPRISE_DAILY` | enterprise_data | 0 1 * * * | bronze.src_enterprise_raw | silver.enterprise_conformed | investment_team_dim, portfolio_group_dim, portfolio_dim |
| 2 | `PL_ENTITY_DAILY` | Source_Entity_Mgmt | 0 2 * * * | bronze.src_entity_mgmt_raw | silver.entity_mgmt_conformed | entity_dim, portfolio_entity_bridge, entity_asset_bridge |
| 3 | `PL_ASSET_DAILY` | Source_Asset_Mgmt | 0 2 * * * | bronze.src_asset_mgmt_raw | silver.asset_mgmt_conformed | asset_dim |
| 4 | `PL_MARKET_DAILY` | Source_WS_Online | 0 3 * * * | bronze.src_ws_online_raw | silver.ws_online_conformed | (feeds PL_SECURITY) |
| 5 | `PL_SECURITY_DAILY` | Source_Security_Mgmt | 0 4 * * * | bronze.src_security_mgmt_raw | silver.security_mgmt_conformed | security_dim |
| 6 | `PL_TXN_DAILY` | Source_Txn_Mgmt | 0 5 * * * | bronze.src_txn_mgmt_raw | silver.txn_mgmt_conformed | position_transactions_fact |
| 7 | `PL_POSITION_SUMMARY` | (derived) | after PL_TXN | — | — | position_fact, position_team_bridge |

### Pipeline DAG (dependency order)

```
PL_ENTERPRISE_DAILY  ─────────┐
PL_ENTITY_DAILY      ─────────┤
PL_ASSET_DAILY       ─────────┤
PL_MARKET_DAILY ──→ PL_SECURITY_DAILY ──┤
                                         ├──→ PL_TXN_DAILY ──→ PL_POSITION_SUMMARY
```

**Key dependency:** PL_SECURITY_DAILY must wait for PL_MARKET_DAILY (WSO data feeds security master). PL_TXN_DAILY must wait for all dimension pipelines to complete so FK lookups resolve.

---

## 5. Revised Key Registry

### Enterprise Keys (internal canonical identifiers)

| key_id | key_name | source_system | notes |
|---|---|---|---|
| 1 | investment_team_enterprise_key | enterprise_data | NEW source assignment |
| 2 | portfolio_group_enterprise_key | enterprise_data | NEW source assignment |
| 3 | portfolio_enterprise_key | enterprise_data | NEW source assignment |
| 4 | entity_enterprise_key | Source_Entity_Mgmt | unchanged |
| 5 | asset_enterprise_key | Source_Asset_Mgmt | NEW source assignment |
| 6 | security_enterprise_key | Source_Security_Mgmt | NEW source assignment |

### Source-Native Keys

| key_id | key_name | source_system | type |
|---|---|---|---|
| 7 | ent_investment_team_id | enterprise_data | PRIMARY |
| 8 | ent_portfolio_group_id | enterprise_data | PRIMARY |
| 9 | ent_portfolio_id | enterprise_data | PRIMARY |
| 10 | sem_entity_id | Source_Entity_Mgmt | PRIMARY |
| 11 | sem_entity_asset_id | Source_Entity_Mgmt | PRIMARY (bridge composite) |
| 12 | sam_asset_id | Source_Asset_Mgmt | PRIMARY |
| 13 | ssm_security_id | Source_Security_Mgmt | PRIMARY |
| 14 | stm_transaction_id | Source_Txn_Mgmt | PRIMARY |
| 15 | stm_portfolio_id | Source_Txn_Mgmt | FOREIGN |
| 16 | stm_entity_id | Source_Txn_Mgmt | FOREIGN |
| 17 | stm_security_id | Source_Txn_Mgmt | FOREIGN |
| 18 | wso_security_id | Source_WS_Online | PRIMARY |
| 19 | wso_ticker | Source_WS_Online | NATURAL |
| 20 | wso_cusip | Source_WS_Online | NATURAL |
| 21 | wso_bank_loan_id | Source_WS_Online | NATURAL (BankLoanID) |

### Key Crosswalks (direct mappings)

| From | To | Type | Notes |
|---|---|---|---|
| ent_investment_team_id → investment_team_ek | 1:1 | EXACT | enterprise_data owns canonical |
| ent_portfolio_group_id → portfolio_group_ek | 1:1 | EXACT | |
| ent_portfolio_id → portfolio_ek | 1:1 | EXACT | |
| sem_entity_id → entity_ek | 1:1 | EXACT | SEM owns canonical |
| sam_asset_id → asset_ek | 1:1 | EXACT | SAM owns canonical |
| ssm_security_id → security_ek | 1:1 | EXACT | SSM owns canonical |
| stm_portfolio_id → portfolio_ek | 1:1 | EXACT | FK resolution |
| stm_entity_id → entity_ek | 1:1 | EXACT | FK resolution |
| stm_security_id → security_ek | 1:1 | EXACT | FK resolution |
| wso_security_id → security_ek | 1:1 | EXACT | External → internal mapping |
| wso_bank_loan_id → security_ek | N:1 | EXACT | Multiple WSO records → 1 security |

---

## 6. Quality Rules (New/Changed)

| Rule | Table | Expression | Severity |
|---|---|---|---|
| `BRIDGE_PE_OWNERSHIP_SUM` | portfolio_entity_bridge | SUM(ownership_pct) per portfolio_key ≤ 1.0 | EXPECT_OR_FAIL |
| `BRIDGE_EA_OWNERSHIP_SUM` | entity_asset_bridge | SUM(ownership_pct) per entity_key ≤ 1.0 | EXPECT_OR_FAIL |
| `SECURITY_HAS_ENTITY` | security_dimension | entity_enterprise_key IS NOT NULL | EXPECT_OR_FAIL |
| `SECURITY_HAS_ASSET` | security_dimension | asset_enterprise_key IS NOT NULL | EXPECT_OR_FAIL |
| `BRIDGE_PE_DATE_VALID` | portfolio_entity_bridge | end_date IS NULL OR end_date > effective_date | EXPECT_OR_FAIL |

**Note:** Ownership bridges use `≤ 1.0` not `= 1.0` because a portfolio may not own 100% of an entity — other portfolios (or external parties) may hold the remainder. The `= 1.0` rule applies only to position_team_bridge allocations.

---

## 7. Open Items (Updated)

1. **Table maintenance strategy** — Which meta tables require manual stewardship vs. auto-population? (Carried forward)
2. **Delta processing strategies** — Incremental extraction patterns to formalize. (Carried forward)
3. **Extraction filter enforcement** — Bronze filtering at extraction. (Carried forward)
4. **enterprise_data source definition** — Need to confirm connectivity method (JDBC linked server vs. direct), schema mapping, and what enterprise_data actually contains. This is the GP/Fund structure source — may be an ERP, fund accounting system, or internal reference database.
5. **Source_Asset_Management definition** — New system. Need to define: what constitutes asset master data, what valuation data it holds, refresh frequency, and whether it manages asset lifecycle events (acquisitions, dispositions, revaluations).
6. **Source_Security_Management composite assembly** — How does SSM reconcile internal security records with WSO external data? Need to define: merge strategy, conflict resolution rules, and which system wins on field-level disagreements.
7. **Ownership bridge temporal handling** — portfolio_entity_bridge and entity_asset_bridge have effective_date/end_date. Need to define: SCD Type 2 handling for ownership changes, point-in-time query patterns, and whether position_fact should join to bridge using as_of_date.
8. **BankLoanID crosswalk placement** — BankLoanID is the primary public-market identifier. Confirm it lives on security_dimension and the crosswalk from wso_bank_loan_id → security_enterprise_key is N:1 (multiple WSO records may map to same internal security).

---

## 8. Migration Notes (from 3-system to 6-system)

### SQL Test File Changes Required

1. **source_systems INSERT** — Add rows for IDs 4 (enterprise_data), 5 (Source_Asset_Mgmt), 6 (Source_Security_Mgmt). Renumber existing: SRC_ENTITY_MGMT=2, SRC_ASSET_MGMT=3, SRC_SECURITY_MGMT=4, SRC_TXN_MGMT=5, SRC_WS_ONLINE=6. Or keep existing IDs and add new ones at 4,5,6.
2. **ingestion_pipelines** — Add pipelines for enterprise_data, asset_mgmt, security_mgmt. Update pipeline steps.
3. **key_registry** — Reassign enterprise key ownership (keys 1-3 from SEM to enterprise_data). Add new source-native keys for new systems.
4. **key_crosswalk** — Add new crosswalk entries for enterprise_data and asset_mgmt keys.
5. **data_contracts** — Add contracts for 3 new source systems.
6. **extraction_filters** — Add filter rows for 3 new source systems.
7. **Gold DDL** — Add portfolio_entity_bridge, entity_asset_bridge tables. Add columns to security_dimension.
8. **Mock data** — Generate test data for new source systems and bridge tables.
