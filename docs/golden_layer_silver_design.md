# Golden Data Layer: Silver Layer Design

**Date:** 2026-02-08
**Depends on:** golden_layer_6_source_architecture.md, golden_layer_meta_framework.md
**Layer role:** Validated, conformed, enterprise-keyed data. Zero trust from bronze → verified trust at silver.

---

## 1. Silver Layer Principles

**What silver does:**
- Type cast from raw STRING/VARIANT to proper types
- Deduplicate on business keys (last-write-wins or most-recent-modified)
- Validate against quality rules (nullability, ranges, formats, referential)
- Translate source-native keys → enterprise keys using crosswalk rules
- Add audit columns (_source_system_id, _bronze_record_id, _conformed_at, _conformed_by)
- Quarantine rows that fail quality rules (not drop, not halt — quarantine for review)

**What silver does NOT do:**
- Generate surrogate keys (that's gold)
- Resolve cross-system relationships (that's gold via crosswalks)
- Aggregate or summarize (that's gold)
- Apply business naming conventions (that's gold via business_glossary)

**Table strategy:** Silver splits bronze into **per-entity tables**, not 1:1 with bronze. A single bronze source that contains investment teams, portfolio groups, and portfolios produces 3 silver tables. This enables independent validation, dedup, and quality checks per entity.

---

## 2. Table Inventory

| # | Silver Table | Source Bronze | Feeds Gold | Enterprise Key |
|---|---|---|---|---|
| 1 | silver.investment_team | bronze.src_enterprise_raw | gold.investment_team_dimension | investment_team_enterprise_key |
| 2 | silver.portfolio_group | bronze.src_enterprise_raw | gold.portfolio_group_dimension | portfolio_group_enterprise_key |
| 3 | silver.portfolio | bronze.src_enterprise_raw | gold.portfolio_dimension | portfolio_enterprise_key |
| 4 | silver.entity | bronze.src_entity_mgmt_raw | gold.entity_dimension | entity_enterprise_key |
| 5 | silver.portfolio_entity_ownership | bronze.src_entity_mgmt_raw | gold.portfolio_entity_bridge | portfolio_enterprise_key + entity_enterprise_key |
| 6 | silver.entity_asset_ownership | bronze.src_entity_mgmt_raw | gold.entity_asset_bridge | entity_enterprise_key + asset_enterprise_key |
| 7 | silver.asset | bronze.src_asset_mgmt_raw | gold.asset_dimension | asset_enterprise_key |
| 8 | silver.security | bronze.src_security_mgmt_raw + silver.ws_online_security | gold.security_dimension | security_enterprise_key |
| 9 | silver.transaction | bronze.src_txn_mgmt_raw | gold.position_transactions_fact | stm_transaction_id (natural key; enterprise keys on FKs) |
| 10 | silver.ws_online_security | bronze.src_ws_online_raw | silver.security (as input) | wso_security_id |
| 11 | silver.ws_online_pricing | bronze.src_ws_online_raw | (future: pricing fact or security enrichment) | wso_security_id + price_date |

**11 silver tables from 6 bronze sources.**

---

## 3. DDL (SQL Server Test Environment)

### 3.1 silver.investment_team

```sql
CREATE TABLE silver.investment_team (
    -- Enterprise key (translated from source-native)
    investment_team_enterprise_key   NVARCHAR(100)   NOT NULL,

    -- Business columns
    investment_team_name             NVARCHAR(500)   NOT NULL,
    investment_team_short_name       NVARCHAR(100)   NULL,
    start_date                       DATE            NOT NULL,
    stop_date                        DATE            NULL,

    -- Source-native key (preserved for lineage)
    src_investment_team_id           NVARCHAR(50)    NOT NULL,

    -- Audit
    _source_system_id                INT             NOT NULL DEFAULT 1,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_inv_team PRIMARY KEY (investment_team_enterprise_key)
);
```

### 3.2 silver.portfolio_group

```sql
CREATE TABLE silver.portfolio_group (
    portfolio_group_enterprise_key   NVARCHAR(100)   NOT NULL,

    portfolio_group_name             NVARCHAR(500)   NOT NULL,
    portfolio_group_short_name       NVARCHAR(100)   NULL,
    portfolio_group_description      NVARCHAR(MAX)   NULL,
    investment_team_enterprise_key   NVARCHAR(100)   NOT NULL,  -- FK resolved at silver

    -- Fund-specific attributes (from industry research)
    vintage_year                     INT             NULL,
    strategy                         NVARCHAR(200)   NULL,
    committed_capital                DECIMAL(18,2)   NULL,
    committed_capital_currency       NVARCHAR(3)     NULL,
    fund_status                      NVARCHAR(50)    NULL,       -- FUNDRAISING, INVESTING, HARVESTING, CLOSED

    src_portfolio_group_id           NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 1,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_pg PRIMARY KEY (portfolio_group_enterprise_key)
);
```

### 3.3 silver.portfolio

```sql
CREATE TABLE silver.portfolio (
    portfolio_enterprise_key         NVARCHAR(100)   NOT NULL,

    portfolio_name                   NVARCHAR(500)   NOT NULL,
    portfolio_short_name             NVARCHAR(100)   NULL,
    portfolio_group_enterprise_key   NVARCHAR(100)   NOT NULL,  -- FK to silver.portfolio_group

    src_portfolio_id                 NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 1,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_portfolio PRIMARY KEY (portfolio_enterprise_key)
);
```

### 3.4 silver.entity

```sql
CREATE TABLE silver.entity (
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,

    entity_name                      NVARCHAR(500)   NOT NULL,
    entity_short_name                NVARCHAR(100)   NULL,
    entity_legal_name                NVARCHAR(500)   NULL,
    entity_type                      NVARCHAR(100)   NULL,       -- LLC, LP, SPV, Corp, etc.
    entity_status                    NVARCHAR(50)    NULL,       -- ACTIVE, EXITED, DISSOLVED
    incorporation_jurisdiction       NVARCHAR(200)   NULL,
    incorporation_date               DATE            NULL,

    src_entity_id                    NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 2,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_entity PRIMARY KEY (entity_enterprise_key)
);
```

### 3.5 silver.portfolio_entity_ownership

```sql
-- Ownership relationship: which portfolios own what % of which entities
-- Source: entity management system tracks these relationships
CREATE TABLE silver.portfolio_entity_ownership (
    portfolio_enterprise_key         NVARCHAR(100)   NOT NULL,
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    ownership_pct                    DECIMAL(5,4)    NOT NULL,
    effective_date                   DATE            NOT NULL,
    end_date                         DATE            NULL,

    src_ownership_id                 NVARCHAR(50)    NULL,       -- source native ID if exists
    _source_system_id                INT             NOT NULL DEFAULT 2,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,

    CONSTRAINT pk_silver_pe_own PRIMARY KEY (portfolio_enterprise_key, entity_enterprise_key, effective_date),
    CONSTRAINT ck_silver_pe_pct CHECK (ownership_pct > 0 AND ownership_pct <= 1.0)
);
```

### 3.6 silver.entity_asset_ownership

```sql
-- Ownership relationship: which entities own what % of which assets
-- Source: entity management system (entity-asset coupling per industry research)
CREATE TABLE silver.entity_asset_ownership (
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    asset_enterprise_key             NVARCHAR(100)   NOT NULL,
    ownership_pct                    DECIMAL(5,4)    NOT NULL,
    effective_date                   DATE            NOT NULL,
    end_date                         DATE            NULL,

    src_ownership_id                 NVARCHAR(50)    NULL,
    _source_system_id                INT             NOT NULL DEFAULT 2,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,

    CONSTRAINT pk_silver_ea_own PRIMARY KEY (entity_enterprise_key, asset_enterprise_key, effective_date),
    CONSTRAINT ck_silver_ea_pct CHECK (ownership_pct > 0 AND ownership_pct <= 1.0)
);
```

### 3.7 silver.asset

```sql
CREATE TABLE silver.asset (
    asset_enterprise_key             NVARCHAR(100)   NOT NULL,

    asset_name                       NVARCHAR(500)   NOT NULL,
    asset_short_name                 NVARCHAR(100)   NULL,
    asset_legal_name                 NVARCHAR(500)   NULL,
    asset_type                       NVARCHAR(100)   NOT NULL,   -- Real Estate, Infrastructure, Equipment, IP, etc.
    asset_subtype                    NVARCHAR(100)   NULL,       -- Office, Warehouse, Wind Farm, etc.
    asset_status                     NVARCHAR(50)    NULL,       -- ACTIVE, DISPOSED, UNDER_CONSTRUCTION
    location_country                 NVARCHAR(100)   NULL,
    location_region                  NVARCHAR(200)   NULL,
    acquisition_date                 DATE            NULL,
    last_valuation_date              DATE            NULL,
    last_valuation_amount            DECIMAL(18,2)   NULL,
    last_valuation_currency          NVARCHAR(3)     NULL,

    src_asset_id                     NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 3,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_asset PRIMARY KEY (asset_enterprise_key)
);
```

### 3.8 silver.security (composite)

```sql
-- Composite security master: assembled from Source_Security_Mgmt internal records
-- enriched with silver.ws_online_security for public market identifiers
CREATE TABLE silver.security (
    security_enterprise_key          NVARCHAR(100)   NOT NULL,

    -- Internal attributes (from Source_Security_Mgmt)
    security_type                    NVARCHAR(100)   NOT NULL,   -- EQUITY, SENIOR_DEBT, MEZZANINE, DERIVATIVE
    security_group                   NVARCHAR(100)   NULL,
    security_name                    NVARCHAR(500)   NULL,
    security_status                  NVARCHAR(50)    NOT NULL DEFAULT 'ACTIVE',
    investment_team_enterprise_key   NVARCHAR(100)   NOT NULL,
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    asset_enterprise_key             NVARCHAR(100)   NOT NULL,

    -- Public market identifiers (enriched from WSO via composite assembly)
    bank_loan_id                     NVARCHAR(50)    NULL,
    cusip                            NVARCHAR(9)     NULL,
    isin                             NVARCHAR(12)    NULL,
    ticker                           NVARCHAR(20)    NULL,

    -- Composite assembly metadata
    _wso_match_status                NVARCHAR(20)    NULL,       -- MATCHED, UNMATCHED, AMBIGUOUS
    _wso_match_key                   NVARCHAR(50)    NULL,       -- which WSO key was used to match
    _wso_match_confidence            NVARCHAR(20)    NULL,       -- EXACT, FUZZY, MANUAL

    src_security_id                  NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 4,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_security PRIMARY KEY (security_enterprise_key)
);
```

### 3.9 silver.transaction

```sql
CREATE TABLE silver.transaction (
    -- Transaction natural key (not enterprise key — transactions are source-specific)
    stm_transaction_id               NVARCHAR(50)    NOT NULL,

    -- FK enterprise keys (translated from source-native at silver)
    portfolio_enterprise_key         NVARCHAR(100)   NOT NULL,
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    security_enterprise_key          NVARCHAR(100)   NOT NULL,

    -- Business columns
    as_of_date                       DATE            NOT NULL,
    transaction_type                 NVARCHAR(100)   NOT NULL,
    transaction_category             NVARCHAR(100)   NULL,
    transaction_status               NVARCHAR(50)    NOT NULL,
    transaction_amount_portfolio     DECIMAL(18,4)   NULL,
    transaction_amount_local         DECIMAL(18,4)   NULL,
    transaction_amount_usd           DECIMAL(18,4)   NULL,
    base_fx_rate                     DECIMAL(18,8)   NULL,
    quantity                         DECIMAL(18,6)   NULL,
    order_id                         NVARCHAR(200)   NULL,
    order_date                       DATE            NULL,
    order_status                     NVARCHAR(50)    NULL,

    -- Source-native FK keys (preserved for lineage)
    src_portfolio_id                 NVARCHAR(50)    NOT NULL,
    src_entity_id                    NVARCHAR(50)    NOT NULL,
    src_security_id                  NVARCHAR(50)    NOT NULL,

    _source_system_id                INT             NOT NULL DEFAULT 5,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_txn PRIMARY KEY (stm_transaction_id)
);
```

### 3.10 silver.ws_online_security

```sql
-- WSO security reference data — feeds into silver.security composite assembly
-- NOT a gold target itself. WSO is a reference input, not a dimension source.
CREATE TABLE silver.ws_online_security (
    wso_security_id                  NVARCHAR(50)    NOT NULL,

    security_type                    NVARCHAR(100)   NULL,
    security_name                    NVARCHAR(500)   NULL,
    bank_loan_id                     NVARCHAR(50)    NULL,
    cusip                            NVARCHAR(9)     NULL,
    isin                             NVARCHAR(12)    NULL,
    ticker                           NVARCHAR(20)    NULL,
    exchange                         NVARCHAR(100)   NULL,
    currency                         NVARCHAR(3)     NULL,
    status                           NVARCHAR(50)    NULL,
    last_updated                     DATETIME2       NULL,

    _source_system_id                INT             NOT NULL DEFAULT 6,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,

    CONSTRAINT pk_silver_wso_sec PRIMARY KEY (wso_security_id)
);
```

### 3.11 silver.ws_online_pricing

```sql
-- WSO pricing data — daily prices by security
-- Future: may feed a pricing fact table or security valuation enrichment
CREATE TABLE silver.ws_online_pricing (
    wso_security_id                  NVARCHAR(50)    NOT NULL,
    price_date                       DATE            NOT NULL,

    price_close                      DECIMAL(18,6)   NULL,
    price_open                       DECIMAL(18,6)   NULL,
    price_high                       DECIMAL(18,6)   NULL,
    price_low                        DECIMAL(18,6)   NULL,
    volume                           BIGINT          NULL,
    currency                         NVARCHAR(3)     NULL,

    _source_system_id                INT             NOT NULL DEFAULT 6,
    _bronze_record_id                NVARCHAR(200)   NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,

    CONSTRAINT pk_silver_wso_price PRIMARY KEY (wso_security_id, price_date)
);
```

---

## 4. Transformation Rules (Bronze → Silver)

### 4.1 Pattern: Standard Conformation

Every bronze → silver transform follows this sequence:

```
1. READ        bronze table (incremental: where _ingested_at > last_watermark)
2. PARSE       extract fields from raw payload (JSON path, column mapping)
3. CAST        STRING → proper types (DATE, DECIMAL, INT, etc.)
4. CLEAN       TRIM whitespace, UPPER/LOWER normalization, NULL coalescing
5. TRANSLATE   source-native keys → enterprise keys via crosswalk rules
6. HASH        compute _row_hash = HASHBYTES('SHA2_256', CONCAT_WS('|', col1, col2, ...))
7. DEDUP       ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY _source_modified_at DESC) = 1
8. VALIDATE    apply quality rules; route failures to quarantine table
9. MERGE       into silver target (match on enterprise key or natural key)
```

### 4.2 Key Translation Rules

Key translation happens at step 5. Rules come from `meta.key_crosswalk.transformation_rule`.

| Source System | Source Key | Enterprise Key | Rule |
|---|---|---|---|
| enterprise_data | ent_investment_team_id | investment_team_enterprise_key | `REPLACE(src, 'ENT-IT-', 'IT-')` |
| enterprise_data | ent_portfolio_group_id | portfolio_group_enterprise_key | `REPLACE(src, 'ENT-PG-', 'PG-')` |
| enterprise_data | ent_portfolio_id | portfolio_enterprise_key | `REPLACE(src, 'ENT-P-', 'P-')` |
| Source_Entity_Mgmt | sem_entity_id | entity_enterprise_key | `REPLACE(src, 'SEM-E-', 'E-')` |
| Source_Entity_Mgmt | sem_portfolio_ref_id | portfolio_enterprise_key | `REPLACE(src, 'SEM-P-', 'P-')` (FK ref) |
| Source_Entity_Mgmt | sem_asset_ref_id | asset_enterprise_key | `REPLACE(src, 'SEM-A-', 'A-')` (FK ref) |
| Source_Asset_Mgmt | sam_asset_id | asset_enterprise_key | `REPLACE(src, 'SAM-A-', 'A-')` |
| Source_Security_Mgmt | ssm_security_id | security_enterprise_key | `REPLACE(src, 'SSM-SEC-', 'SEC-')` |
| Source_Security_Mgmt | ssm_entity_ref_id | entity_enterprise_key | `REPLACE(src, 'SSM-E-', 'E-')` (FK ref) |
| Source_Security_Mgmt | ssm_asset_ref_id | asset_enterprise_key | `REPLACE(src, 'SSM-A-', 'A-')` (FK ref) |
| Source_Security_Mgmt | ssm_team_ref_id | investment_team_enterprise_key | `REPLACE(src, 'SSM-IT-', 'IT-')` (FK ref) |
| Source_Txn_Mgmt | stm_portfolio_id | portfolio_enterprise_key | `REPLACE(src, 'STM-P-', 'P-')` (FK ref) |
| Source_Txn_Mgmt | stm_entity_id | entity_enterprise_key | `REPLACE(src, 'STM-E-', 'E-')` (FK ref) |
| Source_Txn_Mgmt | stm_security_id | security_enterprise_key | `REPLACE(src, 'STM-SEC-', 'SEC-')` (FK ref) |

**FK refs** are foreign key references — the source system doesn't own that entity, it just references it. Translation still happens at silver so gold can join on enterprise keys.

### 4.3 Per-Source Transform Specifications

#### Source 1: enterprise_data → 3 silver tables

**bronze.src_enterprise_raw** contains a multi-entity payload (investment teams, portfolio groups, portfolios in one response or separate API endpoints).

```sql
-- Example: silver.investment_team transform (T-SQL pseudocode)
MERGE INTO silver.investment_team AS t
USING (
    SELECT
        -- Key translation
        REPLACE(src.investment_team_id, 'ENT-IT-', 'IT-')   AS investment_team_enterprise_key,
        -- Type casting + cleaning
        TRIM(src.team_name)                                   AS investment_team_name,
        NULLIF(TRIM(src.team_short_name), '')                 AS investment_team_short_name,
        CAST(src.start_date AS DATE)                          AS start_date,
        CAST(NULLIF(src.stop_date, '') AS DATE)               AS stop_date,
        -- Source lineage
        src.investment_team_id                                AS src_investment_team_id,
        src._ingested_at                                      AS _source_modified_at,
        src._record_id                                        AS _bronze_record_id,
        -- Row hash for change detection
        HASHBYTES('SHA2_256', CONCAT_WS('|',
            TRIM(src.team_name),
            NULLIF(TRIM(src.team_short_name), ''),
            CAST(src.start_date AS NVARCHAR),
            CAST(NULLIF(src.stop_date, '') AS NVARCHAR)
        ))                                                    AS _row_hash
    FROM bronze.src_enterprise_raw src
    WHERE src._record_type = 'investment_team'
      AND src._ingested_at > @last_watermark
    -- Dedup: keep most recent per business key
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.investment_team_id
        ORDER BY src._ingested_at DESC
    ) = 1
) AS s
ON t.investment_team_enterprise_key = s.investment_team_enterprise_key
WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
    t.investment_team_name       = s.investment_team_name,
    t.investment_team_short_name = s.investment_team_short_name,
    t.start_date                 = s.start_date,
    t.stop_date                  = s.stop_date,
    t._source_modified_at        = s._source_modified_at,
    t._bronze_record_id          = s._bronze_record_id,
    t._conformed_at              = GETUTCDATE(),
    t._conformed_by              = SYSTEM_USER,
    t._row_hash                  = s._row_hash
WHEN NOT MATCHED THEN INSERT (
    investment_team_enterprise_key, investment_team_name, investment_team_short_name,
    start_date, stop_date, src_investment_team_id,
    _bronze_record_id, _source_modified_at, _row_hash
) VALUES (
    s.investment_team_enterprise_key, s.investment_team_name, s.investment_team_short_name,
    s.start_date, s.stop_date, s.src_investment_team_id,
    s._bronze_record_id, s._source_modified_at, s._row_hash
);
```

**silver.portfolio_group** follows same pattern. Additional fund-specific columns (vintage_year, strategy, committed_capital) are parsed and cast here.

**silver.portfolio** follows same pattern. FK `portfolio_group_enterprise_key` is translated from source-native and validated referentially against silver.portfolio_group.

#### Source 2: Source_Entity_Mgmt → 3 silver tables

**silver.entity**: core entity conformation. Adds entity_type, entity_status, incorporation fields.

**silver.portfolio_entity_ownership**: parsed from SEM ownership records. Each row = one portfolio's ownership stake in one entity at a point in time.

```sql
-- Example: portfolio_entity_ownership transform
-- SEM delivers records like:
--   { "entity_id": "SEM-E-20001", "portfolio_ref": "SEM-P-40001",
--     "ownership_pct": 0.60, "effective_date": "2024-01-15" }
MERGE INTO silver.portfolio_entity_ownership AS t
USING (
    SELECT
        REPLACE(src.portfolio_ref, 'SEM-P-', 'P-')     AS portfolio_enterprise_key,
        REPLACE(src.entity_id, 'SEM-E-', 'E-')         AS entity_enterprise_key,
        CAST(src.ownership_pct AS DECIMAL(5,4))         AS ownership_pct,
        CAST(src.effective_date AS DATE)                 AS effective_date,
        CAST(NULLIF(src.end_date, '') AS DATE)           AS end_date,
        src.ownership_id                                 AS src_ownership_id,
        src._record_id                                   AS _bronze_record_id,
        src._ingested_at                                 AS _source_modified_at
    FROM bronze.src_entity_mgmt_raw src
    WHERE src._record_type = 'portfolio_entity_ownership'
      AND src._ingested_at > @last_watermark
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.portfolio_ref, src.entity_id, src.effective_date
        ORDER BY src._ingested_at DESC
    ) = 1
) AS s
ON  t.portfolio_enterprise_key = s.portfolio_enterprise_key
AND t.entity_enterprise_key    = s.entity_enterprise_key
AND t.effective_date           = s.effective_date
WHEN MATCHED AND t.ownership_pct != s.ownership_pct OR t.end_date != s.end_date THEN UPDATE SET
    t.ownership_pct      = s.ownership_pct,
    t.end_date           = s.end_date,
    t._source_modified_at = s._source_modified_at,
    t._bronze_record_id  = s._bronze_record_id,
    t._conformed_at      = GETUTCDATE(),
    t._conformed_by      = SYSTEM_USER
WHEN NOT MATCHED THEN INSERT (
    portfolio_enterprise_key, entity_enterprise_key, ownership_pct,
    effective_date, end_date, src_ownership_id,
    _bronze_record_id, _source_modified_at
) VALUES (
    s.portfolio_enterprise_key, s.entity_enterprise_key, s.ownership_pct,
    s.effective_date, s.end_date, s.src_ownership_id,
    s._bronze_record_id, s._source_modified_at
);
```

**silver.entity_asset_ownership**: identical pattern. Entity owns assets with %.

#### Source 3: Source_Asset_Mgmt → 1 silver table

**silver.asset**: standard conformation. Asset-specific columns: asset_type, asset_subtype, location, valuation.

#### Source 4: Source_Security_Mgmt → 1 silver table (composite)

**silver.security** is the composite assembly table. See Section 5 for the full assembly logic.

#### Source 5: Source_Txn_Mgmt → 1 silver table

**silver.transaction**: translates all 3 FK keys (portfolio, entity, security) from STM-native to enterprise format. Transaction natural key (stm_transaction_id) stays as-is since transactions originate from this system.

```sql
-- Key translation for all FK references in transaction records
REPLACE(src.portfolio_id, 'STM-P-', 'P-')      AS portfolio_enterprise_key,
REPLACE(src.entity_id,    'STM-E-', 'E-')       AS entity_enterprise_key,
REPLACE(src.security_id,  'STM-SEC-', 'SEC-')   AS security_enterprise_key,
-- Amounts: cast from string, validate non-negative
CAST(src.amount_portfolio AS DECIMAL(18,4))      AS transaction_amount_portfolio,
CAST(src.amount_local     AS DECIMAL(18,4))      AS transaction_amount_local,
CAST(src.amount_usd       AS DECIMAL(18,4))      AS transaction_amount_usd,
CAST(src.fx_rate          AS DECIMAL(18,8))      AS base_fx_rate,
```

#### Source 6: Source_WS_Online → 2 silver tables

**silver.ws_online_security**: security reference conformation (type cast, dedup on wso_security_id).

**silver.ws_online_pricing**: pricing data conformation (type cast, dedup on wso_security_id + price_date).

---

## 5. Composite Security Assembly (silver.security)

This is the most complex silver transform. It reconciles internal security records from Source_Security_Mgmt with external market data from WSO.

### Assembly Steps

```
1. Load internal security records from bronze.src_security_mgmt_raw
   → Conform to standard types, translate keys to enterprise format

2. Load WSO reference data from silver.ws_online_security
   → Already conformed (ran in PL_MARKET_DAILY before PL_SECURITY_DAILY)

3. Match internal securities to WSO records
   → Match strategy (ordered by precedence):
     a. EXACT match on bank_loan_id (if internal record has one)
     b. EXACT match on cusip
     c. EXACT match on isin
     d. EXACT match on ticker + security_type
     e. No match → _wso_match_status = 'UNMATCHED'

4. Enrich internal record with WSO fields where matched
   → Copy: bank_loan_id, cusip, isin, ticker (WSO wins on external identifiers)
   → Keep: security_type, security_group, entity/asset/team refs (internal wins on internal attributes)

5. Flag ambiguous matches (multiple WSO records match same internal security)
   → _wso_match_status = 'AMBIGUOUS', do NOT auto-enrich, route to manual review

6. MERGE into silver.security
```

### Assembly SQL (T-SQL pseudocode)

```sql
WITH internal_securities AS (
    SELECT
        REPLACE(src.security_id, 'SSM-SEC-', 'SEC-')    AS security_enterprise_key,
        TRIM(src.security_type)                           AS security_type,
        TRIM(src.security_group)                          AS security_group,
        TRIM(src.security_name)                           AS security_name,
        COALESCE(src.security_status, 'ACTIVE')           AS security_status,
        REPLACE(src.team_ref, 'SSM-IT-', 'IT-')          AS investment_team_enterprise_key,
        REPLACE(src.entity_ref, 'SSM-E-', 'E-')          AS entity_enterprise_key,
        REPLACE(src.asset_ref, 'SSM-A-', 'A-')           AS asset_enterprise_key,
        -- Internal may already have some external IDs from deal origination
        NULLIF(TRIM(src.bank_loan_id), '')                AS internal_bank_loan_id,
        NULLIF(TRIM(src.cusip), '')                       AS internal_cusip,
        NULLIF(TRIM(src.isin), '')                        AS internal_isin,
        NULLIF(TRIM(src.ticker), '')                      AS internal_ticker,
        src.security_id                                   AS src_security_id,
        src._record_id                                    AS _bronze_record_id,
        src._ingested_at                                  AS _source_modified_at
    FROM bronze.src_security_mgmt_raw src
    WHERE src._ingested_at > @last_watermark
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.security_id ORDER BY src._ingested_at DESC
    ) = 1
),

-- Attempt WSO match using cascading precedence
wso_matches AS (
    SELECT
        i.security_enterprise_key,
        w.wso_security_id,
        w.bank_loan_id       AS wso_bank_loan_id,
        w.cusip              AS wso_cusip,
        w.isin               AS wso_isin,
        w.ticker             AS wso_ticker,
        CASE
            WHEN i.internal_bank_loan_id IS NOT NULL
                 AND w.bank_loan_id = i.internal_bank_loan_id       THEN 'BANK_LOAN_ID'
            WHEN i.internal_cusip IS NOT NULL
                 AND w.cusip = i.internal_cusip                     THEN 'CUSIP'
            WHEN i.internal_isin IS NOT NULL
                 AND w.isin = i.internal_isin                       THEN 'ISIN'
            WHEN i.internal_ticker IS NOT NULL
                 AND w.ticker = i.internal_ticker
                 AND w.security_type = i.security_type              THEN 'TICKER_TYPE'
        END AS match_key,
        ROW_NUMBER() OVER (
            PARTITION BY i.security_enterprise_key
            ORDER BY
                CASE
                    WHEN w.bank_loan_id = i.internal_bank_loan_id   THEN 1
                    WHEN w.cusip = i.internal_cusip                 THEN 2
                    WHEN w.isin = i.internal_isin                   THEN 3
                    WHEN w.ticker = i.internal_ticker               THEN 4
                    ELSE 99
                END
        ) AS match_rank,
        COUNT(*) OVER (PARTITION BY i.security_enterprise_key) AS match_count
    FROM internal_securities i
    LEFT JOIN silver.ws_online_security w ON (
        (i.internal_bank_loan_id IS NOT NULL AND w.bank_loan_id = i.internal_bank_loan_id)
        OR (i.internal_cusip IS NOT NULL AND w.cusip = i.internal_cusip)
        OR (i.internal_isin IS NOT NULL AND w.isin = i.internal_isin)
        OR (i.internal_ticker IS NOT NULL AND w.ticker = i.internal_ticker
            AND w.security_type = i.security_type)
    )
),

assembled AS (
    SELECT
        i.security_enterprise_key,
        i.security_type,
        i.security_group,
        i.security_name,
        i.security_status,
        i.investment_team_enterprise_key,
        i.entity_enterprise_key,
        i.asset_enterprise_key,

        -- External IDs: WSO wins if matched, else fall back to internal
        CASE
            WHEN m.match_count > 1 THEN i.internal_bank_loan_id  -- AMBIGUOUS: keep internal only
            WHEN m.wso_security_id IS NOT NULL THEN COALESCE(m.wso_bank_loan_id, i.internal_bank_loan_id)
            ELSE i.internal_bank_loan_id
        END AS bank_loan_id,
        CASE
            WHEN m.match_count > 1 THEN i.internal_cusip
            WHEN m.wso_security_id IS NOT NULL THEN COALESCE(m.wso_cusip, i.internal_cusip)
            ELSE i.internal_cusip
        END AS cusip,
        CASE
            WHEN m.match_count > 1 THEN i.internal_isin
            WHEN m.wso_security_id IS NOT NULL THEN COALESCE(m.wso_isin, i.internal_isin)
            ELSE i.internal_isin
        END AS isin,
        CASE
            WHEN m.match_count > 1 THEN i.internal_ticker
            WHEN m.wso_security_id IS NOT NULL THEN COALESCE(m.wso_ticker, i.internal_ticker)
            ELSE i.internal_ticker
        END AS ticker,

        -- Match metadata
        CASE
            WHEN m.match_count > 1          THEN 'AMBIGUOUS'
            WHEN m.wso_security_id IS NOT NULL THEN 'MATCHED'
            ELSE 'UNMATCHED'
        END AS _wso_match_status,
        m.match_key                          AS _wso_match_key,
        CASE
            WHEN m.match_count > 1          THEN NULL
            WHEN m.match_key IS NOT NULL    THEN 'EXACT'
            ELSE NULL
        END AS _wso_match_confidence,

        i.src_security_id,
        i._bronze_record_id,
        i._source_modified_at,
        HASHBYTES('SHA2_256', CONCAT_WS('|',
            i.security_type, i.security_group, i.security_name, i.security_status,
            i.investment_team_enterprise_key, i.entity_enterprise_key, i.asset_enterprise_key,
            COALESCE(m.wso_bank_loan_id, i.internal_bank_loan_id),
            COALESCE(m.wso_cusip, i.internal_cusip),
            COALESCE(m.wso_isin, i.internal_isin),
            COALESCE(m.wso_ticker, i.internal_ticker)
        )) AS _row_hash
    FROM internal_securities i
    LEFT JOIN wso_matches m
        ON i.security_enterprise_key = m.security_enterprise_key
        AND m.match_rank = 1
)

MERGE INTO silver.security AS t
USING assembled AS s
ON t.security_enterprise_key = s.security_enterprise_key
WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
    -- ... all columns ...
WHEN NOT MATCHED THEN INSERT (...)
VALUES (...);
```

### Conflict Resolution Rules

| Field | Internal Wins | WSO Wins | Notes |
|---|---|---|---|
| security_type | ✅ | | Internal system defines instrument classification |
| security_group | ✅ | | Internal grouping |
| security_name | ✅ | | Authoritative internal name |
| security_status | ✅ | | Internal lifecycle |
| entity/asset/team refs | ✅ | | Only internal knows these |
| bank_loan_id | | ✅ | WSO is authoritative for public identifiers |
| cusip | | ✅ | |
| isin | | ✅ | |
| ticker | | ✅ | |

**On ambiguous matches:** Do not auto-enrich. Flag as AMBIGUOUS. Route to quarantine. Require manual resolution via a review queue or data steward intervention.

---

## 6. Quality Rules at Silver

### 6.1 Universal Rules (apply to all silver tables)

| Rule | Expression | Action |
|---|---|---|
| `NOT_NULL_EK` | enterprise key column IS NOT NULL | FAIL (quarantine row) |
| `VALID_EK_FORMAT` | enterprise key matches expected pattern (e.g., `^IT-\d+$`) | FAIL |
| `NO_DUPLICATE_EK` | COUNT(*) per enterprise key = 1 | FAIL (dedup should have caught this) |
| `ROW_HASH_NOT_NULL` | _row_hash IS NOT NULL | FAIL |
| `SOURCE_SYSTEM_VALID` | _source_system_id IN (SELECT source_system_id FROM meta.source_systems) | FAIL |

### 6.2 Per-Table Rules

| Silver Table | Rule | Expression | Action |
|---|---|---|---|
| investment_team | `TEAM_NAME_NOT_EMPTY` | LEN(TRIM(investment_team_name)) > 0 | FAIL |
| investment_team | `START_DATE_VALID` | start_date <= GETUTCDATE() | WARN |
| investment_team | `STOP_AFTER_START` | stop_date IS NULL OR stop_date > start_date | FAIL |
| portfolio_group | `PG_HAS_TEAM` | investment_team_enterprise_key IS NOT NULL | FAIL |
| portfolio_group | `PG_TEAM_EXISTS` | investment_team_enterprise_key IN (SELECT ek FROM silver.investment_team) | FAIL |
| portfolio_group | `VINTAGE_YEAR_RANGE` | vintage_year IS NULL OR (vintage_year >= 1980 AND vintage_year <= YEAR(GETUTCDATE()) + 1) | WARN |
| portfolio | `PORT_HAS_PG` | portfolio_group_enterprise_key IS NOT NULL | FAIL |
| portfolio | `PORT_PG_EXISTS` | portfolio_group_enterprise_key IN (SELECT ek FROM silver.portfolio_group) | FAIL |
| entity | `ENTITY_NAME_NOT_EMPTY` | LEN(TRIM(entity_name)) > 0 | FAIL |
| portfolio_entity_ownership | `PE_PCT_RANGE` | ownership_pct > 0 AND ownership_pct <= 1.0 | FAIL |
| portfolio_entity_ownership | `PE_DATE_VALID` | end_date IS NULL OR end_date > effective_date | FAIL |
| portfolio_entity_ownership | `PE_PORTFOLIO_EXISTS` | portfolio_ek IN (SELECT ek FROM silver.portfolio) | FAIL |
| portfolio_entity_ownership | `PE_ENTITY_EXISTS` | entity_ek IN (SELECT ek FROM silver.entity) | FAIL |
| entity_asset_ownership | `EA_PCT_RANGE` | ownership_pct > 0 AND ownership_pct <= 1.0 | FAIL |
| entity_asset_ownership | `EA_ENTITY_EXISTS` | entity_ek IN (SELECT ek FROM silver.entity) | FAIL |
| entity_asset_ownership | `EA_ASSET_EXISTS` | asset_ek IN (SELECT ek FROM silver.asset) | FAIL |
| asset | `ASSET_TYPE_NOT_EMPTY` | LEN(TRIM(asset_type)) > 0 | FAIL |
| asset | `VALUATION_POSITIVE` | last_valuation_amount IS NULL OR last_valuation_amount >= 0 | WARN |
| security | `SEC_HAS_ENTITY` | entity_enterprise_key IS NOT NULL | FAIL |
| security | `SEC_HAS_ASSET` | asset_enterprise_key IS NOT NULL | FAIL |
| security | `SEC_HAS_TEAM` | investment_team_enterprise_key IS NOT NULL | FAIL |
| security | `SEC_TYPE_VALID` | security_type IN ('EQUITY','SENIOR_DEBT','MEZZANINE','SUBORDINATED_DEBT','DERIVATIVE','PREFERRED','CONVERTIBLE') | FAIL |
| transaction | `TXN_AMOUNT_PRESENT` | transaction_amount_usd IS NOT NULL OR transaction_amount_local IS NOT NULL | FAIL |
| transaction | `TXN_DATE_NOT_FUTURE` | as_of_date <= CAST(GETUTCDATE() AS DATE) | WARN |
| transaction | `TXN_PORTFOLIO_EXISTS` | portfolio_ek IN (SELECT ek FROM silver.portfolio) | FAIL |
| transaction | `TXN_ENTITY_EXISTS` | entity_ek IN (SELECT ek FROM silver.entity) | FAIL |
| transaction | `TXN_SECURITY_EXISTS` | security_ek IN (SELECT ek FROM silver.security) | FAIL |
| transaction | `TXN_FX_RATE_POSITIVE` | base_fx_rate IS NULL OR base_fx_rate > 0 | FAIL |

### 6.3 Referential Integrity at Silver

Silver validates FK references **within silver scope**. This catches bad key translations early before gold attempts surrogate key lookups.

```
silver.portfolio_group.investment_team_enterprise_key   → EXISTS in silver.investment_team
silver.portfolio.portfolio_group_enterprise_key          → EXISTS in silver.portfolio_group
silver.portfolio_entity_ownership.portfolio_enterprise_key → EXISTS in silver.portfolio
silver.portfolio_entity_ownership.entity_enterprise_key   → EXISTS in silver.entity
silver.entity_asset_ownership.entity_enterprise_key       → EXISTS in silver.entity
silver.entity_asset_ownership.asset_enterprise_key        → EXISTS in silver.asset
silver.security.entity_enterprise_key                     → EXISTS in silver.entity
silver.security.asset_enterprise_key                      → EXISTS in silver.asset
silver.security.investment_team_enterprise_key             → EXISTS in silver.investment_team
silver.transaction.portfolio_enterprise_key                → EXISTS in silver.portfolio
silver.transaction.entity_enterprise_key                   → EXISTS in silver.entity
silver.transaction.security_enterprise_key                 → EXISTS in silver.security
```

**Action on referential failure:** Quarantine the row. Do NOT drop silently. The quarantine table retains the failed row with the rule that caught it, enabling data steward review.

---

## 7. Quarantine Pattern

Each silver table has a companion quarantine table:

```sql
CREATE TABLE silver.investment_team_quarantine (
    quarantine_id           INT IDENTITY(1,1) NOT NULL,
    -- Full copy of source row
    raw_payload             NVARCHAR(MAX)     NOT NULL,   -- original bronze row as JSON
    -- Why it failed
    failed_rule             NVARCHAR(100)     NOT NULL,
    failure_detail          NVARCHAR(500)     NULL,
    -- When
    quarantined_at          DATETIME2         NOT NULL DEFAULT GETUTCDATE(),
    quarantined_by          NVARCHAR(255)     NOT NULL DEFAULT SYSTEM_USER,
    -- Resolution
    resolution_status       NVARCHAR(20)      NOT NULL DEFAULT 'PENDING',  -- PENDING, FIXED, DROPPED
    resolved_at             DATETIME2         NULL,
    resolved_by             NVARCHAR(255)     NULL,
    resolution_notes        NVARCHAR(500)     NULL,
    CONSTRAINT pk_silver_it_q PRIMARY KEY (quarantine_id)
);
```

**Quarantine convention:** `silver.<entity>_quarantine` for each silver table. Same schema structure across all quarantine tables (raw_payload + failed_rule + resolution tracking).

---

## 8. Silver → Gold Handoff

Silver delivers conformed, enterprise-keyed, quality-validated rows. Gold's job is:

1. **Surrogate key generation** — IDENTITY columns on dimension tables
2. **Surrogate key lookup** — MERGE joins on enterprise keys, assigns/reuses surrogate keys
3. **Cross-system joins** — portfolio_entity_bridge needs both portfolio_key (from gold.portfolio_dimension) and entity_key (from gold.entity_dimension), resolved by looking up enterprise keys
4. **Bridge allocation computation** — position_team_bridge allocation % derived from security → team relationships

### Gold MERGE Pattern (example: entity_dimension)

```sql
MERGE INTO gold.entity_dimension AS t
USING silver.entity AS s
ON t.entity_enterprise_key = s.entity_enterprise_key
WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
    t.entity_name       = s.entity_name,
    t.entity_short_name = s.entity_short_name,
    t.entity_legal_name = s.entity_legal_name,
    t.modified_date     = GETUTCDATE(),
    t.modified_by       = SYSTEM_USER
WHEN NOT MATCHED THEN INSERT (
    entity_enterprise_key, entity_name, entity_short_name, entity_legal_name
) VALUES (
    s.entity_enterprise_key, s.entity_name, s.entity_short_name, s.entity_legal_name
);
-- entity_key (surrogate) generated by IDENTITY
```

### Gold Bridge Assembly (example: portfolio_entity_bridge)

```sql
-- Bridge requires surrogate keys from both dimension tables
MERGE INTO gold.portfolio_entity_bridge AS t
USING (
    SELECT
        p.portfolio_key,
        e.entity_key,
        s.ownership_pct,
        s.effective_date,
        s.end_date,
        2 AS source_system_key   -- Source_Entity_Mgmt
    FROM silver.portfolio_entity_ownership s
    JOIN gold.portfolio_dimension p ON p.portfolio_enterprise_key = s.portfolio_enterprise_key
    JOIN gold.entity_dimension e    ON e.entity_enterprise_key = s.entity_enterprise_key
) AS src
ON  t.portfolio_key  = src.portfolio_key
AND t.entity_key     = src.entity_key
AND t.effective_date  = src.effective_date
WHEN MATCHED AND (t.ownership_pct != src.ownership_pct OR ISNULL(t.end_date,'9999-12-31') != ISNULL(src.end_date,'9999-12-31'))
    THEN UPDATE SET
        t.ownership_pct     = src.ownership_pct,
        t.end_date          = src.end_date,
        t.modified_date     = GETUTCDATE(),
        t.modified_by       = SYSTEM_USER
WHEN NOT MATCHED THEN INSERT (
    portfolio_key, entity_key, ownership_pct, effective_date, end_date, source_system_key
) VALUES (
    src.portfolio_key, src.entity_key, src.ownership_pct, src.effective_date, src.end_date, src.source_system_key
);
```

---

## 9. Pipeline Execution Order (Silver Focus)

```
PL_ENTERPRISE_DAILY (silver phase):
  1. bronze.src_enterprise_raw → silver.investment_team
  2. bronze.src_enterprise_raw → silver.portfolio_group      (after 1: FK check to investment_team)
  3. bronze.src_enterprise_raw → silver.portfolio             (after 2: FK check to portfolio_group)

PL_ENTITY_DAILY (silver phase):
  4. bronze.src_entity_mgmt_raw → silver.entity
  5. bronze.src_entity_mgmt_raw → silver.portfolio_entity_ownership  (after 3,4: FK check to portfolio + entity)
  6. bronze.src_entity_mgmt_raw → silver.entity_asset_ownership      (after 4,7: FK check to entity + asset)

PL_ASSET_DAILY (silver phase):
  7. bronze.src_asset_mgmt_raw → silver.asset

PL_MARKET_DAILY (silver phase):
  8. bronze.src_ws_online_raw → silver.ws_online_security
  9. bronze.src_ws_online_raw → silver.ws_online_pricing

PL_SECURITY_DAILY (silver phase, runs AFTER PL_MARKET_DAILY):
  10. bronze.src_security_mgmt_raw + silver.ws_online_security → silver.security  (composite assembly)

PL_TXN_DAILY (silver phase, runs AFTER all dimension pipelines):
  11. bronze.src_txn_mgmt_raw → silver.transaction  (FK checks to portfolio, entity, security)
```

**Critical dependencies:**
- Steps 5-6 depend on steps 3-4 and 7 (ownership bridges need portfolio, entity, and asset to exist first)
- Step 10 depends on step 8 (composite assembly needs WSO data)
- Step 11 depends on steps 3, 4, 10 (transactions reference portfolio, entity, security)

---

## 10. Open Items (Silver-Specific)

1. **Quarantine review workflow** — Who reviews quarantined rows? What's the SLA? Need to define data steward process and tooling (Databricks dashboard? Notebook? Email alerts?).
2. **Late-arriving dimension members** — If a transaction references a security_enterprise_key that doesn't exist in silver.security yet (timing gap between PL_TXN and PL_SECURITY), do we quarantine or create a placeholder? Current design: quarantine. May need "late-arriving dimension" pattern if timing gaps are frequent.
3. **Row hash scope** — Current design hashes all business columns. Should audit columns (_source_modified_at, _bronze_record_id) be excluded from hash? Current: excluded. Only business-meaningful columns participate in change detection.
4. **Backfill strategy** — When a new source system is added or a key translation rule changes, how do we re-process historical bronze data through silver? Need to define full-refresh vs. incremental re-processing.
5. **Quarantine table retention** — How long do we keep quarantined rows? Resolved rows could be purged after 90 days. Unresolved rows should persist indefinitely and trigger escalation alerts.
