-- ============================================================================
-- GOLDEN DATA LAYER: DDL (6-Source-System Model)
-- Target: Azure SQL Edge Developer (SQL Server 15.x compatible)
-- Date: 2026-02-09
-- ============================================================================
-- Source System IDs:
--   1 = enterprise_data        (GP/Fund/Portfolio hierarchy)
--   2 = Source_Entity_Mgmt     (entity, ownership bridges)
--   3 = Source_Asset_Mgmt      (asset master, valuations)
--   4 = Source_Security_Mgmt   (security master composite)
--   5 = Source_Txn_Mgmt        (daily transactions)
--   6 = Source_WS_Online       (public market data, pricing)
-- ============================================================================
-- Execution: Run this file first. Creates database, schemas, all tables.
-- ============================================================================

-- ============================================================================
-- PART 0: DATABASE + SCHEMAS
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'GoldenDataLayer')
    CREATE DATABASE GoldenDataLayer;
GO
USE GoldenDataLayer;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta')   EXEC('CREATE SCHEMA meta');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze') EXEC('CREATE SCHEMA bronze');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver') EXEC('CREATE SCHEMA silver');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')   EXEC('CREATE SCHEMA gold');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')  EXEC('CREATE SCHEMA audit');
GO


-- ============================================================================
-- PART 0.5: DROP TABLES IN REVERSE DEPENDENCY ORDER (idempotency)
-- ============================================================================

-- Audit
DROP TABLE IF EXISTS audit.etl_run_log;

-- Gold bridges & facts (depend on dims)
DROP TABLE IF EXISTS gold.position_team_bridge;
DROP TABLE IF EXISTS gold.entity_asset_bridge;
DROP TABLE IF EXISTS gold.portfolio_entity_bridge;
DROP TABLE IF EXISTS gold.position_fact;
DROP TABLE IF EXISTS gold.position_transactions_fact;

-- Gold dimensions (portfolio depends on portfolio_group; security depends on team/entity/asset)
DROP TABLE IF EXISTS gold.portfolio_dimension;
DROP TABLE IF EXISTS gold.security_dimension;
DROP TABLE IF EXISTS gold.asset_dimension;
DROP TABLE IF EXISTS gold.entity_dimension;
DROP TABLE IF EXISTS gold.portfolio_group_dimension;
DROP TABLE IF EXISTS gold.investment_team_dimension;

-- Silver
DROP TABLE IF EXISTS silver.quarantine;
DROP TABLE IF EXISTS silver.ws_online_pricing;
DROP TABLE IF EXISTS silver.ws_online_security;
DROP TABLE IF EXISTS silver.position_transaction;
DROP TABLE IF EXISTS silver.[transaction];
DROP TABLE IF EXISTS silver.security;
DROP TABLE IF EXISTS silver.asset;
DROP TABLE IF EXISTS silver.entity_asset_ownership;
DROP TABLE IF EXISTS silver.portfolio_entity_ownership;
DROP TABLE IF EXISTS silver.entity;
DROP TABLE IF EXISTS silver.portfolio;
DROP TABLE IF EXISTS silver.portfolio_group;
DROP TABLE IF EXISTS silver.investment_team;

-- Bronze
DROP TABLE IF EXISTS bronze.src_ws_online_raw;
DROP TABLE IF EXISTS bronze.src_txn_mgmt_raw;
DROP TABLE IF EXISTS bronze.src_security_mgmt_raw;
DROP TABLE IF EXISTS bronze.src_asset_mgmt_raw;
DROP TABLE IF EXISTS bronze.src_entity_mgmt_raw;
DROP TABLE IF EXISTS bronze.src_enterprise_raw;

-- Meta (reverse dependency order)
DROP TABLE IF EXISTS meta.pipeline_execution_log;
DROP TABLE IF EXISTS meta.extraction_filter_decisions;
DROP TABLE IF EXISTS meta.extraction_filters;
DROP TABLE IF EXISTS meta.business_glossary;
DROP TABLE IF EXISTS meta.retention_policies;
DROP TABLE IF EXISTS meta.consumers;
DROP TABLE IF EXISTS meta.quality_rules;
DROP TABLE IF EXISTS meta.key_crosswalk_paths;
DROP TABLE IF EXISTS meta.key_crosswalk;
DROP TABLE IF EXISTS meta.key_registry;
DROP TABLE IF EXISTS meta.data_contracts;
DROP TABLE IF EXISTS meta.ingestion_pipeline_steps;
DROP TABLE IF EXISTS meta.ingestion_pipelines;
DROP TABLE IF EXISTS meta.source_systems;
GO


-- ============================================================================
-- PART 1: META TABLES (14)
-- ============================================================================

-- 1.1 meta.source_systems
CREATE TABLE meta.source_systems (
    source_system_id        INT IDENTITY(1,1) NOT NULL,
    system_code             NVARCHAR(100)   NOT NULL,
    system_name             NVARCHAR(255)   NOT NULL,
    system_type             NVARCHAR(100)   NOT NULL,
    connectivity_method     NVARCHAR(100)   NOT NULL,
    connection_details      NVARCHAR(MAX)   NULL,
    data_formats            NVARCHAR(MAX)   NULL,
    owning_business_unit    NVARCHAR(255)   NOT NULL,
    data_steward            NVARCHAR(255)   NULL,
    technical_owner         NVARCHAR(255)   NULL,
    environment             NVARCHAR(50)    NOT NULL DEFAULT 'PROD',
    documentation_url       NVARCHAR(500)   NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_source_system PRIMARY KEY (source_system_id),
    CONSTRAINT uq_system_code UNIQUE (system_code)
);
GO

-- 1.2 meta.ingestion_pipelines
CREATE TABLE meta.ingestion_pipelines (
    pipeline_id             INT IDENTITY(1,1) NOT NULL,
    source_system_id        INT             NOT NULL,
    pipeline_code           NVARCHAR(200)   NOT NULL,
    pipeline_name           NVARCHAR(500)   NOT NULL,
    description             NVARCHAR(MAX)   NULL,
    ingestion_pattern       NVARCHAR(100)   NOT NULL,
    schedule_type           NVARCHAR(100)   NULL,
    schedule_expression     NVARCHAR(200)   NULL,
    target_bronze_table     NVARCHAR(500)   NULL,
    target_silver_table     NVARCHAR(500)   NULL,
    target_gold_tables      NVARCHAR(MAX)   NULL,
    job_id                  NVARCHAR(200)   NULL,
    managing_owner          NVARCHAR(255)   NOT NULL,
    sla_minutes             INT             NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_pipeline PRIMARY KEY (pipeline_id),
    CONSTRAINT fk_pipeline_source FOREIGN KEY (source_system_id)
        REFERENCES meta.source_systems (source_system_id),
    CONSTRAINT uq_pipeline_code UNIQUE (pipeline_code)
);
GO

-- 1.3 meta.ingestion_pipeline_steps
CREATE TABLE meta.ingestion_pipeline_steps (
    step_id                 INT IDENTITY(1,1) NOT NULL,
    pipeline_id             INT             NOT NULL,
    step_sequence           INT             NOT NULL,
    step_name               NVARCHAR(255)   NOT NULL,
    step_type               NVARCHAR(100)   NOT NULL,
    description             NVARCHAR(MAX)   NOT NULL,
    executor                NVARCHAR(500)   NULL,
    executor_owner          NVARCHAR(255)   NULL,
    input_reference         NVARCHAR(500)   NULL,
    output_reference        NVARCHAR(500)   NULL,
    key_columns_used        NVARCHAR(MAX)   NULL,
    error_handling          NVARCHAR(MAX)   NULL,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_step PRIMARY KEY (step_id),
    CONSTRAINT fk_step_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES meta.ingestion_pipelines (pipeline_id),
    CONSTRAINT uq_pipeline_step UNIQUE (pipeline_id, step_sequence)
);
GO

-- 1.4 meta.data_contracts
CREATE TABLE meta.data_contracts (
    contract_id             INT IDENTITY(1,1) NOT NULL,
    source_system_id        INT             NOT NULL,
    pipeline_id             INT             NOT NULL,
    contract_version        INT             NOT NULL DEFAULT 1,
    contract_status         NVARCHAR(50)    NOT NULL DEFAULT 'ACTIVE',
    schema_definition       NVARCHAR(MAX)   NOT NULL,
    delivery_sla_minutes    INT             NULL,
    freshness_sla_minutes   INT             NULL,
    volume_expectation      NVARCHAR(MAX)   NULL,
    breaking_change_policy  NVARCHAR(50)    NULL,
    owner                   NVARCHAR(255)   NOT NULL,
    effective_date          DATE            NOT NULL,
    expiration_date         DATE            NULL,
    notes                   NVARCHAR(MAX)   NULL,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_contract PRIMARY KEY (contract_id),
    CONSTRAINT fk_contract_source FOREIGN KEY (source_system_id)
        REFERENCES meta.source_systems (source_system_id),
    CONSTRAINT fk_contract_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES meta.ingestion_pipelines (pipeline_id),
    CONSTRAINT uq_contract_version UNIQUE (pipeline_id, contract_version)
);
GO

-- 1.5 meta.key_registry
CREATE TABLE meta.key_registry (
    key_id                  INT IDENTITY(1,1) NOT NULL,
    source_system_id        INT             NOT NULL,
    key_name                NVARCHAR(255)   NOT NULL,
    key_aliases             NVARCHAR(MAX)   NULL,
    key_type                NVARCHAR(50)    NOT NULL,
    data_type               NVARCHAR(50)    NOT NULL,
    example_values          NVARCHAR(MAX)   NULL,
    source_table            NVARCHAR(500)   NULL,
    source_column           NVARCHAR(255)   NULL,
    databricks_table        NVARCHAR(500)   NULL,
    databricks_column       NVARCHAR(255)   NULL,
    description             NVARCHAR(MAX)   NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_key PRIMARY KEY (key_id),
    CONSTRAINT fk_key_source FOREIGN KEY (source_system_id)
        REFERENCES meta.source_systems (source_system_id)
);
GO

-- 1.6 meta.key_crosswalk
CREATE TABLE meta.key_crosswalk (
    crosswalk_id            INT IDENTITY(1,1) NOT NULL,
    from_key_id             INT             NOT NULL,
    to_key_id               INT             NOT NULL,
    mapping_type            NVARCHAR(50)    NOT NULL,
    mapping_confidence      NVARCHAR(50)    NOT NULL DEFAULT 'EXACT',
    transformation_rule     NVARCHAR(MAX)   NULL,
    conditions              NVARCHAR(MAX)   NULL,
    bidirectional           BIT             NOT NULL DEFAULT 1,
    validated_by            NVARCHAR(255)   NULL,
    validation_date         DATE            NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_crosswalk PRIMARY KEY (crosswalk_id),
    CONSTRAINT fk_cw_from FOREIGN KEY (from_key_id) REFERENCES meta.key_registry (key_id),
    CONSTRAINT fk_cw_to   FOREIGN KEY (to_key_id)   REFERENCES meta.key_registry (key_id)
);
GO

-- 1.7 meta.key_crosswalk_paths
CREATE TABLE meta.key_crosswalk_paths (
    path_id                 INT IDENTITY(1,1) NOT NULL,
    from_key_id             INT             NOT NULL,
    to_key_id               INT             NOT NULL,
    hop_count               INT             NOT NULL,
    path_crosswalk_ids      NVARCHAR(MAX)   NOT NULL,
    path_description        NVARCHAR(MAX)   NULL,
    path_reliability        NVARCHAR(50)    NULL,
    conditions              NVARCHAR(MAX)   NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_path PRIMARY KEY (path_id),
    CONSTRAINT fk_path_from FOREIGN KEY (from_key_id) REFERENCES meta.key_registry (key_id),
    CONSTRAINT fk_path_to   FOREIGN KEY (to_key_id)   REFERENCES meta.key_registry (key_id)
);
GO

-- 1.8 meta.quality_rules
CREATE TABLE meta.quality_rules (
    rule_id                 INT IDENTITY(1,1) NOT NULL,
    rule_code               NVARCHAR(200)   NOT NULL,
    rule_name               NVARCHAR(500)   NOT NULL,
    target_table            NVARCHAR(500)   NOT NULL,
    target_column           NVARCHAR(255)   NULL,
    rule_expression         NVARCHAR(MAX)   NOT NULL,
    rule_type               NVARCHAR(100)   NOT NULL,
    severity                NVARCHAR(50)    NOT NULL,
    layer                   NVARCHAR(50)    NOT NULL,
    owner                   NVARCHAR(255)   NOT NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_quality_rule PRIMARY KEY (rule_id),
    CONSTRAINT uq_rule_code UNIQUE (rule_code)
);
GO

-- 1.9 meta.consumers
CREATE TABLE meta.consumers (
    consumer_id             INT IDENTITY(1,1) NOT NULL,
    consumer_name           NVARCHAR(500)   NOT NULL,
    consumer_type           NVARCHAR(100)   NOT NULL,
    consuming_tables        NVARCHAR(MAX)   NOT NULL,
    owning_team             NVARCHAR(255)   NOT NULL,
    contact                 NVARCHAR(255)   NOT NULL,
    access_method           NVARCHAR(100)   NOT NULL,
    criticality             NVARCHAR(50)    NOT NULL,
    freshness_requirement   NVARCHAR(50)    NULL,
    notification_channel    NVARCHAR(500)   NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_consumer PRIMARY KEY (consumer_id),
    CONSTRAINT uq_consumer_name UNIQUE (consumer_name)
);
GO

-- 1.10 meta.retention_policies
CREATE TABLE meta.retention_policies (
    policy_id               INT IDENTITY(1,1) NOT NULL,
    target_table            NVARCHAR(500)   NOT NULL,
    layer                   NVARCHAR(50)    NOT NULL,
    retention_days          INT             NOT NULL,
    time_travel_days        INT             NOT NULL DEFAULT 7,
    log_retention_days      INT             NOT NULL DEFAULT 30,
    archive_after_days      INT             NULL,
    purge_after_days        INT             NULL,
    vacuum_strategy         NVARCHAR(200)   NULL DEFAULT 'LITE_DAILY_FULL_WEEKLY',
    regulatory_basis        NVARCHAR(200)   NULL,
    owner                   NVARCHAR(255)   NOT NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_retention PRIMARY KEY (policy_id)
);
GO

-- 1.11 meta.business_glossary
CREATE TABLE meta.business_glossary (
    term_id                 INT IDENTITY(1,1) NOT NULL,
    business_term           NVARCHAR(500)   NOT NULL,
    definition              NVARCHAR(MAX)   NOT NULL,
    calculation_logic       NVARCHAR(MAX)   NULL,
    mapped_tables           NVARCHAR(MAX)   NOT NULL,
    mapped_columns          NVARCHAR(MAX)   NOT NULL,
    domain                  NVARCHAR(255)   NOT NULL,
    owner                   NVARCHAR(255)   NOT NULL,
    synonyms                NVARCHAR(MAX)   NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_glossary PRIMARY KEY (term_id),
    CONSTRAINT uq_business_term UNIQUE (business_term)
);
GO

-- 1.12 meta.extraction_filters
CREATE TABLE meta.extraction_filters (
    filter_id               INT IDENTITY(1,1) NOT NULL,
    source_system_id        INT             NOT NULL,
    filter_type             NVARCHAR(50)    NOT NULL,
    filter_value            NVARCHAR(255)   NOT NULL,
    is_active               BIT             NOT NULL DEFAULT 1,
    rationale               NVARCHAR(MAX)   NULL,
    decided_by              NVARCHAR(255)   NOT NULL,
    effective_date          DATE            NOT NULL,
    expiration_date         DATE            NULL,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_filter PRIMARY KEY (filter_id),
    CONSTRAINT fk_filter_source FOREIGN KEY (source_system_id)
        REFERENCES meta.source_systems (source_system_id)
);
GO

-- 1.13 meta.extraction_filter_decisions
CREATE TABLE meta.extraction_filter_decisions (
    decision_id             INT IDENTITY(1,1) NOT NULL,
    filter_id               INT             NOT NULL,
    action                  NVARCHAR(50)    NOT NULL,
    previous_state          NVARCHAR(MAX)   NULL,
    new_state               NVARCHAR(MAX)   NOT NULL,
    rationale               NVARCHAR(MAX)   NOT NULL,
    decided_by              NVARCHAR(255)   NOT NULL,
    approved_by             NVARCHAR(255)   NULL,
    decision_date           DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_decision PRIMARY KEY (decision_id),
    CONSTRAINT fk_decision_filter FOREIGN KEY (filter_id)
        REFERENCES meta.extraction_filters (filter_id)
);
GO

-- 1.14 meta.pipeline_execution_log
CREATE TABLE meta.pipeline_execution_log (
    execution_id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    pipeline_id             INT             NOT NULL,
    step_id                 INT             NULL,
    job_id                  NVARCHAR(200)   NULL,
    run_id                  NVARCHAR(200)   NULL,
    execution_type          NVARCHAR(50)    NOT NULL,
    status                  NVARCHAR(50)    NOT NULL,
    applied_filters         NVARCHAR(MAX)   NULL,
    source_query            NVARCHAR(MAX)   NULL,
    target_table            NVARCHAR(500)   NULL,
    rows_extracted          BIGINT          NULL,
    rows_inserted           BIGINT          NULL,
    rows_updated            BIGINT          NULL,
    rows_deleted            BIGINT          NULL,
    rows_rejected           BIGINT          NULL,
    rows_skipped            BIGINT          NULL,
    start_time              DATETIME2       NOT NULL,
    end_time                DATETIME2       NULL,
    duration_seconds        INT             NULL,
    error_code              NVARCHAR(100)   NULL,
    error_message           NVARCHAR(MAX)   NULL,
    error_stack_trace       NVARCHAR(MAX)   NULL,
    executed_by             NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    compute_resource        NVARCHAR(255)   NULL,
    notebook_path           NVARCHAR(500)   NULL,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_execution PRIMARY KEY (execution_id),
    CONSTRAINT fk_exec_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES meta.ingestion_pipelines (pipeline_id)
);
GO


-- ============================================================================
-- PART 2: BRONZE TABLES (6)
-- All columns NVARCHAR to preserve raw fidelity. Silver casts to proper types.
-- ============================================================================

-- 2.1 bronze.src_enterprise_raw
-- Multi-entity: _record_type IN ('investment_team','portfolio_group','portfolio')
CREATE TABLE bronze.src_enterprise_raw (
    _record_id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    _batch_id               NVARCHAR(100)   NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _source_file            NVARCHAR(500)   NULL,
    _record_type            NVARCHAR(50)    NOT NULL,
    -- investment_team columns
    investment_team_id      NVARCHAR(50)    NULL,
    team_name               NVARCHAR(500)   NULL,
    team_short_name         NVARCHAR(100)   NULL,
    start_date              NVARCHAR(50)    NULL,
    stop_date               NVARCHAR(50)    NULL,
    -- portfolio_group columns
    portfolio_group_id      NVARCHAR(50)    NULL,
    pg_name                 NVARCHAR(500)   NULL,
    pg_short_name           NVARCHAR(100)   NULL,
    pg_description          NVARCHAR(MAX)   NULL,
    pg_team_ref             NVARCHAR(50)    NULL,
    vintage_year            NVARCHAR(10)    NULL,
    strategy                NVARCHAR(200)   NULL,
    committed_capital       NVARCHAR(50)    NULL,
    committed_capital_ccy   NVARCHAR(10)    NULL,
    fund_status             NVARCHAR(50)    NULL,
    -- portfolio columns
    portfolio_id            NVARCHAR(50)    NULL,
    port_name               NVARCHAR(500)   NULL,
    port_short_name         NVARCHAR(100)   NULL,
    port_pg_ref             NVARCHAR(50)    NULL,
    CONSTRAINT pk_bronze_enterprise PRIMARY KEY (_record_id)
);
GO

-- 2.2 bronze.src_entity_mgmt_raw
-- Multi-entity: _record_type IN ('entity','portfolio_entity_ownership','entity_asset_ownership')
CREATE TABLE bronze.src_entity_mgmt_raw (
    _record_id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    _batch_id               NVARCHAR(100)   NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _source_file            NVARCHAR(500)   NULL,
    _record_type            NVARCHAR(50)    NOT NULL,
    -- entity columns
    entity_id               NVARCHAR(50)    NULL,
    entity_name             NVARCHAR(500)   NULL,
    entity_short_name       NVARCHAR(100)   NULL,
    entity_legal_name       NVARCHAR(500)   NULL,
    entity_type             NVARCHAR(100)   NULL,
    entity_status           NVARCHAR(50)    NULL,
    incorporation_jurisdiction NVARCHAR(200) NULL,
    incorporation_date      NVARCHAR(50)    NULL,
    -- ownership columns (used by both bridge types)
    ownership_id            NVARCHAR(50)    NULL,
    portfolio_ref           NVARCHAR(50)    NULL,
    entity_ref              NVARCHAR(50)    NULL,
    asset_ref               NVARCHAR(50)    NULL,
    ownership_pct           NVARCHAR(20)    NULL,
    effective_date          NVARCHAR(50)    NULL,
    end_date                NVARCHAR(50)    NULL,
    CONSTRAINT pk_bronze_entity_mgmt PRIMARY KEY (_record_id)
);
GO

-- 2.3 bronze.src_asset_mgmt_raw
CREATE TABLE bronze.src_asset_mgmt_raw (
    _record_id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    _batch_id               NVARCHAR(100)   NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _source_file            NVARCHAR(500)   NULL,
    asset_id                NVARCHAR(50)    NOT NULL,
    asset_name              NVARCHAR(500)   NULL,
    asset_short_name        NVARCHAR(100)   NULL,
    asset_legal_name        NVARCHAR(500)   NULL,
    asset_type              NVARCHAR(100)   NULL,
    asset_subtype           NVARCHAR(100)   NULL,
    asset_status            NVARCHAR(50)    NULL,
    location_country        NVARCHAR(100)   NULL,
    location_region         NVARCHAR(200)   NULL,
    acquisition_date        NVARCHAR(50)    NULL,
    last_valuation_date     NVARCHAR(50)    NULL,
    last_valuation_amount   NVARCHAR(50)    NULL,
    last_valuation_currency NVARCHAR(10)    NULL,
    CONSTRAINT pk_bronze_asset_mgmt PRIMARY KEY (_record_id)
);
GO

-- 2.4 bronze.src_security_mgmt_raw
CREATE TABLE bronze.src_security_mgmt_raw (
    _record_id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    _batch_id               NVARCHAR(100)   NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _source_file            NVARCHAR(500)   NULL,
    security_id             NVARCHAR(50)    NOT NULL,
    security_type           NVARCHAR(100)   NULL,
    security_group          NVARCHAR(100)   NULL,
    security_name           NVARCHAR(500)   NULL,
    security_status         NVARCHAR(50)    NULL,
    team_ref                NVARCHAR(50)    NULL,
    entity_ref              NVARCHAR(50)    NULL,
    asset_ref               NVARCHAR(50)    NULL,
    bank_loan_id            NVARCHAR(50)    NULL,
    cusip                   NVARCHAR(20)    NULL,
    isin                    NVARCHAR(20)    NULL,
    ticker                  NVARCHAR(20)    NULL,
    CONSTRAINT pk_bronze_security_mgmt PRIMARY KEY (_record_id)
);
GO

-- 2.5 bronze.src_txn_mgmt_raw
CREATE TABLE bronze.src_txn_mgmt_raw (
    _record_id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    _batch_id               NVARCHAR(100)   NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _source_file            NVARCHAR(500)   NULL,
    transaction_id          NVARCHAR(50)    NOT NULL,
    portfolio_id            NVARCHAR(50)    NULL,
    entity_id               NVARCHAR(50)    NULL,
    security_id             NVARCHAR(50)    NULL,
    as_of_date              NVARCHAR(50)    NULL,
    transaction_type        NVARCHAR(100)   NULL,
    transaction_category    NVARCHAR(100)   NULL,
    transaction_status      NVARCHAR(50)    NULL,
    amount_portfolio        NVARCHAR(50)    NULL,
    amount_local            NVARCHAR(50)    NULL,
    amount_usd              NVARCHAR(50)    NULL,
    fx_rate                 NVARCHAR(50)    NULL,
    quantity                NVARCHAR(50)    NULL,
    order_id                NVARCHAR(200)   NULL,
    order_date              NVARCHAR(50)    NULL,
    order_status            NVARCHAR(50)    NULL,
    CONSTRAINT pk_bronze_txn_mgmt PRIMARY KEY (_record_id)
);
GO

-- 2.6 bronze.src_ws_online_raw
-- Multi-entity: _record_type IN ('security','pricing')
CREATE TABLE bronze.src_ws_online_raw (
    _record_id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    _batch_id               NVARCHAR(100)   NOT NULL,
    _ingested_at            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _source_file            NVARCHAR(500)   NULL,
    _record_type            NVARCHAR(50)    NOT NULL,
    -- security reference columns
    wso_security_id         NVARCHAR(50)    NULL,
    security_type           NVARCHAR(100)   NULL,
    security_name           NVARCHAR(500)   NULL,
    bank_loan_id            NVARCHAR(50)    NULL,
    cusip                   NVARCHAR(20)    NULL,
    isin                    NVARCHAR(20)    NULL,
    ticker                  NVARCHAR(20)    NULL,
    exchange                NVARCHAR(100)   NULL,
    currency                NVARCHAR(10)    NULL,
    wso_status              NVARCHAR(50)    NULL,
    last_updated            NVARCHAR(50)    NULL,
    -- pricing columns
    price_date              NVARCHAR(50)    NULL,
    price_close             NVARCHAR(50)    NULL,
    price_open              NVARCHAR(50)    NULL,
    price_high              NVARCHAR(50)    NULL,
    price_low               NVARCHAR(50)    NULL,
    volume                  NVARCHAR(50)    NULL,
    CONSTRAINT pk_bronze_ws_online PRIMARY KEY (_record_id)
);
GO


-- ============================================================================
-- PART 3: SILVER TABLES (11) + QUARANTINE (1 universal)
-- ============================================================================

-- 3.1 silver.investment_team
CREATE TABLE silver.investment_team (
    investment_team_enterprise_key   NVARCHAR(100)   NOT NULL,
    investment_team_name             NVARCHAR(500)   NOT NULL,
    investment_team_short_name       NVARCHAR(100)   NULL,
    start_date                       DATE            NOT NULL,
    stop_date                        DATE            NULL,
    src_investment_team_id           NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 1,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_inv_team PRIMARY KEY (investment_team_enterprise_key)
);
GO

-- 3.2 silver.portfolio_group
CREATE TABLE silver.portfolio_group (
    portfolio_group_enterprise_key   NVARCHAR(100)   NOT NULL,
    portfolio_group_name             NVARCHAR(500)   NOT NULL,
    portfolio_group_short_name       NVARCHAR(100)   NULL,
    portfolio_group_description      NVARCHAR(MAX)   NULL,
    investment_team_enterprise_key   NVARCHAR(100)   NOT NULL,
    vintage_year                     INT             NULL,
    strategy                         NVARCHAR(200)   NULL,
    committed_capital                DECIMAL(18,2)   NULL,
    committed_capital_currency       NVARCHAR(3)     NULL,
    fund_status                      NVARCHAR(50)    NULL,
    src_portfolio_group_id           NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 1,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_pg PRIMARY KEY (portfolio_group_enterprise_key)
);
GO

-- 3.3 silver.portfolio
CREATE TABLE silver.portfolio (
    portfolio_enterprise_key         NVARCHAR(100)   NOT NULL,
    portfolio_name                   NVARCHAR(500)   NOT NULL,
    portfolio_short_name             NVARCHAR(100)   NULL,
    portfolio_group_enterprise_key   NVARCHAR(100)   NOT NULL,
    src_portfolio_id                 NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 1,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_portfolio PRIMARY KEY (portfolio_enterprise_key)
);
GO

-- 3.4 silver.entity
CREATE TABLE silver.entity (
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    entity_name                      NVARCHAR(500)   NOT NULL,
    entity_short_name                NVARCHAR(100)   NULL,
    entity_legal_name                NVARCHAR(500)   NULL,
    entity_type                      NVARCHAR(100)   NULL,
    entity_status                    NVARCHAR(50)    NULL,
    incorporation_jurisdiction       NVARCHAR(200)   NULL,
    incorporation_date               DATE            NULL,
    src_entity_id                    NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 2,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_entity PRIMARY KEY (entity_enterprise_key)
);
GO

-- 3.5 silver.portfolio_entity_ownership
CREATE TABLE silver.portfolio_entity_ownership (
    portfolio_enterprise_key         NVARCHAR(100)   NOT NULL,
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    ownership_pct                    DECIMAL(5,4)    NOT NULL,
    effective_date                   DATE            NOT NULL,
    end_date                         DATE            NULL,
    src_ownership_id                 NVARCHAR(50)    NULL,
    _source_system_id                INT             NOT NULL DEFAULT 2,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NULL,
    CONSTRAINT pk_silver_pe_own PRIMARY KEY (portfolio_enterprise_key, entity_enterprise_key, effective_date),
    CONSTRAINT ck_silver_pe_pct CHECK (ownership_pct > 0 AND ownership_pct <= 1.0),
    CONSTRAINT ck_silver_pe_dates CHECK (end_date IS NULL OR end_date > effective_date)
);
GO

-- 3.6 silver.entity_asset_ownership
CREATE TABLE silver.entity_asset_ownership (
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    asset_enterprise_key             NVARCHAR(100)   NOT NULL,
    ownership_pct                    DECIMAL(5,4)    NOT NULL,
    effective_date                   DATE            NOT NULL,
    end_date                         DATE            NULL,
    src_ownership_id                 NVARCHAR(50)    NULL,
    _source_system_id                INT             NOT NULL DEFAULT 2,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NULL,
    CONSTRAINT pk_silver_ea_own PRIMARY KEY (entity_enterprise_key, asset_enterprise_key, effective_date),
    CONSTRAINT ck_silver_ea_pct CHECK (ownership_pct > 0 AND ownership_pct <= 1.0),
    CONSTRAINT ck_silver_ea_dates CHECK (end_date IS NULL OR end_date > effective_date)
);
GO

-- 3.7 silver.asset
CREATE TABLE silver.asset (
    asset_enterprise_key             NVARCHAR(100)   NOT NULL,
    asset_name                       NVARCHAR(500)   NOT NULL,
    asset_short_name                 NVARCHAR(100)   NULL,
    asset_legal_name                 NVARCHAR(500)   NULL,
    asset_type                       NVARCHAR(100)   NOT NULL,
    asset_subtype                    NVARCHAR(100)   NULL,
    asset_status                     NVARCHAR(50)    NULL,
    location_country                 NVARCHAR(100)   NULL,
    location_region                  NVARCHAR(200)   NULL,
    acquisition_date                 DATE            NULL,
    last_valuation_date              DATE            NULL,
    last_valuation_amount            DECIMAL(18,2)   NULL,
    last_valuation_currency          NVARCHAR(3)     NULL,
    src_asset_id                     NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 3,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_asset PRIMARY KEY (asset_enterprise_key)
);
GO

-- 3.8 silver.security (composite: internal + WSO enrichment)
CREATE TABLE silver.security (
    security_enterprise_key          NVARCHAR(100)   NOT NULL,
    security_type                    NVARCHAR(100)   NOT NULL,
    security_group                   NVARCHAR(100)   NULL,
    security_name                    NVARCHAR(500)   NULL,
    security_status                  NVARCHAR(50)    NOT NULL DEFAULT 'ACTIVE',
    investment_team_enterprise_key   NVARCHAR(100)   NOT NULL,
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    asset_enterprise_key             NVARCHAR(100)   NOT NULL,
    bank_loan_id                     NVARCHAR(50)    NULL,
    cusip                            NVARCHAR(9)     NULL,
    isin                             NVARCHAR(12)    NULL,
    ticker                           NVARCHAR(20)    NULL,
    _wso_match_status                NVARCHAR(20)    NULL,
    _wso_match_key                   NVARCHAR(50)    NULL,
    _wso_match_confidence            NVARCHAR(20)    NULL,
    src_security_id                  NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 4,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_security PRIMARY KEY (security_enterprise_key)
);
GO

-- 3.9 silver.position_transaction (renamed from silver.[transaction])
CREATE TABLE silver.position_transaction (
    stm_transaction_id               NVARCHAR(50)    NOT NULL,
    portfolio_enterprise_key         NVARCHAR(100)   NOT NULL,
    entity_enterprise_key            NVARCHAR(100)   NOT NULL,
    security_enterprise_key          NVARCHAR(100)   NOT NULL,
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
    src_portfolio_id                 NVARCHAR(50)    NOT NULL,
    src_entity_id                    NVARCHAR(50)    NOT NULL,
    src_security_id                  NVARCHAR(50)    NOT NULL,
    _source_system_id                INT             NOT NULL DEFAULT 5,
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_pos_txn PRIMARY KEY (stm_transaction_id)
);
GO

-- 3.10 silver.ws_online_security
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
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NOT NULL,
    CONSTRAINT pk_silver_wso_sec PRIMARY KEY (wso_security_id)
);
GO

-- 3.11 silver.ws_online_pricing
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
    _bronze_record_id                NVARCHAR(36)    NULL,
    _source_modified_at              DATETIME2       NULL,
    _conformed_at                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    _conformed_by                    NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    _row_hash                        VARBINARY(32)   NULL,
    CONSTRAINT pk_silver_wso_price PRIMARY KEY (wso_security_id, price_date)
);
GO

-- 3.12 silver.quarantine (universal — single table for all silver failures)
CREATE TABLE silver.quarantine (
    quarantine_id           INT IDENTITY(1,1) NOT NULL,
    source_table            NVARCHAR(200)   NOT NULL,   -- which silver table was targeted
    raw_payload             NVARCHAR(MAX)   NOT NULL,   -- original bronze row as JSON
    failed_rule             NVARCHAR(100)   NOT NULL,
    failure_detail          NVARCHAR(500)   NULL,
    quarantined_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    quarantined_by          NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    resolution_status       NVARCHAR(20)    NOT NULL DEFAULT 'PENDING',
    resolved_at             DATETIME2       NULL,
    resolved_by             NVARCHAR(255)   NULL,
    resolution_notes        NVARCHAR(500)   NULL,
    CONSTRAINT pk_silver_quarantine PRIMARY KEY (quarantine_id),
    CONSTRAINT ck_quarantine_status CHECK (resolution_status IN ('PENDING','RESOLVED','REJECTED','REPROCESSED'))
);
GO


-- ============================================================================
-- PART 4: GOLD DIMENSION TABLES (6)
-- ============================================================================

-- 4.1 gold.investment_team_dimension
CREATE TABLE gold.investment_team_dimension (
    investment_team_key             INT IDENTITY(1,1) NOT NULL,
    investment_team_enterprise_key  NVARCHAR(100)   NOT NULL,
    investment_team_name            NVARCHAR(500)   NOT NULL,
    investment_team_short_name      NVARCHAR(100)   NULL,
    start_date                      DATE            NOT NULL,
    stop_date                       DATE            NULL,
    _row_hash                       VARBINARY(32)   NULL,
    created_date                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                      NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by                     NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_investment_team PRIMARY KEY (investment_team_key),
    CONSTRAINT uq_inv_team_ek UNIQUE (investment_team_enterprise_key)
);
GO

-- 4.2 gold.portfolio_group_dimension
CREATE TABLE gold.portfolio_group_dimension (
    portfolio_group_key             INT IDENTITY(1,1) NOT NULL,
    portfolio_group_enterprise_key  NVARCHAR(100)   NOT NULL,
    portfolio_group_name            NVARCHAR(500)   NOT NULL,
    portfolio_group_short_name      NVARCHAR(100)   NULL,
    portfolio_group_description     NVARCHAR(MAX)   NULL,
    investment_team_key             INT             NOT NULL,
    vintage_year                    INT             NULL,
    strategy                        NVARCHAR(200)   NULL,
    committed_capital               DECIMAL(18,2)   NULL,
    committed_capital_currency      NVARCHAR(3)     NULL,
    fund_status                     NVARCHAR(50)    NULL,
    _row_hash                       VARBINARY(32)   NULL,
    created_date                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                      NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by                     NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_portfolio_group PRIMARY KEY (portfolio_group_key),
    CONSTRAINT uq_pg_ek UNIQUE (portfolio_group_enterprise_key),
    CONSTRAINT fk_pg_team FOREIGN KEY (investment_team_key)
        REFERENCES gold.investment_team_dimension (investment_team_key)
);
GO

-- 4.3 gold.portfolio_dimension
CREATE TABLE gold.portfolio_dimension (
    portfolio_key                   INT IDENTITY(1,1) NOT NULL,
    portfolio_enterprise_key        NVARCHAR(100)   NOT NULL,
    portfolio_name                  NVARCHAR(500)   NOT NULL,
    portfolio_short_name            NVARCHAR(100)   NULL,
    portfolio_group_key             INT             NOT NULL,
    _row_hash                       VARBINARY(32)   NULL,
    created_date                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                      NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by                     NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_portfolio PRIMARY KEY (portfolio_key),
    CONSTRAINT uq_port_ek UNIQUE (portfolio_enterprise_key),
    CONSTRAINT fk_portfolio_group FOREIGN KEY (portfolio_group_key)
        REFERENCES gold.portfolio_group_dimension (portfolio_group_key)
);
GO

-- 4.4 gold.entity_dimension
CREATE TABLE gold.entity_dimension (
    entity_key                      INT IDENTITY(1,1) NOT NULL,
    entity_enterprise_key           NVARCHAR(100)   NOT NULL,
    entity_name                     NVARCHAR(500)   NOT NULL,
    entity_short_name               NVARCHAR(100)   NULL,
    entity_legal_name               NVARCHAR(500)   NULL,
    entity_type                     NVARCHAR(100)   NULL,
    entity_status                   NVARCHAR(50)    NULL,
    incorporation_jurisdiction      NVARCHAR(200)   NULL,
    incorporation_date              DATE            NULL,
    _row_hash                       VARBINARY(32)   NULL,
    created_date                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                      NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by                     NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_entity PRIMARY KEY (entity_key),
    CONSTRAINT uq_entity_ek UNIQUE (entity_enterprise_key)
);
GO

-- 4.5 gold.asset_dimension
CREATE TABLE gold.asset_dimension (
    asset_key                       INT IDENTITY(1,1) NOT NULL,
    asset_enterprise_key            NVARCHAR(100)   NOT NULL,
    asset_name                      NVARCHAR(500)   NOT NULL,
    asset_short_name                NVARCHAR(100)   NULL,
    asset_legal_name                NVARCHAR(500)   NULL,
    asset_type                      NVARCHAR(100)   NOT NULL,
    asset_subtype                   NVARCHAR(100)   NULL,
    asset_status                    NVARCHAR(50)    NULL,
    location_country                NVARCHAR(100)   NULL,
    location_region                 NVARCHAR(200)   NULL,
    acquisition_date                DATE            NULL,
    last_valuation_date             DATE            NULL,
    last_valuation_amount           DECIMAL(18,2)   NULL,
    last_valuation_currency         NVARCHAR(3)     NULL,
    _row_hash                       VARBINARY(32)   NULL,
    created_date                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                      NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by                     NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_asset PRIMARY KEY (asset_key),
    CONSTRAINT uq_asset_ek UNIQUE (asset_enterprise_key)
);
GO

-- 4.6 gold.security_dimension
CREATE TABLE gold.security_dimension (
    security_key                    INT IDENTITY(1,1) NOT NULL,
    security_enterprise_key         NVARCHAR(100)   NOT NULL,
    security_type                   NVARCHAR(100)   NOT NULL,
    security_group                  NVARCHAR(100)   NULL,
    security_name                   NVARCHAR(500)   NULL,
    security_status                 NVARCHAR(50)    NOT NULL DEFAULT 'ACTIVE',
    investment_team_key             INT             NOT NULL,
    entity_key                      INT             NOT NULL,
    asset_key                       INT             NOT NULL,
    bank_loan_id                    NVARCHAR(50)    NULL,
    cusip                           NVARCHAR(9)     NULL,
    isin                            NVARCHAR(12)    NULL,
    ticker                          NVARCHAR(20)    NULL,
    _row_hash                       VARBINARY(32)   NULL,
    created_date                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                      NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date                   DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by                     NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_security PRIMARY KEY (security_key),
    CONSTRAINT uq_security_ek UNIQUE (security_enterprise_key),
    CONSTRAINT fk_sec_team FOREIGN KEY (investment_team_key)
        REFERENCES gold.investment_team_dimension (investment_team_key),
    CONSTRAINT fk_sec_entity FOREIGN KEY (entity_key)
        REFERENCES gold.entity_dimension (entity_key),
    CONSTRAINT fk_sec_asset FOREIGN KEY (asset_key)
        REFERENCES gold.asset_dimension (asset_key)
);
GO


-- ============================================================================
-- PART 5: GOLD FACT TABLES (2) + BRIDGE TABLES (3)
-- ============================================================================

-- 5.1 gold.position_transactions_fact
CREATE TABLE gold.position_transactions_fact (
    position_transaction_fact_key   INT IDENTITY(1,1) NOT NULL,
    portfolio_key                   INT             NOT NULL,
    entity_key                      INT             NOT NULL,
    security_key                    INT             NOT NULL,
    as_of_date                      DATE            NOT NULL,
    transaction_type                NVARCHAR(100)   NOT NULL,
    transaction_category            NVARCHAR(100)   NULL,
    transaction_status              NVARCHAR(50)    NOT NULL,
    source_system_key               INT             NOT NULL,
    source_system_transaction_id    NVARCHAR(200)   NOT NULL,
    source_system_transaction_type  NVARCHAR(100)   NULL,
    transaction_amount_portfolio    DECIMAL(18,4)   NULL,
    transaction_amount_local        DECIMAL(18,4)   NULL,
    transaction_amount_usd          DECIMAL(18,4)   NULL,
    base_fx_rate                    DECIMAL(18,8)   NULL,
    quantity                        DECIMAL(18,6)   NULL,
    order_id                        NVARCHAR(200)   NULL,
    order_date                      DATE            NULL,
    order_status                    NVARCHAR(50)    NULL,
    created_date                    DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by                      NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_position_txn PRIMARY KEY (position_transaction_fact_key),
    CONSTRAINT fk_ptxn_portfolio FOREIGN KEY (portfolio_key)
        REFERENCES gold.portfolio_dimension (portfolio_key),
    CONSTRAINT fk_ptxn_entity FOREIGN KEY (entity_key)
        REFERENCES gold.entity_dimension (entity_key),
    CONSTRAINT fk_ptxn_security FOREIGN KEY (security_key)
        REFERENCES gold.security_dimension (security_key),
    CONSTRAINT fk_ptxn_source FOREIGN KEY (source_system_key)
        REFERENCES meta.source_systems (source_system_id),
    CONSTRAINT uq_ptxn_source_id UNIQUE (source_system_transaction_id)
);
GO

-- 5.2 gold.position_fact
CREATE TABLE gold.position_fact (
    position_fact_key               INT IDENTITY(1,1) NOT NULL,
    portfolio_key                   INT             NOT NULL,
    entity_key                      INT             NOT NULL,
    security_key                    INT             NOT NULL,
    as_of_date                      DATE            NOT NULL,
    position_type                   NVARCHAR(100)   NULL,
    transaction_type                NVARCHAR(100)   NULL,
    transaction_amount_portfolio    DECIMAL(18,4)   NULL,
    transaction_amount_local        DECIMAL(18,4)   NULL,
    transaction_amount_usd          DECIMAL(18,4)   NULL,
    CONSTRAINT pk_position PRIMARY KEY (position_fact_key),
    CONSTRAINT fk_pos_portfolio FOREIGN KEY (portfolio_key)
        REFERENCES gold.portfolio_dimension (portfolio_key),
    CONSTRAINT fk_pos_entity FOREIGN KEY (entity_key)
        REFERENCES gold.entity_dimension (entity_key),
    CONSTRAINT fk_pos_security FOREIGN KEY (security_key)
        REFERENCES gold.security_dimension (security_key)
);
GO

-- 5.3 gold.portfolio_entity_bridge (NEW: ownership % with temporal SCD)
CREATE TABLE gold.portfolio_entity_bridge (
    portfolio_key           INT             NOT NULL,
    entity_key              INT             NOT NULL,
    ownership_pct           DECIMAL(5,4)    NOT NULL,
    effective_date          DATE            NOT NULL,
    end_date                DATE            NULL,
    source_system_key       INT             NOT NULL,
    created_date            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by              NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date           DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by             NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_portfolio_entity PRIMARY KEY (portfolio_key, entity_key, effective_date),
    CONSTRAINT fk_pe_portfolio FOREIGN KEY (portfolio_key)
        REFERENCES gold.portfolio_dimension (portfolio_key),
    CONSTRAINT fk_pe_entity FOREIGN KEY (entity_key)
        REFERENCES gold.entity_dimension (entity_key),
    CONSTRAINT fk_pe_source FOREIGN KEY (source_system_key)
        REFERENCES meta.source_systems (source_system_id),
    CONSTRAINT ck_pe_ownership CHECK (ownership_pct > 0 AND ownership_pct <= 1.0),
    CONSTRAINT ck_pe_dates CHECK (end_date IS NULL OR end_date > effective_date)
);
GO

-- 5.4 gold.entity_asset_bridge (NEW: ownership % with temporal SCD)
CREATE TABLE gold.entity_asset_bridge (
    entity_key              INT             NOT NULL,
    asset_key               INT             NOT NULL,
    ownership_pct           DECIMAL(5,4)    NOT NULL,
    effective_date          DATE            NOT NULL,
    end_date                DATE            NULL,
    source_system_key       INT             NOT NULL,
    created_date            DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    created_by              NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    modified_date           DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    modified_by             NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT pk_entity_asset PRIMARY KEY (entity_key, asset_key, effective_date),
    CONSTRAINT fk_ea_entity FOREIGN KEY (entity_key)
        REFERENCES gold.entity_dimension (entity_key),
    CONSTRAINT fk_ea_asset FOREIGN KEY (asset_key)
        REFERENCES gold.asset_dimension (asset_key),
    CONSTRAINT fk_ea_source FOREIGN KEY (source_system_key)
        REFERENCES meta.source_systems (source_system_id),
    CONSTRAINT ck_ea_ownership CHECK (ownership_pct > 0 AND ownership_pct <= 1.0),
    CONSTRAINT ck_ea_dates CHECK (end_date IS NULL OR end_date > effective_date)
);
GO

-- 5.5 gold.position_team_bridge (M:N position <-> team allocation)
CREATE TABLE gold.position_team_bridge (
    position_fact_key               INT             NOT NULL,
    investment_team_key             INT             NOT NULL,
    allocation_pct                  DECIMAL(5,4)    NOT NULL,
    CONSTRAINT pk_position_team PRIMARY KEY (position_fact_key, investment_team_key),
    CONSTRAINT fk_ptb_position FOREIGN KEY (position_fact_key)
        REFERENCES gold.position_fact (position_fact_key),
    CONSTRAINT fk_ptb_team FOREIGN KEY (investment_team_key)
        REFERENCES gold.investment_team_dimension (investment_team_key),
    CONSTRAINT ck_allocation_range CHECK (allocation_pct > 0 AND allocation_pct <= 1.0)
);
GO


-- ============================================================================
-- PART 6: AUDIT TABLE
-- ============================================================================
CREATE TABLE audit.etl_run_log (
    run_id                  UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
    pipeline_code           NVARCHAR(200)   NOT NULL,
    target_layer            NVARCHAR(20)    NOT NULL,
    target_table            NVARCHAR(500)   NOT NULL,
    operation               NVARCHAR(50)    NOT NULL,
    rows_read               BIGINT          NULL,
    rows_inserted           BIGINT          NULL,
    rows_updated            BIGINT          NULL,
    rows_deleted            BIGINT          NULL,
    rows_quarantined        BIGINT          NULL,
    start_time              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    end_time                DATETIME2       NULL,
    status                  NVARCHAR(50)    NOT NULL DEFAULT 'RUNNING',
    error_message           NVARCHAR(MAX)   NULL,
    executed_by             NVARCHAR(255)   NOT NULL DEFAULT SYSTEM_USER,
    created_at              DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_etl_run PRIMARY KEY (run_id),
    CONSTRAINT ck_etl_status CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    CONSTRAINT ck_etl_layer CHECK (target_layer IN ('BRONZE','SILVER','GOLD'))
);
GO


-- ============================================================================
-- PART 7: NONCLUSTERED INDEXES
-- ============================================================================

-- Bronze (filter columns)
CREATE NONCLUSTERED INDEX ix_enterprise_raw_type ON bronze.src_enterprise_raw(_record_type) INCLUDE (_batch_id);
CREATE NONCLUSTERED INDEX ix_entity_mgmt_raw_type ON bronze.src_entity_mgmt_raw(_record_type) INCLUDE (_batch_id);
CREATE NONCLUSTERED INDEX ix_ws_online_raw_type ON bronze.src_ws_online_raw(_record_type) INCLUDE (_batch_id);
CREATE NONCLUSTERED INDEX ix_enterprise_raw_ingested ON bronze.src_enterprise_raw(_ingested_at);
CREATE NONCLUSTERED INDEX ix_entity_mgmt_raw_ingested ON bronze.src_entity_mgmt_raw(_ingested_at);
CREATE NONCLUSTERED INDEX ix_asset_mgmt_raw_ingested ON bronze.src_asset_mgmt_raw(_ingested_at);
CREATE NONCLUSTERED INDEX ix_security_mgmt_raw_ingested ON bronze.src_security_mgmt_raw(_ingested_at);
CREATE NONCLUSTERED INDEX ix_txn_mgmt_raw_ingested ON bronze.src_txn_mgmt_raw(_ingested_at);
CREATE NONCLUSTERED INDEX ix_ws_online_raw_ingested ON bronze.src_ws_online_raw(_ingested_at);
GO

-- Silver (FK reference columns)
CREATE NONCLUSTERED INDEX ix_silver_pg_team ON silver.portfolio_group(investment_team_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_port_pg ON silver.portfolio(portfolio_group_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_sec_team ON silver.security(investment_team_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_sec_entity ON silver.security(entity_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_sec_asset ON silver.security(asset_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_txn_portfolio ON silver.position_transaction(portfolio_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_txn_entity ON silver.position_transaction(entity_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_txn_security ON silver.position_transaction(security_enterprise_key);
CREATE NONCLUSTERED INDEX ix_silver_txn_date ON silver.position_transaction(as_of_date);
CREATE NONCLUSTERED INDEX ix_silver_wso_price_sec ON silver.ws_online_pricing(wso_security_id);
CREATE NONCLUSTERED INDEX ix_silver_quarantine_source ON silver.quarantine(source_table, resolution_status);
GO

-- Gold (fact dimension keys + date)
CREATE NONCLUSTERED INDEX ix_ptxn_portfolio ON gold.position_transactions_fact(portfolio_key);
CREATE NONCLUSTERED INDEX ix_ptxn_entity ON gold.position_transactions_fact(entity_key);
CREATE NONCLUSTERED INDEX ix_ptxn_security ON gold.position_transactions_fact(security_key);
CREATE NONCLUSTERED INDEX ix_ptxn_date ON gold.position_transactions_fact(as_of_date);
CREATE NONCLUSTERED INDEX ix_ptxn_source_txn ON gold.position_transactions_fact(source_system_transaction_id);
CREATE NONCLUSTERED INDEX ix_pos_portfolio ON gold.position_fact(portfolio_key);
CREATE NONCLUSTERED INDEX ix_pos_entity ON gold.position_fact(entity_key);
CREATE NONCLUSTERED INDEX ix_pos_security ON gold.position_fact(security_key);
CREATE NONCLUSTERED INDEX ix_pos_date ON gold.position_fact(as_of_date);
CREATE NONCLUSTERED INDEX ix_pe_bridge_entity ON gold.portfolio_entity_bridge(entity_key);
CREATE NONCLUSTERED INDEX ix_ea_bridge_asset ON gold.entity_asset_bridge(asset_key);
GO

-- Audit
CREATE NONCLUSTERED INDEX ix_etl_pipeline ON audit.etl_run_log(pipeline_code, start_time DESC);
CREATE NONCLUSTERED INDEX ix_etl_status ON audit.etl_run_log(status) INCLUDE (pipeline_code, start_time);
GO


-- ============================================================================
-- PART 8: VERIFICATION
-- ============================================================================
PRINT '=== TABLE COUNTS BY SCHEMA ==='
SELECT
    s.name AS [schema],
    COUNT(*) AS table_count
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('meta','bronze','silver','gold','audit')
GROUP BY s.name
ORDER BY s.name;

-- Expected: meta=14, bronze=6, silver=12 (11 + quarantine), gold=11, audit=1 -> 44 total
GO
