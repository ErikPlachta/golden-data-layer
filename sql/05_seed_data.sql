-- ============================================================================
-- GOLDEN DATA LAYER: SEED DATA (6-Source-System Model)
-- Target: Azure SQL Edge Developer (SQL Server 15.x compatible)
-- Date: 2026-02-09
-- Depends on: 01_ddl.sql, 02_meta_programmability.sql, 03_audit.sql, 04_silver.sql
-- ============================================================================
-- Data relationships:
--   3 investment teams → 5 portfolio groups (funds) → 7 portfolios
--   5 entities → 7 assets → 8 securities
--   6 WSO security references (4 matched, 1 ambiguous, 1 unmatched to internal)
--   Transactions: parameterized by @start_date / @end_date
--   Deliberate quality failures: 7 bad rows across sources for quarantine testing
-- ============================================================================
USE GoldenDataLayer;
GO

-- ============================================================================
-- CONFIGURATION: Edit the three dates below to change seed/transaction windows.
-- These values appear as literals; search-replace to change them globally:
--   Seed date:       2025-01-01  (meta timestamps, bronze _ingested_at)
--   Txn start date:  2025-01-15  (transaction generator start)
--   Txn end date:    2025-03-31  (transaction generator end)
-- ============================================================================


-- ============================================================================
-- PART 1: META SEED DATA
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1.1 Source Systems (6)
-- ----------------------------------------------------------------------------
SET IDENTITY_INSERT meta.source_systems ON;
INSERT INTO meta.source_systems (source_system_id, system_code, system_name, system_type, connectivity_method, connection_details, data_formats, owning_business_unit, data_steward, technical_owner, environment)
VALUES
(1, 'SRC_ENTERPRISE',   'Enterprise Data Platform',         'REFERENCE_DATA',         'JDBC',
 '{"host":"enterprise-db.internal","port":1433,"database":"EnterpriseRef","ssl":true}',
 '["JDBC_RESULTSET"]', 'Investment Operations', 'M. Thompson', 'Data Engineering', 'PROD'),

(2, 'SRC_ENTITY_MGMT',  'Source Entity Management',         'ENTITY_MANAGEMENT',      'REST_API',
 '{"base_url":"https://entity-mgmt.internal/api/v2","auth":"oauth2","rate_limit":"1000/min"}',
 '["JSON"]', 'Portfolio Operations', 'J. Martinez', 'Data Engineering', 'PROD'),

(3, 'SRC_ASSET_MGMT',   'Source Asset Management',          'ASSET_MANAGEMENT',       'REST_API',
 '{"base_url":"https://asset-mgmt.internal/api/v1","auth":"oauth2","rate_limit":"500/min"}',
 '["JSON"]', 'Real Assets Operations', 'K. Williams', 'Data Engineering', 'PROD'),

(4, 'SRC_SECURITY_MGMT','Source Security Management',       'SECURITY_MASTER',        'REST_API',
 '{"base_url":"https://sec-master.internal/api/v2","auth":"oauth2","rate_limit":"2000/min"}',
 '["JSON"]', 'Trading Operations', 'L. Garcia', 'Data Engineering', 'PROD'),

(5, 'SRC_TXN_MGMT',     'Source Transaction Management',   'TRANSACTION_MANAGEMENT', 'JDBC',
 '{"host":"txn-db.internal","port":5432,"database":"transactions","ssl":true}',
 '["JDBC_RESULTSET"]', 'Trading Operations', 'R. Chen', 'Data Engineering', 'PROD'),

(6, 'SRC_WS_ONLINE',    'Source Wall Street Online',        'MARKET_DATA',            'REST_API',
 '{"base_url":"https://api.wallstreetonline.example/v1","auth":"api_key","rate_limit":"500/min"}',
 '["JSON","CSV"]', 'Market Data Services', 'A. Patel', 'Data Engineering', 'PROD');
SET IDENTITY_INSERT meta.source_systems OFF;
GO

-- ----------------------------------------------------------------------------
-- 1.2 Ingestion Pipelines (7)
-- ----------------------------------------------------------------------------
SET IDENTITY_INSERT meta.ingestion_pipelines ON;
INSERT INTO meta.ingestion_pipelines (pipeline_id, source_system_id, pipeline_code, pipeline_name, description, ingestion_pattern, schedule_type, schedule_expression, target_bronze_table, target_silver_table, target_gold_tables, managing_owner, sla_minutes)
VALUES
(1, 1, 'PL_ENTERPRISE_DAILY', 'Enterprise Data Daily Extract',
 'Pulls investment teams, portfolio groups (funds), and portfolios from Enterprise Data Platform.',
 'INCREMENTAL_CDC', 'CRON', '0 1 * * *',
 'bronze.src_enterprise_raw', 'silver.investment_team,silver.portfolio_group,silver.portfolio',
 '["gold.investment_team_dimension","gold.portfolio_group_dimension","gold.portfolio_dimension"]',
 'Data Engineering', 45),

(2, 2, 'PL_ENTITY_DAILY', 'Entity Management Daily Extract',
 'Pulls entities, portfolio-entity ownership, and entity-asset ownership from Source Entity Management.',
 'INCREMENTAL_CDC', 'CRON', '0 2 * * *',
 'bronze.src_entity_mgmt_raw', 'silver.entity,silver.portfolio_entity_ownership,silver.entity_asset_ownership',
 '["gold.entity_dimension","gold.portfolio_entity_bridge","gold.entity_asset_bridge"]',
 'Data Engineering', 60),

(3, 3, 'PL_ASSET_DAILY', 'Asset Management Daily Extract',
 'Pulls asset master data, valuations, and lifecycle events from Source Asset Management.',
 'INCREMENTAL_CDC', 'CRON', '0 2 * * *',
 'bronze.src_asset_mgmt_raw', 'silver.asset',
 '["gold.asset_dimension"]',
 'Data Engineering', 45),

(4, 6, 'PL_MARKET_DAILY', 'Wall Street Online Market Data',
 'Pulls public security reference data and daily pricing from Source Wall Street Online.',
 'FULL_LOAD', 'CRON', '0 3 * * *',
 'bronze.src_ws_online_raw', 'silver.ws_online_security,silver.ws_online_pricing',
 '[]',
 'Data Engineering', 45),

(5, 4, 'PL_SECURITY_DAILY', 'Security Master Daily Build',
 'Pulls internal security records, runs composite assembly with WSO data. Depends on PL_MARKET_DAILY.',
 'INCREMENTAL_CDC', 'CRON', '0 4 * * *',
 'bronze.src_security_mgmt_raw', 'silver.security',
 '["gold.security_dimension"]',
 'Data Engineering', 60),

(6, 5, 'PL_TXN_DAILY', 'Transaction Management Daily Extract',
 'Pulls daily transaction records. Depends on all dimension pipelines.',
 'INCREMENTAL_CDC', 'CRON', '0 5 * * *',
 'bronze.src_txn_mgmt_raw', 'silver.transaction',
 '["gold.position_transactions_fact"]',
 'Data Engineering', 90),

(7, 5, 'PL_POSITION_SUMMARY', 'Position Fact Summarization',
 'Summarizes transactions into positions with team bridge allocation. Depends on PL_TXN_DAILY.',
 'FULL_LOAD', 'CRON', '0 6 * * *',
 NULL, NULL,
 '["gold.position_fact","gold.position_team_bridge"]',
 'Data Engineering', 30);
SET IDENTITY_INSERT meta.ingestion_pipelines OFF;
GO

-- ----------------------------------------------------------------------------
-- 1.3 Key Registry (21 keys)
-- ----------------------------------------------------------------------------
SET IDENTITY_INSERT meta.key_registry ON;
INSERT INTO meta.key_registry (key_id, source_system_id, key_name, key_aliases, key_type, data_type, example_values, source_table, source_column, databricks_table, databricks_column, description)
VALUES
-- Enterprise keys (canonical)
( 1, 1, 'investment_team_enterprise_key', '["team_ek","it_ek"]',   'NATURAL', 'NVARCHAR(100)', 'IT-001, IT-002',
  NULL, NULL, 'gold.investment_team_dimension', 'investment_team_enterprise_key', 'Canonical ID for investment teams. Owned by enterprise_data.'),
( 2, 1, 'portfolio_group_enterprise_key', '["pg_ek","fund_ek"]',   'NATURAL', 'NVARCHAR(100)', 'PG-001, PG-002',
  NULL, NULL, 'gold.portfolio_group_dimension', 'portfolio_group_enterprise_key', 'Canonical ID for portfolio groups (funds). Owned by enterprise_data.'),
( 3, 1, 'portfolio_enterprise_key',       '["port_ek"]',           'NATURAL', 'NVARCHAR(100)', 'P-001, P-002',
  NULL, NULL, 'gold.portfolio_dimension', 'portfolio_enterprise_key', 'Canonical ID for portfolios. Owned by enterprise_data.'),
( 4, 2, 'entity_enterprise_key',          '["ent_ek"]',            'NATURAL', 'NVARCHAR(100)', 'E-001, E-002',
  NULL, NULL, 'gold.entity_dimension', 'entity_enterprise_key', 'Canonical ID for entities. Owned by Source_Entity_Mgmt.'),
( 5, 3, 'asset_enterprise_key',           '["asset_ek"]',          'NATURAL', 'NVARCHAR(100)', 'A-001, A-002',
  NULL, NULL, 'gold.asset_dimension', 'asset_enterprise_key', 'Canonical ID for assets. Owned by Source_Asset_Mgmt.'),
( 6, 4, 'security_enterprise_key',        '["sec_ek"]',            'NATURAL', 'NVARCHAR(100)', 'SEC-001, SEC-002',
  NULL, NULL, 'gold.security_dimension', 'security_enterprise_key', 'Canonical ID for securities. Owned by Source_Security_Mgmt.'),

-- Source: enterprise_data native keys
( 7, 1, 'ent_investment_team_id', '["ent_it_id"]',   'PRIMARY', 'NVARCHAR(50)', 'ENT-IT-10001, ENT-IT-10002',
  'enterprise.investment_teams', 'investment_team_id', 'bronze.src_enterprise_raw', 'investment_team_id', 'Enterprise data native investment team ID'),
( 8, 1, 'ent_portfolio_group_id', '["ent_pg_id"]',   'PRIMARY', 'NVARCHAR(50)', 'ENT-PG-20001, ENT-PG-20002',
  'enterprise.portfolio_groups', 'portfolio_group_id', 'bronze.src_enterprise_raw', 'portfolio_group_id', 'Enterprise data native portfolio group ID'),
( 9, 1, 'ent_portfolio_id',       '["ent_p_id"]',    'PRIMARY', 'NVARCHAR(50)', 'ENT-P-30001, ENT-P-30002',
  'enterprise.portfolios', 'portfolio_id', 'bronze.src_enterprise_raw', 'portfolio_id', 'Enterprise data native portfolio ID'),

-- Source: Entity Management native keys
(10, 2, 'sem_entity_id',          '["sem_e_id"]',    'PRIMARY', 'NVARCHAR(50)', 'SEM-E-20001, SEM-E-20002',
  'entity_mgmt.entities', 'entity_id', 'bronze.src_entity_mgmt_raw', 'entity_id', 'Source Entity Mgmt native entity ID'),
(11, 2, 'sem_portfolio_ref_id',   '["sem_p_ref"]',   'FOREIGN', 'NVARCHAR(50)', 'SEM-P-30001',
  'entity_mgmt.ownership', 'portfolio_ref', 'bronze.src_entity_mgmt_raw', 'portfolio_ref', 'SEM portfolio FK reference'),
(12, 2, 'sem_asset_ref_id',       '["sem_a_ref"]',   'FOREIGN', 'NVARCHAR(50)', 'SEM-A-40001',
  'entity_mgmt.ownership', 'asset_ref', 'bronze.src_entity_mgmt_raw', 'asset_ref', 'SEM asset FK reference'),

-- Source: Asset Management native keys
(13, 3, 'sam_asset_id',           '["sam_a_id"]',    'PRIMARY', 'NVARCHAR(50)', 'SAM-A-40001, SAM-A-40002',
  'asset_mgmt.assets', 'asset_id', 'bronze.src_asset_mgmt_raw', 'asset_id', 'Source Asset Mgmt native asset ID'),

-- Source: Security Management native keys
(14, 4, 'ssm_security_id',        '["ssm_sec_id"]',  'PRIMARY', 'NVARCHAR(50)', 'SSM-SEC-50001, SSM-SEC-50002',
  'sec_master.securities', 'security_id', 'bronze.src_security_mgmt_raw', 'security_id', 'Source Security Mgmt native security ID'),

-- Source: Transaction Management native keys
(15, 5, 'stm_transaction_id',     '["stm_txn_id"]',  'PRIMARY', 'NVARCHAR(50)', 'STM-TXN-60001',
  'transactions.trade_log', 'transaction_id', 'bronze.src_txn_mgmt_raw', 'transaction_id', 'Source Txn Mgmt native transaction ID'),
(16, 5, 'stm_portfolio_id',       '["stm_p_id"]',    'FOREIGN', 'NVARCHAR(50)', 'STM-P-30001',
  'transactions.trade_log', 'portfolio_id', 'bronze.src_txn_mgmt_raw', 'portfolio_id', 'STM portfolio FK reference'),
(17, 5, 'stm_entity_id',          '["stm_e_id"]',    'FOREIGN', 'NVARCHAR(50)', 'STM-E-20001',
  'transactions.trade_log', 'entity_id', 'bronze.src_txn_mgmt_raw', 'entity_id', 'STM entity FK reference'),
(18, 5, 'stm_security_id',        '["stm_sec_id"]',  'FOREIGN', 'NVARCHAR(50)', 'STM-SEC-50001',
  'transactions.trade_log', 'security_id', 'bronze.src_txn_mgmt_raw', 'security_id', 'STM security FK reference'),

-- Source: Wall Street Online native keys
(19, 6, 'wso_security_id',        '["wso_sec_id"]',  'PRIMARY', 'NVARCHAR(50)', 'WSO-SEC-70001',
  'ws_online.securities', 'security_id', 'bronze.src_ws_online_raw', 'wso_security_id', 'WSO native security ID'),
(20, 6, 'wso_ticker',             '["symbol"]',      'NATURAL', 'NVARCHAR(20)', 'MER.RE, APX.IN',
  'ws_online.securities', 'ticker', 'bronze.src_ws_online_raw', 'ticker', 'Public market ticker symbol'),
(21, 6, 'wso_cusip',              NULL,               'NATURAL', 'NVARCHAR(9)',  '59156R100, 03783A100',
  'ws_online.securities', 'cusip', 'bronze.src_ws_online_raw', 'cusip', 'CUSIP identifier');
SET IDENTITY_INSERT meta.key_registry OFF;
GO

-- ----------------------------------------------------------------------------
-- 1.4 Key Crosswalks
-- ----------------------------------------------------------------------------
INSERT INTO meta.key_crosswalk (from_key_id, to_key_id, mapping_type, mapping_confidence, transformation_rule, conditions, bidirectional, validated_by, validation_date)
VALUES
( 7,  1, '1:1', 'EXACT', 'REPLACE(from_value, ''ENT-IT-'', ''IT-'')',    NULL, 1, 'M. Thompson', '2025-01-15'),
( 8,  2, '1:1', 'EXACT', 'REPLACE(from_value, ''ENT-PG-'', ''PG-'')',    NULL, 1, 'M. Thompson', '2025-01-15'),
( 9,  3, '1:1', 'EXACT', 'REPLACE(from_value, ''ENT-P-'', ''P-'')',      NULL, 1, 'M. Thompson', '2025-01-15'),
(10,  4, '1:1', 'EXACT', 'REPLACE(from_value, ''SEM-E-'', ''E-'')',      NULL, 1, 'J. Martinez', '2025-01-15'),
(11,  3, '1:1', 'EXACT', 'REPLACE(from_value, ''SEM-P-'', ''P-'')',      'FK ref to portfolio', 1, 'J. Martinez', '2025-01-15'),
(12,  5, '1:1', 'EXACT', 'REPLACE(from_value, ''SEM-A-'', ''A-'')',      'FK ref to asset',     1, 'J. Martinez', '2025-01-15'),
(13,  5, '1:1', 'EXACT', 'REPLACE(from_value, ''SAM-A-'', ''A-'')',      NULL, 1, 'K. Williams', '2025-01-15'),
(14,  6, '1:1', 'EXACT', 'REPLACE(from_value, ''SSM-SEC-'', ''SEC-'')',  NULL, 1, 'L. Garcia',   '2025-01-15'),
(16,  3, '1:1', 'EXACT', 'REPLACE(from_value, ''STM-P-'', ''P-'')',      'FK ref to portfolio', 1, 'R. Chen', '2025-01-15'),
(17,  4, '1:1', 'EXACT', 'REPLACE(from_value, ''STM-E-'', ''E-'')',      'FK ref to entity',    1, 'R. Chen', '2025-01-15'),
(18,  6, '1:1', 'EXACT', 'REPLACE(from_value, ''STM-SEC-'', ''SEC-'')',  'FK ref to security',  1, 'R. Chen', '2025-01-15'),
(19,  6, '1:1', 'EXACT', 'REPLACE(from_value, ''WSO-SEC-'', ''SEC-'')',  'External mapping via composite assembly', 1, 'A. Patel', '2025-01-15');
GO

-- 1.5 Crosswalk Paths
INSERT INTO meta.key_crosswalk_paths (from_key_id, to_key_id, hop_count, path_crosswalk_ids, path_description, path_reliability)
VALUES
(18, 5, 2, '[8,14]', 'STM security_id -> security_ek -> asset_ek via security_dimension', 'HIGH'),
(19, 4, 2, '[12]',   'WSO security_id -> security_ek -> entity_ek via security_dimension', 'HIGH');
GO

-- ----------------------------------------------------------------------------
-- 1.6 Quality Rules
-- ----------------------------------------------------------------------------
INSERT INTO meta.quality_rules (rule_code, rule_name, target_table, target_column, rule_expression, rule_type, severity, layer, owner)
VALUES
('NOT_NULL_EK',            'Enterprise Key Not Null',           'silver.*',                        NULL, 'enterprise_key IS NOT NULL',                                    'COMPLETENESS', 'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('ROW_HASH_NOT_NULL',      'Row Hash Computed',                 'silver.*',                        NULL, '_row_hash IS NOT NULL',                                          'COMPLETENESS', 'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('TEAM_NAME_NOT_EMPTY',    'Team Name Required',                'silver.investment_team',          'investment_team_name',      'LEN(TRIM(investment_team_name)) > 0',      'COMPLETENESS', 'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('START_DATE_VALID',       'Start Date Parseable',              'silver.investment_team',          'start_date',                'start_date IS NOT NULL',                    'VALIDITY',     'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('PG_TEAM_EXISTS',         'Portfolio Group Team FK Valid',     'silver.portfolio_group',          'investment_team_enterprise_key', 'EXISTS in silver.investment_team',       'REFERENTIAL',  'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('PORT_PG_EXISTS',         'Portfolio Group FK Valid',          'silver.portfolio',                'portfolio_group_enterprise_key','EXISTS in silver.portfolio_group',        'REFERENTIAL',  'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('ENTITY_NAME_NOT_EMPTY',  'Entity Name Required',             'silver.entity',                   'entity_name',               'LEN(TRIM(entity_name)) > 0',               'COMPLETENESS', 'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('PE_PCT_RANGE',           'Ownership Pct In Range',           'silver.portfolio_entity_ownership','ownership_pct',             'ownership_pct > 0 AND ownership_pct <= 1.0','VALIDITY',     'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('ASSET_TYPE_NOT_EMPTY',   'Asset Type Required',              'silver.asset',                    'asset_type',                'LEN(TRIM(asset_type)) > 0',                'COMPLETENESS', 'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('SEC_TYPE_VALID',         'Security Type Valid',              'silver.security',                 'security_type',             'security_type IN (EQUITY,SENIOR_DEBT,...)',  'VALIDITY',     'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('TXN_SECURITY_EXISTS',   'Transaction Security FK Valid',     'silver.transaction',              'security_enterprise_key',   'EXISTS in silver.security',                 'REFERENTIAL',  'EXPECT_OR_FAIL', 'SILVER', 'Data Engineering'),
('BRIDGE_ALLOC_SUM',      'Bridge Allocation Sums to 1.0',    'gold.position_team_bridge',       'allocation_pct',            'ABS(SUM(allocation_pct) - 1.0) <= 0.001',  'CONSISTENCY',  'EXPECT_OR_FAIL', 'GOLD',   'Data Engineering'),
('BRIDGE_PE_OWNERSHIP',   'Portfolio-Entity Ownership Valid',  'gold.portfolio_entity_bridge',    'ownership_pct',             'SUM(ownership_pct) per portfolio_key <= 1.0','CONSISTENCY', 'EXPECT_OR_FAIL', 'GOLD',   'Data Engineering');
GO

-- 1.7 Data Contracts
INSERT INTO meta.data_contracts (source_system_id, pipeline_id, contract_version, contract_status, schema_definition, delivery_sla_minutes, freshness_sla_minutes, volume_expectation, breaking_change_policy, owner, effective_date)
VALUES
(1, 1, 1, 'ACTIVE', '{"entities":["investment_team","portfolio_group","portfolio"]}', 45, 120, '{"typical_rows":50}', 'BLOCK', 'M. Thompson', '2025-01-01'),
(2, 2, 1, 'ACTIVE', '{"entities":["entity","portfolio_entity_ownership","entity_asset_ownership"]}', 60, 120, '{"typical_rows":500}', 'BLOCK', 'J. Martinez', '2025-01-01'),
(3, 3, 1, 'ACTIVE', '{"entities":["asset"]}', 45, 120, '{"typical_rows":200}', 'BLOCK', 'K. Williams', '2025-01-01'),
(4, 5, 1, 'ACTIVE', '{"entities":["security"]}', 60, 180, '{"typical_rows":300}', 'BLOCK', 'L. Garcia', '2025-01-01'),
(5, 6, 1, 'ACTIVE', '{"entities":["transaction"]}', 90, 180, '{"typical_rows":5000}', 'BLOCK', 'R. Chen', '2025-01-01'),
(6, 4, 1, 'ACTIVE', '{"entities":["wso_security","wso_pricing"]}', 45, 360, '{"typical_rows":25000}', 'WARN', 'A. Patel', '2025-01-01');
GO

-- 1.8 Consumers
INSERT INTO meta.consumers (consumer_name, consumer_type, consuming_tables, owning_team, contact, access_method, criticality, freshness_requirement, notification_channel)
VALUES
('Portfolio Performance Dashboard',  'DASHBOARD',    '["gold.position_fact","gold.portfolio_dimension","gold.position_team_bridge"]', 'Portfolio Analytics', 'analytics@example.com', 'SQL_WAREHOUSE', 'P0_REVENUE', 'DAILY', 'slack:#portfolio-alerts'),
('Transaction Reconciliation Report','DASHBOARD',    '["gold.position_transactions_fact","gold.entity_dimension","gold.security_dimension"]', 'Trading Operations', 'trading@example.com', 'SQL_WAREHOUSE', 'P1_OPERATIONS', 'DAILY', 'slack:#trading-ops'),
('Risk Analytics ML Model',          'ML_MODEL',     '["gold.position_fact","gold.security_dimension"]', 'Quantitative Research', 'quant@example.com', 'SQL_WAREHOUSE', 'P2_ANALYTICS', 'DAILY', 'email:quant@example.com'),
('External Auditor Extract',         'EXPORT',       '["gold.position_fact","gold.position_transactions_fact"]', 'Compliance', 'compliance@example.com', 'DELTA_SHARING', 'P1_OPERATIONS', 'WEEKLY', 'email:compliance@example.com');
GO

-- 1.9 Retention Policies
INSERT INTO meta.retention_policies (target_table, layer, retention_days, time_travel_days, log_retention_days, archive_after_days, purge_after_days, vacuum_strategy, regulatory_basis, owner)
VALUES
('bronze.src_enterprise_raw',     'BRONZE', 90,   7,  30,  60,   180,  'LITE_DAILY_FULL_WEEKLY', 'INTERNAL_POLICY',  'Data Engineering'),
('bronze.src_entity_mgmt_raw',    'BRONZE', 90,   7,  30,  60,   180,  'LITE_DAILY_FULL_WEEKLY', 'INTERNAL_POLICY',  'Data Engineering'),
('bronze.src_asset_mgmt_raw',     'BRONZE', 90,   7,  30,  60,   180,  'LITE_DAILY_FULL_WEEKLY', 'INTERNAL_POLICY',  'Data Engineering'),
('bronze.src_security_mgmt_raw',  'BRONZE', 90,   7,  30,  60,   180,  'LITE_DAILY_FULL_WEEKLY', 'INTERNAL_POLICY',  'Data Engineering'),
('bronze.src_txn_mgmt_raw',       'BRONZE', 365,  14, 30,  180,  NULL, 'LITE_DAILY_FULL_WEEKLY', 'SOX_7YR',          'Data Engineering'),
('bronze.src_ws_online_raw',      'BRONZE', 90,   7,  30,  60,   180,  'LITE_DAILY_FULL_WEEKLY', 'INTERNAL_POLICY',  'Data Engineering'),
('silver.transaction',             'SILVER', 2555, 30, 365, 730,  NULL, 'LITE_DAILY_FULL_WEEKLY', 'SOX_7YR',          'Data Engineering'),
('gold.position_fact',             'GOLD',   3650, 30, 365, 1825, NULL, 'LITE_DAILY_FULL_WEEKLY', 'SOX_7YR',          'Data Engineering'),
('gold.position_transactions_fact','GOLD',   3650, 30, 365, 1825, NULL, 'LITE_DAILY_FULL_WEEKLY', 'SOX_7YR',          'Data Engineering');
GO

-- 1.10 Business Glossary
INSERT INTO meta.business_glossary (business_term, definition, calculation_logic, mapped_tables, mapped_columns, domain, owner, synonyms)
VALUES
('Fund (Portfolio Group)', 'Pooled investment vehicle with vintage year, strategy, committed capital, and lifecycle.', NULL,
 '["gold.portfolio_group_dimension"]', '["vintage_year","strategy","committed_capital","fund_status"]',
 'Portfolio Management', 'Investment Operations', '["fund","vehicle","pool"]'),
('Ownership Percentage', 'Fractional ownership stake. Ranges >0% to 100%. Has effective_date/end_date for temporal tracking.', NULL,
 '["gold.portfolio_entity_bridge","gold.entity_asset_bridge"]', '["ownership_pct"]',
 'Portfolio Management', 'Portfolio Analytics', '["ownership stake","equity percentage"]'),
('Security Master', 'Authoritative composite record. Assembled from internal SSM + external WSO matching on BankLoanID/CUSIP/ISIN/ticker.', 'Composite assembly: SSM + WSO cascading match',
 '["gold.security_dimension","silver.security"]', '["security_enterprise_key","bank_loan_id","cusip","isin"]',
 'Trading Operations', 'Data Engineering', '["sec master","instrument master"]'),
('Enterprise Key', 'System-agnostic canonical identifier enabling cross-system resolution.', NULL,
 '["gold.*_dimension"]', '["*_enterprise_key"]',
 'Data Architecture', 'Data Engineering', '["EK","canonical key"]');
GO


-- ============================================================================
-- PART 2: BRONZE SEED DATA — DIMENSION RECORDS
-- ============================================================================

DECLARE @seed_batch NVARCHAR(100) = 'SEED-INIT-' + REPLACE('2025-01-01', '-', '');
DECLARE @seed_ts    DATETIME2     = CAST('2025-01-01' AS DATETIME2);
DECLARE @seed_ymd   NVARCHAR(8)   = REPLACE('2025-01-01', '-', '');

DECLARE @f_ent      NVARCHAR(100) = 'ent_'          + @seed_ymd + '.json';
DECLARE @f_sem      NVARCHAR(100) = 'sem_'          + @seed_ymd + '.json';
DECLARE @f_sam      NVARCHAR(100) = 'sam_'          + @seed_ymd + '.json';
DECLARE @f_ssm      NVARCHAR(100) = 'ssm_'          + @seed_ymd + '.json';
DECLARE @f_wso      NVARCHAR(100) = 'wso_'          + @seed_ymd + '.json';
DECLARE @f_wso_px   NVARCHAR(100) = 'wso_pricing_'  + @seed_ymd + '.json';

-- ============================================================================
-- 2.1 Source 1: enterprise_data
-- ============================================================================
-- 3 Investment Teams
INSERT INTO bronze.src_enterprise_raw (_batch_id, _ingested_at, _source_file, _record_type,
    investment_team_id, team_name, team_short_name, start_date, stop_date,
    portfolio_group_id, pg_name, pg_short_name, pg_description, pg_team_ref,
    vintage_year, strategy, committed_capital, committed_capital_ccy, fund_status,
    portfolio_id, port_name, port_short_name, port_pg_ref)
VALUES
(@seed_batch, @seed_ts, @f_ent, 'investment_team',
 'ENT-IT-10001','Real Assets Investment Team','Real Assets','2020-01-01',NULL,
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(@seed_batch, @seed_ts, @f_ent, 'investment_team',
 'ENT-IT-10002','Private Equity Investment Team','Private Equity','2020-01-01',NULL,
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(@seed_batch, @seed_ts, @f_ent, 'investment_team',
 'ENT-IT-10003','Credit Investment Team','Credit','2020-06-01',NULL,
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),

-- 5 Portfolio Groups (Funds)
(@seed_batch, @seed_ts, @f_ent, 'portfolio_group',
 NULL,NULL,NULL,NULL,NULL,
 'ENT-PG-20001','Real Assets Core Fund I','RA Core I','Flagship core fund','ENT-IT-10001',
 '2020','CORE','500000000.00','USD','INVESTING', NULL,NULL,NULL,NULL),
(@seed_batch, @seed_ts, @f_ent, 'portfolio_group',
 NULL,NULL,NULL,NULL,NULL,
 'ENT-PG-20002','Real Assets Core Fund II','RA Core II','Second vintage','ENT-IT-10001',
 '2023','CORE','750000000.00','USD','FUNDRAISING', NULL,NULL,NULL,NULL),
(@seed_batch, @seed_ts, @f_ent, 'portfolio_group',
 NULL,NULL,NULL,NULL,NULL,
 'ENT-PG-20003','PE Buyout Fund IV','PE Fund IV','Mid-market buyout','ENT-IT-10002',
 '2022','BUYOUT','1200000000.00','USD','INVESTING', NULL,NULL,NULL,NULL),
(@seed_batch, @seed_ts, @f_ent, 'portfolio_group',
 NULL,NULL,NULL,NULL,NULL,
 'ENT-PG-20004','Credit Opportunities Fund III','Credit III','Distressed credit','ENT-IT-10003',
 '2021','OPPORTUNISTIC','400000000.00','USD','HARVESTING', NULL,NULL,NULL,NULL),
(@seed_batch, @seed_ts, @f_ent, 'portfolio_group',
 NULL,NULL,NULL,NULL,NULL,
 'ENT-PG-20005','Infrastructure Value Fund I','Infra Value I','Infra value-add','ENT-IT-10001',
 '2024','VALUE_ADD','600000000.00','USD','FUNDRAISING', NULL,NULL,NULL,NULL),

-- 7 Portfolios
(@seed_batch, @seed_ts, @f_ent, 'portfolio',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
 'ENT-P-30001','RA Core I - Main','RA-I Main','ENT-PG-20001'),
(@seed_batch, @seed_ts, @f_ent, 'portfolio',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
 'ENT-P-30002','RA Core I - Co-Invest','RA-I CoInv','ENT-PG-20001'),
(@seed_batch, @seed_ts, @f_ent, 'portfolio',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
 'ENT-P-30003','RA Core II - Main','RA-II Main','ENT-PG-20002'),
(@seed_batch, @seed_ts, @f_ent, 'portfolio',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
 'ENT-P-30004','PE Buyout IV - Main','PE-IV Main','ENT-PG-20003'),
(@seed_batch, @seed_ts, @f_ent, 'portfolio',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
 'ENT-P-30005','PE Buyout IV - Co-Invest','PE-IV CoInv','ENT-PG-20003'),
(@seed_batch, @seed_ts, @f_ent, 'portfolio',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
 'ENT-P-30006','Credit Opp III - Main','Cred-III','ENT-PG-20004'),
(@seed_batch, @seed_ts, @f_ent, 'portfolio',
 NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
 'ENT-P-30007','Infra Value I - Main','Infra-I','ENT-PG-20005');

-- BAD: NULL team name
INSERT INTO bronze.src_enterprise_raw (_batch_id,_ingested_at,_source_file,_record_type,investment_team_id,team_name,team_short_name,start_date,stop_date,portfolio_group_id,pg_name,pg_short_name,pg_description,pg_team_ref,vintage_year,strategy,committed_capital,committed_capital_ccy,fund_status,portfolio_id,port_name,port_short_name,port_pg_ref)
VALUES (@seed_batch,@seed_ts,@f_ent,'investment_team','ENT-IT-10099',NULL,NULL,'2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
-- BAD: ghost team FK
INSERT INTO bronze.src_enterprise_raw (_batch_id,_ingested_at,_source_file,_record_type,investment_team_id,team_name,team_short_name,start_date,stop_date,portfolio_group_id,pg_name,pg_short_name,pg_description,pg_team_ref,vintage_year,strategy,committed_capital,committed_capital_ccy,fund_status,portfolio_id,port_name,port_short_name,port_pg_ref)
VALUES (@seed_batch,@seed_ts,@f_ent,'portfolio_group',NULL,NULL,NULL,NULL,NULL,'ENT-PG-20099','Ghost Fund','Ghost','Bad FK','ENT-IT-99999','2025','CORE','100000000','USD','FUNDRAISING',NULL,NULL,NULL,NULL);


-- ============================================================================
-- 2.2 Source 2: Source_Entity_Mgmt
-- ============================================================================
-- 5 Entities
INSERT INTO bronze.src_entity_mgmt_raw (_batch_id,_ingested_at,_source_file,_record_type,entity_id,entity_name,entity_short_name,entity_legal_name,entity_type,entity_status,incorporation_jurisdiction,incorporation_date,ownership_id,portfolio_ref,entity_ref,asset_ref,ownership_pct,effective_date,end_date)
VALUES
(@seed_batch,@seed_ts,@f_sem,'entity','SEM-E-20001','Meridian Office REIT','Meridian REIT','Meridian Office REIT LLC','LLC','ACTIVE','Delaware','2019-03-15',NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_sem,'entity','SEM-E-20002','Apex Industrial Holdings','Apex Industrial','Apex Industrial Holdings LP','LP','ACTIVE','Delaware','2020-06-01',NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_sem,'entity','SEM-E-20003','Vertex Software Corp','Vertex SW','Vertex Software Corporation','CORP','ACTIVE','Delaware','2018-11-20',NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_sem,'entity','SEM-E-20004','Coastal Infrastructure Partners','Coastal Infra','Coastal Infrastructure Partners LLC','LLC','ACTIVE','Cayman Islands','2021-02-10',NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_sem,'entity','SEM-E-20005','Summit Credit Opportunities','Summit Credit','Summit Credit Opportunities Fund LLC','LLC','ACTIVE','Delaware','2020-09-01',NULL,NULL,NULL,NULL,NULL,NULL,NULL);

-- Portfolio-Entity Ownership (6 rows)
INSERT INTO bronze.src_entity_mgmt_raw (_batch_id,_ingested_at,_source_file,_record_type,entity_id,entity_name,entity_short_name,entity_legal_name,entity_type,entity_status,incorporation_jurisdiction,incorporation_date,ownership_id,portfolio_ref,entity_ref,asset_ref,ownership_pct,effective_date,end_date)
VALUES
(@seed_batch,@seed_ts,@f_sem,'portfolio_entity_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-PE-001','SEM-P-30001','SEM-E-20001',NULL,'0.6000','2020-06-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'portfolio_entity_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-PE-002','SEM-P-30002','SEM-E-20001',NULL,'0.2500','2020-06-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'portfolio_entity_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-PE-003','SEM-P-30001','SEM-E-20002',NULL,'0.8000','2021-01-15',NULL),
(@seed_batch,@seed_ts,@f_sem,'portfolio_entity_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-PE-004','SEM-P-30004','SEM-E-20003',NULL,'1.0000','2022-03-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'portfolio_entity_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-PE-005','SEM-P-30007','SEM-E-20004',NULL,'0.5500','2024-06-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'portfolio_entity_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-PE-006','SEM-P-30006','SEM-E-20005',NULL,'1.0000','2021-10-01',NULL);

-- Entity-Asset Ownership (7 rows)
INSERT INTO bronze.src_entity_mgmt_raw (_batch_id,_ingested_at,_source_file,_record_type,entity_id,entity_name,entity_short_name,entity_legal_name,entity_type,entity_status,incorporation_jurisdiction,incorporation_date,ownership_id,portfolio_ref,entity_ref,asset_ref,ownership_pct,effective_date,end_date)
VALUES
(@seed_batch,@seed_ts,@f_sem,'entity_asset_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-EA-001',NULL,'SEM-E-20001','SEM-A-40001','1.0000','2020-06-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'entity_asset_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-EA-002',NULL,'SEM-E-20001','SEM-A-40002','0.7500','2021-03-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'entity_asset_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-EA-003',NULL,'SEM-E-20002','SEM-A-40003','1.0000','2021-01-15',NULL),
(@seed_batch,@seed_ts,@f_sem,'entity_asset_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-EA-004',NULL,'SEM-E-20003','SEM-A-40004','1.0000','2022-03-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'entity_asset_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-EA-005',NULL,'SEM-E-20004','SEM-A-40005','1.0000','2024-06-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'entity_asset_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-EA-006',NULL,'SEM-E-20004','SEM-A-40006','0.6500','2024-08-01',NULL),
(@seed_batch,@seed_ts,@f_sem,'entity_asset_ownership',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'OWN-EA-007',NULL,'SEM-E-20005','SEM-A-40007','1.0000','2021-10-01',NULL);

-- BAD: entity with whitespace-only name
INSERT INTO bronze.src_entity_mgmt_raw (_batch_id,_ingested_at,_source_file,_record_type,entity_id,entity_name,entity_short_name,entity_legal_name,entity_type,entity_status,incorporation_jurisdiction,incorporation_date,ownership_id,portfolio_ref,entity_ref,asset_ref,ownership_pct,effective_date,end_date)
VALUES (@seed_batch,@seed_ts,@f_sem,'entity','SEM-E-20099','   ',NULL,NULL,'LLC','ACTIVE','Delaware','2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL,NULL);


-- ============================================================================
-- 2.3 Source 3: Source_Asset_Mgmt → 7 assets
-- ============================================================================
INSERT INTO bronze.src_asset_mgmt_raw (_batch_id,_ingested_at,_source_file,asset_id,asset_name,asset_short_name,asset_legal_name,asset_type,asset_subtype,asset_status,location_country,location_region,acquisition_date,last_valuation_date,last_valuation_amount,last_valuation_currency)
VALUES
(@seed_batch,@seed_ts,@f_sam,'SAM-A-40001','Meridian Tower Complex','Meridian Tower','Meridian Tower Office Complex','REAL_ESTATE','OFFICE','ACTIVE','United States','Northeast','2020-06-15','2024-12-31','125000000.00','USD'),
(@seed_batch,@seed_ts,@f_sam,'SAM-A-40002','Meridian Suburban Campus','Meridian Suburban','Meridian Suburban Office Campus LLC','REAL_ESTATE','OFFICE','ACTIVE','United States','Southeast','2021-03-01','2024-12-31','45000000.00','USD'),
(@seed_batch,@seed_ts,@f_sam,'SAM-A-40003','Apex Distribution Center Portfolio','Apex DC','Apex Industrial Distribution Centers','REAL_ESTATE','INDUSTRIAL','ACTIVE','United States','Midwest','2021-01-15','2024-12-31','89000000.00','USD'),
(@seed_batch,@seed_ts,@f_sam,'SAM-A-40004','Vertex SaaS Platform','Vertex SaaS','Vertex Enterprise Software Platform','PRIVATE_EQUITY','SOFTWARE','ACTIVE','United States','West','2022-03-01','2024-12-31','210000000.00','USD'),
(@seed_batch,@seed_ts,@f_sam,'SAM-A-40005','Coastal Wind Farm Alpha','Coastal Wind','Coastal Renewable Wind Farm Alpha','INFRASTRUCTURE','RENEWABLE_ENERGY','ACTIVE','United Kingdom','Scotland','2024-06-01','2024-12-31','78000000.00','GBP'),
(@seed_batch,@seed_ts,@f_sam,'SAM-A-40006','Coastal Solar Array Beta','Coastal Solar','Coastal Renewable Solar Array Beta','INFRASTRUCTURE','RENEWABLE_ENERGY','UNDER_CONSTRUCTION','United Kingdom','Wales','2024-08-01',NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_sam,'SAM-A-40007','Summit Senior Secured Loans','Summit Loans','Summit Senior Secured Loan Portfolio','CREDIT','SENIOR_SECURED','ACTIVE','United States','Multi-Region','2021-10-01','2024-12-31','380000000.00','USD');

-- BAD: NULL asset_type
INSERT INTO bronze.src_asset_mgmt_raw (_batch_id,_ingested_at,_source_file,asset_id,asset_name,asset_short_name,asset_legal_name,asset_type,asset_subtype,asset_status,location_country,location_region,acquisition_date,last_valuation_date,last_valuation_amount,last_valuation_currency)
VALUES (@seed_batch,@seed_ts,@f_sam,'SAM-A-40099','Mystery Asset','Mystery',NULL,NULL,NULL,'ACTIVE','Unknown',NULL,'2025-01-01',NULL,NULL,NULL);


-- ============================================================================
-- 2.4 Source 4: Source_Security_Mgmt → 8 securities
-- ============================================================================
INSERT INTO bronze.src_security_mgmt_raw (_batch_id,_ingested_at,_source_file,security_id,security_type,security_group,security_name,security_status,team_ref,entity_ref,asset_ref,bank_loan_id,cusip,isin,ticker)
VALUES
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50001','EQUITY','Real Assets','Meridian REIT - Class A','ACTIVE','SSM-IT-10001','SSM-E-20001','SSM-A-40001',NULL,'59156R100',NULL,'MER.RE'),
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50002','EQUITY','Real Assets','Meridian Suburban - Equity','ACTIVE','SSM-IT-10001','SSM-E-20001','SSM-A-40002',NULL,NULL,NULL,'MER.SUB'),
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50003','EQUITY','Real Assets','Apex Industrial - LP Units','ACTIVE','SSM-IT-10001','SSM-E-20002','SSM-A-40003',NULL,NULL,'US03783A1007',NULL),
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50004','EQUITY','Private Equity','Vertex SW - Series B Pref','ACTIVE','SSM-IT-10002','SSM-E-20003','SSM-A-40004',NULL,NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50005','SENIOR_DEBT','Infrastructure','Coastal Wind - Senior Secured','ACTIVE','SSM-IT-10001','SSM-E-20004','SSM-A-40005','BL-COAST-001',NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50006','SENIOR_DEBT','Credit','Summit Senior Tranche A','ACTIVE','SSM-IT-10003','SSM-E-20005','SSM-A-40007','BL-SUMMIT-001',NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50007','MEZZANINE','Credit','Summit Mezzanine Tranche B','ACTIVE','SSM-IT-10003','SSM-E-20005','SSM-A-40007',NULL,NULL,NULL,NULL),
(@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50008','DERIVATIVE','Credit','Summit CDS Protection','ACTIVE','SSM-IT-10003','SSM-E-20005','SSM-A-40007',NULL,NULL,NULL,NULL);

-- BAD: invalid security type
INSERT INTO bronze.src_security_mgmt_raw (_batch_id,_ingested_at,_source_file,security_id,security_type,security_group,security_name,security_status,team_ref,entity_ref,asset_ref,bank_loan_id,cusip,isin,ticker)
VALUES (@seed_batch,@seed_ts,@f_ssm,'SSM-SEC-50099','CRYPTO','Unknown','Bad Type','ACTIVE','SSM-IT-10001','SSM-E-20001','SSM-A-40001',NULL,NULL,NULL,NULL);


-- ============================================================================
-- 2.5 Source 6: Source_WS_Online → security refs + pricing
-- ============================================================================
INSERT INTO bronze.src_ws_online_raw (_batch_id,_ingested_at,_source_file,_record_type,wso_security_id,security_type,security_name,bank_loan_id,cusip,isin,ticker,exchange,currency,wso_status,last_updated,price_date,price_close,price_open,price_high,price_low,volume)
VALUES
-- Matches SSM-SEC-50001 on CUSIP
(@seed_batch,@seed_ts,@f_wso,'security','WSO-SEC-70001','EQUITY','Meridian Office REIT Inc',NULL,'59156R100','US59156R1005','MER','NYSE','USD','ACTIVE','2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL),
-- Matches SSM-SEC-50002 on ticker+type
(@seed_batch,@seed_ts,@f_wso,'security','WSO-SEC-70002','EQUITY','Meridian Suburban REIT',NULL,NULL,NULL,'MER.SUB','NYSE','USD','ACTIVE','2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL),
-- Matches SSM-SEC-50003 on ISIN
(@seed_batch,@seed_ts,@f_wso,'security','WSO-SEC-70003','EQUITY','Apex Industrial Holdings LP',NULL,'03783A100','US03783A1007','APX','NASDAQ','USD','ACTIVE','2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL),
-- Matches SSM-SEC-50005 on BankLoanID
(@seed_batch,@seed_ts,@f_wso,'security','WSO-SEC-70004','SENIOR_DEBT','Coastal Wind Farm TL-B','BL-COAST-001',NULL,NULL,NULL,NULL,'GBP','ACTIVE','2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL),
-- AMBIGUOUS: duplicate CUSIP with WSO-001 (tests composite assembly ambiguity)
(@seed_batch,@seed_ts,@f_wso,'security','WSO-SEC-70005','EQUITY','Meridian REIT Alt Listing',NULL,'59156R100',NULL,'MER.ALT','OTC','USD','ACTIVE','2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL),
-- ORPHAN: no matching internal security
(@seed_batch,@seed_ts,@f_wso,'security','WSO-SEC-70006','EQUITY','Unrelated Corp Class A',NULL,'99999X100','US99999X1007','UNRL','NYSE','USD','ACTIVE','2025-01-01',NULL,NULL,NULL,NULL,NULL,NULL);

-- Pricing samples
INSERT INTO bronze.src_ws_online_raw (_batch_id,_ingested_at,_source_file,_record_type,wso_security_id,security_type,security_name,bank_loan_id,cusip,isin,ticker,exchange,currency,wso_status,last_updated,price_date,price_close,price_open,price_high,price_low,volume)
VALUES
(@seed_batch,@seed_ts,@f_wso_px,'pricing','WSO-SEC-70001',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD',NULL,NULL,'2025-01-02','52.35','51.80','52.90','51.50','1250000'),
(@seed_batch,@seed_ts,@f_wso_px,'pricing','WSO-SEC-70001',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD',NULL,NULL,'2025-01-03','53.10','52.40','53.50','52.20','980000'),
(@seed_batch,@seed_ts,@f_wso_px,'pricing','WSO-SEC-70003',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD',NULL,NULL,'2025-01-02','41.20','40.80','41.75','40.50','2100000'),
(@seed_batch,@seed_ts,@f_wso_px,'pricing','WSO-SEC-70003',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD',NULL,NULL,'2025-01-03','41.85','41.30','42.10','41.00','1800000');

-- BAD: unparseable date in pricing
INSERT INTO bronze.src_ws_online_raw (_batch_id,_ingested_at,_source_file,_record_type,wso_security_id,security_type,security_name,bank_loan_id,cusip,isin,ticker,exchange,currency,wso_status,last_updated,price_date,price_close,price_open,price_high,price_low,volume)
VALUES (@seed_batch,@seed_ts,@f_wso_px,'pricing','WSO-SEC-70001',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD',NULL,NULL,'NOT-A-DATE','INVALID','52.40','53.50','52.20','980000');
GO


-- ============================================================================
-- PART 3: PARAMETERIZED TRANSACTION GENERATION
-- ============================================================================
CREATE OR ALTER PROCEDURE bronze.usp_generate_transactions
    @start_date DATE,
    @end_date   DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Valid portfolio → entity → security combos
    CREATE TABLE #combos (
        combo_id     INT IDENTITY(1,1),
        portfolio_id NVARCHAR(50),
        entity_id    NVARCHAR(50),
        security_id  NVARCHAR(50),
        base_amount  DECIMAL(18,2),
        fx_rate      DECIMAL(18,8),
        ccy          NVARCHAR(3)
    );
    INSERT INTO #combos VALUES
    ('STM-P-30001','STM-E-20001','STM-SEC-50001', 5000000.00, 1.00, 'USD'),
    ('STM-P-30002','STM-E-20001','STM-SEC-50001', 2000000.00, 1.00, 'USD'),
    ('STM-P-30001','STM-E-20001','STM-SEC-50002', 1500000.00, 1.00, 'USD'),
    ('STM-P-30001','STM-E-20002','STM-SEC-50003', 8000000.00, 1.00, 'USD'),
    ('STM-P-30004','STM-E-20003','STM-SEC-50004',15000000.00, 1.00, 'USD'),
    ('STM-P-30005','STM-E-20003','STM-SEC-50004', 3000000.00, 1.00, 'USD'),
    ('STM-P-30007','STM-E-20004','STM-SEC-50005', 3000000.00, 1.25, 'GBP'),
    ('STM-P-30006','STM-E-20005','STM-SEC-50006',10000000.00, 1.00, 'USD'),
    ('STM-P-30006','STM-E-20005','STM-SEC-50007', 5000000.00, 1.00, 'USD'),
    ('STM-P-30006','STM-E-20005','STM-SEC-50008',  500000.00, 1.00, 'USD');

    CREATE TABLE #txn_types (
        txn_type     NVARCHAR(100),
        txn_category NVARCHAR(100),
        amount_sign  INT,
        weight       INT
    );
    INSERT INTO #txn_types VALUES
    ('BUY','Capital Call',1,30), ('SELL','Distribution',-1,15),
    ('FEE','Management Fee',-1,20), ('DIVIDEND','Income',1,15),
    ('REVALUATION','Mark-to-Market',1,10), ('INTEREST','Income',1,10);

    -- Business days calendar
    CREATE TABLE #biz_days (biz_date DATE);
    DECLARE @d DATE = @start_date;
    WHILE @d <= @end_date
    BEGIN
        IF DATEPART(WEEKDAY, @d) NOT IN (1, 7)
            INSERT INTO #biz_days VALUES (@d);
        SET @d = DATEADD(DAY, 1, @d);
    END;

    DECLARE @base_id INT = 60001;

    INSERT INTO bronze.src_txn_mgmt_raw (
        _batch_id, _ingested_at, _source_file,
        transaction_id, portfolio_id, entity_id, security_id,
        as_of_date, transaction_type, transaction_category, transaction_status,
        amount_portfolio, amount_local, amount_usd, fx_rate, quantity,
        order_id, order_date, order_status)
    SELECT
        'SEED-TXN-' + CONVERT(NVARCHAR(8), bd.biz_date, 112),
        DATEADD(HOUR, 5, CAST(bd.biz_date AS DATETIME2)),
        'txn_' + CONVERT(NVARCHAR(8), bd.biz_date, 112) + '.json',
        'STM-TXN-' + CAST(ROW_NUMBER() OVER (ORDER BY bd.biz_date, c.combo_id, tt.txn_type) + @base_id AS NVARCHAR(10)),
        c.portfolio_id, c.entity_id, c.security_id,
        CONVERT(NVARCHAR(10), bd.biz_date, 120),
        tt.txn_type, tt.txn_category, 'SETTLED',
        CAST(c.base_amount * (0.001 + (ABS(CHECKSUM(NEWID())) % 50) * 0.001) * tt.amount_sign AS NVARCHAR(50)),
        CAST(c.base_amount * (0.001 + (ABS(CHECKSUM(NEWID())) % 50) * 0.001) * tt.amount_sign / c.fx_rate AS NVARCHAR(50)),
        CAST(c.base_amount * (0.001 + (ABS(CHECKSUM(NEWID())) % 50) * 0.001) * tt.amount_sign AS NVARCHAR(50)),
        CAST(c.fx_rate AS NVARCHAR(50)),
        CAST(ABS(CHECKSUM(NEWID())) % 100000 + 1 AS NVARCHAR(50)),
        'ORD-' + CAST(ROW_NUMBER() OVER (ORDER BY bd.biz_date, c.combo_id, tt.txn_type) + @base_id AS NVARCHAR(10)),
        CONVERT(NVARCHAR(10), DATEADD(DAY, -1, bd.biz_date), 120),
        'FILLED'
    FROM #biz_days bd
    CROSS JOIN #combos c
    CROSS JOIN #txn_types tt
    WHERE (ABS(CHECKSUM(HASHBYTES('MD5', CONCAT(bd.biz_date, c.combo_id, tt.txn_type)))) % 100) < tt.weight * 0.4;

    -- BAD: ghost security FK
    INSERT INTO bronze.src_txn_mgmt_raw (_batch_id,_ingested_at,_source_file,transaction_id,portfolio_id,entity_id,security_id,as_of_date,transaction_type,transaction_category,transaction_status,amount_portfolio,amount_local,amount_usd,fx_rate,quantity,order_id,order_date,order_status)
    VALUES ('SEED-TXN-BAD',GETUTCDATE(),'bad.json','STM-TXN-99999','STM-P-30001','STM-E-20001','STM-SEC-99999',
            CONVERT(NVARCHAR(10),@start_date,120),'BUY','Capital Call','SETTLED','1000000','1000000','1000000','1.00','10000','ORD-BAD-001',CONVERT(NVARCHAR(10),DATEADD(DAY,-1,@start_date),120),'FILLED');

    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM bronze.src_txn_mgmt_raw;
    PRINT 'Generated ' + CAST(@cnt AS NVARCHAR) + ' transactions (' + CONVERT(NVARCHAR(10),@start_date,120) + ' to ' + CONVERT(NVARCHAR(10),@end_date,120) + ')';

    DROP TABLE #combos, #txn_types, #biz_days;
END;
GO

-- Execute with configured date range
EXEC bronze.usp_generate_transactions
    @start_date = '2025-01-15',
    @end_date   = '2025-03-31';
GO


-- ============================================================================
-- PART 4: VERIFICATION
-- ============================================================================
PRINT '=== BRONZE ROW COUNTS ==='
SELECT 'bronze.src_enterprise_raw' AS tbl, COUNT(*) AS cnt FROM bronze.src_enterprise_raw
UNION ALL SELECT 'bronze.src_entity_mgmt_raw', COUNT(*) FROM bronze.src_entity_mgmt_raw
UNION ALL SELECT 'bronze.src_asset_mgmt_raw',  COUNT(*) FROM bronze.src_asset_mgmt_raw
UNION ALL SELECT 'bronze.src_security_mgmt_raw',COUNT(*) FROM bronze.src_security_mgmt_raw
UNION ALL SELECT 'bronze.src_txn_mgmt_raw',    COUNT(*) FROM bronze.src_txn_mgmt_raw
UNION ALL SELECT 'bronze.src_ws_online_raw',   COUNT(*) FROM bronze.src_ws_online_raw
ORDER BY tbl;

PRINT '=== META ROW COUNTS ==='
SELECT 'meta.source_systems' AS tbl,        COUNT(*) AS cnt FROM meta.source_systems
UNION ALL SELECT 'meta.ingestion_pipelines', COUNT(*) FROM meta.ingestion_pipelines
UNION ALL SELECT 'meta.key_registry',        COUNT(*) FROM meta.key_registry
UNION ALL SELECT 'meta.key_crosswalk',       COUNT(*) FROM meta.key_crosswalk
UNION ALL SELECT 'meta.quality_rules',       COUNT(*) FROM meta.quality_rules
UNION ALL SELECT 'meta.data_contracts',      COUNT(*) FROM meta.data_contracts
UNION ALL SELECT 'meta.consumers',           COUNT(*) FROM meta.consumers
UNION ALL SELECT 'meta.retention_policies',  COUNT(*) FROM meta.retention_policies
UNION ALL SELECT 'meta.business_glossary',   COUNT(*) FROM meta.business_glossary
ORDER BY tbl;

PRINT '=== DELIBERATE BAD ROWS (7 total) ==='
SELECT 'ENT: NULL team name' AS bad_row, COUNT(*) AS cnt FROM bronze.src_enterprise_raw WHERE investment_team_id = 'ENT-IT-10099'
UNION ALL SELECT 'ENT: ghost team FK',   COUNT(*) FROM bronze.src_enterprise_raw WHERE portfolio_group_id = 'ENT-PG-20099'
UNION ALL SELECT 'SEM: whitespace name', COUNT(*) FROM bronze.src_entity_mgmt_raw WHERE entity_id = 'SEM-E-20099'
UNION ALL SELECT 'SAM: NULL asset type', COUNT(*) FROM bronze.src_asset_mgmt_raw WHERE asset_id = 'SAM-A-40099'
UNION ALL SELECT 'SSM: CRYPTO type',     COUNT(*) FROM bronze.src_security_mgmt_raw WHERE security_id = 'SSM-SEC-50099'
UNION ALL SELECT 'STM: ghost security',  COUNT(*) FROM bronze.src_txn_mgmt_raw WHERE security_id = 'STM-SEC-99999'
UNION ALL SELECT 'WSO: bad price date',  COUNT(*) FROM bronze.src_ws_online_raw WHERE price_date = 'NOT-A-DATE';
GO
