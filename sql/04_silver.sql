-- ============================================================================
-- GOLDEN DATA LAYER: ALL SILVER PROCEDURES & VIEWS
-- Target: Azure SQL Edge Developer (SQL Server 15.x compatible)
-- Date: 2026-02-09
-- Depends on: 01_ddl.sql, 02_meta_programmability.sql, 03_audit.sql
-- ============================================================================
-- Utility:
--   silver.usp_quarantine_row                 Insert a failed row into quarantine
-- Transforms (11):
--   silver.usp_conform_investment_team        (1. Enterprise → team)
--   silver.usp_conform_portfolio_group        (2. Enterprise → fund)
--   silver.usp_conform_portfolio              (3. Enterprise → portfolio)
--   silver.usp_conform_entity                 (4. Entity Mgmt → entity)
--   silver.usp_conform_asset                  (5. Asset Mgmt → asset)
--   silver.usp_conform_ws_online_security     (6. WSO → ws_online_security)
--   silver.usp_conform_portfolio_entity_ownership (7. Entity Mgmt → PE bridge)
--   silver.usp_conform_entity_asset_ownership     (8. Entity Mgmt → EA bridge)
--   silver.usp_conform_ws_online_pricing      (9. WSO → pricing)
--   silver.usp_conform_security               (10. Security Mgmt + WSO composite)
--   silver.usp_conform_transaction            (11. Transaction Mgmt → position_transaction)
-- Orchestrators (4):
--   silver.usp_run_enterprise_silver          (team → pg → portfolio)
--   silver.usp_run_entity_silver              (entity → PE bridge → EA bridge)
--   silver.usp_run_market_silver              (WSO security → WSO pricing)
--   silver.usp_run_all_silver                 (full silver pipeline, 4 phases)
-- Views:
--   silver.vw_quarantine_summary
-- ============================================================================
USE GoldenDataLayer;
GO


-- ============================================================================
-- UTILITY
-- ============================================================================
-- 2.4 Quarantine writer — insert a failed row
CREATE OR ALTER PROCEDURE silver.usp_quarantine_row
    @source_table   NVARCHAR(200),
    @raw_payload    NVARCHAR(MAX),
    @failed_rule    NVARCHAR(100),
    @failure_detail NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
    VALUES (@source_table, @raw_payload, @failed_rule, @failure_detail);
END;
GO


-- ============================================================================
-- TRANSFORMS 1-6 (from original sources)
-- ============================================================================
-- ============================================================================
-- PART 3: STORED PROCEDURES — SILVER TRANSFORMS
-- ============================================================================

-- 3.1 Bronze → silver.investment_team
CREATE OR ALTER PROCEDURE silver.usp_conform_investment_team
    @batch_id NVARCHAR(100) = NULL  -- NULL = process all unprocessed
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTERPRISE_DAILY', 'SILVER', 'silver.investment_team', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @upd INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        -- Stage: parse, cast, translate, hash, dedup
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.investment_team_id, 'ENT-IT-', 'IT-') AS investment_team_enterprise_key,
                TRIM(src.team_name)                     AS investment_team_name,
                NULLIF(TRIM(src.team_short_name), '')    AS investment_team_short_name,
                TRY_CAST(src.start_date AS DATE)         AS start_date,
                TRY_CAST(NULLIF(src.stop_date, '') AS DATE) AS stop_date,
                src.investment_team_id                   AS src_investment_team_id,
                CAST(src._record_id AS NVARCHAR(200))    AS _bronze_record_id,
                src._ingested_at                         AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    TRIM(src.team_name),
                    NULLIF(TRIM(src.team_short_name), ''),
                    CAST(TRY_CAST(src.start_date AS DATE) AS NVARCHAR),
                    CAST(TRY_CAST(NULLIF(src.stop_date, '') AS DATE) AS NVARCHAR)
                )) AS _row_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY src.investment_team_id
                    ORDER BY src._ingested_at DESC
                ) AS rn
            FROM bronze.src_enterprise_raw src
            WHERE src._record_type = 'investment_team'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_teams FROM staged WHERE rn = 1;

        SET @read = @@ROWCOUNT;

        -- Quarantine: NULL enterprise key
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.investment_team',
               CONCAT('{"src_id":"', src_investment_team_id, '","name":"', investment_team_name, '"}'),
               'NOT_NULL_EK', 'Enterprise key translated to NULL'
        FROM #staged_teams WHERE investment_team_enterprise_key IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL start_date
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.investment_team',
               CONCAT('{"ek":"', investment_team_enterprise_key, '","name":"', investment_team_name, '"}'),
               'START_DATE_VALID', 'start_date could not be parsed'
        FROM #staged_teams WHERE investment_team_enterprise_key IS NOT NULL AND start_date IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL name
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.investment_team',
               CONCAT('{"ek":"', investment_team_enterprise_key, '"}'),
               'TEAM_NAME_NOT_EMPTY', 'team_name is NULL or empty'
        FROM #staged_teams
        WHERE investment_team_enterprise_key IS NOT NULL
          AND (investment_team_name IS NULL OR LEN(TRIM(investment_team_name)) = 0);
        SET @quar = @quar + @@ROWCOUNT;

        -- MERGE: only rows passing all checks
        MERGE INTO silver.investment_team AS t
        USING (
            SELECT * FROM #staged_teams
            WHERE investment_team_enterprise_key IS NOT NULL
              AND start_date IS NOT NULL
              AND investment_team_name IS NOT NULL
              AND LEN(TRIM(investment_team_name)) > 0
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

        -- Capture merge counts (simplified — real impl uses OUTPUT clause)
        SET @ins = @@ROWCOUNT;  -- total affected

        DROP TABLE #staged_teams;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO

-- 3.2 Bronze → silver.portfolio_group
CREATE OR ALTER PROCEDURE silver.usp_conform_portfolio_group
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTERPRISE_DAILY', 'SILVER', 'silver.portfolio_group', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.portfolio_group_id, 'ENT-PG-', 'PG-') AS portfolio_group_enterprise_key,
                TRIM(src.pg_name)                          AS portfolio_group_name,
                NULLIF(TRIM(src.pg_short_name), '')         AS portfolio_group_short_name,
                src.pg_description                          AS portfolio_group_description,
                meta.fn_translate_key(src.pg_team_ref, 'ENT-IT-', 'IT-') AS investment_team_enterprise_key,
                TRY_CAST(src.vintage_year AS INT)           AS vintage_year,
                TRIM(src.strategy)                          AS strategy,
                TRY_CAST(src.committed_capital AS DECIMAL(18,2)) AS committed_capital,
                UPPER(TRIM(src.committed_capital_ccy))      AS committed_capital_currency,
                UPPER(TRIM(src.fund_status))                AS fund_status,
                src.portfolio_group_id                      AS src_portfolio_group_id,
                CAST(src._record_id AS NVARCHAR(200))       AS _bronze_record_id,
                src._ingested_at                            AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    TRIM(src.pg_name), NULLIF(TRIM(src.pg_short_name),''),
                    src.pg_description,
                    meta.fn_translate_key(src.pg_team_ref, 'ENT-IT-', 'IT-'),
                    CAST(TRY_CAST(src.vintage_year AS INT) AS NVARCHAR),
                    TRIM(src.strategy),
                    CAST(TRY_CAST(src.committed_capital AS DECIMAL(18,2)) AS NVARCHAR),
                    UPPER(TRIM(src.committed_capital_ccy)),
                    UPPER(TRIM(src.fund_status))
                )) AS _row_hash,
                ROW_NUMBER() OVER (PARTITION BY src.portfolio_group_id ORDER BY src._ingested_at DESC) AS rn
            FROM bronze.src_enterprise_raw src
            WHERE src._record_type = 'portfolio_group'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_pg FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: FK check — team must exist in silver
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio_group',
               CONCAT('{"ek":"', portfolio_group_enterprise_key, '","team_ek":"', investment_team_enterprise_key, '"}'),
               'PG_TEAM_EXISTS', 'investment_team_enterprise_key not found in silver.investment_team'
        FROM #staged_pg s
        WHERE s.portfolio_group_enterprise_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.investment_team t WHERE t.investment_team_enterprise_key = s.investment_team_enterprise_key);
        SET @quar = @@ROWCOUNT;

        -- Quarantine: NULL enterprise key
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio_group',
               CONCAT('{"src_id":"', src_portfolio_group_id, '"}'),
               'NOT_NULL_EK', 'Enterprise key translated to NULL'
        FROM #staged_pg WHERE portfolio_group_enterprise_key IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL name
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio_group',
               CONCAT('{"ek":"', portfolio_group_enterprise_key, '"}'),
               'PG_NAME_NOT_EMPTY', 'portfolio_group_name is NULL or empty'
        FROM #staged_pg WHERE portfolio_group_enterprise_key IS NOT NULL
          AND (portfolio_group_name IS NULL OR LEN(TRIM(portfolio_group_name)) = 0);
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.portfolio_group AS t
        USING (
            SELECT s.* FROM #staged_pg s
            WHERE s.portfolio_group_enterprise_key IS NOT NULL
              AND s.portfolio_group_name IS NOT NULL
              AND s.investment_team_enterprise_key IS NOT NULL
              AND EXISTS (SELECT 1 FROM silver.investment_team it WHERE it.investment_team_enterprise_key = s.investment_team_enterprise_key)
        ) AS s
        ON t.portfolio_group_enterprise_key = s.portfolio_group_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.portfolio_group_name       = s.portfolio_group_name,
            t.portfolio_group_short_name = s.portfolio_group_short_name,
            t.portfolio_group_description= s.portfolio_group_description,
            t.investment_team_enterprise_key = s.investment_team_enterprise_key,
            t.vintage_year               = s.vintage_year,
            t.strategy                   = s.strategy,
            t.committed_capital          = s.committed_capital,
            t.committed_capital_currency = s.committed_capital_currency,
            t.fund_status                = s.fund_status,
            t._source_modified_at        = s._source_modified_at,
            t._bronze_record_id          = s._bronze_record_id,
            t._conformed_at              = GETUTCDATE(),
            t._conformed_by              = SYSTEM_USER,
            t._row_hash                  = s._row_hash
        WHEN NOT MATCHED THEN INSERT (
            portfolio_group_enterprise_key, portfolio_group_name, portfolio_group_short_name,
            portfolio_group_description, investment_team_enterprise_key,
            vintage_year, strategy, committed_capital, committed_capital_currency, fund_status,
            src_portfolio_group_id, _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.portfolio_group_enterprise_key, s.portfolio_group_name, s.portfolio_group_short_name,
            s.portfolio_group_description, s.investment_team_enterprise_key,
            s.vintage_year, s.strategy, s.committed_capital, s.committed_capital_currency, s.fund_status,
            s.src_portfolio_group_id, s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_pg;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO

-- 3.3 Bronze → silver.portfolio
CREATE OR ALTER PROCEDURE silver.usp_conform_portfolio
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTERPRISE_DAILY', 'SILVER', 'silver.portfolio', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.portfolio_id, 'ENT-P-', 'P-') AS portfolio_enterprise_key,
                TRIM(src.port_name)                       AS portfolio_name,
                NULLIF(TRIM(src.port_short_name), '')      AS portfolio_short_name,
                meta.fn_translate_key(src.port_pg_ref, 'ENT-PG-', 'PG-') AS portfolio_group_enterprise_key,
                src.portfolio_id                           AS src_portfolio_id,
                CAST(src._record_id AS NVARCHAR(200))      AS _bronze_record_id,
                src._ingested_at                           AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    TRIM(src.port_name), NULLIF(TRIM(src.port_short_name),''),
                    meta.fn_translate_key(src.port_pg_ref, 'ENT-PG-', 'PG-')
                )) AS _row_hash,
                ROW_NUMBER() OVER (PARTITION BY src.portfolio_id ORDER BY src._ingested_at DESC) AS rn
            FROM bronze.src_enterprise_raw src
            WHERE src._record_type = 'portfolio'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_port FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- FK check: portfolio_group must exist
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio',
               CONCAT('{"ek":"', portfolio_enterprise_key, '","pg_ek":"', portfolio_group_enterprise_key, '"}'),
               'PORT_PG_EXISTS', 'portfolio_group_enterprise_key not found in silver.portfolio_group'
        FROM #staged_port s
        WHERE s.portfolio_enterprise_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.portfolio_group pg WHERE pg.portfolio_group_enterprise_key = s.portfolio_group_enterprise_key);
        SET @quar = @@ROWCOUNT;

        -- Quarantine: NULL enterprise key
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio',
               CONCAT('{"src_id":"', src_portfolio_id, '"}'),
               'NOT_NULL_EK', 'Enterprise key translated to NULL'
        FROM #staged_port WHERE portfolio_enterprise_key IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL name
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio',
               CONCAT('{"ek":"', portfolio_enterprise_key, '"}'),
               'PORT_NAME_NOT_EMPTY', 'portfolio_name is NULL or empty'
        FROM #staged_port WHERE portfolio_enterprise_key IS NOT NULL
          AND (portfolio_name IS NULL OR LEN(TRIM(portfolio_name)) = 0);
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.portfolio AS t
        USING (
            SELECT s.* FROM #staged_port s
            WHERE s.portfolio_enterprise_key IS NOT NULL
              AND s.portfolio_name IS NOT NULL
              AND EXISTS (SELECT 1 FROM silver.portfolio_group pg WHERE pg.portfolio_group_enterprise_key = s.portfolio_group_enterprise_key)
        ) AS s
        ON t.portfolio_enterprise_key = s.portfolio_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.portfolio_name                 = s.portfolio_name,
            t.portfolio_short_name           = s.portfolio_short_name,
            t.portfolio_group_enterprise_key = s.portfolio_group_enterprise_key,
            t._source_modified_at            = s._source_modified_at,
            t._bronze_record_id              = s._bronze_record_id,
            t._conformed_at                  = GETUTCDATE(),
            t._conformed_by                  = SYSTEM_USER,
            t._row_hash                      = s._row_hash
        WHEN NOT MATCHED THEN INSERT (
            portfolio_enterprise_key, portfolio_name, portfolio_short_name,
            portfolio_group_enterprise_key, src_portfolio_id,
            _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.portfolio_enterprise_key, s.portfolio_name, s.portfolio_short_name,
            s.portfolio_group_enterprise_key, s.src_portfolio_id,
            s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_port;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO

-- 3.4 Bronze → silver.entity
CREATE OR ALTER PROCEDURE silver.usp_conform_entity
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTITY_DAILY', 'SILVER', 'silver.entity', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.entity_id, 'SEM-E-', 'E-') AS entity_enterprise_key,
                TRIM(src.entity_name)                      AS entity_name,
                NULLIF(TRIM(src.entity_short_name), '')     AS entity_short_name,
                NULLIF(TRIM(src.entity_legal_name), '')     AS entity_legal_name,
                UPPER(TRIM(src.entity_type))                AS entity_type,
                UPPER(TRIM(src.entity_status))              AS entity_status,
                TRIM(src.incorporation_jurisdiction)         AS incorporation_jurisdiction,
                TRY_CAST(src.incorporation_date AS DATE)     AS incorporation_date,
                src.entity_id                               AS src_entity_id,
                CAST(src._record_id AS NVARCHAR(200))       AS _bronze_record_id,
                src._ingested_at                            AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    TRIM(src.entity_name), NULLIF(TRIM(src.entity_short_name),''),
                    NULLIF(TRIM(src.entity_legal_name),''),
                    UPPER(TRIM(src.entity_type)), UPPER(TRIM(src.entity_status)),
                    TRIM(src.incorporation_jurisdiction),
                    CAST(TRY_CAST(src.incorporation_date AS DATE) AS NVARCHAR)
                )) AS _row_hash,
                ROW_NUMBER() OVER (PARTITION BY src.entity_id ORDER BY src._ingested_at DESC) AS rn
            FROM bronze.src_entity_mgmt_raw src
            WHERE src._record_type = 'entity'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_ent FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: empty name
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.entity',
               CONCAT('{"ek":"', entity_enterprise_key, '"}'),
               'ENTITY_NAME_NOT_EMPTY', 'entity_name is NULL or empty'
        FROM #staged_ent WHERE entity_name IS NULL OR LEN(TRIM(entity_name)) = 0;
        SET @quar = @@ROWCOUNT;

        -- Quarantine: NULL enterprise key
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.entity',
               CONCAT('{"src_id":"', src_entity_id, '"}'),
               'NOT_NULL_EK', 'Enterprise key translated to NULL'
        FROM #staged_ent WHERE entity_enterprise_key IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.entity AS t
        USING (
            SELECT * FROM #staged_ent
            WHERE entity_enterprise_key IS NOT NULL
              AND entity_name IS NOT NULL AND LEN(TRIM(entity_name)) > 0
        ) AS s
        ON t.entity_enterprise_key = s.entity_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.entity_name                 = s.entity_name,
            t.entity_short_name           = s.entity_short_name,
            t.entity_legal_name           = s.entity_legal_name,
            t.entity_type                 = s.entity_type,
            t.entity_status               = s.entity_status,
            t.incorporation_jurisdiction  = s.incorporation_jurisdiction,
            t.incorporation_date          = s.incorporation_date,
            t._source_modified_at         = s._source_modified_at,
            t._bronze_record_id           = s._bronze_record_id,
            t._conformed_at               = GETUTCDATE(),
            t._conformed_by               = SYSTEM_USER,
            t._row_hash                   = s._row_hash
        WHEN NOT MATCHED THEN INSERT (
            entity_enterprise_key, entity_name, entity_short_name, entity_legal_name,
            entity_type, entity_status, incorporation_jurisdiction, incorporation_date,
            src_entity_id, _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.entity_enterprise_key, s.entity_name, s.entity_short_name, s.entity_legal_name,
            s.entity_type, s.entity_status, s.incorporation_jurisdiction, s.incorporation_date,
            s.src_entity_id, s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_ent;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO

-- 3.5 Bronze → silver.asset
CREATE OR ALTER PROCEDURE silver.usp_conform_asset
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ASSET_DAILY', 'SILVER', 'silver.asset', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.asset_id, 'SAM-A-', 'A-') AS asset_enterprise_key,
                TRIM(src.asset_name)                        AS asset_name,
                NULLIF(TRIM(src.asset_short_name), '')       AS asset_short_name,
                NULLIF(TRIM(src.asset_legal_name), '')       AS asset_legal_name,
                UPPER(TRIM(src.asset_type))                  AS asset_type,
                UPPER(TRIM(src.asset_subtype))               AS asset_subtype,
                UPPER(TRIM(src.asset_status))                AS asset_status,
                TRIM(src.location_country)                   AS location_country,
                TRIM(src.location_region)                    AS location_region,
                TRY_CAST(src.acquisition_date AS DATE)       AS acquisition_date,
                TRY_CAST(src.last_valuation_date AS DATE)    AS last_valuation_date,
                TRY_CAST(src.last_valuation_amount AS DECIMAL(18,2)) AS last_valuation_amount,
                UPPER(TRIM(src.last_valuation_currency))     AS last_valuation_currency,
                src.asset_id                                 AS src_asset_id,
                CAST(src._record_id AS NVARCHAR(200))        AS _bronze_record_id,
                src._ingested_at                             AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    TRIM(src.asset_name), NULLIF(TRIM(src.asset_short_name),''),
                    NULLIF(TRIM(src.asset_legal_name),''),
                    UPPER(TRIM(src.asset_type)),
                    UPPER(TRIM(src.asset_subtype)), UPPER(TRIM(src.asset_status)),
                    TRIM(src.location_country), TRIM(src.location_region),
                    CAST(TRY_CAST(src.acquisition_date AS DATE) AS NVARCHAR),
                    CAST(TRY_CAST(src.last_valuation_date AS DATE) AS NVARCHAR),
                    CAST(TRY_CAST(src.last_valuation_amount AS DECIMAL(18,2)) AS NVARCHAR),
                    UPPER(TRIM(src.last_valuation_currency))
                )) AS _row_hash,
                ROW_NUMBER() OVER (PARTITION BY src.asset_id ORDER BY src._ingested_at DESC) AS rn
            FROM bronze.src_asset_mgmt_raw src
            WHERE (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_asset FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: NULL enterprise key
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.asset',
               CONCAT('{"src_id":"', src_asset_id, '","name":"', asset_name, '"}'),
               'NOT_NULL_EK', 'Enterprise key translated to NULL'
        FROM #staged_asset WHERE asset_enterprise_key IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL asset name
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.asset',
               CONCAT('{"ek":"', asset_enterprise_key, '"}'),
               'ASSET_NAME_NOT_EMPTY', 'asset_name is NULL or empty'
        FROM #staged_asset WHERE asset_enterprise_key IS NOT NULL
          AND (asset_name IS NULL OR LEN(TRIM(asset_name)) = 0);
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL asset type
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.asset',
               CONCAT('{"ek":"', asset_enterprise_key, '"}'),
               'ASSET_TYPE_NOT_EMPTY', 'asset_type is NULL or empty'
        FROM #staged_asset WHERE asset_enterprise_key IS NOT NULL
          AND asset_name IS NOT NULL AND LEN(TRIM(asset_name)) > 0
          AND (asset_type IS NULL OR LEN(TRIM(asset_type)) = 0);
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.asset AS t
        USING (
            SELECT * FROM #staged_asset
            WHERE asset_enterprise_key IS NOT NULL
              AND asset_name IS NOT NULL
              AND asset_type IS NOT NULL AND LEN(TRIM(asset_type)) > 0
        ) AS s
        ON t.asset_enterprise_key = s.asset_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.asset_name              = s.asset_name,
            t.asset_short_name        = s.asset_short_name,
            t.asset_legal_name        = s.asset_legal_name,
            t.asset_type              = s.asset_type,
            t.asset_subtype           = s.asset_subtype,
            t.asset_status            = s.asset_status,
            t.location_country        = s.location_country,
            t.location_region         = s.location_region,
            t.acquisition_date        = s.acquisition_date,
            t.last_valuation_date     = s.last_valuation_date,
            t.last_valuation_amount   = s.last_valuation_amount,
            t.last_valuation_currency = s.last_valuation_currency,
            t._source_modified_at     = s._source_modified_at,
            t._bronze_record_id       = s._bronze_record_id,
            t._conformed_at           = GETUTCDATE(),
            t._conformed_by           = SYSTEM_USER,
            t._row_hash               = s._row_hash
        WHEN NOT MATCHED THEN INSERT (
            asset_enterprise_key, asset_name, asset_short_name, asset_legal_name,
            asset_type, asset_subtype, asset_status,
            location_country, location_region, acquisition_date,
            last_valuation_date, last_valuation_amount, last_valuation_currency,
            src_asset_id, _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.asset_enterprise_key, s.asset_name, s.asset_short_name, s.asset_legal_name,
            s.asset_type, s.asset_subtype, s.asset_status,
            s.location_country, s.location_region, s.acquisition_date,
            s.last_valuation_date, s.last_valuation_amount, s.last_valuation_currency,
            s.src_asset_id, s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_asset;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO

-- 3.6 Bronze → silver.ws_online_security
CREATE OR ALTER PROCEDURE silver.usp_conform_ws_online_security
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_MARKET_DAILY', 'SILVER', 'silver.ws_online_security', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                TRIM(src.wso_security_id)                    AS wso_security_id,
                UPPER(TRIM(src.security_type))               AS security_type,
                TRIM(src.security_name)                      AS security_name,
                NULLIF(TRIM(src.bank_loan_id), '')           AS bank_loan_id,
                NULLIF(TRIM(src.cusip), '')                  AS cusip,
                NULLIF(TRIM(src.isin), '')                   AS isin,
                NULLIF(TRIM(src.ticker), '')                 AS ticker,
                TRIM(src.exchange)                           AS exchange,
                UPPER(TRIM(src.currency))                    AS currency,
                UPPER(TRIM(src.wso_status))                  AS status,
                TRY_CAST(src.last_updated AS DATETIME2)      AS last_updated,
                CAST(src._record_id AS NVARCHAR(200))        AS _bronze_record_id,
                src._ingested_at                             AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    UPPER(TRIM(src.security_type)), TRIM(src.security_name),
                    NULLIF(TRIM(src.bank_loan_id),''), NULLIF(TRIM(src.cusip),''),
                    NULLIF(TRIM(src.isin),''), NULLIF(TRIM(src.ticker),'')
                )) AS _row_hash,
                ROW_NUMBER() OVER (PARTITION BY src.wso_security_id ORDER BY src._ingested_at DESC) AS rn
            FROM bronze.src_ws_online_raw src
            WHERE src._record_type = 'security'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_wso FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: NULL wso_security_id
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.ws_online_security',
               CONCAT('{"name":"', security_name, '"}'),
               'NOT_NULL_EK', 'wso_security_id is NULL'
        FROM #staged_wso WHERE wso_security_id IS NULL;
        SET @quar = @@ROWCOUNT;

        MERGE INTO silver.ws_online_security AS t
        USING (SELECT * FROM #staged_wso WHERE wso_security_id IS NOT NULL) AS s
        ON t.wso_security_id = s.wso_security_id
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.security_type      = s.security_type,
            t.security_name      = s.security_name,
            t.bank_loan_id       = s.bank_loan_id,
            t.cusip              = s.cusip,
            t.isin               = s.isin,
            t.ticker             = s.ticker,
            t.exchange           = s.exchange,
            t.currency           = s.currency,
            t.status             = s.status,
            t.last_updated       = s.last_updated,
            t._source_modified_at= s._source_modified_at,
            t._bronze_record_id  = s._bronze_record_id,
            t._conformed_at      = GETUTCDATE(),
            t._conformed_by      = SYSTEM_USER,
            t._row_hash          = s._row_hash
        WHEN NOT MATCHED THEN INSERT (
            wso_security_id, security_type, security_name, bank_loan_id, cusip, isin, ticker,
            exchange, currency, status, last_updated,
            _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.wso_security_id, s.security_type, s.security_name, s.bank_loan_id, s.cusip, s.isin, s.ticker,
            s.exchange, s.currency, s.status, s.last_updated,
            s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_wso;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- TRANSFORMS 7-11 (ownership bridges, pricing, composite security, transactions)
-- ============================================================================

-- ============================================================================
-- 7. Bronze → silver.portfolio_entity_ownership
-- ============================================================================
CREATE OR ALTER PROCEDURE silver.usp_conform_portfolio_entity_ownership
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTITY_DAILY', 'SILVER', 'silver.portfolio_entity_ownership', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.portfolio_ref, 'SEM-P-', 'P-')  AS portfolio_enterprise_key,
                meta.fn_translate_key(src.entity_ref,    'SEM-E-', 'E-')  AS entity_enterprise_key,
                TRY_CAST(src.ownership_pct AS DECIMAL(5,4))               AS ownership_pct,
                TRY_CAST(src.effective_date AS DATE)                      AS effective_date,
                TRY_CAST(NULLIF(src.end_date, '') AS DATE)                AS end_date,
                src.ownership_id                                          AS src_ownership_id,
                CAST(src._record_id AS NVARCHAR(200))                     AS _bronze_record_id,
                src._ingested_at                                          AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    CAST(TRY_CAST(src.ownership_pct AS DECIMAL(5,4)) AS NVARCHAR),
                    CAST(TRY_CAST(NULLIF(src.end_date, '') AS DATE) AS NVARCHAR)
                )) AS _row_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY src.portfolio_ref, src.entity_ref, src.effective_date
                    ORDER BY src._ingested_at DESC
                ) AS rn
            FROM bronze.src_entity_mgmt_raw src
            WHERE src._record_type = 'portfolio_entity_ownership'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_pe FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: portfolio FK doesn't exist in silver
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio_entity_ownership',
               CONCAT('{"port_ek":"', s.portfolio_enterprise_key, '","ent_ek":"', s.entity_enterprise_key, '"}'),
               'PE_PORTFOLIO_EXISTS', 'portfolio_enterprise_key not found in silver.portfolio'
        FROM #staged_pe s
        WHERE s.portfolio_enterprise_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.portfolio p WHERE p.portfolio_enterprise_key = s.portfolio_enterprise_key);
        SET @quar = @@ROWCOUNT;

        -- Quarantine: entity FK doesn't exist in silver
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio_entity_ownership',
               CONCAT('{"port_ek":"', s.portfolio_enterprise_key, '","ent_ek":"', s.entity_enterprise_key, '"}'),
               'PE_ENTITY_EXISTS', 'entity_enterprise_key not found in silver.entity'
        FROM #staged_pe s
        WHERE s.entity_enterprise_key IS NOT NULL
          AND s.portfolio_enterprise_key IS NOT NULL
          AND EXISTS (SELECT 1 FROM silver.portfolio p WHERE p.portfolio_enterprise_key = s.portfolio_enterprise_key)
          AND NOT EXISTS (SELECT 1 FROM silver.entity e WHERE e.entity_enterprise_key = s.entity_enterprise_key);
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: ownership_pct out of range (CHECK constraint would catch, but quarantine is friendlier)
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.portfolio_entity_ownership',
               CONCAT('{"port_ek":"', s.portfolio_enterprise_key, '","ent_ek":"', s.entity_enterprise_key, '","pct":"', CAST(s.ownership_pct AS NVARCHAR), '"}'),
               'PE_PCT_RANGE', 'ownership_pct not in (0, 1.0]'
        FROM #staged_pe s
        WHERE s.ownership_pct IS NULL OR s.ownership_pct <= 0 OR s.ownership_pct > 1.0;
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.portfolio_entity_ownership AS t
        USING (
            SELECT s.* FROM #staged_pe s
            WHERE s.portfolio_enterprise_key IS NOT NULL
              AND s.entity_enterprise_key IS NOT NULL
              AND s.ownership_pct > 0 AND s.ownership_pct <= 1.0
              AND s.effective_date IS NOT NULL
              AND EXISTS (SELECT 1 FROM silver.portfolio p WHERE p.portfolio_enterprise_key = s.portfolio_enterprise_key)
              AND EXISTS (SELECT 1 FROM silver.entity e WHERE e.entity_enterprise_key = s.entity_enterprise_key)
        ) AS s
        ON  t.portfolio_enterprise_key = s.portfolio_enterprise_key
        AND t.entity_enterprise_key    = s.entity_enterprise_key
        AND t.effective_date           = s.effective_date
        WHEN MATCHED AND t._row_hash != s._row_hash
        THEN UPDATE SET
            t.ownership_pct       = s.ownership_pct,
            t.end_date            = s.end_date,
            t._row_hash           = s._row_hash,
            t._source_modified_at = s._source_modified_at,
            t._bronze_record_id   = s._bronze_record_id,
            t._conformed_at       = GETUTCDATE(),
            t._conformed_by       = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            portfolio_enterprise_key, entity_enterprise_key, ownership_pct,
            effective_date, end_date, src_ownership_id,
            _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.portfolio_enterprise_key, s.entity_enterprise_key, s.ownership_pct,
            s.effective_date, s.end_date, s.src_ownership_id,
            s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_pe;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 8. Bronze → silver.entity_asset_ownership
-- ============================================================================
CREATE OR ALTER PROCEDURE silver.usp_conform_entity_asset_ownership
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTITY_DAILY', 'SILVER', 'silver.entity_asset_ownership', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.entity_ref, 'SEM-E-', 'E-') AS entity_enterprise_key,
                meta.fn_translate_key(src.asset_ref,  'SEM-A-', 'A-') AS asset_enterprise_key,
                TRY_CAST(src.ownership_pct AS DECIMAL(5,4))           AS ownership_pct,
                TRY_CAST(src.effective_date AS DATE)                  AS effective_date,
                TRY_CAST(NULLIF(src.end_date, '') AS DATE)            AS end_date,
                src.ownership_id                                      AS src_ownership_id,
                CAST(src._record_id AS NVARCHAR(200))                 AS _bronze_record_id,
                src._ingested_at                                      AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    CAST(TRY_CAST(src.ownership_pct AS DECIMAL(5,4)) AS NVARCHAR),
                    CAST(TRY_CAST(NULLIF(src.end_date, '') AS DATE) AS NVARCHAR)
                )) AS _row_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY src.entity_ref, src.asset_ref, src.effective_date
                    ORDER BY src._ingested_at DESC
                ) AS rn
            FROM bronze.src_entity_mgmt_raw src
            WHERE src._record_type = 'entity_asset_ownership'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_ea FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: entity FK
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.entity_asset_ownership',
               CONCAT('{"ent_ek":"', s.entity_enterprise_key, '","asset_ek":"', s.asset_enterprise_key, '"}'),
               'EA_ENTITY_EXISTS', 'entity_enterprise_key not found in silver.entity'
        FROM #staged_ea s
        WHERE s.entity_enterprise_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.entity e WHERE e.entity_enterprise_key = s.entity_enterprise_key);
        SET @quar = @@ROWCOUNT;

        -- Quarantine: asset FK
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.entity_asset_ownership',
               CONCAT('{"ent_ek":"', s.entity_enterprise_key, '","asset_ek":"', s.asset_enterprise_key, '"}'),
               'EA_ASSET_EXISTS', 'asset_enterprise_key not found in silver.asset'
        FROM #staged_ea s
        WHERE s.asset_enterprise_key IS NOT NULL
          AND s.entity_enterprise_key IS NOT NULL
          AND EXISTS (SELECT 1 FROM silver.entity e WHERE e.entity_enterprise_key = s.entity_enterprise_key)
          AND NOT EXISTS (SELECT 1 FROM silver.asset a WHERE a.asset_enterprise_key = s.asset_enterprise_key);
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: ownership_pct out of range
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.entity_asset_ownership',
               CONCAT('{"ent_ek":"', s.entity_enterprise_key, '","asset_ek":"', s.asset_enterprise_key, '","pct":"', CAST(s.ownership_pct AS NVARCHAR), '"}'),
               'EA_PCT_RANGE', 'ownership_pct not in (0, 1.0]'
        FROM #staged_ea s
        WHERE s.ownership_pct IS NULL OR s.ownership_pct <= 0 OR s.ownership_pct > 1.0;
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.entity_asset_ownership AS t
        USING (
            SELECT s.* FROM #staged_ea s
            WHERE s.entity_enterprise_key IS NOT NULL
              AND s.asset_enterprise_key IS NOT NULL
              AND s.ownership_pct > 0 AND s.ownership_pct <= 1.0
              AND s.effective_date IS NOT NULL
              AND EXISTS (SELECT 1 FROM silver.entity e WHERE e.entity_enterprise_key = s.entity_enterprise_key)
              AND EXISTS (SELECT 1 FROM silver.asset a WHERE a.asset_enterprise_key = s.asset_enterprise_key)
        ) AS s
        ON  t.entity_enterprise_key = s.entity_enterprise_key
        AND t.asset_enterprise_key  = s.asset_enterprise_key
        AND t.effective_date        = s.effective_date
        WHEN MATCHED AND t._row_hash != s._row_hash
        THEN UPDATE SET
            t.ownership_pct       = s.ownership_pct,
            t.end_date            = s.end_date,
            t._row_hash           = s._row_hash,
            t._source_modified_at = s._source_modified_at,
            t._bronze_record_id   = s._bronze_record_id,
            t._conformed_at       = GETUTCDATE(),
            t._conformed_by       = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            entity_enterprise_key, asset_enterprise_key, ownership_pct,
            effective_date, end_date, src_ownership_id,
            _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.entity_enterprise_key, s.asset_enterprise_key, s.ownership_pct,
            s.effective_date, s.end_date, s.src_ownership_id,
            s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_ea;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 9. Bronze → silver.ws_online_pricing
-- ============================================================================
CREATE OR ALTER PROCEDURE silver.usp_conform_ws_online_pricing
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_MARKET_DAILY', 'SILVER', 'silver.ws_online_pricing', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                TRIM(src.wso_security_id)                   AS wso_security_id,
                TRY_CAST(src.price_date AS DATE)            AS price_date,
                TRY_CAST(src.price_close AS DECIMAL(18,6))  AS price_close,
                TRY_CAST(src.price_open AS DECIMAL(18,6))   AS price_open,
                TRY_CAST(src.price_high AS DECIMAL(18,6))   AS price_high,
                TRY_CAST(src.price_low AS DECIMAL(18,6))    AS price_low,
                TRY_CAST(src.volume AS BIGINT)              AS volume,
                UPPER(TRIM(src.currency))                   AS currency,
                CAST(src._record_id AS NVARCHAR(200))       AS _bronze_record_id,
                src._ingested_at                            AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    CAST(TRY_CAST(src.price_close AS DECIMAL(18,6)) AS NVARCHAR),
                    CAST(TRY_CAST(src.price_open AS DECIMAL(18,6)) AS NVARCHAR),
                    CAST(TRY_CAST(src.price_high AS DECIMAL(18,6)) AS NVARCHAR),
                    CAST(TRY_CAST(src.price_low AS DECIMAL(18,6)) AS NVARCHAR),
                    CAST(TRY_CAST(src.volume AS BIGINT) AS NVARCHAR),
                    UPPER(TRIM(src.currency))
                )) AS _row_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY src.wso_security_id, src.price_date
                    ORDER BY src._ingested_at DESC
                ) AS rn
            FROM bronze.src_ws_online_raw src
            WHERE src._record_type = 'pricing'
              AND (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_price FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: unparseable date
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.ws_online_pricing',
               CONCAT('{"wso_id":"', wso_security_id, '"}'),
               'PRICE_DATE_VALID', 'price_date could not be parsed'
        FROM #staged_price WHERE price_date IS NULL;
        SET @quar = @@ROWCOUNT;

        -- Quarantine: WSO security doesn't exist in silver
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.ws_online_pricing',
               CONCAT('{"wso_id":"', s.wso_security_id, '","date":"', CAST(s.price_date AS NVARCHAR), '"}'),
               'PRICE_WSO_SEC_EXISTS', 'wso_security_id not found in silver.ws_online_security'
        FROM #staged_price s
        WHERE s.price_date IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.ws_online_security w WHERE w.wso_security_id = s.wso_security_id);
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.ws_online_pricing AS t
        USING (
            SELECT s.* FROM #staged_price s
            WHERE s.price_date IS NOT NULL
              AND s.wso_security_id IS NOT NULL
              AND EXISTS (SELECT 1 FROM silver.ws_online_security w WHERE w.wso_security_id = s.wso_security_id)
        ) AS s
        ON  t.wso_security_id = s.wso_security_id
        AND t.price_date      = s.price_date
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.price_close       = s.price_close,
            t.price_open        = s.price_open,
            t.price_high        = s.price_high,
            t.price_low         = s.price_low,
            t.volume            = s.volume,
            t.currency          = s.currency,
            t._row_hash         = s._row_hash,
            t._source_modified_at = s._source_modified_at,
            t._bronze_record_id = s._bronze_record_id,
            t._conformed_at     = GETUTCDATE(),
            t._conformed_by     = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            wso_security_id, price_date, price_close, price_open,
            price_high, price_low, volume, currency,
            _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.wso_security_id, s.price_date, s.price_close, s.price_open,
            s.price_high, s.price_low, s.volume, s.currency,
            s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_price;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 10. Bronze → silver.security (COMPOSITE ASSEMBLY)
--     Cascading match precedence: BankLoanID → CUSIP → ISIN → ticker+type
--     Enriches internal SSM records with WSO market identifiers
-- ============================================================================
CREATE OR ALTER PROCEDURE silver.usp_conform_security
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_SECURITY_DAILY', 'SILVER', 'silver.security', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        -- Stage 1: Parse and translate SSM bronze records
        ;WITH staged AS (
            SELECT
                meta.fn_translate_key(src.security_id, 'SSM-SEC-', 'SEC-') AS security_enterprise_key,
                UPPER(TRIM(src.security_type))                             AS security_type,
                TRIM(src.security_group)                                   AS security_group,
                TRIM(src.security_name)                                    AS security_name,
                UPPER(TRIM(src.security_status))                           AS security_status,
                meta.fn_translate_key(src.team_ref,   'SSM-IT-', 'IT-')    AS investment_team_enterprise_key,
                meta.fn_translate_key(src.entity_ref, 'SSM-E-',  'E-')     AS entity_enterprise_key,
                meta.fn_translate_key(src.asset_ref,  'SSM-A-',  'A-')     AS asset_enterprise_key,
                NULLIF(TRIM(src.bank_loan_id), '')                         AS bank_loan_id,
                NULLIF(TRIM(src.cusip), '')                                AS cusip,
                NULLIF(TRIM(src.isin), '')                                 AS isin,
                NULLIF(TRIM(src.ticker), '')                               AS ticker,
                src.security_id                                            AS src_security_id,
                CAST(src._record_id AS NVARCHAR(200))                      AS _bronze_record_id,
                src._ingested_at                                           AS _source_modified_at,
                ROW_NUMBER() OVER (PARTITION BY src.security_id ORDER BY src._ingested_at DESC) AS rn
            FROM bronze.src_security_mgmt_raw src
            WHERE (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_sec FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: invalid security_type (FIX: exact-match using delimited CHARINDEX)
        DECLARE @valid_types NVARCHAR(MAX) = 'EQUITY,SENIOR_DEBT,MEZZANINE,SUBORDINATED_DEBT,CONVERTIBLE,PREFERRED,DERIVATIVE,WARRANT,OPTION';
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.security',
               CONCAT('{"ek":"', s.security_enterprise_key, '","type":"', s.security_type, '"}'),
               'SEC_TYPE_VALID', 'security_type not in valid list'
        FROM #staged_sec s
        WHERE CHARINDEX(',' + s.security_type + ',', ',' + @valid_types + ',') = 0;
        SET @quar = @@ROWCOUNT;

        -- Quarantine: missing entity reference
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.security',
               CONCAT('{"ek":"', s.security_enterprise_key, '"}'),
               'SEC_HAS_ENTITY', 'entity_enterprise_key is NULL'
        FROM #staged_sec s
        WHERE s.entity_enterprise_key IS NULL
          AND CHARINDEX(',' + s.security_type + ',', ',' + @valid_types + ',') > 0;
        SET @quar = @quar + @@ROWCOUNT;

        -- Remove quarantined rows from staging
        DELETE FROM #staged_sec
        WHERE CHARINDEX(',' + security_type + ',', ',' + @valid_types + ',') = 0
           OR entity_enterprise_key IS NULL;

        -- Stage 2: Composite assembly — match SSM records to WSO records
        -- Cascading precedence: BankLoanID(1) → CUSIP(2) → ISIN(3) → ticker+type(4)
        SELECT
            s.*,
            -- Match attempt 1: BankLoanID
            wso1.wso_security_id AS match1_wso_id,
            -- Match attempt 2: CUSIP
            wso2.wso_security_id AS match2_wso_id,
            wso2_dup.dup_count   AS match2_dup_count,
            -- Match attempt 3: ISIN
            wso3.wso_security_id AS match3_wso_id,
            -- Match attempt 4: ticker + type
            wso4.wso_security_id AS match4_wso_id
        INTO #sec_with_matches
        FROM #staged_sec s
        -- 1: BankLoanID exact match
        LEFT JOIN silver.ws_online_security wso1
            ON s.bank_loan_id IS NOT NULL
            AND wso1.bank_loan_id = s.bank_loan_id
        -- 2: CUSIP exact match (check for ambiguity)
        LEFT JOIN silver.ws_online_security wso2
            ON s.cusip IS NOT NULL
            AND wso2.cusip = s.cusip
            AND wso1.wso_security_id IS NULL  -- only if no BankLoanID match
        LEFT JOIN (
            SELECT cusip, COUNT(*) AS dup_count
            FROM silver.ws_online_security
            WHERE cusip IS NOT NULL
            GROUP BY cusip
        ) wso2_dup ON wso2_dup.cusip = s.cusip
        -- 3: ISIN exact match
        LEFT JOIN silver.ws_online_security wso3
            ON s.isin IS NOT NULL
            AND wso3.isin = s.isin
            AND wso1.wso_security_id IS NULL
            AND wso2.wso_security_id IS NULL
        -- 4: ticker + type match
        LEFT JOIN silver.ws_online_security wso4
            ON s.ticker IS NOT NULL
            AND wso4.ticker = s.ticker
            AND wso4.security_type = s.security_type
            AND wso1.wso_security_id IS NULL
            AND wso2.wso_security_id IS NULL
            AND wso3.wso_security_id IS NULL;

        -- Stage 3: Resolve match results into final columns
        SELECT
            m.security_enterprise_key,
            m.security_type,
            m.security_group,
            m.security_name,
            m.security_status,
            m.investment_team_enterprise_key,
            m.entity_enterprise_key,
            m.asset_enterprise_key,
            -- Enrich identifiers from WSO if matched (SSM values take precedence for non-null)
            COALESCE(m.bank_loan_id,  wso_final.bank_loan_id)  AS bank_loan_id,
            COALESCE(m.cusip,         wso_final.cusip)          AS cusip,
            COALESCE(m.isin,          wso_final.isin)           AS isin,
            COALESCE(m.ticker,        wso_final.ticker)         AS ticker,
            -- Match metadata
            CASE
                WHEN m.match1_wso_id IS NOT NULL THEN 'MATCHED'
                WHEN m.match2_wso_id IS NOT NULL AND ISNULL(m.match2_dup_count, 0) <= 1 THEN 'MATCHED'
                WHEN m.match2_wso_id IS NOT NULL AND m.match2_dup_count > 1 THEN 'AMBIGUOUS'
                WHEN m.match3_wso_id IS NOT NULL THEN 'MATCHED'
                WHEN m.match4_wso_id IS NOT NULL THEN 'MATCHED'
                ELSE 'UNMATCHED'
            END AS _wso_match_status,
            CASE
                WHEN m.match1_wso_id IS NOT NULL THEN 'BANK_LOAN_ID'
                WHEN m.match2_wso_id IS NOT NULL AND ISNULL(m.match2_dup_count, 0) <= 1 THEN 'CUSIP'
                WHEN m.match2_wso_id IS NOT NULL AND m.match2_dup_count > 1 THEN 'CUSIP_AMBIGUOUS'
                WHEN m.match3_wso_id IS NOT NULL THEN 'ISIN'
                WHEN m.match4_wso_id IS NOT NULL THEN 'TICKER_TYPE'
                ELSE NULL
            END AS _wso_match_key,
            CASE
                WHEN m.match1_wso_id IS NOT NULL THEN 'HIGH'
                WHEN m.match2_wso_id IS NOT NULL AND ISNULL(m.match2_dup_count, 0) <= 1 THEN 'HIGH'
                WHEN m.match2_wso_id IS NOT NULL AND m.match2_dup_count > 1 THEN 'LOW'
                WHEN m.match3_wso_id IS NOT NULL THEN 'HIGH'
                WHEN m.match4_wso_id IS NOT NULL THEN 'MEDIUM'
                ELSE NULL
            END AS _wso_match_confidence,
            m.src_security_id,
            m._bronze_record_id,
            m._source_modified_at,
            HASHBYTES('SHA2_256', CONCAT_WS('|',
                m.security_type, m.security_group, m.security_name, m.security_status,
                m.investment_team_enterprise_key, m.entity_enterprise_key, m.asset_enterprise_key,
                COALESCE(m.bank_loan_id, wso_final.bank_loan_id),
                COALESCE(m.cusip, wso_final.cusip),
                COALESCE(m.isin, wso_final.isin),
                COALESCE(m.ticker, wso_final.ticker)
            )) AS _row_hash
        INTO #sec_final_raw
        FROM #sec_with_matches m
        LEFT JOIN silver.ws_online_security wso_final
            ON wso_final.wso_security_id = COALESCE(m.match1_wso_id, m.match2_wso_id, m.match3_wso_id, m.match4_wso_id);

        -- Deduplicate: one row per security_enterprise_key (prefer MATCHED over UNMATCHED)
        SELECT * INTO #sec_final
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY security_enterprise_key
                ORDER BY CASE _wso_match_status WHEN 'MATCHED' THEN 1 WHEN 'AMBIGUOUS' THEN 2 ELSE 3 END
            ) AS _dedup_rn
            FROM #sec_final_raw
        ) d WHERE _dedup_rn = 1;

        -- MERGE into silver.security
        MERGE INTO silver.security AS t
        USING #sec_final AS s
        ON t.security_enterprise_key = s.security_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.security_type                  = s.security_type,
            t.security_group                 = s.security_group,
            t.security_name                  = s.security_name,
            t.security_status                = s.security_status,
            t.investment_team_enterprise_key  = s.investment_team_enterprise_key,
            t.entity_enterprise_key           = s.entity_enterprise_key,
            t.asset_enterprise_key            = s.asset_enterprise_key,
            t.bank_loan_id                   = s.bank_loan_id,
            t.cusip                          = s.cusip,
            t.isin                           = s.isin,
            t.ticker                         = s.ticker,
            t._wso_match_status              = s._wso_match_status,
            t._wso_match_key                 = s._wso_match_key,
            t._wso_match_confidence          = s._wso_match_confidence,
            t._source_modified_at            = s._source_modified_at,
            t._bronze_record_id              = s._bronze_record_id,
            t._conformed_at                  = GETUTCDATE(),
            t._conformed_by                  = SYSTEM_USER,
            t._row_hash                      = s._row_hash
        WHEN NOT MATCHED THEN INSERT (
            security_enterprise_key, security_type, security_group, security_name, security_status,
            investment_team_enterprise_key, entity_enterprise_key, asset_enterprise_key,
            bank_loan_id, cusip, isin, ticker,
            _wso_match_status, _wso_match_key, _wso_match_confidence,
            src_security_id, _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.security_enterprise_key, s.security_type, s.security_group, s.security_name, s.security_status,
            s.investment_team_enterprise_key, s.entity_enterprise_key, s.asset_enterprise_key,
            s.bank_loan_id, s.cusip, s.isin, s.ticker,
            s._wso_match_status, s._wso_match_key, s._wso_match_confidence,
            s.src_security_id, s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_sec, #sec_with_matches, #sec_final_raw, #sec_final;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 11. Bronze → silver.position_transaction (renamed from silver.[transaction])
-- ============================================================================
CREATE OR ALTER PROCEDURE silver.usp_conform_transaction
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_TXN_DAILY', 'SILVER', 'silver.position_transaction', 'MERGE', @run_id OUTPUT;

    DECLARE @ins INT = 0, @quar INT = 0, @read INT = 0;

    BEGIN TRY
        ;WITH staged AS (
            SELECT
                TRIM(src.transaction_id)                                    AS stm_transaction_id,
                meta.fn_translate_key(src.portfolio_id, 'STM-P-',   'P-')   AS portfolio_enterprise_key,
                meta.fn_translate_key(src.entity_id,    'STM-E-',   'E-')   AS entity_enterprise_key,
                meta.fn_translate_key(src.security_id,  'STM-SEC-', 'SEC-') AS security_enterprise_key,
                TRY_CAST(src.as_of_date AS DATE)                            AS as_of_date,
                UPPER(TRIM(src.transaction_type))                           AS transaction_type,
                TRIM(src.transaction_category)                              AS transaction_category,
                UPPER(TRIM(src.transaction_status))                         AS transaction_status,
                TRY_CAST(src.amount_portfolio AS DECIMAL(18,4))             AS transaction_amount_portfolio,
                TRY_CAST(src.amount_local AS DECIMAL(18,4))                 AS transaction_amount_local,
                TRY_CAST(src.amount_usd AS DECIMAL(18,4))                   AS transaction_amount_usd,
                TRY_CAST(src.fx_rate AS DECIMAL(18,8))                      AS base_fx_rate,
                TRY_CAST(src.quantity AS DECIMAL(18,6))                      AS quantity,
                TRIM(src.order_id)                                          AS order_id,
                TRY_CAST(src.order_date AS DATE)                            AS order_date,
                UPPER(TRIM(src.order_status))                               AS order_status,
                src.portfolio_id                                            AS src_portfolio_id,
                src.entity_id                                               AS src_entity_id,
                src.security_id                                             AS src_security_id,
                CAST(src._record_id AS NVARCHAR(200))                       AS _bronze_record_id,
                src._ingested_at                                            AS _source_modified_at,
                HASHBYTES('SHA2_256', CONCAT_WS('|',
                    TRIM(src.transaction_id),
                    CAST(TRY_CAST(src.as_of_date AS DATE) AS NVARCHAR),
                    UPPER(TRIM(src.transaction_type)),
                    UPPER(TRIM(src.transaction_status)),
                    CAST(TRY_CAST(src.amount_portfolio AS DECIMAL(18,4)) AS NVARCHAR),
                    CAST(TRY_CAST(src.amount_local AS DECIMAL(18,4)) AS NVARCHAR),
                    CAST(TRY_CAST(src.amount_usd AS DECIMAL(18,4)) AS NVARCHAR)
                )) AS _row_hash,
                ROW_NUMBER() OVER (PARTITION BY src.transaction_id ORDER BY src._ingested_at DESC) AS rn
            FROM bronze.src_txn_mgmt_raw src
            WHERE (@batch_id IS NULL OR src._batch_id = @batch_id)
        )
        SELECT * INTO #staged_txn FROM staged WHERE rn = 1;
        SET @read = @@ROWCOUNT;

        -- Quarantine: security FK doesn't exist in silver
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.position_transaction',
               CONCAT('{"txn_id":"', s.stm_transaction_id, '","sec_ek":"', s.security_enterprise_key, '"}'),
               'TXN_SECURITY_EXISTS', 'security_enterprise_key not found in silver.security'
        FROM #staged_txn s
        WHERE s.security_enterprise_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.security sec WHERE sec.security_enterprise_key = s.security_enterprise_key);
        SET @quar = @@ROWCOUNT;

        -- Quarantine: no amount at all
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.position_transaction',
               CONCAT('{"txn_id":"', s.stm_transaction_id, '"}'),
               'TXN_AMOUNT_PRESENT', 'All amount columns are NULL'
        FROM #staged_txn s
        WHERE s.transaction_amount_usd IS NULL
          AND s.transaction_amount_local IS NULL
          AND s.transaction_amount_portfolio IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: unparseable as_of_date
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.position_transaction',
               CONCAT('{"txn_id":"', s.stm_transaction_id, '"}'),
               'TXN_DATE_VALID', 'as_of_date could not be parsed'
        FROM #staged_txn s WHERE s.as_of_date IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: portfolio FK doesn't exist in silver
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.position_transaction',
               CONCAT('{"txn_id":"', s.stm_transaction_id, '","port_ek":"', s.portfolio_enterprise_key, '"}'),
               'TXN_PORTFOLIO_EXISTS', 'portfolio_enterprise_key not found in silver.portfolio'
        FROM #staged_txn s
        WHERE s.portfolio_enterprise_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.portfolio p WHERE p.portfolio_enterprise_key = s.portfolio_enterprise_key);
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: entity FK doesn't exist in silver
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.position_transaction',
               CONCAT('{"txn_id":"', s.stm_transaction_id, '","ent_ek":"', s.entity_enterprise_key, '"}'),
               'TXN_ENTITY_EXISTS', 'entity_enterprise_key not found in silver.entity'
        FROM #staged_txn s
        WHERE s.entity_enterprise_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.entity e WHERE e.entity_enterprise_key = s.entity_enterprise_key);
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL transaction_type
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.position_transaction',
               CONCAT('{"txn_id":"', s.stm_transaction_id, '"}'),
               'TXN_TYPE_NOT_NULL', 'transaction_type is NULL'
        FROM #staged_txn s WHERE s.transaction_type IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        -- Quarantine: NULL transaction_status
        INSERT INTO silver.quarantine (source_table, raw_payload, failed_rule, failure_detail)
        SELECT 'silver.position_transaction',
               CONCAT('{"txn_id":"', s.stm_transaction_id, '"}'),
               'TXN_STATUS_NOT_NULL', 'transaction_status is NULL'
        FROM #staged_txn s WHERE s.transaction_status IS NULL;
        SET @quar = @quar + @@ROWCOUNT;

        MERGE INTO silver.position_transaction AS t
        USING (
            SELECT s.* FROM #staged_txn s
            WHERE s.stm_transaction_id IS NOT NULL
              AND s.as_of_date IS NOT NULL
              AND s.portfolio_enterprise_key IS NOT NULL
              AND s.entity_enterprise_key IS NOT NULL
              AND s.security_enterprise_key IS NOT NULL
              AND s.transaction_type IS NOT NULL
              AND s.transaction_status IS NOT NULL
              AND (s.transaction_amount_usd IS NOT NULL OR s.transaction_amount_local IS NOT NULL OR s.transaction_amount_portfolio IS NOT NULL)
              AND EXISTS (SELECT 1 FROM silver.portfolio p WHERE p.portfolio_enterprise_key = s.portfolio_enterprise_key)
              AND EXISTS (SELECT 1 FROM silver.entity e WHERE e.entity_enterprise_key = s.entity_enterprise_key)
              AND EXISTS (SELECT 1 FROM silver.security sec WHERE sec.security_enterprise_key = s.security_enterprise_key)
        ) AS s
        ON t.stm_transaction_id = s.stm_transaction_id
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.portfolio_enterprise_key      = s.portfolio_enterprise_key,
            t.entity_enterprise_key         = s.entity_enterprise_key,
            t.security_enterprise_key       = s.security_enterprise_key,
            t.as_of_date                    = s.as_of_date,
            t.transaction_type              = s.transaction_type,
            t.transaction_category          = s.transaction_category,
            t.transaction_status            = s.transaction_status,
            t.transaction_amount_portfolio  = s.transaction_amount_portfolio,
            t.transaction_amount_local      = s.transaction_amount_local,
            t.transaction_amount_usd        = s.transaction_amount_usd,
            t.base_fx_rate                  = s.base_fx_rate,
            t.quantity                      = s.quantity,
            t.order_id                      = s.order_id,
            t.order_date                    = s.order_date,
            t.order_status                  = s.order_status,
            t._source_modified_at           = s._source_modified_at,
            t._bronze_record_id             = s._bronze_record_id,
            t._conformed_at                 = GETUTCDATE(),
            t._conformed_by                 = SYSTEM_USER,
            t._row_hash                     = s._row_hash
        WHEN NOT MATCHED THEN INSERT (
            stm_transaction_id, portfolio_enterprise_key, entity_enterprise_key, security_enterprise_key,
            as_of_date, transaction_type, transaction_category, transaction_status,
            transaction_amount_portfolio, transaction_amount_local, transaction_amount_usd,
            base_fx_rate, quantity, order_id, order_date, order_status,
            src_portfolio_id, src_entity_id, src_security_id,
            _bronze_record_id, _source_modified_at, _row_hash
        ) VALUES (
            s.stm_transaction_id, s.portfolio_enterprise_key, s.entity_enterprise_key, s.security_enterprise_key,
            s.as_of_date, s.transaction_type, s.transaction_category, s.transaction_status,
            s.transaction_amount_portfolio, s.transaction_amount_local, s.transaction_amount_usd,
            s.base_fx_rate, s.quantity, s.order_id, s.order_date, s.order_status,
            s.src_portfolio_id, s.src_entity_id, s.src_security_id,
            s._bronze_record_id, s._source_modified_at, s._row_hash
        );
        SET @ins = @@ROWCOUNT;

        DROP TABLE #staged_txn;
        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @ins, 0, 0, @quar;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, @quar, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- UPDATED ORCHESTRATORS
-- ============================================================================


-- ============================================================================
-- ORCHESTRATORS
-- ============================================================================
-- 4.1 Enterprise daily pipeline (silver phase): team → pg → portfolio in order
CREATE OR ALTER PROCEDURE silver.usp_run_enterprise_silver
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        PRINT '>>> silver.usp_conform_investment_team';
        EXEC silver.usp_conform_investment_team @batch_id;
        PRINT '>>> silver.usp_conform_portfolio_group';
        EXEC silver.usp_conform_portfolio_group @batch_id;
        PRINT '>>> silver.usp_conform_portfolio';
        EXEC silver.usp_conform_portfolio @batch_id;
        PRINT '>>> PL_ENTERPRISE_DAILY silver phase complete';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR in usp_run_enterprise_silver: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO
CREATE OR ALTER PROCEDURE silver.usp_run_entity_silver
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        PRINT '>>> silver.usp_conform_entity';
        EXEC silver.usp_conform_entity @batch_id;
        PRINT '>>> silver.usp_conform_portfolio_entity_ownership';
        EXEC silver.usp_conform_portfolio_entity_ownership @batch_id;
        PRINT '>>> silver.usp_conform_entity_asset_ownership';
        EXEC silver.usp_conform_entity_asset_ownership @batch_id;
        PRINT '>>> PL_ENTITY_DAILY silver phase complete';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR in usp_run_entity_silver: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

-- Market data daily: WSO security → WSO pricing
CREATE OR ALTER PROCEDURE silver.usp_run_market_silver
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        PRINT '>>> silver.usp_conform_ws_online_security';
        EXEC silver.usp_conform_ws_online_security @batch_id;
        PRINT '>>> silver.usp_conform_ws_online_pricing';
        EXEC silver.usp_conform_ws_online_pricing @batch_id;
        PRINT '>>> PL_MARKET_DAILY silver phase complete';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR in usp_run_market_silver: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

-- Full silver orchestrator (replaces incomplete version in 02_functions.sql)
CREATE OR ALTER PROCEDURE silver.usp_run_all_silver
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @phase NVARCHAR(50) = 'INIT';
    BEGIN TRY
        SET @phase = 'PHASE 1';
        PRINT '=== PHASE 1: Independent dimension sources ===';
        EXEC silver.usp_run_enterprise_silver @batch_id;
        EXEC silver.usp_conform_entity @batch_id;
        EXEC silver.usp_conform_asset @batch_id;
        EXEC silver.usp_run_market_silver @batch_id;

        SET @phase = 'PHASE 2';
        PRINT '=== PHASE 2: Ownership bridges ===';
        EXEC silver.usp_conform_portfolio_entity_ownership @batch_id;
        EXEC silver.usp_conform_entity_asset_ownership @batch_id;

        SET @phase = 'PHASE 3';
        PRINT '=== PHASE 3: Security composite assembly ===';
        EXEC silver.usp_conform_security @batch_id;

        SET @phase = 'PHASE 4';
        PRINT '=== PHASE 4: Transactions ===';
        EXEC silver.usp_conform_transaction @batch_id;

        PRINT '=== Silver phase complete ===';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR in usp_run_all_silver during ' + @phase + ': ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO


-- ============================================================================


-- ============================================================================
-- VIEWS
-- ============================================================================
-- 5.5 Silver data quality dashboard
CREATE OR ALTER VIEW silver.vw_quarantine_summary AS
SELECT
    source_table,
    failed_rule,
    resolution_status,
    COUNT(*) AS row_count,
    MIN(quarantined_at) AS earliest,
    MAX(quarantined_at) AS latest
FROM silver.quarantine
GROUP BY source_table, failed_rule, resolution_status;
GO


-- ============================================================================
-- VERIFICATION
-- ============================================================================
PRINT '=== SILVER OBJECTS (04) ==='
SELECT s.name + '.' + o.name AS object_name,
       CASE o.type WHEN 'P' THEN 'PROC' WHEN 'V' THEN 'VIEW' END AS type
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE s.name = 'silver' AND o.type IN ('P','V')
ORDER BY o.type, o.name;
-- Expected: 16 procedures + 1 view = 17 objects
GO
