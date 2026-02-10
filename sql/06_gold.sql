-- ============================================================================
-- GOLDEN DATA LAYER: ALL GOLD PROCEDURES & VIEWS
-- Target: Azure SQL Edge Developer (SQL Server 15.x compatible)
-- Date: 2026-02-09
-- Depends on: 01_ddl.sql, 02_meta_programmability.sql, 03_audit.sql, 04_silver.sql
-- ============================================================================
-- Dimension loads (6):
--   gold.usp_load_investment_team_dimension
--   gold.usp_load_portfolio_group_dimension
--   gold.usp_load_portfolio_dimension
--   gold.usp_load_entity_dimension
--   gold.usp_load_asset_dimension
--   gold.usp_load_security_dimension
-- Bridge loads (2):
--   gold.usp_load_portfolio_entity_bridge
--   gold.usp_load_entity_asset_bridge
-- Fact loads (3):
--   gold.usp_load_position_transactions_fact
--   gold.usp_load_position_fact
--   gold.usp_load_position_team_bridge
-- Orchestrators:
--   gold.usp_run_all_gold                   (dims → bridges → facts)
--   dbo.usp_run_full_pipeline               (silver → gold end-to-end)
-- Views (4):
--   gold.vw_investment_hierarchy             (team → fund → portfolio → entity)
--   gold.vw_entity_asset_hierarchy           (entity → asset via bridge)
--   gold.vw_position_detail                  (fully resolved position)
--   gold.vw_position_by_team                 (weighted by team allocation)
-- ============================================================================
--  21. gold.usp_load_position_fact
--  22. gold.usp_load_position_team_bridge
--  Orchestrator: gold.usp_run_all_gold
-- ============================================================================
USE GoldenDataLayer;
GO


-- ============================================================================
-- 12. silver → gold.investment_team_dimension
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_investment_team_dimension
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTERPRISE_DAILY', 'GOLD', 'gold.investment_team_dimension', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.investment_team;

        MERGE INTO gold.investment_team_dimension AS t
        USING (
            SELECT
                investment_team_enterprise_key,
                investment_team_name,
                investment_team_short_name,
                start_date,
                stop_date,
                _row_hash
            FROM silver.investment_team
        ) AS s
        ON t.investment_team_enterprise_key = s.investment_team_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.investment_team_name       = s.investment_team_name,
            t.investment_team_short_name = s.investment_team_short_name,
            t.start_date                 = s.start_date,
            t.stop_date                  = s.stop_date,
            t._row_hash                  = s._row_hash,
            t.modified_date              = GETUTCDATE(),
            t.modified_by                = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            investment_team_enterprise_key, investment_team_name,
            investment_team_short_name, start_date, stop_date, _row_hash
        ) VALUES (
            s.investment_team_enterprise_key, s.investment_team_name,
            s.investment_team_short_name, s.start_date, s.stop_date, s._row_hash
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 13. silver → gold.portfolio_group_dimension
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_portfolio_group_dimension
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTERPRISE_DAILY', 'GOLD', 'gold.portfolio_group_dimension', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.portfolio_group;

        MERGE INTO gold.portfolio_group_dimension AS t
        USING (
            SELECT
                portfolio_group_enterprise_key,
                portfolio_group_name,
                portfolio_group_short_name,
                portfolio_group_description,
                investment_team_enterprise_key,
                vintage_year, strategy,
                committed_capital, committed_capital_currency,
                fund_status,
                _row_hash
            FROM silver.portfolio_group
        ) AS s
        ON t.portfolio_group_enterprise_key = s.portfolio_group_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.portfolio_group_name          = s.portfolio_group_name,
            t.portfolio_group_short_name    = s.portfolio_group_short_name,
            t.portfolio_group_description   = s.portfolio_group_description,
            t.investment_team_enterprise_key = s.investment_team_enterprise_key,
            t.vintage_year                  = s.vintage_year,
            t.strategy                      = s.strategy,
            t.committed_capital             = s.committed_capital,
            t.committed_capital_currency    = s.committed_capital_currency,
            t.fund_status                   = s.fund_status,
            t._row_hash                     = s._row_hash,
            t.modified_date                 = GETUTCDATE(),
            t.modified_by                   = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            portfolio_group_enterprise_key, portfolio_group_name, portfolio_group_short_name,
            portfolio_group_description, investment_team_enterprise_key,
            vintage_year, strategy, committed_capital, committed_capital_currency, fund_status,
            _row_hash
        ) VALUES (
            s.portfolio_group_enterprise_key, s.portfolio_group_name, s.portfolio_group_short_name,
            s.portfolio_group_description, s.investment_team_enterprise_key,
            s.vintage_year, s.strategy, s.committed_capital, s.committed_capital_currency, s.fund_status,
            s._row_hash
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 14. silver → gold.portfolio_dimension
--     Resolves portfolio_group_key via EK lookup
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_portfolio_dimension
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTERPRISE_DAILY', 'GOLD', 'gold.portfolio_dimension', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.portfolio;

        MERGE INTO gold.portfolio_dimension AS t
        USING (
            SELECT
                sp.portfolio_enterprise_key,
                sp.portfolio_name,
                sp.portfolio_short_name,
                gpg.portfolio_group_key,
                sp._row_hash
            FROM silver.portfolio sp
            INNER JOIN gold.portfolio_group_dimension gpg
                ON gpg.portfolio_group_enterprise_key = sp.portfolio_group_enterprise_key
        ) AS s
        ON t.portfolio_enterprise_key = s.portfolio_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.portfolio_name       = s.portfolio_name,
            t.portfolio_short_name = s.portfolio_short_name,
            t.portfolio_group_key  = s.portfolio_group_key,
            t._row_hash            = s._row_hash,
            t.modified_date        = GETUTCDATE(),
            t.modified_by          = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            portfolio_enterprise_key, portfolio_name, portfolio_short_name,
            portfolio_group_key, _row_hash
        ) VALUES (
            s.portfolio_enterprise_key, s.portfolio_name, s.portfolio_short_name,
            s.portfolio_group_key, s._row_hash
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 15. silver → gold.entity_dimension
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_entity_dimension
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTITY_DAILY', 'GOLD', 'gold.entity_dimension', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.entity;

        MERGE INTO gold.entity_dimension AS t
        USING (
            SELECT entity_enterprise_key, entity_name, entity_short_name, entity_legal_name,
                   entity_type, entity_status, incorporation_jurisdiction, incorporation_date,
                   _row_hash
            FROM silver.entity
        ) AS s
        ON t.entity_enterprise_key = s.entity_enterprise_key
        WHEN MATCHED AND t._row_hash != s._row_hash THEN UPDATE SET
            t.entity_name                  = s.entity_name,
            t.entity_short_name            = s.entity_short_name,
            t.entity_legal_name            = s.entity_legal_name,
            t.entity_type                  = s.entity_type,
            t.entity_status                = s.entity_status,
            t.incorporation_jurisdiction   = s.incorporation_jurisdiction,
            t.incorporation_date           = s.incorporation_date,
            t._row_hash                    = s._row_hash,
            t.modified_date                = GETUTCDATE(),
            t.modified_by                  = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            entity_enterprise_key, entity_name, entity_short_name, entity_legal_name,
            entity_type, entity_status, incorporation_jurisdiction, incorporation_date,
            _row_hash
        ) VALUES (
            s.entity_enterprise_key, s.entity_name, s.entity_short_name, s.entity_legal_name,
            s.entity_type, s.entity_status, s.incorporation_jurisdiction, s.incorporation_date,
            s._row_hash
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 16. silver → gold.asset_dimension
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_asset_dimension
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ASSET_DAILY', 'GOLD', 'gold.asset_dimension', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.asset;

        MERGE INTO gold.asset_dimension AS t
        USING (
            SELECT asset_enterprise_key, asset_name, asset_short_name, asset_legal_name,
                   asset_type, asset_subtype, asset_status,
                   location_country, location_region, acquisition_date,
                   last_valuation_date, last_valuation_amount, last_valuation_currency,
                   _row_hash
            FROM silver.asset
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
            t._row_hash               = s._row_hash,
            t.modified_date           = GETUTCDATE(),
            t.modified_by             = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            asset_enterprise_key, asset_name, asset_short_name, asset_legal_name,
            asset_type, asset_subtype, asset_status,
            location_country, location_region, acquisition_date,
            last_valuation_date, last_valuation_amount, last_valuation_currency,
            _row_hash
        ) VALUES (
            s.asset_enterprise_key, s.asset_name, s.asset_short_name, s.asset_legal_name,
            s.asset_type, s.asset_subtype, s.asset_status,
            s.location_country, s.location_region, s.acquisition_date,
            s.last_valuation_date, s.last_valuation_amount, s.last_valuation_currency,
            s._row_hash
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 17. silver → gold.security_dimension
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_security_dimension
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_SECURITY_DAILY', 'GOLD', 'gold.security_dimension', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.security;

        MERGE INTO gold.security_dimension AS t
        USING (
            SELECT security_enterprise_key, security_type, security_group, security_name,
                   security_status, investment_team_enterprise_key,
                   entity_enterprise_key, asset_enterprise_key,
                   bank_loan_id, cusip, isin, ticker,
                   _row_hash
            FROM silver.security
        ) AS s
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
            t._row_hash                      = s._row_hash,
            t.modified_date                  = GETUTCDATE(),
            t.modified_by                    = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            security_enterprise_key, security_type, security_group, security_name,
            security_status, investment_team_enterprise_key,
            entity_enterprise_key, asset_enterprise_key,
            bank_loan_id, cusip, isin, ticker, _row_hash
        ) VALUES (
            s.security_enterprise_key, s.security_type, s.security_group, s.security_name,
            s.security_status, s.investment_team_enterprise_key,
            s.entity_enterprise_key, s.asset_enterprise_key,
            s.bank_loan_id, s.cusip, s.isin, s.ticker, s._row_hash
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 18. silver → gold.portfolio_entity_bridge
--     Resolves surrogate keys via EK lookups on gold dimensions
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_portfolio_entity_bridge
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTITY_DAILY', 'GOLD', 'gold.portfolio_entity_bridge', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.portfolio_entity_ownership;

        MERGE INTO gold.portfolio_entity_bridge AS t
        USING (
            SELECT
                gp.portfolio_key,
                ge.entity_key,
                spe.ownership_pct,
                spe.effective_date,
                spe.end_date,
                spe._source_system_id AS source_system_key
            FROM silver.portfolio_entity_ownership spe
            INNER JOIN gold.portfolio_dimension gp
                ON gp.portfolio_enterprise_key = spe.portfolio_enterprise_key
            INNER JOIN gold.entity_dimension ge
                ON ge.entity_enterprise_key = spe.entity_enterprise_key
        ) AS s
        ON  t.portfolio_key  = s.portfolio_key
        AND t.entity_key     = s.entity_key
        AND t.effective_date = s.effective_date
        WHEN MATCHED AND (t.ownership_pct != s.ownership_pct
                       OR ISNULL(t.end_date, '9999-12-31') != ISNULL(s.end_date, '9999-12-31'))
        THEN UPDATE SET
            t.ownership_pct    = s.ownership_pct,
            t.end_date         = s.end_date,
            t.modified_date    = GETUTCDATE(),
            t.modified_by      = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            portfolio_key, entity_key, ownership_pct, effective_date, end_date, source_system_key
        ) VALUES (
            s.portfolio_key, s.entity_key, s.ownership_pct, s.effective_date, s.end_date, s.source_system_key
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 19. silver → gold.entity_asset_bridge
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_entity_asset_bridge
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_ENTITY_DAILY', 'GOLD', 'gold.entity_asset_bridge', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.entity_asset_ownership;

        MERGE INTO gold.entity_asset_bridge AS t
        USING (
            SELECT
                ge.entity_key,
                ga.asset_key,
                sea.ownership_pct,
                sea.effective_date,
                sea.end_date,
                sea._source_system_id AS source_system_key
            FROM silver.entity_asset_ownership sea
            INNER JOIN gold.entity_dimension ge
                ON ge.entity_enterprise_key = sea.entity_enterprise_key
            INNER JOIN gold.asset_dimension ga
                ON ga.asset_enterprise_key = sea.asset_enterprise_key
        ) AS s
        ON  t.entity_key     = s.entity_key
        AND t.asset_key      = s.asset_key
        AND t.effective_date = s.effective_date
        WHEN MATCHED AND (t.ownership_pct != s.ownership_pct
                       OR ISNULL(t.end_date, '9999-12-31') != ISNULL(s.end_date, '9999-12-31'))
        THEN UPDATE SET
            t.ownership_pct    = s.ownership_pct,
            t.end_date         = s.end_date,
            t.modified_date    = GETUTCDATE(),
            t.modified_by      = SYSTEM_USER
        WHEN NOT MATCHED THEN INSERT (
            entity_key, asset_key, ownership_pct, effective_date, end_date, source_system_key
        ) VALUES (
            s.entity_key, s.asset_key, s.ownership_pct, s.effective_date, s.end_date, s.source_system_key
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 20. silver → gold.position_transactions_fact
--     Resolves all dimension surrogate keys from enterprise keys
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_position_transactions_fact
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_TXN_DAILY', 'GOLD', 'gold.position_transactions_fact', 'MERGE', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM silver.[transaction];

        -- Insert only new transactions (fact table is append-only for settled txns)
        INSERT INTO gold.position_transactions_fact (
            portfolio_key, entity_key, security_key,
            as_of_date, transaction_type, transaction_category, transaction_status,
            source_system_key, source_system_transaction_id, source_system_transaction_type,
            transaction_amount_portfolio, transaction_amount_local, transaction_amount_usd,
            base_fx_rate, quantity, order_id, order_date, order_status
        )
        SELECT
            gp.portfolio_key,
            ge.entity_key,
            gs.security_key,
            st.as_of_date,
            st.transaction_type,
            st.transaction_category,
            st.transaction_status,
            st._source_system_id,
            st.stm_transaction_id,
            st.transaction_type,
            st.transaction_amount_portfolio,
            st.transaction_amount_local,
            st.transaction_amount_usd,
            st.base_fx_rate,
            st.quantity,
            st.order_id,
            st.order_date,
            st.order_status
        FROM silver.[transaction] st
        INNER JOIN gold.portfolio_dimension gp
            ON gp.portfolio_enterprise_key = st.portfolio_enterprise_key
        INNER JOIN gold.entity_dimension ge
            ON ge.entity_enterprise_key = st.entity_enterprise_key
        INNER JOIN gold.security_dimension gs
            ON gs.security_enterprise_key = st.security_enterprise_key
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.position_transactions_fact ptf
            WHERE ptf.source_system_transaction_id = st.stm_transaction_id
        );
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 21. gold.position_transactions_fact → gold.position_fact (summarized)
--     Aggregates transactions by portfolio × entity × security × date × type
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_position_fact
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_POSITION_SUMMARY', 'GOLD', 'gold.position_fact', 'REBUILD', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM gold.position_transactions_fact;

        -- Full rebuild strategy (simple for local dev; Databricks would use incremental)
        DELETE FROM gold.position_team_bridge;   -- child FK first
        DELETE FROM gold.position_fact;

        INSERT INTO gold.position_fact (
            portfolio_key, entity_key, security_key,
            as_of_date, position_type, transaction_type,
            transaction_amount_portfolio, transaction_amount_local, transaction_amount_usd
        )
        SELECT
            portfolio_key,
            entity_key,
            security_key,
            as_of_date,
            'DAILY_SUMMARY' AS position_type,
            transaction_type,
            SUM(transaction_amount_portfolio),
            SUM(transaction_amount_local),
            SUM(transaction_amount_usd)
        FROM gold.position_transactions_fact
        GROUP BY portfolio_key, entity_key, security_key, as_of_date, transaction_type;
        SET @affected = @@ROWCOUNT;

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- 22. gold.position_fact → gold.position_team_bridge
--     Allocates each position to investment team(s) via security → team EK lookup
--     For positions where security maps to a single team: allocation = 1.0
--     Multi-team positions would require a separate allocation table (future)
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_load_position_team_bridge
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @run_id UNIQUEIDENTIFIER;
    EXEC audit.usp_start_etl_run 'PL_POSITION_SUMMARY', 'GOLD', 'gold.position_team_bridge', 'REBUILD', @run_id OUTPUT;

    DECLARE @affected INT = 0, @read INT = 0;

    BEGIN TRY
        SELECT @read = COUNT(*) FROM gold.position_fact;

        -- Clear and rebuild (follows position_fact rebuild)
        DELETE FROM gold.position_team_bridge;

        -- For each position, resolve the team from security → team EK → team dimension
        INSERT INTO gold.position_team_bridge (
            position_fact_key,
            investment_team_key,
            allocation_pct
        )
        SELECT
            pf.position_fact_key,
            git.investment_team_key,
            CAST(1.0 AS DECIMAL(5,4)) AS allocation_pct  -- default: 100% to primary team
        FROM gold.position_fact pf
        INNER JOIN gold.security_dimension gs
            ON gs.security_key = pf.security_key
        INNER JOIN gold.investment_team_dimension git
            ON git.investment_team_enterprise_key = gs.investment_team_enterprise_key;
        SET @affected = @@ROWCOUNT;

        -- Validation: every position should have exactly 1.0 total allocation
        DECLARE @bad_alloc INT;
        SELECT @bad_alloc = COUNT(*)
        FROM (
            SELECT position_fact_key, SUM(allocation_pct) AS total_alloc
            FROM gold.position_team_bridge
            GROUP BY position_fact_key
            HAVING ABS(SUM(allocation_pct) - 1.0) > 0.001
        ) violations;

        IF @bad_alloc > 0
            PRINT 'WARNING: ' + CAST(@bad_alloc AS NVARCHAR) + ' positions have allocation != 1.0';

        EXEC audit.usp_complete_etl_run @run_id, 'SUCCEEDED', @read, @affected, 0, 0, 0;
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC audit.usp_complete_etl_run @run_id, 'FAILED', @read, 0, 0, 0, 0, @err_msg;
        THROW;
    END CATCH
END;
GO


-- ============================================================================
-- GOLD ORCHESTRATOR
-- ============================================================================
CREATE OR ALTER PROCEDURE gold.usp_run_all_gold
AS
BEGIN
    SET NOCOUNT ON;

    -- Phase 1: Independent dimensions (can parallelize in Databricks)
    PRINT '=== GOLD PHASE 1: Dimensions ===';
    EXEC gold.usp_load_investment_team_dimension;
    EXEC gold.usp_load_entity_dimension;
    EXEC gold.usp_load_asset_dimension;

    -- Phase 2: Dependent dimensions (portfolio needs pg_key, security needs all EKs)
    PRINT '=== GOLD PHASE 2: Dependent dimensions ===';
    EXEC gold.usp_load_portfolio_group_dimension;
    EXEC gold.usp_load_portfolio_dimension;
    EXEC gold.usp_load_security_dimension;

    -- Phase 3: Bridges (need dimension surrogate keys)
    PRINT '=== GOLD PHASE 3: Bridges ===';
    EXEC gold.usp_load_portfolio_entity_bridge;
    EXEC gold.usp_load_entity_asset_bridge;

    -- Phase 4: Facts (need all dimension keys)
    PRINT '=== GOLD PHASE 4: Facts ===';
    EXEC gold.usp_load_position_transactions_fact;
    EXEC gold.usp_load_position_fact;
    EXEC gold.usp_load_position_team_bridge;

    PRINT '=== Gold phase complete ===';
END;
GO


-- ============================================================================
-- FULL PIPELINE: SILVER → GOLD END-TO-END
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_run_full_pipeline
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '======================================';
    PRINT '  FULL PIPELINE: ' + CONVERT(NVARCHAR, GETUTCDATE(), 120);
    PRINT '======================================';

    EXEC silver.usp_run_all_silver @batch_id;
    EXEC gold.usp_run_all_gold;

    PRINT '======================================';
    PRINT '  PIPELINE COMPLETE: ' + CONVERT(NVARCHAR, GETUTCDATE(), 120);
    PRINT '======================================';

    -- Summary
    SELECT 'silver.investment_team' AS tbl, COUNT(*) AS cnt FROM silver.investment_team
    UNION ALL SELECT 'silver.portfolio_group', COUNT(*) FROM silver.portfolio_group
    UNION ALL SELECT 'silver.portfolio', COUNT(*) FROM silver.portfolio
    UNION ALL SELECT 'silver.entity', COUNT(*) FROM silver.entity
    UNION ALL SELECT 'silver.asset', COUNT(*) FROM silver.asset
    UNION ALL SELECT 'silver.security', COUNT(*) FROM silver.security
    UNION ALL SELECT 'silver.transaction', COUNT(*) FROM silver.[transaction]
    UNION ALL SELECT 'silver.quarantine', COUNT(*) FROM silver.quarantine
    UNION ALL SELECT 'gold.investment_team_dimension', COUNT(*) FROM gold.investment_team_dimension
    UNION ALL SELECT 'gold.portfolio_group_dimension', COUNT(*) FROM gold.portfolio_group_dimension
    UNION ALL SELECT 'gold.portfolio_dimension', COUNT(*) FROM gold.portfolio_dimension
    UNION ALL SELECT 'gold.entity_dimension', COUNT(*) FROM gold.entity_dimension
    UNION ALL SELECT 'gold.asset_dimension', COUNT(*) FROM gold.asset_dimension
    UNION ALL SELECT 'gold.security_dimension', COUNT(*) FROM gold.security_dimension
    UNION ALL SELECT 'gold.portfolio_entity_bridge', COUNT(*) FROM gold.portfolio_entity_bridge
    UNION ALL SELECT 'gold.entity_asset_bridge', COUNT(*) FROM gold.entity_asset_bridge
    UNION ALL SELECT 'gold.position_transactions_fact', COUNT(*) FROM gold.position_transactions_fact
    UNION ALL SELECT 'gold.position_fact', COUNT(*) FROM gold.position_fact
    UNION ALL SELECT 'gold.position_team_bridge', COUNT(*) FROM gold.position_team_bridge
    ORDER BY tbl;
END;
GO




-- ============================================================================
-- VIEWS
-- ============================================================================
-- 5.1 Full hierarchy: Team → Fund → Portfolio → Entity (via bridge)
CREATE OR ALTER VIEW gold.vw_investment_hierarchy AS
SELECT
    it.investment_team_key,
    it.investment_team_name,
    pg.portfolio_group_key,
    pg.portfolio_group_name,
    pg.vintage_year,
    pg.strategy,
    pg.fund_status,
    p.portfolio_key,
    p.portfolio_name,
    peb.entity_key,
    peb.ownership_pct,
    peb.effective_date,
    peb.end_date,
    e.entity_name,
    e.entity_type,
    e.entity_status
FROM gold.investment_team_dimension it
JOIN gold.portfolio_group_dimension pg ON pg.investment_team_enterprise_key = it.investment_team_enterprise_key
JOIN gold.portfolio_dimension p ON p.portfolio_group_key = pg.portfolio_group_key
LEFT JOIN gold.portfolio_entity_bridge peb ON peb.portfolio_key = p.portfolio_key
    AND (peb.end_date IS NULL OR peb.end_date > CAST(GETUTCDATE() AS DATE))
LEFT JOIN gold.entity_dimension e ON e.entity_key = peb.entity_key;
GO

-- 5.2 Entity → Asset hierarchy (via bridge)
CREATE OR ALTER VIEW gold.vw_entity_asset_hierarchy AS
SELECT
    e.entity_key,
    e.entity_name,
    e.entity_type,
    eab.asset_key,
    eab.ownership_pct,
    eab.effective_date,
    eab.end_date,
    a.asset_name,
    a.asset_type,
    a.asset_subtype,
    a.asset_status,
    a.location_country,
    a.last_valuation_amount,
    a.last_valuation_currency
FROM gold.entity_dimension e
LEFT JOIN gold.entity_asset_bridge eab ON eab.entity_key = e.entity_key
    AND (eab.end_date IS NULL OR eab.end_date > CAST(GETUTCDATE() AS DATE))
LEFT JOIN gold.asset_dimension a ON a.asset_key = eab.asset_key;
GO

-- 5.3 Position with all dimensions resolved
CREATE OR ALTER VIEW gold.vw_position_detail AS
SELECT
    pf.position_fact_key,
    pf.as_of_date,
    pf.position_type,
    pf.transaction_type,
    pf.transaction_amount_usd,
    pf.transaction_amount_local,
    pf.transaction_amount_portfolio,
    p.portfolio_name,
    p.portfolio_enterprise_key,
    pg.portfolio_group_name,
    pg.vintage_year,
    e.entity_name,
    e.entity_type,
    s.security_type,
    s.security_group,
    s.security_name,
    s.bank_loan_id,
    s.cusip,
    a.asset_name,
    a.asset_type,
    it.investment_team_name
FROM gold.position_fact pf
JOIN gold.portfolio_dimension p ON p.portfolio_key = pf.portfolio_key
JOIN gold.portfolio_group_dimension pg ON pg.portfolio_group_key = p.portfolio_group_key
JOIN gold.entity_dimension e ON e.entity_key = pf.entity_key
JOIN gold.security_dimension s ON s.security_key = pf.security_key
JOIN gold.asset_dimension a ON a.asset_enterprise_key = s.asset_enterprise_key
JOIN gold.investment_team_dimension it ON it.investment_team_enterprise_key = s.investment_team_enterprise_key;
GO

-- 5.4 Weighted position by team (bridge allocation applied)
CREATE OR ALTER VIEW gold.vw_position_by_team AS
SELECT
    it.investment_team_name,
    pf.as_of_date,
    SUM(pf.transaction_amount_usd * ptb.allocation_pct) AS weighted_amount_usd,
    SUM(pf.transaction_amount_usd) AS total_exposure_usd,
    COUNT(DISTINCT pf.position_fact_key) AS position_count
FROM gold.position_fact pf
JOIN gold.position_team_bridge ptb ON ptb.position_fact_key = pf.position_fact_key
JOIN gold.investment_team_dimension it ON it.investment_team_key = ptb.investment_team_key
GROUP BY it.investment_team_name, pf.as_of_date;
GO


-- ============================================================================
-- VERIFICATION
-- ============================================================================
PRINT '=== GOLD + PIPELINE OBJECTS (06) ==='
SELECT s.name + '.' + o.name AS object_name,
       CASE o.type WHEN 'P' THEN 'PROC' WHEN 'V' THEN 'VIEW' END AS type
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE (s.name = 'gold' AND o.type IN ('P','V'))
   OR (s.name = 'dbo' AND o.name = 'usp_run_full_pipeline')
ORDER BY s.name, o.type, o.name;
-- Expected: 12 gold procs + 1 dbo proc + 4 gold views = 17 objects
GO
