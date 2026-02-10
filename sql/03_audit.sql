-- ============================================================================
-- GOLDEN DATA LAYER: AUDIT PROCEDURES & VIEWS
-- Target: Azure SQL Edge Developer (SQL Server 15.x compatible)
-- Date: 2026-02-09
-- Depends on: 01_ddl.sql
-- ============================================================================
-- Procedures:
--   audit.usp_start_etl_run       Start an ETL run (creates log entry)
--   audit.usp_complete_etl_run    Complete an ETL run (updates log entry)
-- Views:
--   audit.vw_recent_runs          ETL run history with duration
-- ============================================================================
USE GoldenDataLayer;
GO


-- ============================================================================
-- PROCEDURES
-- ============================================================================

-- ETL run logger — start a run
CREATE OR ALTER PROCEDURE audit.usp_start_etl_run
    @pipeline_code  NVARCHAR(200),
    @target_layer   NVARCHAR(20),
    @target_table   NVARCHAR(500),
    @operation      NVARCHAR(50),
    @run_id         UNIQUEIDENTIFIER OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @run_id = NEWID();

    INSERT INTO audit.etl_run_log (run_id, pipeline_code, target_layer, target_table, operation, start_time, status)
    VALUES (@run_id, @pipeline_code, @target_layer, @target_table, @operation, GETUTCDATE(), 'RUNNING');
END;
GO

-- ETL run logger — complete a run
CREATE OR ALTER PROCEDURE audit.usp_complete_etl_run
    @run_id             UNIQUEIDENTIFIER,
    @status             NVARCHAR(50),
    @rows_read          BIGINT = NULL,
    @rows_inserted      BIGINT = NULL,
    @rows_updated       BIGINT = NULL,
    @rows_deleted       BIGINT = NULL,
    @rows_quarantined   BIGINT = NULL,
    @error_message      NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE audit.etl_run_log
    SET status           = @status,
        end_time         = GETUTCDATE(),
        rows_read        = @rows_read,
        rows_inserted    = @rows_inserted,
        rows_updated     = @rows_updated,
        rows_deleted     = @rows_deleted,
        rows_quarantined = @rows_quarantined,
        error_message    = @error_message
    WHERE run_id = @run_id;
END;
GO


-- ============================================================================
-- VIEWS
-- ============================================================================

-- ETL run history with duration
CREATE OR ALTER VIEW audit.vw_recent_runs AS
SELECT
    run_id,
    pipeline_code,
    target_layer,
    target_table,
    operation,
    status,
    rows_read,
    rows_inserted,
    rows_updated,
    rows_quarantined,
    DATEDIFF(SECOND, start_time, ISNULL(end_time, GETUTCDATE())) AS duration_seconds,
    start_time,
    end_time,
    error_message
FROM audit.etl_run_log;
GO


-- ============================================================================
-- VERIFICATION
-- ============================================================================
PRINT '=== AUDIT OBJECTS (03) ==='
SELECT s.name + '.' + o.name AS object_name,
       CASE o.type WHEN 'P' THEN 'PROC' WHEN 'V' THEN 'VIEW' END AS type
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE s.name = 'audit' AND o.type IN ('P','V')
ORDER BY o.type, o.name;
-- Expected: 2 procedures + 1 view = 3 objects
GO
