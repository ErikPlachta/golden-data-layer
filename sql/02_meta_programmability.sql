-- ============================================================================
-- GOLDEN DATA LAYER: META LAYER PROGRAMMABILITY
-- Target: Azure SQL Edge Developer (SQL Server 15.x compatible)
-- Date: 2026-02-09
-- Depends on: 01_ddl.sql
-- ============================================================================
-- FUNCTIONS (4):
--   meta.fn_row_hash_2            Row hash for change detection
--   meta.fn_translate_key         Enterprise key translator (crosswalk prefix swap)
--   meta.fn_is_valid_date         Date validation helper
--   meta.fn_is_valid_decimal      Decimal validation helper
--
-- UTILITY PROCS (1):
--   meta.usp_find_key_path        Recursive crosswalk traversal
--
-- CRUD PROCS — CONFIG TABLES (24):
--   meta.usp_upsert_source_system / meta.usp_deactivate_source_system
--   meta.usp_upsert_ingestion_pipeline / meta.usp_deactivate_ingestion_pipeline
--   meta.usp_upsert_pipeline_step / meta.usp_delete_pipeline_step
--   meta.usp_upsert_data_contract / meta.usp_deactivate_data_contract
--   meta.usp_upsert_key_registry / meta.usp_deactivate_key_registry
--   meta.usp_upsert_key_crosswalk / meta.usp_deactivate_key_crosswalk
--   meta.usp_upsert_key_crosswalk_path / meta.usp_deactivate_key_crosswalk_path
--   meta.usp_upsert_quality_rule / meta.usp_deactivate_quality_rule
--   meta.usp_upsert_consumer / meta.usp_deactivate_consumer
--   meta.usp_upsert_retention_policy / meta.usp_deactivate_retention_policy
--   meta.usp_upsert_business_glossary / meta.usp_deactivate_business_glossary
--   meta.usp_upsert_extraction_filter / meta.usp_deactivate_extraction_filter
--
-- LOG PROCS — APPEND-ONLY TABLES (2):
--   meta.usp_log_filter_decision        Insert extraction filter decision
--   meta.usp_log_pipeline_execution     Insert pipeline execution record
--
-- QUERY PROCS (6):
--   meta.usp_get_source_systems         List active source systems
--   meta.usp_get_pipelines_for_source   List pipelines for a source system
--   meta.usp_get_rules_for_table        List quality rules for a target table
--   meta.usp_get_active_contracts       List active data contracts
--   meta.usp_get_consumers_for_table    List consumers of a given table
--   meta.usp_get_retention_for_table    Get retention policy for a table
-- ============================================================================
USE GoldenDataLayer;
GO


-- ============================================================================
-- PART 1: FUNCTIONS
-- ============================================================================

-- Row hash helper — consistent hash for change detection (2 columns)
CREATE OR ALTER FUNCTION meta.fn_row_hash_2(
    @col1 NVARCHAR(MAX),
    @col2 NVARCHAR(MAX)
) RETURNS VARBINARY(32)
AS
BEGIN
    RETURN HASHBYTES('SHA2_256', CONCAT_WS('|', @col1, @col2));
END;
GO

-- Enterprise key translator — applies crosswalk rule (prefix replacement)
CREATE OR ALTER FUNCTION meta.fn_translate_key(
    @source_key   NVARCHAR(200),
    @strip_prefix NVARCHAR(50),
    @add_prefix   NVARCHAR(50)
) RETURNS NVARCHAR(200)
AS
BEGIN
    IF @source_key IS NULL RETURN NULL;
    RETURN @add_prefix + SUBSTRING(@source_key, LEN(@strip_prefix) + 1, LEN(@source_key));
END;
GO

-- Date validation helper
CREATE OR ALTER FUNCTION meta.fn_is_valid_date(@val NVARCHAR(50))
RETURNS BIT
AS
BEGIN
    RETURN CASE WHEN TRY_CAST(@val AS DATE) IS NOT NULL THEN 1 ELSE 0 END;
END;
GO

-- Decimal validation helper
CREATE OR ALTER FUNCTION meta.fn_is_valid_decimal(@val NVARCHAR(50))
RETURNS BIT
AS
BEGIN
    RETURN CASE WHEN TRY_CAST(@val AS DECIMAL(18,4)) IS NOT NULL THEN 1 ELSE 0 END;
END;
GO


-- ============================================================================
-- PART 2: UTILITY PROCEDURES
-- ============================================================================

-- Key path finder — recursive crosswalk traversal
CREATE OR ALTER PROCEDURE meta.usp_find_key_path
    @source_key NVARCHAR(200),
    @max_hops   INT = 5
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH path_cte AS (
        SELECT
            kc.from_key_id,
            kc.to_key_id,
            kr_s.key_name AS source_key_name,
            kr_t.key_name AS target_key_name,
            CAST(kr_s.key_name + N' → ' + kr_t.key_name AS NVARCHAR(MAX)) AS path,
            1 AS hop
        FROM meta.key_crosswalk kc
        JOIN meta.key_registry kr_s ON kr_s.key_id = kc.from_key_id
        JOIN meta.key_registry kr_t ON kr_t.key_id = kc.to_key_id
        WHERE kr_s.key_name = @source_key
          AND kc.is_active = 1

        UNION ALL

        SELECT
            kc.from_key_id,
            kc.to_key_id,
            kr_s.key_name,
            kr_t.key_name,
            p.path + N' → ' + kr_t.key_name,
            p.hop + 1
        FROM path_cte p
        JOIN meta.key_crosswalk kc ON kc.from_key_id = p.to_key_id
        JOIN meta.key_registry kr_s ON kr_s.key_id = kc.from_key_id
        JOIN meta.key_registry kr_t ON kr_t.key_id = kc.to_key_id
        WHERE p.hop < @max_hops
          AND kc.is_active = 1
    )
    SELECT path, hop FROM path_cte ORDER BY hop;
END;
GO


-- ============================================================================
-- PART 3: CRUD — SOURCE SYSTEMS
-- Natural key: system_code
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_source_system
    @system_code          NVARCHAR(100),
    @system_name          NVARCHAR(255),
    @system_type          NVARCHAR(100),
    @connectivity_method  NVARCHAR(100),
    @owning_business_unit NVARCHAR(255),
    @connection_details   NVARCHAR(MAX)  = NULL,
    @data_formats         NVARCHAR(MAX)  = NULL,
    @data_steward         NVARCHAR(255)  = NULL,
    @technical_owner      NVARCHAR(255)  = NULL,
    @environment          NVARCHAR(50)   = 'PROD',
    @documentation_url    NVARCHAR(500)  = NULL,
    @is_active            BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    MERGE meta.source_systems AS tgt
    USING (SELECT @system_code AS system_code) AS src
        ON tgt.system_code = src.system_code
    WHEN MATCHED THEN UPDATE SET
        system_name          = @system_name,
        system_type          = @system_type,
        connectivity_method  = @connectivity_method,
        connection_details   = @connection_details,
        data_formats         = @data_formats,
        owning_business_unit = @owning_business_unit,
        data_steward         = @data_steward,
        technical_owner      = @technical_owner,
        environment          = @environment,
        documentation_url    = @documentation_url,
        is_active            = @is_active,
        updated_at           = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (system_code, system_name, system_type, connectivity_method, connection_details,
         data_formats, owning_business_unit, data_steward, technical_owner, environment,
         documentation_url, is_active)
    VALUES
        (@system_code, @system_name, @system_type, @connectivity_method, @connection_details,
         @data_formats, @owning_business_unit, @data_steward, @technical_owner, @environment,
         @documentation_url, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_source_system
    @system_code NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE meta.source_systems
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE system_code = @system_code;
END;
GO


-- ============================================================================
-- PART 4: CRUD — INGESTION PIPELINES
-- Natural key: pipeline_code
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_ingestion_pipeline
    @pipeline_code       NVARCHAR(200),
    @source_system_code  NVARCHAR(100),   -- resolved to source_system_id
    @pipeline_name       NVARCHAR(500),
    @ingestion_pattern   NVARCHAR(100),
    @managing_owner      NVARCHAR(255),
    @description         NVARCHAR(MAX)  = NULL,
    @schedule_type       NVARCHAR(100)  = NULL,
    @schedule_expression NVARCHAR(200)  = NULL,
    @target_bronze_table NVARCHAR(500)  = NULL,
    @target_silver_table NVARCHAR(500)  = NULL,
    @target_gold_tables  NVARCHAR(MAX)  = NULL,
    @job_id              NVARCHAR(200)  = NULL,
    @sla_minutes         INT            = NULL,
    @is_active           BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @source_system_id INT;
    SELECT @source_system_id = source_system_id
    FROM meta.source_systems
    WHERE system_code = @source_system_code AND is_active = 1;

    IF @source_system_id IS NULL
    BEGIN
        RAISERROR('Source system not found or inactive: %s', 16, 1, @source_system_code);
        RETURN;
    END;

    MERGE meta.ingestion_pipelines AS tgt
    USING (SELECT @pipeline_code AS pipeline_code) AS src
        ON tgt.pipeline_code = src.pipeline_code
    WHEN MATCHED THEN UPDATE SET
        source_system_id     = @source_system_id,
        pipeline_name        = @pipeline_name,
        description          = @description,
        ingestion_pattern    = @ingestion_pattern,
        schedule_type        = @schedule_type,
        schedule_expression  = @schedule_expression,
        target_bronze_table  = @target_bronze_table,
        target_silver_table  = @target_silver_table,
        target_gold_tables   = @target_gold_tables,
        job_id               = @job_id,
        managing_owner       = @managing_owner,
        sla_minutes          = @sla_minutes,
        is_active            = @is_active,
        updated_at           = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (source_system_id, pipeline_code, pipeline_name, description, ingestion_pattern,
         schedule_type, schedule_expression, target_bronze_table, target_silver_table,
         target_gold_tables, job_id, managing_owner, sla_minutes, is_active)
    VALUES
        (@source_system_id, @pipeline_code, @pipeline_name, @description, @ingestion_pattern,
         @schedule_type, @schedule_expression, @target_bronze_table, @target_silver_table,
         @target_gold_tables, @job_id, @managing_owner, @sla_minutes, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_ingestion_pipeline
    @pipeline_code NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE meta.ingestion_pipelines
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE pipeline_code = @pipeline_code;
END;
GO


-- ============================================================================
-- PART 5: CRUD — PIPELINE STEPS
-- Natural key: pipeline_code + step_sequence
-- Hard delete (not soft) — steps are structural, not config
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_pipeline_step
    @pipeline_code    NVARCHAR(200),   -- resolved to pipeline_id
    @step_sequence    INT,
    @step_name        NVARCHAR(255),
    @step_type        NVARCHAR(100),
    @description      NVARCHAR(MAX),
    @executor         NVARCHAR(500)  = NULL,
    @executor_owner   NVARCHAR(255)  = NULL,
    @input_reference  NVARCHAR(500)  = NULL,
    @output_reference NVARCHAR(500)  = NULL,
    @key_columns_used NVARCHAR(MAX)  = NULL,
    @error_handling   NVARCHAR(MAX)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @pipeline_id INT;
    SELECT @pipeline_id = pipeline_id
    FROM meta.ingestion_pipelines
    WHERE pipeline_code = @pipeline_code AND is_active = 1;

    IF @pipeline_id IS NULL
    BEGIN
        RAISERROR('Pipeline not found or inactive: %s', 16, 1, @pipeline_code);
        RETURN;
    END;

    MERGE meta.ingestion_pipeline_steps AS tgt
    USING (SELECT @pipeline_id AS pid, @step_sequence AS seq) AS src
        ON tgt.pipeline_id = src.pid AND tgt.step_sequence = src.seq
    WHEN MATCHED THEN UPDATE SET
        step_name        = @step_name,
        step_type        = @step_type,
        description      = @description,
        executor         = @executor,
        executor_owner   = @executor_owner,
        input_reference  = @input_reference,
        output_reference = @output_reference,
        key_columns_used = @key_columns_used,
        error_handling   = @error_handling,
        updated_at       = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (pipeline_id, step_sequence, step_name, step_type, description,
         executor, executor_owner, input_reference, output_reference,
         key_columns_used, error_handling)
    VALUES
        (@pipeline_id, @step_sequence, @step_name, @step_type, @description,
         @executor, @executor_owner, @input_reference, @output_reference,
         @key_columns_used, @error_handling);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_delete_pipeline_step
    @pipeline_code NVARCHAR(200),
    @step_sequence INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @pipeline_id INT;
    SELECT @pipeline_id = pipeline_id
    FROM meta.ingestion_pipelines WHERE pipeline_code = @pipeline_code;

    DELETE FROM meta.ingestion_pipeline_steps
    WHERE pipeline_id = @pipeline_id AND step_sequence = @step_sequence;
END;
GO


-- ============================================================================
-- PART 6: CRUD — DATA CONTRACTS
-- Natural key: pipeline_code + contract_version
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_data_contract
    @pipeline_code          NVARCHAR(200),   -- resolved to pipeline_id + source_system_id
    @contract_version       INT              = 1,
    @schema_definition      NVARCHAR(MAX),
    @owner                  NVARCHAR(255),
    @effective_date         DATE,
    @contract_status        NVARCHAR(50)     = 'ACTIVE',
    @delivery_sla_minutes   INT              = NULL,
    @freshness_sla_minutes  INT              = NULL,
    @volume_expectation     NVARCHAR(MAX)    = NULL,
    @breaking_change_policy NVARCHAR(50)     = NULL,
    @expiration_date        DATE             = NULL,
    @notes                  NVARCHAR(MAX)    = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @pipeline_id INT, @source_system_id INT;
    SELECT @pipeline_id = pipeline_id, @source_system_id = source_system_id
    FROM meta.ingestion_pipelines
    WHERE pipeline_code = @pipeline_code;

    IF @pipeline_id IS NULL
    BEGIN
        RAISERROR('Pipeline not found: %s', 16, 1, @pipeline_code);
        RETURN;
    END;

    MERGE meta.data_contracts AS tgt
    USING (SELECT @pipeline_id AS pid, @contract_version AS ver) AS src
        ON tgt.pipeline_id = src.pid AND tgt.contract_version = src.ver
    WHEN MATCHED THEN UPDATE SET
        source_system_id       = @source_system_id,
        contract_status        = @contract_status,
        schema_definition      = @schema_definition,
        delivery_sla_minutes   = @delivery_sla_minutes,
        freshness_sla_minutes  = @freshness_sla_minutes,
        volume_expectation     = @volume_expectation,
        breaking_change_policy = @breaking_change_policy,
        owner                  = @owner,
        effective_date         = @effective_date,
        expiration_date        = @expiration_date,
        notes                  = @notes,
        updated_at             = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (source_system_id, pipeline_id, contract_version, contract_status, schema_definition,
         delivery_sla_minutes, freshness_sla_minutes, volume_expectation, breaking_change_policy,
         owner, effective_date, expiration_date, notes)
    VALUES
        (@source_system_id, @pipeline_id, @contract_version, @contract_status, @schema_definition,
         @delivery_sla_minutes, @freshness_sla_minutes, @volume_expectation, @breaking_change_policy,
         @owner, @effective_date, @expiration_date, @notes);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_data_contract
    @pipeline_code    NVARCHAR(200),
    @contract_version INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @pipeline_id INT;
    SELECT @pipeline_id = pipeline_id
    FROM meta.ingestion_pipelines WHERE pipeline_code = @pipeline_code;

    UPDATE meta.data_contracts
    SET contract_status = 'RETIRED', updated_at = GETUTCDATE()
    WHERE pipeline_id = @pipeline_id AND contract_version = @contract_version;
END;
GO


-- ============================================================================
-- PART 7: CRUD — KEY REGISTRY
-- Natural key: source_system_code + key_name
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_key_registry
    @source_system_code NVARCHAR(100),   -- resolved to source_system_id
    @key_name           NVARCHAR(255),
    @key_type           NVARCHAR(50),
    @data_type          NVARCHAR(50),
    @key_aliases        NVARCHAR(MAX)  = NULL,
    @example_values     NVARCHAR(MAX)  = NULL,
    @source_table       NVARCHAR(500)  = NULL,
    @source_column      NVARCHAR(255)  = NULL,
    @databricks_table   NVARCHAR(500)  = NULL,
    @databricks_column  NVARCHAR(255)  = NULL,
    @description        NVARCHAR(MAX)  = NULL,
    @is_active          BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @source_system_id INT;
    SELECT @source_system_id = source_system_id
    FROM meta.source_systems WHERE system_code = @source_system_code;

    IF @source_system_id IS NULL
    BEGIN
        RAISERROR('Source system not found: %s', 16, 1, @source_system_code);
        RETURN;
    END;

    MERGE meta.key_registry AS tgt
    USING (SELECT @source_system_id AS ssid, @key_name AS kn) AS src
        ON tgt.source_system_id = src.ssid AND tgt.key_name = src.kn
    WHEN MATCHED THEN UPDATE SET
        key_type          = @key_type,
        key_aliases       = @key_aliases,
        data_type         = @data_type,
        example_values    = @example_values,
        source_table      = @source_table,
        source_column     = @source_column,
        databricks_table  = @databricks_table,
        databricks_column = @databricks_column,
        description       = @description,
        is_active         = @is_active,
        updated_at        = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (source_system_id, key_name, key_aliases, key_type, data_type, example_values,
         source_table, source_column, databricks_table, databricks_column, description, is_active)
    VALUES
        (@source_system_id, @key_name, @key_aliases, @key_type, @data_type, @example_values,
         @source_table, @source_column, @databricks_table, @databricks_column, @description, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_key_registry
    @source_system_code NVARCHAR(100),
    @key_name           NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @source_system_id INT;
    SELECT @source_system_id = source_system_id
    FROM meta.source_systems WHERE system_code = @source_system_code;

    UPDATE meta.key_registry
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE source_system_id = @source_system_id AND key_name = @key_name;
END;
GO


-- ============================================================================
-- PART 8: CRUD — KEY CROSSWALK
-- Natural key: from_key_id + to_key_id (resolved via key_name pairs)
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_key_crosswalk
    @from_key_name         NVARCHAR(255),   -- resolved to from_key_id
    @to_key_name           NVARCHAR(255),   -- resolved to to_key_id
    @mapping_type          NVARCHAR(50),
    @mapping_confidence    NVARCHAR(50)   = 'EXACT',
    @transformation_rule   NVARCHAR(MAX)  = NULL,
    @conditions            NVARCHAR(MAX)  = NULL,
    @bidirectional         BIT            = 1,
    @validated_by          NVARCHAR(255)  = NULL,
    @validation_date       DATE           = NULL,
    @is_active             BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @from_key_id INT, @to_key_id INT;
    SELECT @from_key_id = key_id FROM meta.key_registry WHERE key_name = @from_key_name;
    SELECT @to_key_id   = key_id FROM meta.key_registry WHERE key_name = @to_key_name;

    IF @from_key_id IS NULL OR @to_key_id IS NULL
    BEGIN
        RAISERROR('Key not found. from=%s, to=%s', 16, 1, @from_key_name, @to_key_name);
        RETURN;
    END;

    MERGE meta.key_crosswalk AS tgt
    USING (SELECT @from_key_id AS fk, @to_key_id AS tk) AS src
        ON tgt.from_key_id = src.fk AND tgt.to_key_id = src.tk
    WHEN MATCHED THEN UPDATE SET
        mapping_type        = @mapping_type,
        mapping_confidence  = @mapping_confidence,
        transformation_rule = @transformation_rule,
        conditions          = @conditions,
        bidirectional       = @bidirectional,
        validated_by        = @validated_by,
        validation_date     = @validation_date,
        is_active           = @is_active,
        updated_at          = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (from_key_id, to_key_id, mapping_type, mapping_confidence, transformation_rule,
         conditions, bidirectional, validated_by, validation_date, is_active)
    VALUES
        (@from_key_id, @to_key_id, @mapping_type, @mapping_confidence, @transformation_rule,
         @conditions, @bidirectional, @validated_by, @validation_date, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_key_crosswalk
    @from_key_name NVARCHAR(255),
    @to_key_name   NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @from_key_id INT, @to_key_id INT;
    SELECT @from_key_id = key_id FROM meta.key_registry WHERE key_name = @from_key_name;
    SELECT @to_key_id   = key_id FROM meta.key_registry WHERE key_name = @to_key_name;

    UPDATE meta.key_crosswalk
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE from_key_id = @from_key_id AND to_key_id = @to_key_id;
END;
GO


-- ============================================================================
-- PART 9: CRUD — KEY CROSSWALK PATHS
-- Natural key: from_key_name + to_key_name + hop_count
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_key_crosswalk_path
    @from_key_name      NVARCHAR(255),   -- resolved to from_key_id
    @to_key_name        NVARCHAR(255),   -- resolved to to_key_id
    @hop_count          INT,
    @path_crosswalk_ids NVARCHAR(MAX),
    @path_description   NVARCHAR(MAX)  = NULL,
    @path_reliability   NVARCHAR(50)   = NULL,
    @conditions         NVARCHAR(MAX)  = NULL,
    @is_active          BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @from_key_id INT, @to_key_id INT;
    SELECT @from_key_id = key_id FROM meta.key_registry WHERE key_name = @from_key_name;
    SELECT @to_key_id   = key_id FROM meta.key_registry WHERE key_name = @to_key_name;

    IF @from_key_id IS NULL OR @to_key_id IS NULL
    BEGIN
        RAISERROR('Key not found. from=%s, to=%s', 16, 1, @from_key_name, @to_key_name);
        RETURN;
    END;

    MERGE meta.key_crosswalk_paths AS tgt
    USING (SELECT @from_key_id AS fk, @to_key_id AS tk, @hop_count AS hc) AS src
        ON tgt.from_key_id = src.fk AND tgt.to_key_id = src.tk AND tgt.hop_count = src.hc
    WHEN MATCHED THEN UPDATE SET
        path_crosswalk_ids = @path_crosswalk_ids,
        path_description   = @path_description,
        path_reliability   = @path_reliability,
        conditions         = @conditions,
        is_active          = @is_active,
        updated_at         = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (from_key_id, to_key_id, hop_count, path_crosswalk_ids,
         path_description, path_reliability, conditions, is_active)
    VALUES
        (@from_key_id, @to_key_id, @hop_count, @path_crosswalk_ids,
         @path_description, @path_reliability, @conditions, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_key_crosswalk_path
    @from_key_name NVARCHAR(255),
    @to_key_name   NVARCHAR(255),
    @hop_count     INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @from_key_id INT, @to_key_id INT;
    SELECT @from_key_id = key_id FROM meta.key_registry WHERE key_name = @from_key_name;
    SELECT @to_key_id   = key_id FROM meta.key_registry WHERE key_name = @to_key_name;

    UPDATE meta.key_crosswalk_paths
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE from_key_id = @from_key_id AND to_key_id = @to_key_id AND hop_count = @hop_count;
END;
GO


-- ============================================================================
-- PART 10: CRUD — QUALITY RULES
-- Natural key: rule_code
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_quality_rule
    @rule_code       NVARCHAR(200),
    @rule_name       NVARCHAR(500),
    @target_table    NVARCHAR(500),
    @rule_expression NVARCHAR(MAX),
    @rule_type       NVARCHAR(100),
    @severity        NVARCHAR(50),
    @layer           NVARCHAR(50),
    @owner           NVARCHAR(255),
    @target_column   NVARCHAR(255)  = NULL,
    @is_active       BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    MERGE meta.quality_rules AS tgt
    USING (SELECT @rule_code AS rc) AS src
        ON tgt.rule_code = src.rc
    WHEN MATCHED THEN UPDATE SET
        rule_name       = @rule_name,
        target_table    = @target_table,
        target_column   = @target_column,
        rule_expression = @rule_expression,
        rule_type       = @rule_type,
        severity        = @severity,
        layer           = @layer,
        owner           = @owner,
        is_active       = @is_active,
        updated_at      = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (rule_code, rule_name, target_table, target_column, rule_expression,
         rule_type, severity, layer, owner, is_active)
    VALUES
        (@rule_code, @rule_name, @target_table, @target_column, @rule_expression,
         @rule_type, @severity, @layer, @owner, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_quality_rule
    @rule_code NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE meta.quality_rules
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE rule_code = @rule_code;
END;
GO


-- ============================================================================
-- PART 11: CRUD — CONSUMERS
-- Natural key: consumer_name
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_consumer
    @consumer_name         NVARCHAR(500),
    @consumer_type         NVARCHAR(100),
    @consuming_tables      NVARCHAR(MAX),
    @owning_team           NVARCHAR(255),
    @contact               NVARCHAR(255),
    @access_method         NVARCHAR(100),
    @criticality           NVARCHAR(50),
    @freshness_requirement NVARCHAR(50)   = NULL,
    @notification_channel  NVARCHAR(500)  = NULL,
    @is_active             BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    MERGE meta.consumers AS tgt
    USING (SELECT @consumer_name AS cn) AS src
        ON tgt.consumer_name = src.cn
    WHEN MATCHED THEN UPDATE SET
        consumer_type         = @consumer_type,
        consuming_tables      = @consuming_tables,
        owning_team           = @owning_team,
        contact               = @contact,
        access_method         = @access_method,
        criticality           = @criticality,
        freshness_requirement = @freshness_requirement,
        notification_channel  = @notification_channel,
        is_active             = @is_active,
        updated_at            = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (consumer_name, consumer_type, consuming_tables, owning_team, contact,
         access_method, criticality, freshness_requirement, notification_channel, is_active)
    VALUES
        (@consumer_name, @consumer_type, @consuming_tables, @owning_team, @contact,
         @access_method, @criticality, @freshness_requirement, @notification_channel, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_consumer
    @consumer_name NVARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE meta.consumers
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE consumer_name = @consumer_name;
END;
GO


-- ============================================================================
-- PART 12: CRUD — RETENTION POLICIES
-- Natural key: target_table + layer
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_retention_policy
    @target_table       NVARCHAR(500),
    @layer              NVARCHAR(50),
    @retention_days     INT,
    @owner              NVARCHAR(255),
    @time_travel_days   INT            = 7,
    @log_retention_days INT            = 30,
    @archive_after_days INT            = NULL,
    @purge_after_days   INT            = NULL,
    @vacuum_strategy    NVARCHAR(200)  = 'LITE_DAILY_FULL_WEEKLY',
    @regulatory_basis   NVARCHAR(200)  = NULL,
    @is_active          BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    MERGE meta.retention_policies AS tgt
    USING (SELECT @target_table AS tt, @layer AS ly) AS src
        ON tgt.target_table = src.tt AND tgt.layer = src.ly
    WHEN MATCHED THEN UPDATE SET
        retention_days     = @retention_days,
        time_travel_days   = @time_travel_days,
        log_retention_days = @log_retention_days,
        archive_after_days = @archive_after_days,
        purge_after_days   = @purge_after_days,
        vacuum_strategy    = @vacuum_strategy,
        regulatory_basis   = @regulatory_basis,
        owner              = @owner,
        is_active          = @is_active,
        updated_at         = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (target_table, layer, retention_days, time_travel_days, log_retention_days,
         archive_after_days, purge_after_days, vacuum_strategy, regulatory_basis, owner, is_active)
    VALUES
        (@target_table, @layer, @retention_days, @time_travel_days, @log_retention_days,
         @archive_after_days, @purge_after_days, @vacuum_strategy, @regulatory_basis, @owner, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_retention_policy
    @target_table NVARCHAR(500),
    @layer        NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE meta.retention_policies
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE target_table = @target_table AND layer = @layer;
END;
GO


-- ============================================================================
-- PART 13: CRUD — BUSINESS GLOSSARY
-- Natural key: business_term
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_business_glossary
    @business_term    NVARCHAR(500),
    @definition       NVARCHAR(MAX),
    @mapped_tables    NVARCHAR(MAX),
    @mapped_columns   NVARCHAR(MAX),
    @domain           NVARCHAR(255),
    @owner            NVARCHAR(255),
    @calculation_logic NVARCHAR(MAX) = NULL,
    @synonyms         NVARCHAR(MAX)  = NULL,
    @is_active        BIT            = 1
AS
BEGIN
    SET NOCOUNT ON;

    MERGE meta.business_glossary AS tgt
    USING (SELECT @business_term AS bt) AS src
        ON tgt.business_term = src.bt
    WHEN MATCHED THEN UPDATE SET
        definition       = @definition,
        calculation_logic = @calculation_logic,
        mapped_tables    = @mapped_tables,
        mapped_columns   = @mapped_columns,
        domain           = @domain,
        owner            = @owner,
        synonyms         = @synonyms,
        is_active        = @is_active,
        updated_at       = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (business_term, definition, calculation_logic, mapped_tables, mapped_columns,
         domain, owner, synonyms, is_active)
    VALUES
        (@business_term, @definition, @calculation_logic, @mapped_tables, @mapped_columns,
         @domain, @owner, @synonyms, @is_active);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_business_glossary
    @business_term NVARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE meta.business_glossary
    SET is_active = 0, updated_at = GETUTCDATE()
    WHERE business_term = @business_term;
END;
GO


-- ============================================================================
-- PART 14: CRUD — EXTRACTION FILTERS
-- Natural key: source_system_code + filter_type + filter_value
-- ============================================================================

CREATE OR ALTER PROCEDURE meta.usp_upsert_extraction_filter
    @source_system_code NVARCHAR(100),   -- resolved to source_system_id
    @filter_type        NVARCHAR(50),
    @filter_value       NVARCHAR(255),
    @decided_by         NVARCHAR(255),
    @effective_date     DATE,
    @is_enabled         BIT            = 1,
    @rationale          NVARCHAR(MAX)  = NULL,
    @expiration_date    DATE           = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @source_system_id INT;
    SELECT @source_system_id = source_system_id
    FROM meta.source_systems WHERE system_code = @source_system_code;

    IF @source_system_id IS NULL
    BEGIN
        RAISERROR('Source system not found: %s', 16, 1, @source_system_code);
        RETURN;
    END;

    MERGE meta.extraction_filters AS tgt
    USING (SELECT @source_system_id AS ssid, @filter_type AS ft, @filter_value AS fv) AS src
        ON tgt.source_system_id = src.ssid AND tgt.filter_type = src.ft AND tgt.filter_value = src.fv
    WHEN MATCHED THEN UPDATE SET
        is_enabled      = @is_enabled,
        rationale       = @rationale,
        decided_by      = @decided_by,
        effective_date  = @effective_date,
        expiration_date = @expiration_date,
        updated_at      = GETUTCDATE()
    WHEN NOT MATCHED THEN INSERT
        (source_system_id, filter_type, filter_value, is_enabled, rationale,
         decided_by, effective_date, expiration_date)
    VALUES
        (@source_system_id, @filter_type, @filter_value, @is_enabled, @rationale,
         @decided_by, @effective_date, @expiration_date);
END;
GO

CREATE OR ALTER PROCEDURE meta.usp_deactivate_extraction_filter
    @source_system_code NVARCHAR(100),
    @filter_type        NVARCHAR(50),
    @filter_value       NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @source_system_id INT;
    SELECT @source_system_id = source_system_id
    FROM meta.source_systems WHERE system_code = @source_system_code;

    UPDATE meta.extraction_filters
    SET is_enabled = 0, updated_at = GETUTCDATE()
    WHERE source_system_id = @source_system_id
      AND filter_type = @filter_type
      AND filter_value = @filter_value;
END;
GO


-- ============================================================================
-- PART 15: LOG PROCS — APPEND-ONLY TABLES
-- ============================================================================

-- Log an extraction filter decision (audit trail, immutable)
CREATE OR ALTER PROCEDURE meta.usp_log_filter_decision
    @filter_id      INT,
    @action         NVARCHAR(50),
    @new_state      NVARCHAR(MAX),
    @rationale      NVARCHAR(MAX),
    @decided_by     NVARCHAR(255),
    @previous_state NVARCHAR(MAX)  = NULL,
    @approved_by    NVARCHAR(255)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM meta.extraction_filters WHERE filter_id = @filter_id)
    BEGIN
        RAISERROR('Extraction filter not found: %d', 16, 1, @filter_id);
        RETURN;
    END;

    INSERT INTO meta.extraction_filter_decisions
        (filter_id, action, previous_state, new_state, rationale, decided_by, approved_by)
    VALUES
        (@filter_id, @action, @previous_state, @new_state, @rationale, @decided_by, @approved_by);
END;
GO

-- Log a pipeline execution (runtime telemetry, immutable)
CREATE OR ALTER PROCEDURE meta.usp_log_pipeline_execution
    @pipeline_code     NVARCHAR(200),   -- resolved to pipeline_id
    @execution_type    NVARCHAR(50),
    @status            NVARCHAR(50),
    @start_time        DATETIME2,
    @step_sequence     INT              = NULL,   -- resolved to step_id
    @job_id            NVARCHAR(200)    = NULL,
    @run_id            NVARCHAR(200)    = NULL,
    @applied_filters   NVARCHAR(MAX)    = NULL,
    @source_query      NVARCHAR(MAX)    = NULL,
    @target_table      NVARCHAR(500)    = NULL,
    @rows_extracted    BIGINT           = NULL,
    @rows_inserted     BIGINT           = NULL,
    @rows_updated      BIGINT           = NULL,
    @rows_deleted      BIGINT           = NULL,
    @rows_rejected     BIGINT           = NULL,
    @rows_skipped      BIGINT           = NULL,
    @end_time          DATETIME2        = NULL,
    @error_code        NVARCHAR(100)    = NULL,
    @error_message     NVARCHAR(MAX)    = NULL,
    @error_stack_trace NVARCHAR(MAX)    = NULL,
    @compute_resource  NVARCHAR(255)    = NULL,
    @notebook_path     NVARCHAR(500)    = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @pipeline_id INT, @step_id INT, @duration INT;

    SELECT @pipeline_id = pipeline_id
    FROM meta.ingestion_pipelines WHERE pipeline_code = @pipeline_code;

    IF @pipeline_id IS NULL
    BEGIN
        RAISERROR('Pipeline not found: %s', 16, 1, @pipeline_code);
        RETURN;
    END;

    IF @step_sequence IS NOT NULL
        SELECT @step_id = step_id
        FROM meta.ingestion_pipeline_steps
        WHERE pipeline_id = @pipeline_id AND step_sequence = @step_sequence;

    SET @duration = CASE WHEN @end_time IS NOT NULL
                         THEN DATEDIFF(SECOND, @start_time, @end_time) END;

    INSERT INTO meta.pipeline_execution_log
        (pipeline_id, step_id, job_id, run_id, execution_type, status,
         applied_filters, source_query, target_table,
         rows_extracted, rows_inserted, rows_updated, rows_deleted, rows_rejected, rows_skipped,
         start_time, end_time, duration_seconds,
         error_code, error_message, error_stack_trace,
         compute_resource, notebook_path)
    VALUES
        (@pipeline_id, @step_id, @job_id, @run_id, @execution_type, @status,
         @applied_filters, @source_query, @target_table,
         @rows_extracted, @rows_inserted, @rows_updated, @rows_deleted, @rows_rejected, @rows_skipped,
         @start_time, @end_time, @duration,
         @error_code, @error_message, @error_stack_trace,
         @compute_resource, @notebook_path);
END;
GO


-- ============================================================================
-- PART 16: QUERY PROCS
-- ============================================================================

-- List all active source systems
CREATE OR ALTER PROCEDURE meta.usp_get_source_systems
    @include_inactive BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SELECT source_system_id, system_code, system_name, system_type,
           connectivity_method, owning_business_unit, data_steward,
           environment, is_active
    FROM meta.source_systems
    WHERE is_active = 1 OR @include_inactive = 1
    ORDER BY system_code;
END;
GO

-- List pipelines for a source system
CREATE OR ALTER PROCEDURE meta.usp_get_pipelines_for_source
    @source_system_code NVARCHAR(100),
    @include_inactive   BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SELECT p.pipeline_id, p.pipeline_code, p.pipeline_name,
           p.ingestion_pattern, p.schedule_type, p.schedule_expression,
           p.target_bronze_table, p.target_silver_table,
           p.managing_owner, p.sla_minutes, p.is_active
    FROM meta.ingestion_pipelines p
    JOIN meta.source_systems ss ON ss.source_system_id = p.source_system_id
    WHERE ss.system_code = @source_system_code
      AND (p.is_active = 1 OR @include_inactive = 1)
    ORDER BY p.pipeline_code;
END;
GO

-- List quality rules for a target table
CREATE OR ALTER PROCEDURE meta.usp_get_rules_for_table
    @target_table     NVARCHAR(500),
    @include_inactive BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SELECT rule_id, rule_code, rule_name, target_column,
           rule_expression, rule_type, severity, layer, owner, is_active
    FROM meta.quality_rules
    WHERE target_table = @target_table
      AND (is_active = 1 OR @include_inactive = 1)
    ORDER BY severity DESC, rule_code;
END;
GO

-- List active data contracts
CREATE OR ALTER PROCEDURE meta.usp_get_active_contracts
    @pipeline_code NVARCHAR(200) = NULL   -- NULL = all active contracts
AS
BEGIN
    SET NOCOUNT ON;
    SELECT dc.contract_id, p.pipeline_code, ss.system_code,
           dc.contract_version, dc.contract_status,
           dc.delivery_sla_minutes, dc.freshness_sla_minutes,
           dc.owner, dc.effective_date, dc.expiration_date
    FROM meta.data_contracts dc
    JOIN meta.ingestion_pipelines p ON p.pipeline_id = dc.pipeline_id
    JOIN meta.source_systems ss ON ss.source_system_id = dc.source_system_id
    WHERE dc.contract_status = 'ACTIVE'
      AND (@pipeline_code IS NULL OR p.pipeline_code = @pipeline_code)
    ORDER BY p.pipeline_code, dc.contract_version DESC;
END;
GO

-- List consumers of a given table
CREATE OR ALTER PROCEDURE meta.usp_get_consumers_for_table
    @target_table     NVARCHAR(500),
    @include_inactive BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SELECT consumer_id, consumer_name, consumer_type, owning_team,
           contact, access_method, criticality, freshness_requirement,
           notification_channel, is_active
    FROM meta.consumers
    WHERE consuming_tables LIKE '%' + @target_table + '%'
      AND (is_active = 1 OR @include_inactive = 1)
    ORDER BY criticality DESC, consumer_name;
END;
GO

-- Get retention policy for a table
CREATE OR ALTER PROCEDURE meta.usp_get_retention_for_table
    @target_table NVARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT policy_id, target_table, layer, retention_days,
           time_travel_days, log_retention_days,
           archive_after_days, purge_after_days,
           vacuum_strategy, regulatory_basis, owner
    FROM meta.retention_policies
    WHERE target_table = @target_table AND is_active = 1;
END;
GO


-- ============================================================================
-- VERIFICATION
-- ============================================================================
PRINT '=== META PROGRAMMABILITY OBJECTS (02) ==='
SELECT s.name + '.' + o.name AS object_name,
       CASE o.type WHEN 'P' THEN 'PROC' WHEN 'FN' THEN 'FUNC' END AS type
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE s.name = 'meta' AND o.type IN ('FN','P')
ORDER BY o.type, o.name;
-- Expected: 4 functions + 33 procedures = 37 objects
GO
