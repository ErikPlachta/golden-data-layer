# Golden Data Layer

Medallion architecture (bronze → silver → gold) data governance framework for PE / real assets investment management. SQL Server test harness for a Databricks production deployment.

## Quick Start

```sql
-- Execute files in order (SSMS, Azure Data Studio, or sqlcmd)
-- 01_ddl.sql → 02_meta_programmability.sql → 03_audit.sql → 04_silver.sql → 05_seed_data.sql → 06_gold.sql

-- Run the full pipeline
EXEC dbo.usp_run_full_pipeline;
```

See `docs/Setup.md` for detailed prerequisites and configuration.

## What's Here

```
sql/       6 sequential SQL files (44 tables, 75 programmable objects)
docs/      Architecture specs, silver design, industry research
scripts/   Deployment utilities
CLAUDE.md  Project context for Claude Code collaboration
```

## Architecture

6-source-system model covering the full PE investment hierarchy:

**GP → Fund → Portfolio → Entity → Asset → Security**

With position/transaction facts, ownership bridge tables, and a 14-table metadata/governance framework.

See `CLAUDE.md` for full technical context.
