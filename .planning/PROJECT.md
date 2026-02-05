# WZ Extension

## What This Is

A DuckDB C++ extension that imports accounting data into MSSQL "Wirtschaftszahlen" (WZ) tables. It provides an `into_wz` table function that reads from any DuckDB source, creates tblVorlauf header records, maps and inserts tblPrimanota booking records, and handles column name mapping for German accounting terminology. Used by accountants importing bookings into WZ via DuckDB.

## Core Value

Reliable, transactional import of accounting data from DuckDB into MSSQL WZ tables — if an insert partially fails, nothing gets committed.

## Requirements

### Validated

- ✓ `into_wz` table function reads source data and inserts into tblVorlauf + tblPrimanota — existing
- ✓ Flexible column name mapping (German accounting terms: Konto, Gegenkonto, Soll/Haben, etc.) — existing
- ✓ Auto-generates tblVorlauf records (UUID, date range, Bezeichnung) — existing
- ✓ Duplicate guiPrimanotaID detection before insert — existing
- ✓ Batched inserts (100 rows per batch) — existing
- ✓ Detailed error messages with table name and row number — existing
- ✓ Case-insensitive column matching — existing

### Active

- [ ] Fix transaction handling — BEGIN/COMMIT/ROLLBACK don't work with attached MSSQL databases, causing "cannot rollback - no transaction is active" errors
- [ ] Remove dead code — primanota_mapper.cpp and vorlauf_builder.cpp contain unused earlier implementations that duplicate into_wz_function.cpp logic
- [ ] Consolidate duplicated helpers — FindColumnIndex, timestamp generation exist in multiple files
- [ ] Implement FK constraint validation — ValidateConstraints stub should actually check foreign key values exist in referenced MSSQL tables before attempting insert
- [ ] Remove goto statements — into_wz_function.cpp uses goto for error flow, replace with structured control flow

### Out of Scope

- New functions beyond `into_wz` — this is about fixing what exists, not adding features
- Schema migration or DDL operations — extension only inserts data
- Support for other database targets — MSSQL only via hugr-lab/mssql-extension

## Context

- Built as a DuckDB C++ extension using the extension template pattern
- Depends on hugr-lab/mssql-extension for MSSQL connectivity via attached databases
- MSSQL access is through DuckDB's attached database syntax: `ATTACH '' AS mssql_conn (TYPE mssql, SECRET ...)`
- Queries to MSSQL tables use `mssql_conn.dbo.tableName` syntax
- DuckDB auto-manages transactions on Connection objects — explicit `BEGIN TRANSACTION` SQL strings conflict with this
- The extension builds against DuckDB v1.0+ with C++ ABI entry point `wz_duckdb_cpp_init(ExtensionLoader&)`
- Target tables: tblVorlauf (header) and tblPrimanota (line items) in German accounting schema

## Constraints

- **Tech stack**: C++ extension for DuckDB — must use DuckDB's extension API and build system
- **Dependency**: MSSQL connectivity via hugr-lab/mssql-extension — cannot control how that extension handles transactions
- **Compatibility**: Must work with DuckDB v1.0+ and the attached database syntax for MSSQL

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Use attached database syntax for MSSQL | Simpler than mssql_query for INSERT operations | ⚠️ Revisit — transaction handling doesn't work as expected |
| Batch size of 100 rows | Balance between query size limits and performance | — Pending |

---
*Last updated: 2026-02-05 after initialization*
