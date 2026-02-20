# Design: `move_to_mssql` Table Function

**Date:** 2026-02-20
**Status:** Approved

## Summary

A new DuckDB table function that bulk-transfers tables from DuckDB to MSSQL Server using BCP (Bulk Copy Program). Supports transferring all tables or a specific list, with optional exclusions.

## Function Signature

```sql
-- Transfer all tables (default)
SELECT * FROM move_to_mssql(secret := 'my_mssql_secret');

-- Transfer specific tables
SELECT * FROM move_to_mssql(secret := 'my_mssql_secret', tables := ['customers', 'orders']);

-- Transfer all except some
SELECT * FROM move_to_mssql(secret := 'my_mssql_secret', exclude := ['temp_debug', 'staging']);

-- Custom schema
SELECT * FROM move_to_mssql(secret := 'my_mssql_secret', schema := 'import');
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `secret` | VARCHAR | `'mssql_conn'` | MSSQL connection secret name |
| `all_tables` | BOOLEAN | `true` | Transfer all visible DuckDB tables |
| `tables` | VARCHAR[] | `[]` | Explicit list of table names to transfer |
| `exclude` | VARCHAR[] | `[]` | Tables to skip (only meaningful with `all_tables=true`) |
| `schema` | VARCHAR | `'dbo'` | Target MSSQL schema |

### Validation Rules

- If `tables` is non-empty, `all_tables` is implicitly false
- `exclude` is only valid when `all_tables=true` (ignored otherwise)
- Table names matched case-insensitively
- Must have at least one table to transfer after filtering

### Table Discovery (when `all_tables=true`)

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
```

Then filter out any names in `exclude`.

## Data Flow (Per Table)

1. **Discover columns** — `DESCRIBE <table_name>` to get column names + DuckDB types
2. **Map types** — DuckDB type to MSSQL type (see mapping table below)
3. **DROP + CREATE** on MSSQL via attached connection:
   ```sql
   DROP TABLE IF EXISTS [schema].[table_name];
   CREATE TABLE [schema].[table_name] (col1 INT, col2 NVARCHAR(MAX), ...);
   ```
4. **Export to TSV** — `COPY <table> TO '<temp>.tsv' (DELIMITER '\t', HEADER false, NULL '')`
5. **Generate .fmt file** — Dynamic based on actual columns
6. **BCP in** — `bcp <db>.<schema>.<table> in <file> -S server -f fmt -k -b 5000`
7. **Fallback** — If BCP fails, batched multi-row INSERT VALUES (1000 rows/statement, 5 statements/roundtrip)
8. **Cleanup** — Remove temp TSV + .fmt files

## Type Mapping (DuckDB to MSSQL)

| DuckDB Type | MSSQL Type |
|-------------|------------|
| VARCHAR | NVARCHAR(MAX) |
| INTEGER / INT | INT |
| BIGINT | BIGINT |
| SMALLINT | SMALLINT |
| TINYINT | TINYINT |
| DOUBLE | FLOAT |
| FLOAT | REAL |
| DECIMAL(p,s) | DECIMAL(p,s) |
| BOOLEAN | BIT |
| DATE | DATE |
| TIMESTAMP | DATETIME2 |
| BLOB | VARBINARY(MAX) |
| *anything else* | NVARCHAR(MAX) |

## Behavior on Existing Tables

Always **DROP and recreate**. Every call produces a clean copy of the DuckDB table on MSSQL.

## Error Handling

- If one table fails, **continue** with remaining tables
- Report success/failure per table in the result set
- BCP failure triggers automatic fallback to batched INSERT VALUES
- If both BCP and INSERT fail, mark that table as failed and continue

## Result Set

One row per table transferred, plus a SUMMARY row:

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | VARCHAR | Source table name, or "SUMMARY" for final row |
| `rows_transferred` | BIGINT | Row count (0 on failure) |
| `method` | VARCHAR | "BCP" or "INSERT" (whichever succeeded) |
| `duration` | VARCHAR | Per-table time as HH:MM:SS |
| `success` | BOOLEAN | Per-table success flag |
| `error_message` | VARCHAR | Empty on success, details on failure |

Example output:

```
| table_name | rows_transferred | method | duration | success | error_message                    |
|------------|-----------------|--------|----------|---------|----------------------------------|
| customers  | 15000           | BCP    | 00:00:02 | true    |                                  |
| orders     | 250000          | BCP    | 00:00:08 | true    |                                  |
| temp_debug | 0               | —      | 00:00:00 | false   | BCP failed: ... INSERT failed: ...|
| SUMMARY    | 265000          | —      | 00:00:12 | true    | 2/3 tables transferred           |
```

## Code Organization

### New Files

| File | Purpose |
|------|---------|
| `src/move_to_mssql_function.cpp` | New table function: bind, execute, state machine |
| `src/include/mssql_utils.hpp` | Shared MSSQL helpers extracted from `into_wz_function.cpp` |

### Refactoring: Extract Shared Code

Move these from `into_wz_function.cpp` into `mssql_utils.hpp` (header-only, matching `wz_utils.hpp` pattern):

- **`MssqlConnInfo` struct** — host, database, user, password, port, trusted_connection
- **`GetMssqlConnInfo()`** — Extract connection info from DuckDB secret
- **`InvokeBcp()`** — Execute bcp.exe with parameters
- **`ExportTableToCsv()`** — Generalized COPY TO for any table (rename from ExportStagingToCsv)
- **`GenerateBcpFormatFile()`** — Generalized for dynamic column lists
- **`FormatDuration()`** — HH:MM:SS formatting

After extraction, `into_wz_function.cpp` includes `mssql_utils.hpp` and calls the shared functions.

### Registration

In `wz_extension.cpp`:
```cpp
void RegisterMoveToMssqlFunction(DatabaseInstance &db);
// Called from both wz_duckdb_cpp_init and wz_init
```

### Build System

Add to `CMakeLists.txt`:
```cmake
set(EXTENSION_SOURCES
    src/wz_extension.cpp
    src/into_wz_function.cpp
    src/constraint_checker.cpp
    src/move_to_mssql_function.cpp   # NEW
)
```

## State Machine Phases

Simpler than `into_wz` (no UUIDs, no Vorlauf, no FK validation):

```
DISCOVER_TABLES  →  Process each table:
  CREATE_TABLE   →    DROP + CREATE on MSSQL
  EXPORT_DATA    →    COPY TO TSV
  TRANSFER_DATA  →    BCP (fallback: INSERT)
  CLEANUP        →    Remove temp files
OUTPUT_RESULTS   →  Drain result rows
DONE
```

## What This Function Does NOT Do

- No column alias mapping (transfers columns as-is)
- No UUID generation
- No FK constraint validation
- No Vorlauf/Primanota-specific logic
- No transaction wrapping across tables (each table is independent)

This is a general-purpose bulk transfer tool, not an accounting-specific function.
