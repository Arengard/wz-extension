# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build (all platforms, requires git submodules initialized)
make release

# Debug build
make debug

# Clean
make clean

# Windows-specific (initializes submodules, builds, copies to dist/)
build-windows.bat

# Initialize submodules (required before first build)
git submodule update --init --recursive
```

The built extension lands at `build/release/extension/wz/wz.duckdb_extension`.

## Testing

No automated test suite. Manual testing via DuckDB CLI:

```bash
duckdb -unsigned -c "LOAD 'build/release/extension/wz/wz.duckdb_extension'; SELECT 'OK';"
```

Full integration testing requires a live MSSQL connection with the WZ schema (tblVorlauf, tblPrimanota).

## Architecture

This is a DuckDB C++ extension that provides the `into_wz()` table function. It reads data from a DuckDB table and inserts it into MSSQL WZ accounting tables (tblVorlauf + tblPrimanota) via DuckDB's attached MSSQL database.

### Source Files

- **`src/wz_extension.cpp`** — Extension entry point. Registers the table function. Exports `wz_duckdb_cpp_init(ExtensionLoader&)` (new ABI) and `wz_init(DatabaseInstance&)` (legacy).
- **`src/into_wz_function.cpp`** — Core logic (~1160 lines). Contains the `into_wz` table function: parameter binding, source data loading, column mapping with alias fallbacks, Vorlauf record creation, Primanota batch inserts (100 rows/batch), and result reporting.
- **`src/constraint_checker.cpp`** — FK constraint validation. Queries MSSQL `sys.foreign_keys` metadata, maps FK columns to source columns using the same alias logic, and batch-validates values exist in referenced tables.
- **`src/include/wz_extension.hpp`** — Shared types: `ForeignKeyConstraint`, `ConstraintViolation`, `InsertResult`, and forward declarations.
- **`src/include/wz_utils.hpp`** — Header-only utilities: `FindColumnIndex()` (case-insensitive), `GetCurrentTimestamp()`, `DeriveVorlaufBezeichnung()`.

### Data Flow

1. **Bind** — Parse named parameters (secret, source_table, gui_verfahren_id, etc.)
2. **Load** — Materialize all source rows into memory
3. **Validate** — Check duplicate guiPrimanotaID, validate FK constraints against MSSQL
4. **Transform** — Extract date range, generate UUID, derive Vorlauf description
5. **Insert** — BEGIN TRANSACTION → insert/update tblVorlauf → batch insert tblPrimanota → COMMIT (or ROLLBACK on error)
6. **Report** — Return InsertResult rows as a table

### Critical Patterns

**Connection handling in table function callbacks:** Always create a separate `Connection conn(db)` and use `conn.Query()`. Never use `context.Query()` (deadlocks on context lock) or C++ transaction API like `conn.BeginTransaction()`/`Commit()`/`Rollback()` (deadlocks on database-level write locks). Use SQL strings `"BEGIN TRANSACTION"`, `"COMMIT"`, `"ROLLBACK"` via `conn.Query()`.

**DuckDB API specifics:**
- Use `result->types[i]` not `result->GetTypes()[i]`
- Dereference shared_ptr context: `*con.context` for Catalog functions
- `ColumnDataCollection` iteration: use `.Chunks()` then `chunk.data[col].GetValue(row_idx)`

**Column mapping:** Source columns are matched case-insensitively with multiple German/English aliases (e.g., `konto`, `kontonr`, `account` all map to `decKontoNr`). The same alias logic is shared between `into_wz_function.cpp` and `constraint_checker.cpp`.

## Build System

CMake 3.15-3.28, C++17. Uses DuckDB's extension build infrastructure via the `extension-ci-tools` submodule. Links against `duckdb_yyjson` for JSON parsing. The `Makefile` is a thin wrapper around `extension-ci-tools/makefiles/duckdb_extension.Makefile`.

## External Dependencies

- **DuckDB** — Git submodule at `duckdb/`
- **extension-ci-tools** — Git submodule at `extension-ci-tools/`
- **MSSQL extension** — Runtime dependency, loaded separately (`LOAD mssql`)
- **MSSQL Server** — Target database with WZ schema tables
