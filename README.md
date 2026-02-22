# WZ DuckDB Extension

A DuckDB extension for importing data into WZ (Wirtschaftszahlen) MSSQL tables with automatic constraint handling, intelligent insert ordering, and meaningful error messages.

## Overview

This extension provides three table functions:

### `into_wz` — WZ Accounting Import
The `into_wz` function:
- Reads source data from any DuckDB table or query result
- Automatically creates `tblVorlauf` records (derives date range, generates UUID)
- Maps and inserts `tblPrimanota` records with proper column mapping
- Handles foreign key constraints (inserts parent before child)
- Checks for duplicate `guiPrimanotaID` before insert
- Uses transactions for data integrity (all-or-nothing)
- Provides detailed error messages on failure

### `move_to_mssql` — Bulk Table Transfer
The `move_to_mssql` function:
- Transfers DuckDB tables to MSSQL Server in bulk
- Supports transferring all tables, specific tables, or all-except-excluded tables
- **Auto-creates the target MSSQL database** if it doesn't exist (via `sqlcmd`)
- **Auto-creates target schemas** in MSSQL as needed
- **Multi-schema support**: `duckdb_schema='all'` discovers and transfers tables from all DuckDB schemas
- **Auto-attach source**: `source` parameter accepts a `.db` file path to auto-attach as source
- DROPs and recreates target tables with auto-mapped column types
- Uses BCP bulk copy for maximum speed, with batched INSERT VALUES as fallback
- Reports per-table results (rows transferred, method used, duration, errors)
- Continues on per-table errors and reports failures at the end

### `stps_drop_all` — Clean DuckDB State
The `stps_drop_all` function:
- Drops all views, tables, and user-created schemas in DuckDB
- Detaches all non-default attached databases
- Takes no parameters — a "nuke everything" reset function
- Returns a result table showing each dropped/detached object and its status

## Prerequisites

1. **DuckDB** (v1.0+)
2. **MSSQL Extension** - Installed from [hugr-lab/mssql-extension](https://github.com/hugr-lab/mssql-extension) (auto-loaded by this extension)
3. **MSSQL Server** with WZ database containing `tblVorlauf` and `tblPrimanota` tables
4. **bcp.exe** (optional, recommended) - SQL Server BCP utility for fast bulk transfer. Falls back to batched INSERT VALUES if unavailable.
5. **sqlcmd** (optional) - SQL Server command-line tool for auto-creating databases. Part of the same [SQL Server tools](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility) package as `bcp`.

## Installation

### Option 1: Download Pre-built Extension

Download the latest release for your platform from the [Releases](https://github.com/Arengard/wz-extension/releases) page.

```sql
INSTALL './wz.duckdb_extension';
LOAD wz;
```

### Option 2: Build from Source

```bash
# Clone the repository with submodules
git clone --recursive https://github.com/Arengard/wz-extension.git
cd wz-extension

# Build (Linux/macOS)
make release

# Build (Windows)
build-windows.bat
```

The extension will be in `build/release/extension/wz/wz.duckdb_extension`.

## Quick Start

```sql
-- 1. Load the extension (automatically loads MSSQL extension)
LOAD wz;

-- 2. Create MSSQL connection secret
CREATE SECRET mssql_secret (
    TYPE mssql,
    host 'your-server.database.windows.net',
    port 1433,
    database 'YourDatabase',
    user 'your_user',
    password 'your_password'
);

-- 3. Create your source data table
CREATE TABLE my_bookings AS
SELECT
    'ddebd948-4084-5c56-b339-f9b50474b586' AS guiPrimanotaID,
    '2025-09-01' AS dtmBelegDatum,
    630014.0 AS decKontoNr,
    307000.0 AS decGegenkontoNr,
    false AS ysnSoll,
    854.15 AS curEingabeBetrag,
    854.15 AS curBasisBetrag,
    '0' AS strBeleg1,
    '330# Personalgewinnung 08 2025' AS strBuchText
UNION ALL
SELECT
    '5e9ef551-4e05-5bca-a699-051e680451fa',
    '2025-09-15',
    1800.0,
    1590.0,
    false,
    8851.87,
    8851.87,
    '0',
    'Sauna Poo org Gegenkonto:137005';

-- 4. Import data into WZ
SELECT * FROM into_wz(
    secret := 'mssql_secret',
    source_table := 'my_bookings',
    gui_verfahren_id := '6cd5c439-110a-4e65-b7b6-0be000b58588',
    lng_kanzlei_konten_rahmen_id := 56,
    str_angelegt := 'myuser'
);
```

## Function Reference

### `into_wz`

Imports data from a DuckDB table into WZ MSSQL tables (tblVorlauf and tblPrimanota).

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `secret` | VARCHAR | No | `mssql_conn` | Name of the MSSQL connection secret |
| `source_table` | VARCHAR | **Yes** | - | Name of the DuckDB table containing source data |
| `gui_verfahren_id` | VARCHAR | **Yes** | - | The VerfahrenID (procedure ID) for both tables |
| `lng_kanzlei_konten_rahmen_id` | BIGINT | **Yes** | - | The KontenRahmenID (chart of accounts ID) |
| `str_angelegt` | VARCHAR | No | `wz_extension` | User who created the record |
| `generate_vorlauf_id` | BOOLEAN | No | `true` | Auto-generate guiVorlaufID |
| `monatsvorlauf` | BOOLEAN | No | `false` | Split inserts by month — creates a separate tblVorlauf + tblPrimanota batch per calendar month |
| `skip_duplicate_check` | BOOLEAN | No | `false` | Skip duplicate guiPrimanotaID check before insert |
| `skip_fk_check` | BOOLEAN | No | `false` | Skip foreign key constraint validation before insert |

#### Return Value

Returns a table with insert status for each table:

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | VARCHAR | Name of the table (tblVorlauf, tblPrimanota, or ERROR) |
| `rows_inserted` | BIGINT | Number of rows inserted |
| `gui_vorlauf_id` | VARCHAR | The generated VorlaufID |
| `duration_seconds` | DOUBLE | Time taken for the insert |
| `success` | BOOLEAN | Whether the insert succeeded |
| `error_message` | VARCHAR | Error details if failed |

#### Example Output (Success)

```
┌──────────────┬───────────────┬──────────────────────────────────────┬──────────────────┬─────────┬───────────────┐
│ table_name   │ rows_inserted │ gui_vorlauf_id                       │ duration_seconds │ success │ error_message │
├──────────────┼───────────────┼──────────────────────────────────────┼──────────────────┼─────────┼───────────────┤
│ tblVorlauf   │ 1             │ 96719177-432b-5a03-ad2d-3a01cac13a53 │ 0.12             │ true    │               │
│ tblPrimanota │ 150           │ 96719177-432b-5a03-ad2d-3a01cac13a53 │ 1.45             │ true    │               │
└──────────────┴───────────────┴──────────────────────────────────────┴──────────────────┴─────────┴───────────────┘
```

#### Example Output (Duplicate Error)

```
┌────────────┬───────────────┬────────────────┬──────────────────┬─────────┬─────────────────────────────────────────────────────────┐
│ table_name │ rows_inserted │ gui_vorlauf_id │ duration_seconds │ success │ error_message                                           │
├────────────┼───────────────┼────────────────┼──────────────────┼─────────┼─────────────────────────────────────────────────────────┤
│ ERROR      │ 0             │                │ 0.0              │ false   │ Duplicate guiPrimanotaID found: abc-123, def-456, ...   │
└────────────┴───────────────┴────────────────┴──────────────────┴─────────┴─────────────────────────────────────────────────────────┘
```

#### Example: Monthly Split (Monatsvorlauf)

```sql
-- Source data spans multiple months
SELECT * FROM into_wz(
    source_table := 'my_bookings',
    gui_verfahren_id := '6cd5c439-110a-4e65-b7b6-0be000b58588',
    lng_kanzlei_konten_rahmen_id := 56,
    monatsvorlauf := true
);
```

Example output with `monatsvorlauf := true` (one Vorlauf per month):
```
┌──────────────┬───────────────┬──────────────────────────────────────┬──────────────────┬─────────┬───────────────┐
│ table_name   │ rows_inserted │ gui_vorlauf_id                       │ duration_seconds │ success │ error_message │
├──────────────┼───────────────┼──────────────────────────────────────┼──────────────────┼─────────┼───────────────┤
│ tblVorlauf   │ 1             │ a1b2c3d4-e5f6-5a7b-8c9d-0e1f2a3b4c5d │ 0.08             │ true    │               │
│ tblPrimanota │ 45            │ a1b2c3d4-e5f6-5a7b-8c9d-0e1f2a3b4c5d │ 0.52             │ true    │               │
│ tblVorlauf   │ 1             │ f6a7b8c9-d0e1-5f2a-3b4c-5d6e7f8a9b0c │ 0.07             │ true    │               │
│ tblPrimanota │ 105           │ f6a7b8c9-d0e1-5f2a-3b4c-5d6e7f8a9b0c │ 1.12             │ true    │               │
└──────────────┴───────────────┴──────────────────────────────────────┴──────────────────┴─────────┴───────────────┘
```

### `move_to_mssql`

Bulk-transfers DuckDB tables to MSSQL Server. DROPs and recreates each target table, then loads data via BCP (with INSERT VALUES fallback).

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `secret` | VARCHAR | **Yes** | - | Name of the MSSQL connection secret |
| `source` | VARCHAR | No | - | File path to a DuckDB database file to auto-attach as source (overrides `duckdb_catalog`) |
| `all_tables` | BOOLEAN | No | `true` | Transfer all tables in DuckDB |
| `tables` | LIST(VARCHAR) | No | `[]` | Explicit list of table names to transfer (sets `all_tables` to false) |
| `exclude` | LIST(VARCHAR) | No | `[]` | Tables to exclude when `all_tables` is true |
| `schema` | VARCHAR | No | `dbo` | Target schema on MSSQL Server (alias: `mssql_schema`) |
| `mssql_schema` | VARCHAR | No | `dbo` | Alias for `schema` — target schema on MSSQL Server. When not set with `duckdb_schema='all'`, mirrors each DuckDB schema name. |
| `duckdb_schema` | VARCHAR | No | `main` | Source schema in DuckDB. Use `'all'` to discover and transfer tables from all schemas. |
| `duckdb_catalog` | VARCHAR | No | `memory` | Source catalog in DuckDB |

#### Return Value

Returns a table with transfer status for each table plus a summary row:

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | VARCHAR | Name of the table (or `SUMMARY`) |
| `rows_transferred` | BIGINT | Number of rows transferred |
| `method` | VARCHAR | Transfer method used (`BCP` or `INSERT`) |
| `duration` | VARCHAR | Time taken (hh:mm:ss) |
| `success` | BOOLEAN | Whether the transfer succeeded |
| `error_message` | VARCHAR | Error details if failed |

#### Examples

```sql
-- Transfer all DuckDB tables to MSSQL
SELECT * FROM move_to_mssql(secret='my_mssql_secret');

-- Transfer specific tables only
SELECT * FROM move_to_mssql(
    secret='my_mssql_secret',
    all_tables=false,
    tables=['orders', 'customers', 'products']
);

-- Transfer all tables except some
SELECT * FROM move_to_mssql(
    secret='my_mssql_secret',
    exclude=['temp_staging', 'debug_log']
);

-- Transfer to a custom MSSQL schema
SELECT * FROM move_to_mssql(
    secret='my_mssql_secret',
    mssql_schema='staging'
);

-- Transfer from a specific DuckDB schema/catalog
SELECT * FROM move_to_mssql(
    secret='my_mssql_secret',
    duckdb_catalog='my_db',
    duckdb_schema='analytics',
    mssql_schema='dbo',
    tables=['orders', 'customers']
);

-- Transfer from all DuckDB schemas (mirrors schema names in MSSQL)
SELECT * FROM move_to_mssql(
    secret='my_mssql_secret',
    duckdb_schema='all'
);

-- Transfer from all schemas into a single MSSQL schema
SELECT * FROM move_to_mssql(
    secret='my_mssql_secret',
    duckdb_schema='all',
    mssql_schema='staging'
);

-- Auto-attach a DuckDB database file and transfer its tables
SELECT * FROM move_to_mssql(
    secret='my_mssql_secret',
    source='C:\Users\Ramon\Downloads\my_database.db',
    duckdb_schema='main',
    mssql_schema='imported'
);
```

#### Example Output

```
┌──────────────┬──────────────────┬────────┬──────────┬─────────┬───────────────┐
│ table_name   │ rows_transferred │ method │ duration │ success │ error_message │
├──────────────┼──────────────────┼────────┼──────────┼─────────┼───────────────┤
│ orders       │ 15000            │ BCP    │ 00:00:03 │ true    │               │
│ customers    │ 500              │ BCP    │ 00:00:01 │ true    │               │
│ products     │ 200              │ INSERT │ 00:00:01 │ true    │               │
│ SUMMARY      │ 15700            │        │ 00:00:05 │ true    │               │
└──────────────┴──────────────────┴────────┴──────────┴─────────┴───────────────┘
```

### `stps_drop_all`

Drops all tables, views, and user-created schemas in DuckDB and detaches all non-default databases. Takes no parameters.

#### Return Value

| Column | Type | Description |
|--------|------|-------------|
| `object_type` | VARCHAR | Type of object (`VIEW`, `TABLE`, `SCHEMA`, `DATABASE`, or `INFO`) |
| `object_name` | VARCHAR | Qualified name of the dropped/detached object |
| `status` | VARCHAR | Result (`DROPPED`, `DETACHED`, or error message) |

#### Example

```sql
-- Reset DuckDB to a clean state
SELECT * FROM stps_drop_all();
```

```
┌─────────────┬────────────────┬─────────┐
│ object_type │  object_name   │ status  │
├─────────────┼────────────────┼─────────┤
│ VIEW        │ "main"."v1"    │ DROPPED │
│ TABLE       │ "main"."test1" │ DROPPED │
│ TABLE       │ "main"."test2" │ DROPPED │
│ SCHEMA      │ myschema       │ DROPPED │
└─────────────┴────────────────┴─────────┘
```

---

## Source Data Format (into_wz)

The source table should contain Primanota booking records. Column names are matched case-insensitively.

### Required Columns

| Column | Type | Description |
|--------|------|-------------|
| `guiPrimanotaID` | VARCHAR/UUID | Unique ID for each booking (auto-generated if missing) |
| `dtmBelegDatum` | DATE/VARCHAR | Booking date (YYYY-MM-DD format) |
| `decKontoNr` | DECIMAL | Account number |
| `decGegenkontoNr` | DECIMAL | Counter account number |
| `ysnSoll` | BOOLEAN | Debit (true) or Credit (false) |
| `curEingabeBetrag` | DECIMAL | Amount |

### Optional Columns

| Column | Type | Description |
|--------|------|-------------|
| `curBasisBetrag` | DECIMAL | Base amount (defaults to curEingabeBetrag) |
| `strBeleg1` | VARCHAR | Document number 1 |
| `strBeleg2` | VARCHAR | Document number 2 |
| `strBuchText` | VARCHAR | Booking text |

### Alternative Column Names

The extension recognizes many alternative column names (case-insensitive):

| Alternative Names | Maps To |
|------------------|---------|
| `konto`, `kontonr`, `konto_nr`, `account` | `decKontoNr` |
| `gegenkonto`, `gegenkontonr`, `gegenkonto_nr` | `decGegenkontoNr` |
| `eakonto`, `ea_konto`, `eakontonr` | `decEaKontoNr` |
| `umsatz`, `betrag`, `amount` | `curEingabeBetrag` |
| `umsatz_mit_vorzeichen`, `basisbetrag` | `curBasisBetrag` |
| `sh`, `soll_haben`, `sollhaben` | `ysnSoll` |
| `buchungstext`, `buchtext`, `text` | `strBuchText` |
| `belegdatum`, `datum`, `date` | `dtmBelegDatum` |
| `beleg1`, `belegfeld1`, `belegnr`, `belegnummer` | `strBeleg1` |
| `beleg2`, `belegfeld2`, `trans_nr`, `transnr` | `strBeleg2` |

### Soll/Haben (sh) Column Parsing

The `sh` column (or `ysnSoll`) accepts multiple formats:

| Input Value | Interpreted As |
|-------------|----------------|
| `S`, `Soll`, `1`, `true`, `D`, `Debit` | Soll (Debit) = **true** |
| `H`, `Haben`, `0`, `false`, `C`, `Credit` | Haben (Credit) = **false** |

Example:
```sql
-- All of these are equivalent for ysnSoll
SELECT 'S' AS sh;      -- Soll
SELECT 'Soll' AS sh;   -- Soll
SELECT 1 AS ysnSoll;   -- Soll
SELECT 'H' AS sh;      -- Haben
SELECT 'Haben' AS sh;  -- Haben
SELECT 0 AS ysnSoll;   -- Haben
```

### Example Source Data

```sql
-- Load from JSON file
CREATE TABLE my_data AS
SELECT * FROM read_json_auto('bookings.json');

-- Or from CSV
CREATE TABLE my_data AS
SELECT * FROM read_csv_auto('bookings.csv');
```

### Example with Alternative Column Names

Your source data can use common German accounting column names:

```sql
CREATE TABLE buchungen AS
SELECT
    'abc-123-def' AS guiPrimanotaID,
    '2025-09-01' AS datum,           -- maps to dtmBelegDatum
    630014 AS konto,                 -- maps to decKontoNr
    307000 AS gegenkonto,            -- maps to decGegenkontoNr
    1234 AS eakonto,                 -- maps to decEaKontoNr
    'H' AS sh,                       -- maps to ysnSoll (Haben = false)
    854.15 AS umsatz,                -- maps to curEingabeBetrag
    '12345' AS belegnummer,          -- maps to strBeleg1
    'TRX-001' AS transnr,            -- maps to strBeleg2
    'Buchungstext hier' AS buchungstext;  -- maps to strBuchText

-- Import using the extension
SELECT * FROM into_wz(
    source_table := 'buchungen',
    gui_verfahren_id := '6cd5c439-110a-4e65-b7b6-0be000b58588',
    lng_kanzlei_konten_rahmen_id := 56
);
```

## How It Works

### 1. Data Loading
- Reads all rows from the specified `source_table`
- Extracts column names and types for mapping

### 2. Duplicate Detection
- Extracts all `guiPrimanotaID` values from source data
- Queries MSSQL `tblPrimanota` to check for existing IDs
- Fails with detailed error listing duplicates if any found

### 3. Vorlauf Creation
- Generates a new UUID for `guiVorlaufID`
- Scans all dates to find MIN/MAX `dtmBelegDatum`
- Derives `strBezeichnung` like "Vorlauf 09/2025-09/2025"
- Sets `lngStatus = 1` (active)
- If the Vorlauf already exists (same UUID), it is **updated** (`strBezeichnung`, `dtmVorlaufDatumBis`, `strGeaendert`, `dtmGeaendert`) instead of inserted

### 4. Transaction Handling
```
BEGIN TRANSACTION
  → INSERT or UPDATE tblVorlauf (1 row)
  → INSERT tblPrimanota (via BCP bulk copy, or batched INSERT VALUES as fallback)
COMMIT
```

If any insert fails, the entire transaction is rolled back.

### 4a. Monthly Split (`monatsvorlauf`)

When `monatsvorlauf := true`, the function groups source rows by `dtmBelegDatum` month and creates a **separate tblVorlauf + tblPrimanota batch per calendar month**:

```
BEGIN TRANSACTION
  → For each YYYY-MM in source data (chronological order):
      → INSERT or UPDATE tblVorlauf (date range = first to last day of that month)
      → INSERT tblPrimanota (only rows belonging to that month, in batches of 1,000)
COMMIT
```

- Each monthly Vorlauf receives a deterministic UUID v5 derived from the verfahren ID, year-month, and row keys
- The `strBezeichnung` is derived per month (e.g. "Vorlauf 09/2025")
- All months are wrapped in a **single transaction** — either all succeed or all are rolled back

### 5. Column Mapping
Source columns are mapped to tblPrimanota columns case-insensitively:

```
Source Data              →  tblPrimanota
─────────────────────────────────────────
guiPrimanotaID           →  guiPrimanotaID
dtmBelegDatum            →  dtmBelegDatum
decKontoNr               →  decKontoNr
decGegenkontoNr          →  decGegenkontoNr
ysnSoll                  →  ysnSoll
curEingabeBetrag/umsatz  →  curEingabeBetrag
curBasisBetrag           →  curBasisBetrag
strBeleg1                →  strBeleg1
strBeleg2                →  strBeleg2
strBuchText              →  strBuchText

(Generated by extension)
─────────────────────────────────────────
guiVorlaufID             →  guiVorlaufID (from Vorlauf)
guiVerfahrenID           →  guiVerfahrenID (from parameter)
lngStatus                →  1 (active)
lngEingabeWaehrungID     →  1 (EUR)
dtmVorlaufDatumBis       →  (from Vorlauf)
strAngelegt              →  (from parameter)
dtmAngelegt              →  (current timestamp)
```

## Performance

The extension is optimized for large datasets (100k+ rows):

- **BCP bulk transfer** (primary): Uses `bcp.exe` for native bulk copy into SQL Server — fastest possible path with a generated format file for correct type handling
- **Batched INSERT VALUES** (fallback): 1,000 rows per INSERT statement, 10 statements per network round-trip — used automatically if BCP is unavailable
- **Pre-computed UUIDs**: All primanota IDs are generated before the insert loop
- **Minimal overhead**: Column indices cached, constants pre-escaped, zero-copy row access

### Maximum Speed for Trusted Data

When your source data is pre-validated (correct UUIDs, valid FK references), skip the validation steps:

```sql
SELECT * FROM into_wz(
    source_table := 'my_large_table',
    gui_verfahren_id := '6cd5c439-110a-4e65-b7b6-0be000b58588',
    lng_kanzlei_konten_rahmen_id := 56,
    skip_duplicate_check := true,
    skip_fk_check := true
);
```

| Validation | Default | With skip flags |
|------------|---------|-----------------|
| Duplicate ID check | 500 queries (500k rows) | Skipped |
| FK constraint check | ~5-10 queries | Skipped |
| INSERT round-trips | ~50 | ~50 |

## Error Handling

### Duplicate IDs
```
ERROR: Duplicate guiPrimanotaID found: abc-123, def-456, ghi-789.
       3 records already exist in tblPrimanota.
```

### Missing Required Parameter
```
ERROR: gui_verfahren_id is required
```

### Insert Failure
```
tblPrimanota: Insert failed at row 50: [SQL Server error message]
```

When an error occurs during insert:
1. Transaction is automatically rolled back
2. No partial data is committed
3. Error message indicates which table and row failed

## Checking Constraints Manually

You can inspect the FK constraints using the mssql_scan function:

```sql
-- Check tblPrimanota foreign keys
SELECT * FROM mssql_scan('mssql_conn', $$
    SELECT
        fk.name AS constraint_name,
        pc.name AS column_name,
        rt.name AS referenced_table,
        rc.name AS referenced_column
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
    JOIN sys.tables t ON t.object_id = fk.parent_object_id
    JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
    JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
    WHERE t.name = 'tblPrimanota'
$$);
```

## Table Schemas

### tblVorlauf (Created by Extension)

| Column | Type | Source |
|--------|------|--------|
| guiVorlaufID | UNIQUEIDENTIFIER | Auto-generated UUID |
| guiVerfahrenID | UNIQUEIDENTIFIER | Parameter: `gui_verfahren_id` |
| lngKanzleiKontenRahmenID | INT | Parameter: `lng_kanzlei_konten_rahmen_id` |
| lngStatus | INT | Always 1 (active) |
| dtmVorlaufDatumVon | DATETIME | MIN(dtmBelegDatum) from data |
| dtmVorlaufDatumBis | DATETIME | MAX(dtmBelegDatum) from data |
| strBezeichnung | VARCHAR | "Vorlauf MM/YYYY-MM/YYYY" |
| strAngelegt | VARCHAR | Parameter: `str_angelegt` |
| dtmAngelegt | DATETIME | Current timestamp |

### tblPrimanota (Mapped from Source)

| Column | Type | Source |
|--------|------|--------|
| guiPrimanotaID | UNIQUEIDENTIFIER | Source column or auto-generated |
| guiVorlaufID | UNIQUEIDENTIFIER | From created tblVorlauf |
| guiVerfahrenID | UNIQUEIDENTIFIER | Parameter: `gui_verfahren_id` |
| decKontoNr | DECIMAL | Source: `konto`, `decKontoNr` |
| decGegenkontoNr | DECIMAL | Source: `gegenkonto`, `decGegenkontoNr` |
| decEaKontoNr | DECIMAL | Source: `eakonto`, `decEaKontoNr` |
| dtmBelegDatum | DATETIME | Source: `datum`, `dtmBelegDatum` |
| ysnSoll | BIT | Source: `sh`, `ysnSoll` (parsed from S/H/Soll/Haben) |
| curEingabeBetrag | MONEY | Source: `umsatz`, `betrag`, `curEingabeBetrag` |
| curBasisBetrag | MONEY | Source: `curBasisBetrag` (defaults to curEingabeBetrag) |
| strBuchText | VARCHAR | Source: `buchungstext`, `strBuchText` |
| strBeleg1 | VARCHAR | Source: `belegnummer`, `belegfeld1`, `strBeleg1` |
| strBeleg2 | VARCHAR | Source: `transnr`, `belegfeld2`, `strBeleg2` |

## Development

### Building

```bash
# Debug build
make debug

# Release build
make release

# Clean build artifacts
make clean
```

### Project Structure

```
wz-extension/
├── src/
│   ├── include/
│   │   ├── wz_extension.hpp      # Headers and structs
│   │   ├── wz_utils.hpp          # Shared utilities (column aliases, SQL helpers)
│   │   └── mssql_utils.hpp       # Shared MSSQL/BCP utilities (connection, bulk transfer)
│   ├── wz_extension.cpp          # Extension entry point
│   ├── into_wz_function.cpp      # into_wz function implementation
│   ├── move_to_mssql_function.cpp  # move_to_mssql function implementation
│   ├── stps_drop_all_function.cpp # stps_drop_all function implementation
│   └── constraint_checker.cpp     # FK constraint validation
├── duckdb/                       # DuckDB submodule
├── extension-ci-tools/           # Build infrastructure
├── CMakeLists.txt
├── Makefile
└── README.md
```

## License

MIT

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Support

For issues and feature requests, please use the [GitHub Issues](https://github.com/Arengard/wz-extension/issues) page.
