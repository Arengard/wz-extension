# WZ DuckDB Extension

A DuckDB extension for importing data into WZ (Wirtschaftszahlen) MSSQL tables with automatic constraint handling, intelligent insert ordering, and meaningful error messages.

## Overview

This extension provides the `into_wz` function that:
- Reads source data from any DuckDB table or query result
- Automatically creates `tblVorlauf` records (derives date range, generates UUID)
- Maps and inserts `tblPrimanota` records with proper column mapping
- Handles foreign key constraints (inserts parent before child)
- Checks for duplicate `guiPrimanotaID` before insert
- Uses transactions for data integrity (all-or-nothing)
- Provides detailed error messages on failure

## Prerequisites

1. **DuckDB** (v1.0+)
2. **MSSQL Extension** - Install from [hugr-lab/mssql-extension](https://github.com/hugr-lab/mssql-extension)
3. **MSSQL Server** with WZ database containing `tblVorlauf` and `tblPrimanota` tables

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
-- 1. Load required extensions
LOAD mssql;
LOAD wz;

-- 2. Create MSSQL connection secret
CREATE SECRET mssql_conn (
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
    secret := 'mssql_conn',
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

## Source Data Format

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

The extension also recognizes these alternative column names:
- `umsatz` → maps to `curEingabeBetrag`
- `umsatz_mit_vorzeichen` → maps to `curBasisBetrag`
- `strbuchtext` → maps to `strBuchText`

### Example Source Data

```sql
CREATE TABLE my_data AS
SELECT * FROM read_json_auto('bookings.json');

-- Or from CSV
CREATE TABLE my_data AS
SELECT * FROM read_csv_auto('bookings.csv');

-- Or manually
CREATE TABLE my_data (
    guiPrimanotaID VARCHAR,
    dtmBelegDatum DATE,
    decKontoNr DECIMAL,
    decGegenkontoNr DECIMAL,
    ysnSoll BOOLEAN,
    curEingabeBetrag DECIMAL,
    strBeleg1 VARCHAR,
    strBuchText VARCHAR
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

### 4. Transaction Handling
```
BEGIN TRANSACTION
  → INSERT tblVorlauf (1 row)
  → INSERT tblPrimanota (N rows in batches of 100)
COMMIT
```

If any insert fails, the entire transaction is rolled back.

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
| decKontoNr | DECIMAL | Source: `decKontoNr` |
| decGegenkontoNr | DECIMAL | Source: `decGegenkontoNr` |
| dtmBelegDatum | DATETIME | Source: `dtmBelegDatum` |
| ysnSoll | BIT | Source: `ysnSoll` |
| curEingabeBetrag | MONEY | Source: `curEingabeBetrag` or `umsatz` |
| curBasisBetrag | MONEY | Source: `curBasisBetrag` or `curEingabeBetrag` |
| strBuchText | VARCHAR | Source: `strBuchText` |
| strBeleg1 | VARCHAR | Source: `strBeleg1` |
| strBeleg2 | VARCHAR | Source: `strBeleg2` |

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
│   │   └── wz_extension.hpp      # Headers and structs
│   ├── wz_extension.cpp          # Extension entry point
│   ├── into_wz_function.cpp      # Main function implementation
│   ├── constraint_checker.cpp    # FK constraint validation
│   ├── vorlauf_builder.cpp       # Vorlauf record builder
│   └── primanota_mapper.cpp      # Primanota data mapper
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
