# WZ DuckDB Extension

A DuckDB extension for importing data into WZ (Wirtschaftszahlen) MSSQL tables with automatic constraint handling, intelligent insert ordering, and meaningful error messages.

## Overview

This extension provides the `into_wz` function that:
- Automatically creates `tblVorlauf` records from your data
- Maps and inserts `tblPrimanota` records
- Handles foreign key constraints (inserts parent before child)
- Validates data before insert to provide clear error messages
- Uses transactions for data integrity

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

-- 3. Prepare your source data (example)
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
    '330# Verbr. Rst. Personalgewinnung 08 2025' AS strBuchText;

-- 4. Import data into WZ
SELECT * FROM into_wz(
    gui_verfahren_id := '6cd5c439-110a-4e65-b7b6-0be000b58588',
    lng_kanzlei_konten_rahmen_id := 56,
    str_angelegt := 'myuser'
);
```

## Function Reference

### `into_wz`

Imports data into WZ tables (tblVorlauf and tblPrimanota).

#### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `gui_verfahren_id` | VARCHAR | Yes | The VerfahrenID (procedure ID) for both tables |
| `lng_kanzlei_konten_rahmen_id` | BIGINT | Yes | The KontenRahmenID (chart of accounts ID) |
| `str_angelegt` | VARCHAR | No | User who created the record (default: 'wz_extension') |
| `database` | VARCHAR | No | Target database name (default: 'finaldatabase') |
| `generate_vorlauf_id` | BOOLEAN | No | Auto-generate guiVorlaufID (default: true) |

#### Return Value

Returns a table with insert status:

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | VARCHAR | Name of the table (tblVorlauf or tblPrimanota) |
| `rows_inserted` | BIGINT | Number of rows inserted |
| `gui_vorlauf_id` | VARCHAR | The generated or used VorlaufID |
| `duration_seconds` | DOUBLE | Time taken for the insert |
| `success` | BOOLEAN | Whether the insert succeeded |
| `error_message` | VARCHAR | Error details if failed |

#### Example Output

```
┌──────────────┬───────────────┬──────────────────────────────────────┬──────────────────┬─────────┬───────────────┐
│ table_name   │ rows_inserted │ gui_vorlauf_id                       │ duration_seconds │ success │ error_message │
├──────────────┼───────────────┼──────────────────────────────────────┼──────────────────┼─────────┼───────────────┤
│ tblVorlauf   │ 1             │ 96719177-432b-5a03-ad2d-3a01cac13a53 │ 0.12             │ true    │               │
│ tblPrimanota │ 150           │ 96719177-432b-5a03-ad2d-3a01cac13a53 │ 1.45             │ true    │               │
└──────────────┴───────────────┴──────────────────────────────────────┴──────────────────┴─────────┴───────────────┘
```

## Source Data Format

The source data should contain Primanota booking records with these columns:

### Required Columns

| Column | Type | Description |
|--------|------|-------------|
| `guiPrimanotaID` | VARCHAR/UUID | Unique ID for each booking |
| `dtmBelegDatum` | DATE | Booking date |
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

### Example JSON Record

```json
{
    "guiPrimanotaID": "ddebd948-4084-5c56-b339-f9b50474b586",
    "dtmBelegDatum": "2025-09-01",
    "decKontoNr": 630014,
    "decGegenkontoNr": 307000,
    "ysnSoll": false,
    "curEingabeBetrag": 854.15,
    "strBeleg1": "0",
    "strBuchText": "330# Verbr. Rst. Personalgewinnung 08 2025"
}
```

## How It Works

### 1. Vorlauf Creation

The extension automatically creates a `tblVorlauf` record by:
- Generating a new UUID for `guiVorlaufID`
- Deriving `dtmVorlaufDatumVon` from MIN(dtmBelegDatum)
- Deriving `dtmVorlaufDatumBis` from MAX(dtmBelegDatum)
- Generating `strBezeichnung` like "Vorlauf 01/2025-04/2025"

### 2. Constraint Validation

Before inserting, the extension:
- Queries `sys.foreign_keys` to discover FK relationships
- Checks for duplicate `guiPrimanotaID` values
- Validates all FK references exist

### 3. Insert Order

The extension respects FK constraints:
1. Insert `tblVorlauf` (parent) first
2. Insert `tblPrimanota` (child) second
3. All within a single transaction

### 4. Error Handling

If a constraint violation is detected:

```
ERROR: Foreign key constraint violation
  Constraint: FK_Primanota_Vorlauf
  Table: tblPrimanota
  Column: guiVorlaufID
  Referenced: tblVorlauf.guiVorlaufID
  Invalid rows: 3 (guiPrimanotaID: abc123, def456, ghi789)
```

## Checking Constraints Manually

You can inspect the FK constraints using the mssql_scan function:

```sql
-- Check tblPrimanota constraints
SELECT * FROM mssql_scan('mssql_conn', $$
    SELECT
        fk.name AS constraint_name,
        pc.name AS column_name
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc
        ON fk.object_id = fkc.constraint_object_id
    JOIN sys.columns pc
        ON pc.object_id = fkc.parent_object_id
       AND pc.column_id = fkc.parent_column_id
    JOIN sys.tables t
        ON t.object_id = fk.parent_object_id
    WHERE t.name = 'tblPrimanota'
$$);

-- Check tblVorlauf constraints
SELECT * FROM mssql_scan('mssql_conn', $$
    SELECT
        fk.name AS constraint_name,
        pc.name AS column_name
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc
        ON fk.object_id = fkc.constraint_object_id
    JOIN sys.columns pc
        ON pc.object_id = fkc.parent_object_id
       AND pc.column_id = fkc.parent_column_id
    JOIN sys.tables t
        ON t.object_id = fk.parent_object_id
    WHERE t.name = 'tblVorlauf'
$$);
```

## Table Schemas

### tblVorlauf

| Column | Type | Description |
|--------|------|-------------|
| guiVorlaufID | UNIQUEIDENTIFIER | Primary key |
| guiVerfahrenID | UNIQUEIDENTIFIER | Procedure reference |
| lngKanzleiKontenRahmenID | INT | Chart of accounts |
| lngStatus | INT | Status (1 = active) |
| dtmVorlaufDatumVon | DATETIME | Period start date |
| dtmVorlaufDatumBis | DATETIME | Period end date |
| strBezeichnung | VARCHAR | Description |
| strAngelegt | VARCHAR | Created by |
| dtmAngelegt | DATETIME | Created at |

### tblPrimanota

| Column | Type | Description |
|--------|------|-------------|
| guiPrimanotaID | UNIQUEIDENTIFIER | Primary key |
| guiVorlaufID | UNIQUEIDENTIFIER | FK to tblVorlauf |
| guiVerfahrenID | UNIQUEIDENTIFIER | Procedure reference |
| decKontoNr | DECIMAL | Account number |
| decGegenkontoNr | DECIMAL | Counter account |
| dtmBelegDatum | DATETIME | Booking date |
| ysnSoll | BIT | Debit flag |
| curEingabeBetrag | MONEY | Amount |
| strBuchText | VARCHAR | Booking text |

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
